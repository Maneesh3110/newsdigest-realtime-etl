import requests
import json
import os
import time
import threading
import re
import html
from datetime import datetime, timedelta
from flask import Flask, render_template, request, jsonify, redirect, url_for
from flask_bootstrap import Bootstrap
import mysql.connector
from mysql.connector import Error
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import sent_tokenize, word_tokenize
from nltk.cluster.util import cosine_distance
import numpy as np
import networkx as nx
import logging
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables from .env if present
load_dotenv()


def _get_int_env(var_name, default_value):
    value = os.getenv(var_name)
    if value is None or value == '':
        return int(default_value)
    try:
        return int(value)
    except ValueError:
        logger.warning(f"Invalid integer for {var_name}; using default {default_value}.")
        return int(default_value)

# Optional imports - handle gracefully if not available
try:
    from confluent_kafka import Producer as KafkaProducer, Consumer as KafkaConsumer
    KAFKA_AVAILABLE = True
    logger.info("Kafka (confluent-kafka) loaded successfully")
except ImportError:
    logger.warning("Kafka not available. Kafka features will be disabled.")
    KafkaProducer = None
    KafkaConsumer = None
    KAFKA_AVAILABLE = False

try:
    from pyspark.sql import SparkSession
    from pyspark.ml.feature import Tokenizer, StopWordsRemover, CountVectorizer, IDF
    from pyspark.ml.classification import LogisticRegression
    from pyspark.ml import Pipeline
    SPARK_AVAILABLE = True
except ImportError:
    logger.warning("PySpark not available. PySpark features will be disabled.")
    SPARK_AVAILABLE = False

# Initialize Flask application
app = Flask(__name__)
Bootstrap(app)

# ---- Configuration ----
NEWS_API_KEY = os.getenv('NEWS_API_KEY', '')
NEWS_LOOKBACK_DAYS = _get_int_env('NEWS_LOOKBACK_DAYS', 7)
NEWS_LANGUAGE = os.getenv('NEWS_LANGUAGE', 'en')
NEWS_FALLBACK_COUNTRY = os.getenv('NEWS_FALLBACK_COUNTRY', 'us')
SORT_BY = os.getenv('NEWS_SORT_BY', 'publishedAt')
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'news_articles')
ENABLE_KAFKA = os.getenv('ENABLE_KAFKA', 'false').lower() == 'true'
ENABLE_SPARK = os.getenv('ENABLE_SPARK', 'false').lower() == 'true'
MYSQL_HOST = os.getenv('MYSQL_HOST', 'localhost')
MYSQL_PORT = _get_int_env('MYSQL_PORT', 3306)
MYSQL_USER = os.getenv('MYSQL_USER', 'root')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD', '')
MYSQL_DB_NAME = (os.getenv('MYSQL_DB_NAME', 'news_etl') or 'news_etl').strip()
SAFE_DB_NAME = MYSQL_DB_NAME.replace('`', '') or 'news_etl'
CATEGORIES = ['business', 'technology', 'sports', 'politics', 'health', 'entertainment']

if ENABLE_KAFKA and not KAFKA_AVAILABLE:
    logger.warning("ENABLE_KAFKA is true but confluent-kafka is not installed.")
KAFKA_ENABLED = KAFKA_AVAILABLE and ENABLE_KAFKA

if ENABLE_SPARK and not SPARK_AVAILABLE:
    logger.warning("ENABLE_SPARK is true but PySpark is not installed.")
SPARK_ENABLED = SPARK_AVAILABLE and ENABLE_SPARK

# Download NLTK resources
try:
    nltk.download('punkt', quiet=True)
    nltk.download('stopwords', quiet=True)
except:
    logger.warning("NLTK resources could not be downloaded. Summarization may not work properly.")

# Initialize database connection
def init_db():
    try:
        conn = mysql.connector.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            port=MYSQL_PORT
        )
        cursor = conn.cursor()
        
        # Create database if it doesn't exist
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{SAFE_DB_NAME}`")
        cursor.execute(f"USE `{SAFE_DB_NAME}`")
        conn.database = SAFE_DB_NAME
        
        # Create tables
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS articles (
                id INT AUTO_INCREMENT PRIMARY KEY,
                title VARCHAR(255) NOT NULL,
                source VARCHAR(100),
                author VARCHAR(100),
                published_at DATETIME,
                category VARCHAR(50),
                url VARCHAR(255),
                content TEXT,
                summary TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        conn.commit()
        logger.info("Database initialized successfully")
        return conn
    except Error as e:
        logger.error(f"Database initialization error: {e}")
        return None


def get_db_connection():
    try:
        conn = mysql.connector.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=SAFE_DB_NAME,
            port=MYSQL_PORT
        )
        return conn
    except Error as e:
        logger.error(f"Database connection error: {e}")
        return None

# Strip HTML tags and decode HTML entities from text
def strip_html_tags(text):
    if not text:
        return ""
    # Remove HTML tags
    clean = re.sub(r'<[^>]+>', '', text)
    # Decode HTML entities like &amp; -> &
    clean = html.unescape(clean)
    # Remove the truncation marker like [+1234 chars]
    clean = re.sub(r'\[\+\d+ chars\]', '', clean)
    # Remove extra whitespace
    clean = re.sub(r'\s+', ' ', clean).strip()
    return clean

# Initialize Kafka producer
def init_kafka_producer():
    if not KAFKA_ENABLED:
        logger.info("Kafka is disabled or unavailable, skipping producer initialization")
        return None
    try:
        producer = KafkaProducer({
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS
        })
        logger.info("Kafka producer initialized successfully")
        return producer
    except Exception as e:
        logger.error(f"Kafka producer initialization error: {e}")
        return None

# Initialize PySpark session
def init_spark():
    if not SPARK_ENABLED:
        logger.warning("PySpark is disabled or unavailable, skipping Spark initialization")
        return None
    try:
        spark = SparkSession.builder \
            .appName("NewsETL") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0") \
            .getOrCreate()
        logger.info("Spark session initialized successfully")
        return spark
    except Exception as e:
        logger.error(f"Spark initialization error: {e}")
        return None

# Text summarization using TextRank algorithm
def text_rank_summarize(text, num_sentences=3):
    if not text:
        return ""
    
    # Clean the text first
    text = strip_html_tags(text)
    
    if len(text) < 50:
        return text
        
    try:
        sentences = sent_tokenize(text)
        if len(sentences) <= num_sentences:
            return text
        
        # Need at least 2 sentences for comparison
        if len(sentences) < 2:
            return text
            
        # Create similarity matrix
        similarity_matrix = np.zeros((len(sentences), len(sentences)))
        for i in range(len(sentences)):
            for j in range(len(sentences)):
                if i != j:
                    sim = sentence_similarity(sentences[i], sentences[j])
                    # Handle NaN values
                    if np.isnan(sim):
                        sim = 0.0
                    similarity_matrix[i][j] = sim
        
        # Check if matrix has valid values
        if np.all(similarity_matrix == 0):
            # If no similarity found, just return first few sentences
            return " ".join(sentences[:num_sentences])
                    
        # Create graph and apply PageRank
        nx_graph = nx.from_numpy_array(similarity_matrix)
        try:
            scores = nx.pagerank(nx_graph, max_iter=200)
        except:
            # If PageRank fails, return first few sentences
            return " ".join(sentences[:num_sentences])
        
        # Extract top sentences
        ranked_sentences = sorted(((scores[i], s) for i, s in enumerate(sentences)), reverse=True)
        summary = " ".join([ranked_sentences[i][1] for i in range(min(num_sentences, len(ranked_sentences)))])
        
        return summary
    except Exception as e:
        logger.warning(f"Summarization failed: {e}, returning original text")
        return text[:500] if len(text) > 500 else text

def sentence_similarity(sent1, sent2):
    try:
        words1 = [word.lower() for word in word_tokenize(sent1) if word.isalnum()]
        words2 = [word.lower() for word in word_tokenize(sent2) if word.isalnum()]
        
        if not words1 or not words2:
            return 0.0
        
        all_words = list(set(words1 + words2))
        
        if not all_words:
            return 0.0
        
        vector1 = [0] * len(all_words)
        vector2 = [0] * len(all_words)
        
        for w in words1:
            if w in all_words:
                vector1[all_words.index(w)] += 1
                
        for w in words2:
            if w in all_words:
                vector2[all_words.index(w)] += 1
        
        # Check for zero vectors
        if sum(vector1) == 0 or sum(vector2) == 0:
            return 0.0
                
        result = 1 - cosine_distance(vector1, vector2)
        return 0.0 if np.isnan(result) else result
    except:
        return 0.0

# Simple rule-based categorization
def categorize_article(title, content):
    text = (title + " " + content).lower()
    
    category_keywords = {
        'business': ['business', 'economy', 'market', 'stock', 'finance', 'investment', 'company'],
        'technology': ['tech', 'technology', 'software', 'hardware', 'ai', 'digital', 'computer', 'internet'],
        'sports': ['sport', 'football', 'soccer', 'basketball', 'tennis', 'olympics', 'athlete'],
        'politics': ['politics', 'government', 'election', 'president', 'minister', 'policy', 'vote'],
        'health': ['health', 'medical', 'doctor', 'hospital', 'disease', 'covid', 'vaccine'],
        'entertainment': ['entertainment', 'movie', 'film', 'music', 'celebrity', 'actor', 'actress']
    }
    
    category_scores = {}
    for category, keywords in category_keywords.items():
        score = sum(1 for keyword in keywords if keyword in text)
        category_scores[category] = score
    
    # Get category with highest score
    max_category = max(category_scores.items(), key=lambda x: x[1])
    
    # If no category has a score, return 'general'
    if max_category[1] == 0:
        return 'general'
    
    return max_category[0]

# Fetch news articles from API
def fetch_news(topic=None, category=None):
    if not NEWS_API_KEY:
        logger.error("NEWS_API_KEY is not configured; cannot fetch news.")
        return []
    lookback_days = NEWS_LOOKBACK_DAYS if NEWS_LOOKBACK_DAYS > 0 else 7
    if topic:
        url = 'https://newsapi.org/v2/everything'
        # Use a date from a week ago for better results
        from_date = (datetime.now() - timedelta(days=lookback_days)).strftime('%Y-%m-%d')
        params = {
            'q': topic,
            'from': from_date,
            'sortBy': SORT_BY,
            'apiKey': NEWS_API_KEY,
            'language': NEWS_LANGUAGE,
            'pageSize': 20  # Limit results
        }
    elif category:
        url = 'https://newsapi.org/v2/top-headlines'
        params = {
            'category': category,
            'apiKey': NEWS_API_KEY,
            'language': NEWS_LANGUAGE,
            'pageSize': 20  # Limit results
        }
    else:
        url = 'https://newsapi.org/v2/top-headlines'
        params = {
            'country': NEWS_FALLBACK_COUNTRY,
            'apiKey': NEWS_API_KEY,
            'pageSize': 20  # Limit results
        }
    
    try:
        logger.info(f"Fetching news: URL={url}, Params={params}")
        response = requests.get(url, params=params)
        logger.info(f"Response status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            articles = data.get('articles', [])
            logger.info(f"Found {len(articles)} articles")
            return articles
        else:
            logger.error(f"Failed to fetch news: {response.status_code}, Response: {response.text}")
            return []
    except Exception as e:
        logger.error(f"Error fetching news: {e}")
        return []

# Process articles and send to Kafka
def process_and_send_to_kafka(articles, producer):
    if not producer:
        logger.error("Kafka producer not available")
        return
        
    for article in articles:
        try:
            # Extract relevant information
            processed_article = {
                'title': article.get('title', ''),
                'source': article.get('source', {}).get('name', ''),
                'author': article.get('author', ''),
                'published_at': article.get('publishedAt', ''),
                'url': article.get('url', ''),
                'content': article.get('content', '') or article.get('description', '')
            }
            
            # Send to Kafka
            producer.produce(KAFKA_TOPIC, json.dumps(processed_article).encode('utf-8'))
            logger.info(f"Sent article to Kafka: {processed_article['title']}")
        except Exception as e:
            logger.error(f"Error processing article: {e}")
    
    producer.flush()

# Kafka consumer thread for processing articles
def kafka_consumer_thread(db_conn):
    if not KAFKA_ENABLED:
        logger.info("Kafka is disabled, consumer thread not started")
        return
    try:
        consumer = KafkaConsumer({
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': 'news_consumer_group',
            'auto.offset.reset': 'earliest'
        })
        consumer.subscribe([KAFKA_TOPIC])
        
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error(f"Consumer error: {msg.error()}")
                continue
                
            article = json.loads(msg.value().decode('utf-8'))
            
            # Process the article - strip HTML tags from content
            content = strip_html_tags(article.get('content', '') or '')
            title = article.get('title', '') or ''
            
            # Generate summary
            summary = text_rank_summarize(content)
            
            # Categorize article
            category = categorize_article(title, content)
            
            # Parse and convert datetime format for MySQL
            published_at_str = article.get('published_at', '')
            published_at = None
            if published_at_str:
                try:
                    # Convert ISO 8601 format to MySQL datetime format
                    dt = datetime.fromisoformat(published_at_str.replace('Z', '+00:00'))
                    published_at = dt.strftime('%Y-%m-%d %H:%M:%S')
                except:
                    published_at = None
            
            # Store in database
            if db_conn:
                try:
                    cursor = db_conn.cursor()
                    raw_source = article.get('source', '')
                    source_name = raw_source.get('name', '') if isinstance(raw_source, dict) else (raw_source or '')
                    query = """
                        INSERT INTO articles 
                        (title, source, author, published_at, category, url, content, summary) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    cursor.execute(query, (
                        title,
                        source_name,
                        article.get('author', '') or '',
                        published_at,
                        category,
                        article.get('url', '') or '',
                        content,
                        summary
                    ))
                    db_conn.commit()
                    logger.info(f"Stored article in database: {title}")
                except Error as e:
                    logger.error(f"Database error: {e}")
    except Exception as e:
        logger.error(f"Kafka consumer error: {e}")

# Flask routes
@app.route('/')
def index():
    return render_template('index.html', categories=CATEGORIES)

@app.route('/search', methods=['POST'])
def search():
    topic = request.form.get('topic')
    category = request.form.get('category')
    
    logger.info(f"Search request received - Topic: '{topic}', Category: '{category}'")
    
    if not topic and not category:
        logger.warning("No topic or category provided, redirecting to index")
        return redirect(url_for('index'))
    
    # Clean up the inputs
    topic = topic.strip() if topic else None
    category = category.strip() if category else None
    
    logger.info(f"Cleaned inputs - Topic: '{topic}', Category: '{category}'")
    
    articles = fetch_news(topic, category)
    
    logger.info(f"Search completed - Found {len(articles)} articles")
    
    # Process and send to Kafka
    if articles and KAFKA_ENABLED:
        producer = init_kafka_producer()
        if producer:
            process_and_send_to_kafka(articles, producer)
    
    return render_template('results.html', articles=articles, topic=topic, category=category)

@app.route('/summaries')
def summaries():
    try:
        conn = get_db_connection()
        if not conn:
            raise Error("Database connection failed")
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM articles ORDER BY published_at DESC LIMIT 20")
        articles = cursor.fetchall()
        conn.close()
        
        return render_template('summaries.html', articles=articles)
    except Error as e:
        logger.error(f"Database error: {e}")
        return render_template('summaries.html', articles=[], error=str(e))

@app.route('/api/articles')
def api_articles():
    category = request.args.get('category')
    limit = request.args.get('limit', 10, type=int)
    
    try:
        conn = get_db_connection()
        if not conn:
            raise Error("Database connection failed")
        cursor = conn.cursor(dictionary=True)
        
        if category:
            cursor.execute("SELECT * FROM articles WHERE category = %s ORDER BY published_at DESC LIMIT %s", 
                          (category, limit))
        else:
            cursor.execute("SELECT * FROM articles ORDER BY published_at DESC LIMIT %s", (limit,))
            
        articles = cursor.fetchall()
        conn.close()
        
        return jsonify(articles)
    except Error as e:
        logger.error(f"Database error: {e}")
        return jsonify({"error": str(e)}), 500

# Start background services
def start_services():
    # Initialize database
    db_conn = init_db()
    
    # Start Kafka consumer thread
    if db_conn and KAFKA_ENABLED:
        consumer_thread = threading.Thread(target=kafka_consumer_thread, args=(db_conn,))
        consumer_thread.daemon = True
        consumer_thread.start()
        logger.info("Kafka consumer thread started")
    else:
        logger.info("Kafka consumer thread not started (disabled or unavailable)")

if __name__ == '__main__':
    # Start background services
    start_services()
    
    # Create templates directory if it doesn't exist
    os.makedirs('templates', exist_ok=True)
    
    # Start Flask app
    app.run(debug=True, port=5000)
