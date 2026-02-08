"""
Kafka Configuration Settings
"""
import os
from dotenv import load_dotenv

load_dotenv()

# Kafka Broker Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

# Topic Configuration
TOPIC_NAME = 'transactions'
TOPIC_PARTITIONS = 12
TOPIC_REPLICATION_FACTOR = 1  # Set to 3 for production with multiple brokers

# Producer Configuration
PRODUCER_CONFIG = {
    'bootstrap_servers': KAFKA_BOOTSTRAP_SERVERS,
    'compression_type': 'lz4',
    'acks': 'all',  # Wait for all replicas to acknowledge
    'retries': 3,
    'max_in_flight_requests_per_connection': 5,
    'linger_ms': 10,  # Batch messages for 10ms
    'batch_size': 16384,  # 16KB batch size
}

# Consumer Configuration
CONSUMER_CONFIG = {
    'bootstrap_servers': KAFKA_BOOTSTRAP_SERVERS,
    'group_id': os.getenv('CONSUMER_GROUP_ID', 'fraud-detection-consumer'),
    'auto_offset_reset': 'earliest',  # Start from beginning if no offset
    'enable_auto_commit': True,
    'auto_commit_interval_ms': 1000,
    'max_poll_records': 100,
    'session_timeout_ms': 30000,
    'heartbeat_interval_ms': 10000,
}

# Redis Configuration
REDIS_CONFIG = {
    'host': os.getenv('REDIS_HOST', 'localhost'),
    'port': int(os.getenv('REDIS_PORT', 6379)),
    'db': int(os.getenv('REDIS_DB', 0)),
    'max_connections': 50,
    'socket_timeout': 5,
    'socket_connect_timeout': 5,
}

# Model Configuration
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MODEL_PATH = os.getenv('MODEL_PATH', os.path.join(BASE_DIR, '..', 'model.pkl'))

# Prediction Storage Configuration
PREDICTION_DB_PATH = os.getenv('PREDICTION_DB_PATH', os.path.join(BASE_DIR, 'predictions.db'))

# Data Processing Configuration
# BASE_DIR is kafka/src/utils
# Dataset is at credit_card/fraudTrain.csv
# So from BASE_DIR, go up to src (..), then to kafka (..), then to credit_card (..)
DEFAULT_DATASET_PATH = os.path.normpath(os.path.join(BASE_DIR, '..', '..', '..', 'fraudTrain.csv'))

raw_path = os.getenv('DATASET_PATH', DEFAULT_DATASET_PATH)

# Determine final path
if os.path.isabs(raw_path):
    DATASET_PATH = raw_path
else:
    # If path is relative, resolve it relative to project root (kafka folder)
    project_root = os.path.normpath(os.path.join(BASE_DIR, '..', '..'))
    DATASET_PATH = os.path.normpath(os.path.join(project_root, raw_path))
BATCH_SIZE = 1000  # Number of records to process at once
RATE_LIMIT = 100  # Messages per second (0 = no limit)

# Feature Engineering Configuration
FEATURE_CONFIG = {
    'velocity_windows': {
        '10m': 600,     # 10 minutes in seconds
        '1h': 3600,     # 1 hour in seconds
        '24h': 86400    # 24 hours in seconds
    },
    'rolling_avg_alpha': 0.1,  # Exponential smoothing factor for rolling averages
    'amount_clip_percentile': 99.0,  # Clip outliers at 99th percentile
    'default_avg_amount': 75.0,  # Global average amount for cold start
}

# Redis TTL Configuration (in seconds)
REDIS_TTL = {
    'tx_history': 86400,      # 24 hours - transaction history
    'merchant_set': 86400,    # 24 hours - unique merchant tracking
    'card_stats': 2592000,    # 30 days - card statistics
}
