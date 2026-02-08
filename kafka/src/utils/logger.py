"""
Centralized Logging Configuration
Provides structured logging for all Kafka pipeline components
"""
import logging
import sys
from pathlib import Path
from logging.handlers import RotatingFileHandler


def setup_logger(
    name: str,
    log_file: str = None,
    level: int = logging.INFO,
    max_bytes: int = 10 * 1024 * 1024,  # 10MB
    backup_count: int = 5
) -> logging.Logger:
    """
    Setup a logger with both file and console handlers
    
    Args:
        name: Logger name (typically __name__)
        log_file: Path to log file (optional)
        level: Logging level
        max_bytes: Max size of log file before rotation
        backup_count: Number of backup files to keep
    
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Prevent duplicate handlers
    if logger.handlers:
        return logger
    
    # Create formatter
    formatter = logging.Formatter(
        fmt='%(asctime)s | %(name)s | %(levelname)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # File handler (if log_file specified)
    if log_file:
        # Create logs directory if it doesn't exist
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=max_bytes,
            backupCount=backup_count
        )
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger


# Default loggers for common components
def get_producer_logger():
    """Get logger for Kafka producer"""
    return setup_logger(
        'kafka.producer',
        log_file='logs/producer.log'
    )


def get_consumer_logger():
    """Get logger for Kafka consumer"""
    return setup_logger(
        'kafka.consumer',
        log_file='logs/consumer.log'
    )


def get_feature_store_logger():
    """Get logger for Redis feature store"""
    return setup_logger(
        'kafka.feature_store',
        log_file='logs/feature_store.log'
    )


def get_feature_extractor_logger():
    """Get logger for feature extraction operations"""
    return setup_logger(
        'kafka.feature_extractor',
        log_file='logs/feature_extractor.log'
    )


def get_model_service_logger():
    """Get logger for model service"""
    return setup_logger(
        'kafka.model_service',
        log_file='logs/model_service.log'
    )


def get_prediction_store_logger():
    """Get logger for prediction store"""
    return setup_logger(
        'kafka.prediction_store',
        log_file='logs/prediction_store.log'
    )
