"""
Redis Feature Store Interface
Manages real-time feature retrieval for fraud detection
"""
import redis
from typing import Dict, Any, Optional, List
from src.utils.logger import get_feature_store_logger

logger = get_feature_store_logger()


class FeatureStore:
    """Redis-backed feature store for real-time features"""
    
    def __init__(
        self,
        host: str = 'localhost',
        port: int = 6379,
        db: int = 0,
        max_connections: int = 50,
        socket_timeout: int = 5,
        socket_connect_timeout: int = 5
    ):
        """
        Initialize Redis connection pool
        
        Args:
            host: Redis host
            port: Redis port
            db: Redis database number
            max_connections: Max connections in pool
            socket_timeout: Socket timeout in seconds
            socket_connect_timeout: Connection timeout in seconds
        """
        self.pool = redis.ConnectionPool(
            host=host,
            port=port,
            db=db,
            max_connections=max_connections,
            socket_timeout=socket_timeout,
            socket_connect_timeout=socket_connect_timeout,
            decode_responses=True
        )
        self.redis_client = redis.Redis(connection_pool=self.pool)
        
        # Test connection
        try:
            self.redis_client.ping()
            logger.info(f"✅ Connected to Redis at {host}:{port}")
        except redis.ConnectionError as e:
            logger.error(f"❌ Failed to connect to Redis: {e}")
            raise
    
    def get_card_features(self, card_id: str) -> Dict[str, Any]:
        """
        Get features for a specific card
        
        Args:
            card_id: Card identifier
        
        Returns:
            Dictionary of features with defaults if not found
        """
        try:
            key = f"features:card:{card_id}"
            features = self.redis_client.hgetall(key)
            
            if not features:
                logger.debug(f"No features found for card {card_id}, using defaults")
                return self._get_default_card_features()
            
            # Convert string values to appropriate types
            return {
                'tx_count_10m': int(features.get('tx_count_10m', 0)),
                'tx_count_1h': int(features.get('tx_count_1h', 0)),
                'tx_count_24h': int(features.get('tx_count_24h', 0)),
                'total_amount_10m': float(features.get('total_amount_10m', 0.0)),
                'total_amount_1h': float(features.get('total_amount_1h', 0.0)),
                'total_amount_24h': float(features.get('total_amount_24h', 0.0)),
                'unique_merchants_24h': int(features.get('unique_merchants_24h', 0)),
                'avg_tx_amount_30d': float(features.get('avg_tx_amount_30d', 75.0)),
                'last_tx_timestamp': int(features.get('last_tx_timestamp', 0)),
                'is_new_card': int(features.get('is_new_card', 1))
            }
            
        except redis.RedisError as e:
            logger.error(f"Redis error fetching card features: {e}")
            return self._get_default_card_features()
        except Exception as e:
            logger.error(f"Unexpected error fetching card features: {e}")
            return self._get_default_card_features()
    
    def get_merchant_features(self, merchant_id: str) -> Dict[str, Any]:
        """
        Get features for a specific merchant
        
        Args:
            merchant_id: Merchant identifier
        
        Returns:
            Dictionary of merchant features
        """
        try:
            key = f"features:merchant:{merchant_id}"
            features = self.redis_client.hgetall(key)
            
            if not features:
                logger.debug(f"No features found for merchant {merchant_id}, using defaults")
                return self._get_default_merchant_features()
            
            return {
                'risk_score': float(features.get('risk_score', 0.5)),
                'fraud_rate': float(features.get('fraud_rate', 0.002)),
                'total_transactions': int(features.get('total_transactions', 100))
            }
            
        except redis.RedisError as e:
            logger.error(f"Redis error fetching merchant features: {e}")
            return self._get_default_merchant_features()
        except Exception as e:
            logger.error(f"Unexpected error fetching merchant features: {e}")
            return self._get_default_merchant_features()
    
    def get_all_features(
        self,
        card_id: str,
        merchant_id: str
    ) -> Dict[str, Any]:
        """
        Get all features for a transaction (card + merchant)
        
        Args:
            card_id: Card identifier
            merchant_id: Merchant identifier
        
        Returns:
            Combined feature dictionary
        """
        card_features = self.get_card_features(card_id)
        merchant_features = self.get_merchant_features(merchant_id)
        
        # Combine with prefixes to avoid key collisions
        all_features = {}
        all_features.update({f'card_{k}': v for k, v in card_features.items()})
        all_features.update({f'merchant_{k}': v for k, v in merchant_features.items()})
        
        return all_features
    
    def _get_default_card_features(self) -> Dict[str, Any]:
        """
        Get default card features for cold start or Redis failure
        
        Returns:
            Dictionary with default values
        """
        return {
            'tx_count_10m': 0,
            'tx_count_1h': 0,
            'tx_count_24h': 0,
            'total_amount_10m': 0.0,
            'total_amount_1h': 0.0,
            'total_amount_24h': 0.0,
            'unique_merchants_24h': 0,
            'avg_tx_amount_30d': 75.0,  # Global average
            'last_tx_timestamp': 0,
            'is_new_card': 1  # Flag for cold start
        }
    
    def _get_default_merchant_features(self) -> Dict[str, Any]:
        """
        Get default merchant features
        
        Returns:
            Dictionary with default values
        """
        return {
            'risk_score': 0.5,  # Neutral risk
            'fraud_rate': 0.002,  # Global fraud rate
            'total_transactions': 100
        }
    
    def update_card_features(
        self,
        card_id: str,
        features: Dict[str, Any],
        ttl: int = 2592000  # 30 days
    ) -> bool:
        """
        Update card features in Redis
        
        Args:
            card_id: Card identifier
            features: Feature dictionary
            ttl: Time-to-live in seconds
        
        Returns:
            True if successful, False otherwise
        """
        try:
            key = f"features:card:{card_id}"
            self.redis_client.hset(key, mapping=features)
            self.redis_client.expire(key, ttl)
            return True
        except Exception as e:
            logger.error(f"Error updating card features: {e}")
            return False
    
    def add_to_transaction_history(
        self,
        card_id: str,
        transaction: Dict[str, Any],
        ttl: int = 86400  # 24 hours
    ) -> bool:
        """
        Add transaction to card's history using Redis sorted set
        Score = timestamp for time-based queries
        
        Args:
            card_id: Card identifier
            transaction: Transaction dictionary
            ttl: Time-to-live in seconds
        
        Returns:
            True if successful, False otherwise
        """
        try:
            import json
            key = f"card:{card_id}:tx_history"
            timestamp = transaction['timestamp']
            
            # Store transaction as JSON with timestamp as score
            tx_data = json.dumps({
                'amount': transaction['amount'],
                'merchant_id': transaction['merchant_id'],
                'timestamp': timestamp
            })
            
            # Add to sorted set
            self.redis_client.zadd(key, {tx_data: timestamp})
            
            # Set TTL
            self.redis_client.expire(key, ttl)
            
            # Clean up old entries (older than TTL)
            min_timestamp = timestamp - ttl
            self.redis_client.zremrangebyscore(key, '-inf', min_timestamp)
            
            return True
        except Exception as e:
            logger.error(f"Error adding to transaction history: {e}")
            return False
    
    def get_transaction_history(
        self,
        card_id: str,
        window_seconds: int,
        current_timestamp: int
    ) -> List[Dict[str, Any]]:
        """
        Retrieve transactions within a time window
        
        Args:
            card_id: Card identifier
            window_seconds: Time window in seconds
            current_timestamp: Current timestamp
        
        Returns:
            List of transaction dictionaries
        """
        try:
            import json
            key = f"card:{card_id}:tx_history"
            min_timestamp = current_timestamp - window_seconds
            
            # Get transactions in time range
            tx_data_list = self.redis_client.zrangebyscore(
                key,
                min_timestamp,
                current_timestamp
            )
            
            # Parse JSON data
            transactions = []
            for tx_data in tx_data_list:
                try:
                    tx = json.loads(tx_data)
                    transactions.append(tx)
                except json.JSONDecodeError:
                    continue
            
            return transactions
        except redis.RedisError as e:
            logger.error(f"Redis error getting transaction history: {e}")
            return []
        except Exception as e:
            logger.error(f"Error getting transaction history: {e}")
            return []
    
    def add_merchant_to_set(
        self,
        card_id: str,
        merchant_id: str,
        ttl: int = 86400  # 24 hours
    ) -> bool:
        """
        Add merchant to card's unique merchant set
        
        Args:
            card_id: Card identifier
            merchant_id: Merchant identifier
            ttl: Time-to-live in seconds
        
        Returns:
            True if successful, False otherwise
        """
        try:
            key = f"card:{card_id}:merchants:24h"
            self.redis_client.sadd(key, merchant_id)
            self.redis_client.expire(key, ttl)
            return True
        except Exception as e:
            logger.error(f"Error adding merchant to set: {e}")
            return False
    
    def get_unique_merchant_count(
        self,
        card_id: str,
        window_seconds: int = 86400
    ) -> int:
        """
        Get count of unique merchants for a card
        
        Args:
            card_id: Card identifier
            window_seconds: Time window (not used, set is already time-limited)
        
        Returns:
            Count of unique merchants
        """
        try:
            key = f"card:{card_id}:merchants:24h"
            return self.redis_client.scard(key)
        except Exception as e:
            logger.error(f"Error getting unique merchant count: {e}")
            return 0
    
    def update_rolling_average(
        self,
        card_id: str,
        amount: float,
        alpha: float = 0.1
    ) -> float:
        """
        Update exponential moving average for transaction amount
        
        Formula: new_avg = alpha * amount + (1 - alpha) * old_avg
        
        Args:
            card_id: Card identifier
            amount: Current transaction amount
            alpha: Smoothing factor (0-1)
        
        Returns:
            Updated average
        """
        try:
            key = f"card:{card_id}:stats"
            
            # Get current average
            old_avg_str = self.redis_client.hget(key, 'avg_amount')
            old_avg = float(old_avg_str) if old_avg_str else 75.0  # Default
            
            # Compute new average
            new_avg = alpha * amount + (1 - alpha) * old_avg
            
            # Update in Redis
            self.redis_client.hset(key, 'avg_amount', new_avg)
            self.redis_client.expire(key, 2592000)  # 30 days TTL
            
            return new_avg
        except Exception as e:
            logger.error(f"Error updating rolling average: {e}")
            return 75.0  # Return default on error
    
    def get_rolling_average(self, card_id: str) -> Optional[float]:
        """
        Get current rolling average for a card
        
        Args:
            card_id: Card identifier
        
        Returns:
            Rolling average or None if not found
        """
        try:
            key = f"card:{card_id}:stats"
            avg_str = self.redis_client.hget(key, 'avg_amount')
            return float(avg_str) if avg_str else None
        except Exception as e:
            logger.error(f"Error getting rolling average: {e}")
            return None
    
    def update_last_transaction_timestamp(
        self,
        card_id: str,
        timestamp: int
    ) -> bool:
        """
        Update the last transaction timestamp for a card
        
        Args:
            card_id: Card identifier
            timestamp: Unix timestamp
        
        Returns:
            True if successful, False otherwise
        """
        try:
            key = f"card:{card_id}:stats"
            self.redis_client.hset(key, 'last_tx_timestamp', timestamp)
            self.redis_client.expire(key, 2592000)  # 30 days TTL
            return True
        except Exception as e:
            logger.error(f"Error updating last transaction timestamp: {e}")
            return False
    
    def get_last_transaction_timestamp(self, card_id: str) -> Optional[int]:
        """
        Get the last transaction timestamp for a card
        
        Args:
            card_id: Card identifier
        
        Returns:
            Unix timestamp or None if not found
        """
        try:
            key = f"card:{card_id}:stats"
            ts_str = self.redis_client.hget(key, 'last_tx_timestamp')
            return int(ts_str) if ts_str else None
        except Exception as e:
            logger.error(f"Error getting last transaction timestamp: {e}")
            return None
    
    def health_check(self) -> bool:
        """
        Check if Redis is healthy
        
        Returns:
            True if Redis is reachable, False otherwise
        """
        try:
            return self.redis_client.ping()
        except Exception:
            return False
    
    def close(self):
        """Close Redis connection pool"""
        try:
            self.redis_client.close()
            logger.info("Redis connection closed")
        except Exception as e:
            logger.error(f"Error closing Redis connection: {e}")
