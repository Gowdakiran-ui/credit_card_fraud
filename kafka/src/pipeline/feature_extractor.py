"""
Feature Extraction Engine
Computes real-time fraud detection features from transaction events
"""
import time
from typing import Dict, Any, List
from datetime import datetime
from src.utils.logger import get_feature_extractor_logger

logger = get_feature_extractor_logger()


class FeatureExtractor:
    """
    Extracts fraud-focused features from transaction events
    
    Features computed:
    - Transaction-level: amount, normalized amount, temporal features
    - Velocity features: transaction counts and amounts in time windows
    - Rolling aggregations: 30-day average, amount deviation
    - Temporal features: hour of day, day of week
    """
    
    def __init__(self, feature_store, feature_config: Dict[str, Any]):
        """
        Initialize feature extractor
        
        Args:
            feature_store: FeatureStore instance for Redis operations
            feature_config: Configuration dict with velocity windows, etc.
        """
        self.feature_store = feature_store
        self.config = feature_config
        self.velocity_windows = feature_config.get('velocity_windows', {
            '10m': 600,
            '1h': 3600,
            '24h': 86400
        })
        self.rolling_avg_alpha = feature_config.get('rolling_avg_alpha', 0.1)
        self.default_avg_amount = feature_config.get('default_avg_amount', 75.0)
    
    def extract_features(self, transaction: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract all features for a transaction
        
        Args:
            transaction: Preprocessed transaction dictionary
        
        Returns:
            Dictionary of computed features
        """
        start_time = time.time()
        
        card_id = transaction['card_id']
        merchant_id = transaction['merchant_id']
        amount = transaction['amount']
        timestamp = transaction['timestamp']
        
        # Initialize features dictionary
        features = {}
        
        # 1. Transaction-level features
        tx_features = self._compute_transaction_features(transaction)
        features.update(tx_features)
        
        # 2. Velocity features (requires Redis state)
        velocity_features = self._compute_velocity_features(card_id, timestamp)
        features.update(velocity_features)
        
        # 3. Rolling aggregation features
        rolling_features = self._compute_rolling_features(card_id, amount)
        features.update(rolling_features)
        
        # 4. Temporal features
        temporal_features = self._compute_temporal_features(timestamp)
        features.update(temporal_features)
        
        # 5. Merchant features (from Redis)
        merchant_features = self.feature_store.get_merchant_features(merchant_id)
        features.update({f'merchant_{k}': v for k, v in merchant_features.items()})
        
        # Log feature extraction time
        extraction_time = (time.time() - start_time) * 1000
        logger.debug(f"Feature extraction for {transaction['transaction_id']}: {extraction_time:.1f}ms")
        
        return features
    
    def _compute_transaction_features(self, transaction: Dict[str, Any]) -> Dict[str, Any]:
        """
        Compute raw transaction-level features
        
        Args:
            transaction: Transaction dictionary
        
        Returns:
            Dictionary of transaction features
        """
        amount = transaction['amount']
        
        return {
            'amount': amount,
            'amount_log': self._safe_log(amount),
            'merchant_category': transaction.get('merchant_category', 'UNKNOWN'),
            'has_location': 1 if transaction.get('location_lat') is not None else 0,
        }
    
    def _compute_velocity_features(self, card_id: str, current_timestamp: int) -> Dict[str, Any]:
        """
        Compute velocity features from Redis transaction history
        
        Features:
        - tx_count_10m, tx_count_1h, tx_count_24h
        - total_amount_10m, total_amount_1h, total_amount_24h
        - unique_merchants_24h
        - time_since_last_tx
        
        Args:
            card_id: Card identifier
            current_timestamp: Current transaction timestamp
        
        Returns:
            Dictionary of velocity features
        """
        features = {}
        
        # Get transaction history from Redis
        for window_name, window_seconds in self.velocity_windows.items():
            window_start = current_timestamp - window_seconds
            
            # Get transactions in this window
            tx_history = self.feature_store.get_transaction_history(
                card_id=card_id,
                window_seconds=window_seconds,
                current_timestamp=current_timestamp
            )
            
            # Count transactions
            features[f'tx_count_{window_name}'] = len(tx_history)
            
            # Sum amounts
            total_amount = sum(tx.get('amount', 0) for tx in tx_history)
            features[f'total_amount_{window_name}'] = round(total_amount, 2)
        
        # Get unique merchants in 24h
        unique_merchants = self.feature_store.get_unique_merchant_count(
            card_id=card_id,
            window_seconds=self.velocity_windows['24h']
        )
        features['unique_merchants_24h'] = unique_merchants
        
        # Time since last transaction
        last_tx_timestamp = self.feature_store.get_last_transaction_timestamp(card_id)
        if last_tx_timestamp and last_tx_timestamp > 0:
            features['time_since_last_tx'] = current_timestamp - last_tx_timestamp
        else:
            features['time_since_last_tx'] = 0  # First transaction
        
        return features
    
    def _compute_rolling_features(self, card_id: str, current_amount: float) -> Dict[str, Any]:
        """
        Compute rolling aggregation features
        
        Features:
        - avg_tx_amount_30d: 30-day exponential moving average
        - amount_deviation: (current - average) / average
        
        Args:
            card_id: Card identifier
            current_amount: Current transaction amount
        
        Returns:
            Dictionary of rolling features
        """
        # Get current rolling average from Redis
        current_avg = self.feature_store.get_rolling_average(card_id)
        
        if current_avg is None or current_avg == 0:
            current_avg = self.default_avg_amount
        
        # Compute deviation
        amount_deviation = (current_amount - current_avg) / current_avg if current_avg > 0 else 0
        
        return {
            'avg_tx_amount_30d': round(current_avg, 2),
            'amount_deviation': round(amount_deviation, 3),
            'amount_vs_avg_ratio': round(current_amount / current_avg, 3) if current_avg > 0 else 1.0
        }
    
    def _compute_temporal_features(self, timestamp: int) -> Dict[str, Any]:
        """
        Compute temporal features from timestamp
        
        Features:
        - hour_of_day: 0-23
        - day_of_week: 0-6 (Monday=0)
        - is_weekend: 1 if Saturday/Sunday, 0 otherwise
        - is_night: 1 if 22:00-06:00, 0 otherwise
        
        Args:
            timestamp: Unix epoch timestamp
        
        Returns:
            Dictionary of temporal features
        """
        dt = datetime.fromtimestamp(timestamp)
        
        hour = dt.hour
        day_of_week = dt.weekday()
        
        return {
            'hour_of_day': hour,
            'day_of_week': day_of_week,
            'is_weekend': 1 if day_of_week >= 5 else 0,
            'is_night': 1 if (hour >= 22 or hour < 6) else 0,
        }
    
    def update_card_state(self, card_id: str, transaction: Dict[str, Any]):
        """
        Update Redis with new transaction data for future feature computation
        
        This updates:
        - Transaction history (sorted set)
        - Merchant tracking (set)
        - Rolling averages (hash)
        - Last transaction timestamp
        
        Args:
            card_id: Card identifier
            transaction: Transaction dictionary
        """
        timestamp = transaction['timestamp']
        amount = transaction['amount']
        merchant_id = transaction['merchant_id']
        
        # 1. Add to transaction history
        self.feature_store.add_to_transaction_history(
            card_id=card_id,
            transaction=transaction
        )
        
        # 2. Track unique merchant
        self.feature_store.add_merchant_to_set(
            card_id=card_id,
            merchant_id=merchant_id
        )
        
        # 3. Update rolling average
        self.feature_store.update_rolling_average(
            card_id=card_id,
            amount=amount,
            alpha=self.rolling_avg_alpha
        )
        
        # 4. Update last transaction timestamp
        self.feature_store.update_last_transaction_timestamp(
            card_id=card_id,
            timestamp=timestamp
        )
        
        logger.debug(f"Updated state for card {card_id}")
    
    @staticmethod
    def _safe_log(value: float) -> float:
        """
        Compute log safely (handle zero/negative)
        
        Args:
            value: Input value
        
        Returns:
            log(value + 1)
        """
        import math
        return math.log(max(value, 0) + 1)
