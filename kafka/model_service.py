"""
Model Service for Fraud Detection
Handles model loading, feature engineering, and inference
"""
import pickle
import numpy as np
from typing import Dict, Any, Tuple
from pathlib import Path
from datetime import datetime
from logger import get_model_service_logger

logger = get_model_service_logger()


class ModelService:
    """LightGBM model service for fraud detection"""
    
    def __init__(self, model_path: str = "../model.pkl"):
        """
        Initialize model service
        
        Args:
            model_path: Path to pickled LightGBM model
        """
        self.model_path = model_path
        self.model = None
        self.model_version = "v1.0.0"
        self.feature_names = None
        
        # Decision thresholds
        self.high_risk_threshold = 0.7
        self.medium_risk_threshold = 0.3
        
        logger.info(f"Model service initialized (lazy loading from {model_path})")
    
    def _load_model(self):
        """Lazy load the model on first inference"""
        if self.model is not None:
            return
        
        try:
            model_file = Path(self.model_path)
            if not model_file.exists():
                logger.warning(f"Model file not found at {self.model_path}")
                logger.warning("Using dummy model for testing purposes")
                self.model = "DUMMY_MODEL"
                return
            
            with open(model_file, 'rb') as f:
                self.model = pickle.load(f)
            
            # Try to get feature names from model
            if hasattr(self.model, 'feature_name_'):
                self.feature_names = self.model.feature_name_
            
            logger.info(f"✅ Model loaded successfully from {self.model_path}")
            
        except Exception as e:
            logger.error(f"Error loading model: {e}")
            logger.warning("Using dummy model for testing purposes")
            self.model = "DUMMY_MODEL"
    
    def engineer_features(
        self,
        transaction: Dict[str, Any],
        redis_features: Dict[str, Any]
    ) -> Dict[str, float]:
        """
        Engineer features for model inference
        
        Args:
            transaction: Raw transaction data
            redis_features: Features from Redis (card + merchant)
        
        Returns:
            Feature dictionary ready for model
        """
        features = {}
        
        # Transaction-level features
        features['amount'] = float(transaction.get('amount', 0.0))
        
        # Time-based features
        timestamp = transaction.get('timestamp', int(datetime.now().timestamp()))
        dt = datetime.fromtimestamp(timestamp)
        features['hour_of_day'] = dt.hour
        features['day_of_week'] = dt.weekday()
        features['is_weekend'] = 1 if dt.weekday() >= 5 else 0
        
        # Merchant category encoding (simple hash for demo)
        merchant_category = transaction.get('merchant_category', 'UNKNOWN')
        features['merchant_category_hash'] = hash(merchant_category) % 1000
        
        # Card velocity features from Redis
        features['tx_count_10m'] = redis_features.get('card_tx_count_10m', 0)
        features['tx_count_1h'] = redis_features.get('card_tx_count_1h', 0)
        features['tx_count_24h'] = redis_features.get('card_tx_count_24h', 0)
        features['total_amount_10m'] = redis_features.get('card_total_amount_10m', 0.0)
        features['total_amount_1h'] = redis_features.get('card_total_amount_1h', 0.0)
        features['total_amount_24h'] = redis_features.get('card_total_amount_24h', 0.0)
        features['unique_merchants_24h'] = redis_features.get('card_unique_merchants_24h', 0)
        
        # Historical features
        avg_amount = redis_features.get('card_avg_tx_amount_30d', 75.0)
        features['avg_tx_amount_30d'] = avg_amount
        features['amount_vs_avg_ratio'] = features['amount'] / avg_amount if avg_amount > 0 else 1.0
        
        # Cold start indicator
        features['is_new_card'] = redis_features.get('card_is_new_card', 1)
        
        # Merchant features
        features['merchant_risk_score'] = redis_features.get('merchant_risk_score', 0.5)
        features['merchant_fraud_rate'] = redis_features.get('merchant_fraud_rate', 0.002)
        
        # Time since last transaction
        last_tx_timestamp = redis_features.get('card_last_tx_timestamp', 0)
        if last_tx_timestamp > 0:
            features['time_since_last_tx'] = timestamp - last_tx_timestamp
        else:
            features['time_since_last_tx'] = 86400  # Default to 1 day
        
        # Velocity ratios
        if features['tx_count_1h'] > 0:
            features['amount_per_tx_1h'] = features['total_amount_1h'] / features['tx_count_1h']
        else:
            features['amount_per_tx_1h'] = 0.0
        
        return features
    
    def predict(
        self,
        transaction: Dict[str, Any],
        redis_features: Dict[str, Any]
    ) -> Tuple[float, str, str]:
        """
        Make fraud prediction
        
        Args:
            transaction: Transaction data
            redis_features: Features from Redis
        
        Returns:
            Tuple of (fraud_probability, risk_level, decision)
        """
        # Lazy load model
        self._load_model()
        
        # Engineer features
        features = self.engineer_features(transaction, redis_features)
        
        # Make prediction
        if self.model == "DUMMY_MODEL":
            # Dummy prediction for testing
            fraud_probability = self._dummy_predict(transaction, features)
        else:
            try:
                # Convert features to array in correct order
                if self.feature_names:
                    feature_array = np.array([features.get(name, 0.0) for name in self.feature_names])
                else:
                    feature_array = np.array(list(features.values()))
                
                feature_array = feature_array.reshape(1, -1)
                fraud_probability = float(self.model.predict_proba(feature_array)[0][1])
                
            except Exception as e:
                logger.error(f"Error during model inference: {e}")
                # Fallback to rule-based
                fraud_probability = self._dummy_predict(transaction, features)
        
        # Determine risk level and decision
        risk_level = self._get_risk_level(fraud_probability)
        decision = self._make_decision(fraud_probability, transaction)
        
        return fraud_probability, risk_level, decision
    
    def _dummy_predict(
        self,
        transaction: Dict[str, Any],
        features: Dict[str, float]
    ) -> float:
        """
        Dummy prediction based on simple rules (for testing without real model)
        
        Args:
            transaction: Transaction data
            features: Engineered features
        
        Returns:
            Fraud probability (0-1)
        """
        score = 0.0
        
        # High amount
        if features['amount'] > 1000:
            score += 0.3
        
        # High velocity
        if features['tx_count_10m'] > 5:
            score += 0.25
        
        # Amount much higher than average
        if features['amount_vs_avg_ratio'] > 3:
            score += 0.2
        
        # High merchant risk
        if features['merchant_risk_score'] > 0.7:
            score += 0.15
        
        # New card
        if features['is_new_card'] == 1:
            score += 0.1
        
        return min(score, 1.0)
    
    def _get_risk_level(self, fraud_probability: float) -> str:
        """
        Classify risk level based on probability
        
        Args:
            fraud_probability: Fraud probability (0-1)
        
        Returns:
            Risk level (LOW/MEDIUM/HIGH)
        """
        if fraud_probability >= self.high_risk_threshold:
            return "HIGH"
        elif fraud_probability >= self.medium_risk_threshold:
            return "MEDIUM"
        else:
            return "LOW"
    
    def _make_decision(
        self,
        fraud_probability: float,
        transaction: Dict[str, Any]
    ) -> str:
        """
        Make final decision based on probability and business rules
        
        Args:
            fraud_probability: Fraud probability (0-1)
            transaction: Transaction data
        
        Returns:
            Decision (APPROVE/REVIEW/BLOCK)
        """
        amount = transaction.get('amount', 0.0)
        
        # High risk: Block
        if fraud_probability >= self.high_risk_threshold:
            return "BLOCK"
        
        # Medium risk: Send to review
        elif fraud_probability >= self.medium_risk_threshold:
            # High-value transactions go to review even at medium risk
            if amount > 5000:
                return "REVIEW"
            else:
                return "APPROVE"
        
        # Low risk: Approve
        else:
            return "APPROVE"
    
    def get_model_info(self) -> Dict[str, Any]:
        """
        Get model metadata
        
        Returns:
            Dictionary with model information
        """
        return {
            'model_version': self.model_version,
            'model_path': self.model_path,
            'model_loaded': self.model is not None,
            'high_risk_threshold': self.high_risk_threshold,
            'medium_risk_threshold': self.medium_risk_threshold
        }
