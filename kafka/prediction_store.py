"""
Prediction Storage Module
SQLite database for storing fraud predictions and audit trail
"""
import sqlite3
import json
from datetime import datetime
from typing import Dict, Any, Optional, List
from pathlib import Path
from logger import get_prediction_store_logger

logger = get_prediction_store_logger()


class PredictionStore:
    """SQLite-backed storage for fraud predictions"""
    
    def __init__(self, db_path: str = "predictions.db"):
        """
        Initialize SQLite database
        
        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = db_path
        self._init_database()
        logger.info(f"✅ Prediction store initialized at {db_path}")
    
    def _init_database(self):
        """Create database schema if it doesn't exist"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Create predictions table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS predictions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    transaction_id TEXT UNIQUE NOT NULL,
                    card_id TEXT NOT NULL,
                    amount REAL NOT NULL,
                    merchant_id TEXT,
                    merchant_category TEXT,
                    fraud_probability REAL NOT NULL,
                    risk_level TEXT NOT NULL,
                    decision TEXT NOT NULL,
                    model_version TEXT,
                    features_json TEXT,
                    actual_label INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create indexes for fast queries
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_card_id 
                ON predictions(card_id)
            """)
            
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_created_at 
                ON predictions(created_at)
            """)
            
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_transaction_id 
                ON predictions(transaction_id)
            """)
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            logger.error(f"Error initializing database: {e}")
            raise
    
    def store_prediction(
        self,
        transaction_id: str,
        card_id: str,
        amount: float,
        merchant_id: str,
        merchant_category: str,
        fraud_probability: float,
        risk_level: str,
        decision: str,
        model_version: str = "v1.0.0",
        features: Optional[Dict[str, Any]] = None,
        actual_label: Optional[int] = None
    ) -> bool:
        """
        Store a prediction in the database
        
        Args:
            transaction_id: Unique transaction identifier
            card_id: Card identifier
            amount: Transaction amount
            merchant_id: Merchant identifier
            merchant_category: Merchant category
            fraud_probability: Model's fraud probability (0-1)
            risk_level: Risk level (LOW/MEDIUM/HIGH)
            decision: Decision (APPROVE/REVIEW/BLOCK)
            model_version: Model version used
            features: Feature dictionary (will be JSON serialized)
            actual_label: Actual fraud label if known (0/1)
        
        Returns:
            True if successful, False otherwise
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            features_json = json.dumps(features) if features else None
            
            cursor.execute("""
                INSERT INTO predictions (
                    transaction_id, card_id, amount, merchant_id, merchant_category,
                    fraud_probability, risk_level, decision, model_version,
                    features_json, actual_label
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                transaction_id, card_id, amount, merchant_id, merchant_category,
                fraud_probability, risk_level, decision, model_version,
                features_json, actual_label
            ))
            
            conn.commit()
            conn.close()
            
            logger.debug(f"Stored prediction for transaction {transaction_id}")
            return True
            
        except sqlite3.IntegrityError:
            logger.warning(f"Duplicate transaction_id: {transaction_id}")
            return False
        except Exception as e:
            logger.error(f"Error storing prediction: {e}")
            return False
    
    def get_prediction(self, transaction_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve a prediction by transaction ID
        
        Args:
            transaction_id: Transaction identifier
        
        Returns:
            Prediction dictionary or None if not found
        """
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT * FROM predictions 
                WHERE transaction_id = ?
            """, (transaction_id,))
            
            row = cursor.fetchone()
            conn.close()
            
            if row:
                result = dict(row)
                # Parse JSON features
                if result.get('features_json'):
                    result['features'] = json.loads(result['features_json'])
                return result
            return None
            
        except Exception as e:
            logger.error(f"Error retrieving prediction: {e}")
            return None
    
    def get_recent_predictions(
        self,
        limit: int = 100,
        card_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get recent predictions
        
        Args:
            limit: Maximum number of predictions to return
            card_id: Optional filter by card_id
        
        Returns:
            List of prediction dictionaries
        """
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            if card_id:
                cursor.execute("""
                    SELECT * FROM predictions 
                    WHERE card_id = ?
                    ORDER BY created_at DESC 
                    LIMIT ?
                """, (card_id, limit))
            else:
                cursor.execute("""
                    SELECT * FROM predictions 
                    ORDER BY created_at DESC 
                    LIMIT ?
                """, (limit,))
            
            rows = cursor.fetchall()
            conn.close()
            
            results = []
            for row in rows:
                result = dict(row)
                if result.get('features_json'):
                    result['features'] = json.loads(result['features_json'])
                results.append(result)
            
            return results
            
        except Exception as e:
            logger.error(f"Error retrieving recent predictions: {e}")
            return []
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Get prediction statistics
        
        Returns:
            Dictionary with statistics
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Total predictions
            cursor.execute("SELECT COUNT(*) FROM predictions")
            total = cursor.fetchone()[0]
            
            # Predictions by risk level
            cursor.execute("""
                SELECT risk_level, COUNT(*) 
                FROM predictions 
                GROUP BY risk_level
            """)
            risk_counts = dict(cursor.fetchall())
            
            # Predictions by decision
            cursor.execute("""
                SELECT decision, COUNT(*) 
                FROM predictions 
                GROUP BY decision
            """)
            decision_counts = dict(cursor.fetchall())
            
            # Average fraud probability
            cursor.execute("""
                SELECT AVG(fraud_probability) 
                FROM predictions
            """)
            avg_fraud_prob = cursor.fetchone()[0] or 0.0
            
            conn.close()
            
            return {
                'total_predictions': total,
                'risk_level_distribution': risk_counts,
                'decision_distribution': decision_counts,
                'avg_fraud_probability': avg_fraud_prob
            }
            
        except Exception as e:
            logger.error(f"Error getting statistics: {e}")
            return {}
    
    def close(self):
        """Close database connection (no-op for SQLite, but kept for consistency)"""
        logger.info("Prediction store closed")
