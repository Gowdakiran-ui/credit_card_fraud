"""
Complete Fraud Detection Integration with Feast
Demonstrates end-to-end integration: Feast features -> Model -> Prediction
"""

import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
import sys

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from feast import FeatureStore


class FraudDetectionService:
    """
    Complete fraud detection service using Feast for feature retrieval.
    """
    
    def __init__(self, feature_repo_path=None, model_path=None):
        """
        Initialize the fraud detection service.
        
        Args:
            feature_repo_path: Path to Feast feature repository
            model_path: Path to trained model (optional)
        """
        if feature_repo_path is None:
            feature_repo_path = Path(__file__).parent
        
        print("🔧 Initializing Fraud Detection Service...")
        
        # Initialize Feast
        self.store = FeatureStore(repo_path=str(feature_repo_path))
        print("  ✅ Feast FeatureStore loaded")
        
        # Load model (if provided)
        self.model = None
        if model_path:
            try:
                import joblib
                self.model = joblib.load(model_path)
                print(f"  ✅ Model loaded from {model_path}")
            except Exception as e:
                print(f"  ⚠️  Could not load model: {e}")
                print("  ℹ️  Will use rule-based scoring instead")
        else:
            print("  ℹ️  No model provided, using rule-based scoring")
        
        print("✅ Service initialized successfully!\n")
    
    def get_features(self, card_number, transaction_data):
        """
        Retrieve features for a transaction.
        
        Args:
            card_number: Credit card number
            transaction_data: Dictionary with current transaction details
        
        Returns:
            DataFrame with all features
        """
        # Prepare entity dataframe
        entity_df = pd.DataFrame({
            "cc_num": [card_number],
            **transaction_data
        })
        
        # Define features to retrieve
        features = [
            # Historical features
            "transaction_features:amt",
            "transaction_features:category",
            "transaction_features:zip",
            "transaction_features:lat",
            "transaction_features:long",
            "transaction_features:city_pop",
            "transaction_features:merchant",
            "transaction_features:gender",
            "transaction_features:age",
            "transaction_features:avg_amt_7d",
            "transaction_features:tx_count_24h",
            "transaction_features:unique_merchants_7d",
            "transaction_features:distance_from_home",
            "transaction_features:distance_from_last_tx",
            "transaction_features:hour_of_day",
            "transaction_features:day_of_week",
            
            # On-demand features
            "request_features:amt_ratio_to_avg",
            "request_features:is_high_value",
            "request_features:is_same_merchant",
            "request_features:velocity_score",
        ]
        
        # Retrieve from Feast
        feature_vector = self.store.get_online_features(
            features=features,
            entity_rows=entity_df.to_dict('records')
        )
        
        return feature_vector.to_df()
    
    def rule_based_score(self, features_df):
        """
        Rule-based fraud scoring (fallback when no ML model is available).
        
        Args:
            features_df: DataFrame with features
        
        Returns:
            Fraud probability (0-1)
        """
        score = 0.0
        
        # Rule 1: High amount ratio (weight: 0.3)
        amt_ratio = features_df['amt_ratio_to_avg'].iloc[0]
        if amt_ratio > 5:
            score += 0.3
        elif amt_ratio > 3:
            score += 0.15
        
        # Rule 2: High velocity (weight: 0.25)
        velocity = features_df['velocity_score'].iloc[0]
        if velocity > 5:
            score += 0.25
        elif velocity > 3:
            score += 0.15
        
        # Rule 3: High value transaction (weight: 0.2)
        if features_df['is_high_value'].iloc[0] == 1:
            score += 0.2
        
        # Rule 4: Distance from home (weight: 0.15)
        distance = features_df['distance_from_home'].iloc[0]
        if distance > 1000:  # More than 1000 km
            score += 0.15
        elif distance > 500:
            score += 0.08
        
        # Rule 5: Unusual time (weight: 0.1)
        hour = features_df['hour_of_day'].iloc[0]
        if hour < 6 or hour > 23:  # Late night/early morning
            score += 0.1
        
        return min(score, 1.0)  # Cap at 1.0
    
    def predict(self, card_number, transaction_data):
        """
        Predict fraud probability for a transaction.
        
        Args:
            card_number: Credit card number
            transaction_data: Dictionary with current transaction details
        
        Returns:
            Dictionary with prediction results
        """
        # Get features
        features_df = self.get_features(card_number, transaction_data)
        
        # Make prediction
        if self.model:
            # Use ML model
            model_features = features_df.drop(columns=['cc_num'], errors='ignore')
            fraud_prob = self.model.predict_proba(model_features)[:, 1][0]
            method = "ML Model"
        else:
            # Use rule-based scoring
            fraud_prob = self.rule_based_score(features_df)
            method = "Rule-Based"
        
        # Determine decision
        threshold = 0.5
        is_fraud = fraud_prob > threshold
        
        return {
            'card_number': card_number,
            'fraud_probability': fraud_prob,
            'is_fraud': is_fraud,
            'decision': 'BLOCK' if is_fraud else 'APPROVE',
            'method': method,
            'features': features_df,
        }
    
    def process_transaction_stream(self, transactions):
        """
        Process a stream of transactions.
        
        Args:
            transactions: List of transaction dictionaries
        
        Returns:
            List of prediction results
        """
        results = []
        
        print(f"🔄 Processing {len(transactions)} transactions...\n")
        
        for i, tx in enumerate(transactions, 1):
            # Extract card number
            card_num = tx.pop('cc_num')
            
            # Predict
            result = self.predict(card_num, tx)
            
            # Display
            print(f"Transaction {i}:")
            print(f"  Card: {card_num}")
            print(f"  Amount: ${tx['current_amt']:.2f}")
            print(f"  Merchant: {tx['current_merchant']}")
            print(f"  Fraud Probability: {result['fraud_probability']:.2%}")
            print(f"  Decision: {result['decision']}")
            print(f"  Method: {result['method']}")
            print()
            
            results.append(result)
        
        return results


def demo_real_time_detection():
    """Demo: Real-time fraud detection"""
    print("="*80)
    print("DEMO: REAL-TIME FRAUD DETECTION WITH FEAST")
    print("="*80 + "\n")
    
    # Initialize service
    service = FraudDetectionService()
    
    # Simulate incoming transactions
    transactions = [
        {
            'cc_num': 2703186189652095,
            'current_amt': 1500.00,
            'current_category': 'shopping_net',
            'current_zip': 28654,
            'current_merchant': 'Amazon',
            'current_lat': 36.08,
            'current_long': -81.18,
            'time_since_last_tx': 300,  # 5 minutes
        },
        {
            'cc_num': 2703186189652095,
            'current_amt': 45.00,
            'current_category': 'grocery_pos',
            'current_zip': 28654,
            'current_merchant': 'Walmart',
            'current_lat': 36.08,
            'current_long': -81.18,
            'time_since_last_tx': 7200,  # 2 hours
        },
        {
            'cc_num': 2703186189652095,
            'current_amt': 2500.00,
            'current_category': 'shopping_net',
            'current_zip': 10001,  # Different location
            'current_merchant': 'Unknown Store',
            'current_lat': 40.75,
            'current_long': -73.99,
            'time_since_last_tx': 600,  # 10 minutes
        },
    ]
    
    # Process transactions
    results = service.process_transaction_stream(transactions)
    
    # Summary
    print("="*80)
    print("SUMMARY")
    print("="*80 + "\n")
    
    summary_df = pd.DataFrame([
        {
            'Card': r['card_number'],
            'Fraud Prob': f"{r['fraud_probability']:.2%}",
            'Decision': r['decision'],
        }
        for r in results
    ])
    
    print(summary_df.to_string(index=False))
    print()
    
    blocked = sum(1 for r in results if r['is_fraud'])
    print(f"📊 Blocked: {blocked}/{len(results)} transactions")
    print()


def demo_feature_analysis():
    """Demo: Analyze features for a specific card"""
    print("="*80)
    print("DEMO: FEATURE ANALYSIS")
    print("="*80 + "\n")
    
    service = FraudDetectionService()
    
    card_number = 2703186189652095
    transaction_data = {
        'current_amt': 125.50,
        'current_category': 'gas_transport',
        'current_zip': 28654,
        'current_merchant': 'Shell',
        'current_lat': 36.08,
        'current_long': -81.18,
        'time_since_last_tx': 3600,
    }
    
    print(f"Analyzing card: {card_number}\n")
    
    # Get features
    features_df = service.get_features(card_number, transaction_data)
    
    # Display key features
    print("📊 Key Features:")
    print("-" * 80)
    
    key_features = [
        'avg_amt_7d',
        'tx_count_24h',
        'unique_merchants_7d',
        'distance_from_home',
        'amt_ratio_to_avg',
        'velocity_score',
        'is_high_value',
    ]
    
    for feature in key_features:
        if feature in features_df.columns:
            value = features_df[feature].iloc[0]
            print(f"  {feature:25s}: {value}")
    
    print()


def main():
    """Main function"""
    try:
        # Demo 1: Real-time detection
        demo_real_time_detection()
        
        # Demo 2: Feature analysis
        # demo_feature_analysis()
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        print("\n💡 Make sure:")
        print("  1. Redis is running")
        print("  2. Features are materialized (run ingest_data.py)")
        print("  3. Feast definitions are applied (feast apply)")
        sys.exit(1)


if __name__ == "__main__":
    main()
