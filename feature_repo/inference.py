"""
Feast Inference Script
Demonstrates how to fetch online features for real-time fraud detection.
"""

import pandas as pd
from datetime import datetime
from feast import FeatureStore
import sys
from pathlib import Path


class FeastFeatureRetriever:
    """Wrapper class for Feast feature retrieval"""
    
    def __init__(self, feature_repo_path=None):
        """
        Initialize Feast feature store.
        
        Args:
            feature_repo_path: Path to feature_repo directory
        """
        if feature_repo_path is None:
            feature_repo_path = Path(__file__).parent
        
        print(f"🔧 Initializing Feast FeatureStore from: {feature_repo_path}")
        self.store = FeatureStore(repo_path=str(feature_repo_path))
        print("✅ FeatureStore initialized successfully!")
    
    def get_online_features(self, card_numbers, request_data=None):
        """
        Retrieve online features for fraud detection.
        
        Args:
            card_numbers: List of credit card numbers
            request_data: Dictionary of real-time request features (optional)
        
        Returns:
            DataFrame with features for each card
        """
        # Create entity dataframe
        entity_df = pd.DataFrame({
            "cc_num": card_numbers
        })
        
        # Define features to retrieve
        features = [
            "transaction_features:amt",
            "transaction_features:category",
            "transaction_features:zip",
            "transaction_features:lat",
            "transaction_features:long",
            "transaction_features:city_pop",
            "transaction_features:merchant",
            "transaction_features:merch_lat",
            "transaction_features:merch_long",
            "transaction_features:gender",
            "transaction_features:age",
            "transaction_features:avg_amt_7d",
            "transaction_features:tx_count_24h",
            "transaction_features:unique_merchants_7d",
            "transaction_features:distance_from_home",
            "transaction_features:distance_from_last_tx",
            "transaction_features:hour_of_day",
            "transaction_features:day_of_week",
        ]
        
        # Add request features if provided
        if request_data:
            # Add on-demand features
            features.extend([
                "request_features:amt_ratio_to_avg",
                "request_features:is_high_value",
                "request_features:is_same_merchant",
                "request_features:velocity_score",
            ])
            
            # Merge request data with entity dataframe
            request_df = pd.DataFrame([request_data] * len(card_numbers))
            entity_df = pd.concat([entity_df, request_df], axis=1)
        
        # Retrieve features
        print(f"\n🔍 Retrieving features for {len(card_numbers)} card(s)...")
        feature_vector = self.store.get_online_features(
            features=features,
            entity_rows=entity_df.to_dict('records')
        )
        
        # Convert to DataFrame
        features_df = feature_vector.to_df()
        
        print(f"✅ Retrieved {len(features_df.columns)} features")
        
        return features_df
    
    def get_historical_features(self, entity_df, features):
        """
        Retrieve historical features for training.
        
        Args:
            entity_df: DataFrame with entity keys and timestamps
            features: List of feature references
        
        Returns:
            DataFrame with historical features
        """
        print(f"\n📊 Retrieving historical features...")
        
        training_df = self.store.get_historical_features(
            entity_df=entity_df,
            features=features
        ).to_df()
        
        print(f"✅ Retrieved {len(training_df)} rows with {len(training_df.columns)} columns")
        
        return training_df


def example_real_time_inference():
    """Example: Real-time fraud detection inference"""
    print("\n" + "="*80)
    print("EXAMPLE: REAL-TIME FRAUD DETECTION WITH FEAST")
    print("="*80 + "\n")
    
    # Initialize feature retriever
    retriever = FeastFeatureRetriever()
    
    # Simulate a real-time transaction
    print("💳 Simulating incoming transaction...")
    
    card_number = 2703186189652095  # Example card from dataset
    
    # Real-time request data (current transaction being processed)
    request_data = {
        "current_amt": 125.50,
        "current_category": "gas_transport",
        "current_zip": 28654,
        "current_merchant": "Shell Gas Station",
        "current_lat": 36.08,
        "current_long": -81.18,
        "time_since_last_tx": 3600,  # 1 hour since last transaction
    }
    
    print(f"  Card Number: {card_number}")
    print(f"  Amount: ${request_data['current_amt']}")
    print(f"  Category: {request_data['current_category']}")
    print(f"  Merchant: {request_data['current_merchant']}")
    
    # Retrieve features
    features_df = retriever.get_online_features(
        card_numbers=[card_number],
        request_data=request_data
    )
    
    # Display features
    print("\n📊 Retrieved Features:")
    print("-" * 80)
    
    # Transpose for better readability
    features_display = features_df.T
    features_display.columns = ['Value']
    print(features_display)
    
    # ========================================================================
    # FRAUD PREDICTION (Placeholder - integrate your model here)
    # ========================================================================
    
    print("\n" + "="*80)
    print("🤖 FRAUD PREDICTION")
    print("="*80 + "\n")
    
    # Extract features for model
    model_features = features_df.drop(columns=['cc_num'], errors='ignore')
    
    print("📝 Features ready for model input:")
    print(f"  Shape: {model_features.shape}")
    print(f"  Columns: {list(model_features.columns)}")
    
    # TODO: Load your trained model and make prediction
    # Example:
    # import joblib
    # model = joblib.load('path/to/your/model.pkl')
    # prediction = model.predict(model_features)
    # fraud_probability = model.predict_proba(model_features)[:, 1]
    
    # Simulated prediction
    fraud_probability = 0.15  # Placeholder
    
    print(f"\n🎯 Fraud Probability: {fraud_probability:.2%}")
    
    if fraud_probability > 0.5:
        print("🚨 ALERT: High fraud risk detected!")
    else:
        print("✅ Transaction appears legitimate")
    
    return features_df


def example_batch_inference():
    """Example: Batch inference for multiple cards"""
    print("\n" + "="*80)
    print("EXAMPLE: BATCH INFERENCE FOR MULTIPLE CARDS")
    print("="*80 + "\n")
    
    # Initialize feature retriever
    retriever = FeastFeatureRetriever()
    
    # Multiple cards to check
    card_numbers = [
        2703186189652095,
        630423337322001,
        3534093764340240,
    ]
    
    print(f"💳 Checking {len(card_numbers)} cards...")
    
    # Retrieve features (without request data for historical features only)
    features_df = retriever.get_online_features(card_numbers=card_numbers)
    
    print("\n📊 Retrieved Features for All Cards:")
    print("-" * 80)
    print(features_df)
    
    return features_df


def example_model_integration():
    """Example: Complete integration with fraud detection model"""
    print("\n" + "="*80)
    print("EXAMPLE: COMPLETE MODEL INTEGRATION")
    print("="*80 + "\n")
    
    # Initialize feature retriever
    retriever = FeastFeatureRetriever()
    
    # Simulate incoming transaction stream
    transactions = [
        {
            "cc_num": 2703186189652095,
            "current_amt": 1250.00,
            "current_category": "shopping_net",
            "current_zip": 28654,
            "current_merchant": "Amazon",
            "current_lat": 36.08,
            "current_long": -81.18,
            "time_since_last_tx": 300,
        },
        {
            "cc_num": 630423337322001,
            "current_amt": 45.00,
            "current_category": "grocery_pos",
            "current_zip": 10001,
            "current_merchant": "Walmart",
            "current_lat": 40.75,
            "current_long": -73.99,
            "time_since_last_tx": 7200,
        },
    ]
    
    print(f"🔄 Processing {len(transactions)} transactions...\n")
    
    results = []
    
    for i, tx in enumerate(transactions, 1):
        print(f"Transaction {i}:")
        print(f"  Card: {tx['cc_num']}")
        print(f"  Amount: ${tx['current_amt']}")
        print(f"  Merchant: {tx['current_merchant']}")
        
        # Extract card number and request data
        card_num = tx.pop('cc_num')
        request_data = tx
        
        # Get features
        features_df = retriever.get_online_features(
            card_numbers=[card_num],
            request_data=request_data
        )
        
        # Prepare for model
        model_input = features_df.drop(columns=['cc_num'], errors='ignore')
        
        # Simulated prediction
        fraud_score = 0.75 if tx['current_amt'] > 1000 else 0.12
        
        print(f"  Fraud Score: {fraud_score:.2%}")
        print(f"  Decision: {'🚨 BLOCK' if fraud_score > 0.5 else '✅ APPROVE'}")
        print()
        
        results.append({
            'card_num': card_num,
            'amount': request_data['current_amt'],
            'fraud_score': fraud_score,
            'decision': 'BLOCK' if fraud_score > 0.5 else 'APPROVE'
        })
    
    # Summary
    print("="*80)
    print("📊 BATCH PROCESSING SUMMARY")
    print("="*80)
    results_df = pd.DataFrame(results)
    print(results_df)
    
    return results_df


def main():
    """Main function - run examples"""
    print("\n" + "="*80)
    print("FEAST INFERENCE EXAMPLES - CREDIT CARD FRAUD DETECTION")
    print("="*80)
    
    try:
        # Example 1: Real-time inference with request features
        example_real_time_inference()
        
        # Example 2: Batch inference
        # example_batch_inference()
        
        # Example 3: Complete model integration
        # example_model_integration()
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        print("\n💡 Make sure you have:")
        print("  1. Run 'feast apply' in the feature_repo directory")
        print("  2. Materialized features to Redis with 'feast materialize'")
        print("  3. Redis is running on localhost:6379")
        sys.exit(1)
    
    print("\n" + "="*80)
    print("✅ INFERENCE EXAMPLES COMPLETE")
    print("="*80 + "\n")


if __name__ == "__main__":
    main()
