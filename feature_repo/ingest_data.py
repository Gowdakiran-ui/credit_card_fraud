"""
Feast Data Ingestion Script
Loads historical transaction data, preprocesses features, and materializes to Redis.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import subprocess
import sys
import os
from pathlib import Path


def calculate_age(dob_str, trans_date_str):
    """Calculate age at time of transaction"""
    try:
        dob = pd.to_datetime(dob_str)
        trans_date = pd.to_datetime(trans_date_str)
        age = (trans_date - dob).days // 365
        return age
    except:
        return 30  # Default age if calculation fails


def haversine_distance(lat1, lon1, lat2, lon2):
    """Calculate distance between two points in km"""
    R = 6371  # Earth's radius in km
    
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    
    a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
    c = 2 * np.arcsin(np.sqrt(a))
    
    return R * c


def preprocess_data(csv_path, output_path, sample_size=None):
    """
    Load and preprocess transaction data for Feast.
    
    Args:
        csv_path: Path to fraudTrain.csv
        output_path: Path to save processed parquet file
        sample_size: Number of rows to sample (None for all data)
    """
    print(f"\n{'='*80}")
    print("FEAST DATA PREPROCESSING")
    print(f"{'='*80}\n")
    
    print(f"📂 Loading data from: {csv_path}")
    df = pd.read_csv(csv_path)
    
    if sample_size:
        print(f"🎲 Sampling {sample_size} rows...")
        df = df.sample(n=min(sample_size, len(df)), random_state=42)
    
    print(f"✅ Loaded {len(df)} transactions")
    
    # ========================================================================
    # FEATURE ENGINEERING
    # ========================================================================
    
    print("\n🔧 Engineering features...")
    
    # 1. Timestamp conversion
    df['event_timestamp'] = pd.to_datetime(df['trans_date_trans_time'])
    
    # 2. Calculate age
    print("  - Calculating cardholder age...")
    df['age'] = df.apply(lambda row: calculate_age(row['dob'], row['trans_date_trans_time']), axis=1)
    
    # 3. Time features
    print("  - Extracting time features...")
    df['hour_of_day'] = df['event_timestamp'].dt.hour
    df['day_of_week'] = df['event_timestamp'].dt.dayofweek
    
    # 4. Sort by card and timestamp for rolling calculations
    print("  - Sorting by card and timestamp...")
    df = df.sort_values(['cc_num', 'event_timestamp'])
    
    # 5. Calculate historical aggregates (7-day rolling)
    print("  - Computing 7-day rolling averages...")
    df['avg_amt_7d'] = df.groupby('cc_num')['amt'].transform(
        lambda x: x.rolling(window=7, min_periods=1).mean()
    )
    
    # 6. Transaction count in last 24 hours
    print("  - Computing 24-hour transaction counts...")
    df['tx_count_24h'] = df.groupby('cc_num').cumcount() + 1  # Simplified version
    
    # 7. Unique merchants (simplified - cumulative unique count per card)
    print("  - Computing unique merchant counts...")
    df['unique_merchants_7d'] = df.groupby('cc_num')['merchant'].transform(
        lambda x: [len(set(x.iloc[:i+1])) for i in range(len(x))]
    )
    
    # 8. Distance from home (using first transaction as "home")
    print("  - Calculating distance from home...")
    home_locations = df.groupby('cc_num')[['lat', 'long']].first().reset_index()
    home_locations.columns = ['cc_num', 'home_lat', 'home_long']
    df = df.merge(home_locations, on='cc_num', how='left')
    
    df['distance_from_home'] = df.apply(
        lambda row: haversine_distance(row['lat'], row['long'], row['home_lat'], row['home_long']),
        axis=1
    )
    
    # 9. Distance from last transaction
    print("  - Calculating distance from last transaction...")
    df['prev_lat'] = df.groupby('cc_num')['lat'].shift(1)
    df['prev_long'] = df.groupby('cc_num')['long'].shift(1)
    
    df['distance_from_last_tx'] = df.apply(
        lambda row: haversine_distance(row['lat'], row['long'], row['prev_lat'], row['prev_long']) 
        if pd.notna(row['prev_lat']) else 0.0,
        axis=1
    )
    
    # ========================================================================
    # SELECT FEATURES FOR FEAST
    # ========================================================================
    
    feature_columns = [
        'cc_num',              # Entity key
        'event_timestamp',     # Timestamp
        'amt',                 # Transaction amount
        'category',            # Category
        'zip',                 # ZIP code
        'lat',                 # Latitude
        'long',                # Longitude
        'city_pop',            # City population
        'merchant',            # Merchant name
        'merch_lat',           # Merchant latitude
        'merch_long',          # Merchant longitude
        'gender',              # Gender
        'age',                 # Age
        'avg_amt_7d',          # 7-day average amount
        'tx_count_24h',        # 24-hour transaction count
        'unique_merchants_7d', # Unique merchants in 7 days
        'distance_from_home',  # Distance from home
        'distance_from_last_tx', # Distance from last transaction
        'hour_of_day',         # Hour of day
        'day_of_week',         # Day of week
    ]
    
    df_features = df[feature_columns].copy()
    
    # Handle missing values
    df_features = df_features.fillna({
        'avg_amt_7d': 0.0,
        'tx_count_24h': 0,
        'unique_merchants_7d': 0,
        'distance_from_home': 0.0,
        'distance_from_last_tx': 0.0,
    })
    
    # Convert types
    df_features['zip'] = df_features['zip'].astype('int64')
    df_features['city_pop'] = df_features['city_pop'].astype('int64')
    df_features['age'] = df_features['age'].astype('int64')
    df_features['tx_count_24h'] = df_features['tx_count_24h'].astype('int64')
    df_features['unique_merchants_7d'] = df_features['unique_merchants_7d'].astype('int64')
    df_features['hour_of_day'] = df_features['hour_of_day'].astype('int64')
    df_features['day_of_week'] = df_features['day_of_week'].astype('int64')
    
    # ========================================================================
    # SAVE TO PARQUET
    # ========================================================================
    
    print(f"\n💾 Saving features to: {output_path}")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df_features.to_parquet(output_path, index=False)
    
    print(f"✅ Saved {len(df_features)} rows with {len(feature_columns)} columns")
    
    # Print sample
    print("\n📊 Sample of processed features:")
    print(df_features.head(3))
    
    return df_features


def apply_feast_definitions(feature_repo_path):
    """Apply Feast feature definitions to registry"""
    print(f"\n{'='*80}")
    print("APPLYING FEAST DEFINITIONS")
    print(f"{'='*80}\n")
    
    print(f"📂 Feature repository: {feature_repo_path}")
    
    try:
        result = subprocess.run(
            ['feast', 'apply'],
            cwd=feature_repo_path,
            capture_output=True,
            text=True,
            check=True
        )
        print(result.stdout)
        print("✅ Feast definitions applied successfully!")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Error applying Feast definitions:")
        print(e.stderr)
        return False


def materialize_features(feature_repo_path, start_date=None, end_date=None):
    """Materialize features to Redis online store"""
    print(f"\n{'='*80}")
    print("MATERIALIZING FEATURES TO REDIS")
    print(f"{'='*80}\n")
    
    if start_date is None:
        start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
    if end_date is None:
        end_date = datetime.now().strftime('%Y-%m-%d')
    
    print(f"📅 Date range: {start_date} to {end_date}")
    
    try:
        result = subprocess.run(
            ['feast', 'materialize', start_date, end_date],
            cwd=feature_repo_path,
            capture_output=True,
            text=True,
            check=True
        )
        print(result.stdout)
        print("✅ Features materialized to Redis successfully!")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Error materializing features:")
        print(e.stderr)
        return False


def main():
    """Main ingestion pipeline"""
    print("\n" + "="*80)
    print("FEAST INGESTION PIPELINE - CREDIT CARD FRAUD DETECTION")
    print("="*80)
    
    # Paths
    project_root = Path(__file__).parent.parent
    csv_path = project_root / "fraudTrain.csv"
    output_path = project_root / "feature_repo" / "data" / "features.parquet"
    feature_repo_path = project_root / "feature_repo"
    
    # Step 1: Preprocess data
    print("\n📊 STEP 1: Data Preprocessing")
    df = preprocess_data(
        csv_path=str(csv_path),
        output_path=str(output_path),
        sample_size=50000  # Use 50k rows for faster testing, set to None for full dataset
    )
    
    # Step 2: Apply Feast definitions
    print("\n🔧 STEP 2: Apply Feast Definitions")
    if not apply_feast_definitions(str(feature_repo_path)):
        print("\n❌ Failed to apply Feast definitions. Exiting.")
        sys.exit(1)
    
    # Step 3: Materialize features
    print("\n🚀 STEP 3: Materialize Features to Redis")
    
    # Get date range from data
    start_date = df['event_timestamp'].min().strftime('%Y-%m-%d')
    end_date = df['event_timestamp'].max().strftime('%Y-%m-%d')
    
    if not materialize_features(str(feature_repo_path), start_date, end_date):
        print("\n❌ Failed to materialize features. Exiting.")
        sys.exit(1)
    
    print("\n" + "="*80)
    print("✅ INGESTION COMPLETE!")
    print("="*80)
    print("\n📝 Next steps:")
    print("  1. Verify features in Redis: redis-cli KEYS '*'")
    print("  2. Test feature retrieval with inference script")
    print("  3. Integrate with your fraud detection model\n")


if __name__ == "__main__":
    main()
