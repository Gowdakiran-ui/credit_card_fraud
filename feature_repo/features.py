"""
Feast Feature Definitions for Credit Card Fraud Detection
Defines entities, feature views for real-time fraud detection.
"""

from datetime import timedelta
from feast import Entity, FeatureView, Field, FileSource
from feast.types import Float32, Float64, Int64, String


# ============================================================================
# ENTITY DEFINITION
# ============================================================================

# Define the card entity - represents a unique credit card
card_entity = Entity(
    name="card_number",
    description="Credit card number - unique identifier for each card",
    join_keys=["cc_num"]
)


# ============================================================================
# DATA SOURCE DEFINITION
# ============================================================================

# Historical transaction data source
transaction_source = FileSource(
    name="transaction_data",
    path="data/features.parquet",  # We'll create this from fraudTrain.csv
    timestamp_field="event_timestamp",
)


# ============================================================================
# FEATURE VIEW - Historical Transaction Features
# ============================================================================

transaction_features = FeatureView(
    name="transaction_features",
    entities=[card_entity],
    ttl=timedelta(days=365),  # Features are valid for 1 year
    schema=[
        # Transaction amount
        Field(name="amt", dtype=Float64, description="Transaction amount"),
        
        # Category (we'll encode this as a string, you can preprocess to numeric)
        Field(name="category", dtype=String, description="Transaction category"),
        
        # Location
        Field(name="zip", dtype=Int64, description="ZIP code of transaction"),
        Field(name="lat", dtype=Float32, description="Latitude of transaction"),
        Field(name="long", dtype=Float32, description="Longitude of transaction"),
        Field(name="city_pop", dtype=Int64, description="City population"),
        
        # Merchant information
        Field(name="merchant", dtype=String, description="Merchant name"),
        Field(name="merch_lat", dtype=Float32, description="Merchant latitude"),
        Field(name="merch_long", dtype=Float32, description="Merchant longitude"),
        
        # Cardholder demographics
        Field(name="gender", dtype=String, description="Cardholder gender"),
        Field(name="age", dtype=Int64, description="Cardholder age at transaction"),
        
        # Historical aggregates (you can compute these during preprocessing)
        Field(name="avg_amt_7d", dtype=Float64, description="Average transaction amount in last 7 days"),
        Field(name="tx_count_24h", dtype=Int64, description="Transaction count in last 24 hours"),
        Field(name="unique_merchants_7d", dtype=Int64, description="Unique merchants in last 7 days"),
        
        # Distance features
        Field(name="distance_from_home", dtype=Float32, description="Distance from home address"),
        Field(name="distance_from_last_tx", dtype=Float32, description="Distance from last transaction"),
        
        # Time features
        Field(name="hour_of_day", dtype=Int64, description="Hour of day (0-23)"),
        Field(name="day_of_week", dtype=Int64, description="Day of week (0-6)"),
    ],
    source=transaction_source,
    online=True,
    tags={"team": "fraud_detection", "version": "v1"},
)
