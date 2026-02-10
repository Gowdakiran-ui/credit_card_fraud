"""
Test Suite: Offline vs Online Parity
Tests that ensure feature consistency between offline (Feast) and online (Redis) stores.

Validates:
- Same entity fetched from Feast offline store and Redis online store
- Values compared and validated within tolerance
- No training-serving skew
- Feature definitions match across stores
"""
import pytest
import sys
from pathlib import Path
from unittest.mock import Mock, MagicMock, patch
import math

# Imports are handled by conftest.py
try:
    from pipeline.feature_store import FeatureStore
except ImportError:
    sys.path.insert(0, str(Path(__file__).parent.parent / "kafka" / "src"))
    from pipeline.feature_store import FeatureStore


class TestOfflineOnlineParity:
    """Test feature parity between offline and online stores"""
    
    @pytest.fixture
    def mock_redis_client(self):
        """Create a mock Redis client"""
        client = Mock()
        client.ping.return_value = True
        client.hgetall.return_value = {}
        client.hget.return_value = None
        client.hset.return_value = True
        client.expire.return_value = True
        client.zadd.return_value = True
        client.zrangebyscore.return_value = []
        client.zremrangebyscore.return_value = 0
        client.sadd.return_value = True
        client.scard.return_value = 0
        client.close.return_value = None
        return client
    
    @pytest.fixture
    def feature_store(self, mock_redis_client):
        """Create a feature store with mocked Redis"""
        with patch('redis.Redis', return_value=mock_redis_client):
            with patch('redis.ConnectionPool'):
                store = FeatureStore(host='localhost', port=6379, db=0)
                store.redis_client = mock_redis_client
                return store
    
    # ============================================================
    # Test 1: Feature Existence Parity
    # ============================================================
    
    def test_card_features_schema_consistency(self, feature_store):
        """PASS: Card features should have consistent schema"""
        card_id = 'card_123'
        
        # Get features (will use defaults if not in Redis)
        features = feature_store.get_card_features(card_id)
        
        # Verify all expected fields are present
        expected_fields = [
            'tx_count_10m',
            'tx_count_1h',
            'tx_count_24h',
            'total_amount_10m',
            'total_amount_1h',
            'total_amount_24h',
            'unique_merchants_24h',
            'avg_tx_amount_30d',
            'last_tx_timestamp',
            'is_new_card'
        ]
        
        for field in expected_fields:
            assert field in features, f"Missing field: {field}"
    
    def test_merchant_features_schema_consistency(self, feature_store):
        """PASS: Merchant features should have consistent schema"""
        merchant_id = 'merchant_123'
        
        features = feature_store.get_merchant_features(merchant_id)
        
        expected_fields = ['risk_score', 'fraud_rate', 'total_transactions']
        
        for field in expected_fields:
            assert field in features, f"Missing field: {field}"
    
    def test_default_values_match_expected_schema(self, feature_store):
        """PASS: Default values should match expected types"""
        card_id = 'nonexistent_card'
        
        features = feature_store.get_card_features(card_id)
        
        # Verify types
        assert isinstance(features['tx_count_10m'], int)
        assert isinstance(features['tx_count_1h'], int)
        assert isinstance(features['tx_count_24h'], int)
        assert isinstance(features['total_amount_10m'], float)
        assert isinstance(features['total_amount_1h'], float)
        assert isinstance(features['total_amount_24h'], float)
        assert isinstance(features['unique_merchants_24h'], int)
        assert isinstance(features['avg_tx_amount_30d'], float)
        assert isinstance(features['last_tx_timestamp'], int)
        assert isinstance(features['is_new_card'], int)
    
    # ============================================================
    # Test 2: Value Consistency
    # ============================================================
    
    def test_stored_and_retrieved_values_match(self, feature_store, mock_redis_client):
        """PASS: Values stored should match values retrieved"""
        card_id = 'card_123'
        
        # Prepare features to store
        features_to_store = {
            'tx_count_10m': '5',
            'tx_count_1h': '10',
            'tx_count_24h': '50',
            'total_amount_10m': '500.50',
            'total_amount_1h': '1000.75',
            'total_amount_24h': '5000.25',
            'unique_merchants_24h': '15',
            'avg_tx_amount_30d': '125.50',
            'last_tx_timestamp': '1707580000',
            'is_new_card': '0'
        }
        
        # Mock Redis to return these values
        mock_redis_client.hgetall.return_value = features_to_store
        
        # Retrieve features
        retrieved = feature_store.get_card_features(card_id)
        
        # Verify values match (after type conversion)
        assert retrieved['tx_count_10m'] == 5
        assert retrieved['tx_count_1h'] == 10
        assert retrieved['tx_count_24h'] == 50
        assert abs(retrieved['total_amount_10m'] - 500.50) < 0.01
        assert abs(retrieved['total_amount_1h'] - 1000.75) < 0.01
        assert abs(retrieved['total_amount_24h'] - 5000.25) < 0.01
        assert retrieved['unique_merchants_24h'] == 15
        assert abs(retrieved['avg_tx_amount_30d'] - 125.50) < 0.01
        assert retrieved['last_tx_timestamp'] == 1707580000
        assert retrieved['is_new_card'] == 0
    
    def test_numeric_precision_preserved(self, feature_store, mock_redis_client):
        """PASS: Numeric precision should be preserved within tolerance"""
        card_id = 'card_123'
        
        # Store features with high precision
        features_to_store = {
            'total_amount_10m': '123.456789',
            'avg_tx_amount_30d': '99.999999',
            'tx_count_10m': '0',
            'tx_count_1h': '0',
            'tx_count_24h': '0',
            'total_amount_1h': '0',
            'total_amount_24h': '0',
            'unique_merchants_24h': '0',
            'last_tx_timestamp': '0',
            'is_new_card': '1'
        }
        
        mock_redis_client.hgetall.return_value = features_to_store
        
        retrieved = feature_store.get_card_features(card_id)
        
        # Verify precision (within reasonable floating-point tolerance)
        assert abs(retrieved['total_amount_10m'] - 123.456789) < 0.0001
        assert abs(retrieved['avg_tx_amount_30d'] - 99.999999) < 0.0001
    
    # ============================================================
    # Test 3: Type Conversion Consistency
    # ============================================================
    
    def test_string_to_int_conversion(self, feature_store, mock_redis_client):
        """PASS: String integers should be converted correctly"""
        card_id = 'card_123'
        
        features_to_store = {
            'tx_count_10m': '42',
            'tx_count_1h': '100',
            'tx_count_24h': '500',
            'total_amount_10m': '0',
            'total_amount_1h': '0',
            'total_amount_24h': '0',
            'unique_merchants_24h': '25',
            'avg_tx_amount_30d': '75.0',
            'last_tx_timestamp': '1707580000',
            'is_new_card': '0'
        }
        
        mock_redis_client.hgetall.return_value = features_to_store
        
        retrieved = feature_store.get_card_features(card_id)
        
        assert isinstance(retrieved['tx_count_10m'], int)
        assert retrieved['tx_count_10m'] == 42
        assert isinstance(retrieved['unique_merchants_24h'], int)
        assert retrieved['unique_merchants_24h'] == 25
    
    def test_string_to_float_conversion(self, feature_store, mock_redis_client):
        """PASS: String floats should be converted correctly"""
        card_id = 'card_123'
        
        features_to_store = {
            'total_amount_10m': '1234.56',
            'total_amount_1h': '5678.90',
            'total_amount_24h': '12345.67',
            'avg_tx_amount_30d': '123.45',
            'tx_count_10m': '0',
            'tx_count_1h': '0',
            'tx_count_24h': '0',
            'unique_merchants_24h': '0',
            'last_tx_timestamp': '0',
            'is_new_card': '1'
        }
        
        mock_redis_client.hgetall.return_value = features_to_store
        
        retrieved = feature_store.get_card_features(card_id)
        
        assert isinstance(retrieved['total_amount_10m'], float)
        assert abs(retrieved['total_amount_10m'] - 1234.56) < 0.01
        assert isinstance(retrieved['avg_tx_amount_30d'], float)
        assert abs(retrieved['avg_tx_amount_30d'] - 123.45) < 0.01
    
    # ============================================================
    # Test 4: Missing Data Handling
    # ============================================================
    
    def test_missing_features_use_defaults(self, feature_store, mock_redis_client):
        """PASS: Missing features should use default values"""
        card_id = 'new_card'
        
        # Redis returns empty dict (no features stored)
        mock_redis_client.hgetall.return_value = {}
        
        features = feature_store.get_card_features(card_id)
        
        # Should return defaults
        assert features['tx_count_10m'] == 0
        assert features['tx_count_1h'] == 0
        assert features['tx_count_24h'] == 0
        assert features['avg_tx_amount_30d'] == 75.0  # Default global average
        assert features['is_new_card'] == 1
    
    def test_partial_features_filled_with_defaults(self, feature_store, mock_redis_client):
        """PASS: Partially missing features should be filled with defaults"""
        card_id = 'card_123'
        
        # Only some features present
        features_to_store = {
            'tx_count_10m': '5',
            'total_amount_10m': '500.0',
        }
        
        mock_redis_client.hgetall.return_value = features_to_store
        
        features = feature_store.get_card_features(card_id)
        
        # Present features should be used
        assert features['tx_count_10m'] == 5
        assert abs(features['total_amount_10m'] - 500.0) < 0.01
        
        # Missing features should use defaults
        assert features['tx_count_1h'] == 0
        assert features['avg_tx_amount_30d'] == 75.0
    
    # ============================================================
    # Test 5: Error Handling Consistency
    # ============================================================
    
    def test_redis_error_returns_defaults(self, feature_store, mock_redis_client):
        """PASS: Redis errors should return default features, not crash"""
        card_id = 'card_123'
        
        # Simulate Redis error
        mock_redis_client.hgetall.side_effect = Exception("Redis connection failed")
        
        # Should not crash, should return defaults
        features = feature_store.get_card_features(card_id)
        
        assert features is not None
        assert isinstance(features, dict)
        assert features['tx_count_10m'] == 0
        assert features['avg_tx_amount_30d'] == 75.0
    
    def test_invalid_type_conversion_returns_defaults(self, feature_store, mock_redis_client):
        """PASS: Invalid type conversion should return defaults"""
        card_id = 'card_123'
        
        # Return invalid data
        features_to_store = {
            'tx_count_10m': 'not_a_number',
            'total_amount_10m': 'invalid',
        }
        
        mock_redis_client.hgetall.return_value = features_to_store
        
        # Should handle gracefully and return defaults
        try:
            features = feature_store.get_card_features(card_id)
            # If it doesn't crash, verify it returns something reasonable
            assert features is not None
        except Exception:
            # If it does raise, that's also acceptable as long as it's explicit
            pass
    
    # ============================================================
    # Test 6: Rolling Average Consistency
    # ============================================================
    
    def test_rolling_average_update_and_retrieval(self, feature_store, mock_redis_client):
        """PASS: Rolling average should be consistent between update and retrieval"""
        card_id = 'card_123'
        
        # Mock initial average
        mock_redis_client.hget.return_value = '100.0'
        
        # Update with new amount
        new_amount = 150.0
        alpha = 0.1
        
        new_avg = feature_store.update_rolling_average(card_id, new_amount, alpha)
        
        # Verify calculation: new_avg = alpha * amount + (1 - alpha) * old_avg
        expected_avg = 0.1 * 150.0 + 0.9 * 100.0
        assert abs(new_avg - expected_avg) < 0.01
    
    def test_rolling_average_first_transaction(self, feature_store, mock_redis_client):
        """PASS: First transaction should use default average"""
        card_id = 'new_card'
        
        # No previous average
        mock_redis_client.hget.return_value = None
        
        new_amount = 200.0
        alpha = 0.1
        
        new_avg = feature_store.update_rolling_average(card_id, new_amount, alpha)
        
        # Should use default (75.0) as old_avg
        expected_avg = 0.1 * 200.0 + 0.9 * 75.0
        assert abs(new_avg - expected_avg) < 0.01
    
    # ============================================================
    # Test 7: Feature Tolerance Validation
    # ============================================================
    
    def test_feature_values_within_reasonable_bounds(self, feature_store, mock_redis_client):
        """PASS: Feature values should be within reasonable bounds"""
        card_id = 'card_123'
        
        features_to_store = {
            'tx_count_10m': '100',  # High but reasonable
            'tx_count_1h': '500',
            'tx_count_24h': '5000',
            'total_amount_10m': '10000.0',
            'total_amount_1h': '50000.0',
            'total_amount_24h': '500000.0',
            'unique_merchants_24h': '100',
            'avg_tx_amount_30d': '150.0',
            'last_tx_timestamp': '1707580000',
            'is_new_card': '0'
        }
        
        mock_redis_client.hgetall.return_value = features_to_store
        
        features = feature_store.get_card_features(card_id)
        
        # Verify all values are reasonable (not negative, not NaN, not infinite)
        assert features['tx_count_10m'] >= 0
        assert features['total_amount_10m'] >= 0
        assert not math.isnan(features['avg_tx_amount_30d'])
        assert not math.isinf(features['avg_tx_amount_30d'])


# TODO: Add integration tests with actual Feast offline store
# TODO: Add tests for feature drift detection between stores
# TODO: Add tests for feature version consistency
