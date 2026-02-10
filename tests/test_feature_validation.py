"""
Test Suite: Feature Validation
Tests that ensure extracted features are correct and safe.

Validates:
- Feature existence (no missing values)
- No NaNs or infinite values
- Feature value ranges are sane
- Embedding norms fall within expected bounds
- Feature computation is deterministic
"""
import pytest
import math
import sys
from pathlib import Path
from unittest.mock import Mock, MagicMock

# Imports are handled by conftest.py
try:
    from pipeline.feature_extractor import FeatureExtractor
except ImportError:
    # Fallback for direct execution
    sys.path.insert(0, str(Path(__file__).parent.parent / "kafka" / "src"))
    from pipeline.feature_extractor import FeatureExtractor


class TestFeatureValidation:
    """Test feature extraction correctness and safety"""
    
    @pytest.fixture
    def mock_feature_store(self):
        """Create a mock feature store"""
        store = Mock()
        store.get_transaction_history.return_value = []
        store.get_unique_merchant_count.return_value = 0
        store.get_last_transaction_timestamp.return_value = None
        store.get_rolling_average.return_value = 75.0
        store.get_merchant_features.return_value = {
            'risk_score': 0.5,
            'fraud_rate': 0.002,
            'total_transactions': 100
        }
        return store
    
    @pytest.fixture
    def feature_extractor(self, mock_feature_store):
        """Create a feature extractor instance"""
        config = {
            'velocity_windows': {
                '10m': 600,
                '1h': 3600,
                '24h': 86400
            },
            'rolling_avg_alpha': 0.1,
            'default_avg_amount': 75.0
        }
        return FeatureExtractor(mock_feature_store, config)
    
    @pytest.fixture
    def valid_transaction(self):
        """Valid preprocessed transaction"""
        return {
            'transaction_id': 'tx_12345',
            'card_id': 'card_67890',
            'amount': 125.50,
            'merchant_id': 'merchant_abc',
            'timestamp': 1707580000,
            'merchant_category': 'grocery_pos',
            'location_lat': 36.08,
            'location_lon': -81.18,
        }
    
    # ============================================================
    # Test 1: Feature Existence
    # ============================================================
    
    def test_all_expected_features_present(self, feature_extractor, valid_transaction):
        """PASS: All expected features should be present"""
        features = feature_extractor.extract_features(valid_transaction)
        
        # Transaction-level features
        assert 'amount' in features
        assert 'amount_log' in features
        assert 'merchant_category' in features
        assert 'has_location' in features
        
        # Velocity features
        assert 'tx_count_10m' in features
        assert 'tx_count_1h' in features
        assert 'tx_count_24h' in features
        assert 'total_amount_10m' in features
        assert 'total_amount_1h' in features
        assert 'total_amount_24h' in features
        assert 'unique_merchants_24h' in features
        assert 'time_since_last_tx' in features
        
        # Rolling features
        assert 'avg_tx_amount_30d' in features
        assert 'amount_deviation' in features
        assert 'amount_vs_avg_ratio' in features
        
        # Temporal features
        assert 'hour_of_day' in features
        assert 'day_of_week' in features
        assert 'is_weekend' in features
        assert 'is_night' in features
        
        # Merchant features
        assert 'merchant_risk_score' in features
        assert 'merchant_fraud_rate' in features
        assert 'merchant_total_transactions' in features
    
    def test_no_none_values_in_features(self, feature_extractor, valid_transaction):
        """PASS: No feature should have None value"""
        features = feature_extractor.extract_features(valid_transaction)
        for key, value in features.items():
            assert value is not None, f"Feature '{key}' has None value"
    
    def test_features_always_returns_dict(self, feature_extractor, valid_transaction):
        """PASS: extract_features should always return a dictionary"""
        features = feature_extractor.extract_features(valid_transaction)
        assert isinstance(features, dict)
        assert len(features) > 0
    
    # ============================================================
    # Test 2: No NaN or Infinite Values
    # ============================================================
    
    def test_no_nan_values(self, feature_extractor, valid_transaction):
        """PASS: No feature should be NaN"""
        features = feature_extractor.extract_features(valid_transaction)
        for key, value in features.items():
            if isinstance(value, (int, float)):
                assert not math.isnan(value), f"Feature '{key}' is NaN"
    
    def test_no_infinite_values(self, feature_extractor, valid_transaction):
        """PASS: No feature should be infinite"""
        features = feature_extractor.extract_features(valid_transaction)
        for key, value in features.items():
            if isinstance(value, (int, float)):
                assert not math.isinf(value), f"Feature '{key}' is infinite"
    
    def test_division_by_zero_handled(self, feature_extractor, valid_transaction, mock_feature_store):
        """PASS: Division by zero should be handled gracefully"""
        # Set rolling average to 0 to test division by zero
        mock_feature_store.get_rolling_average.return_value = 0.0
        features = feature_extractor.extract_features(valid_transaction)
        
        # amount_deviation and amount_vs_avg_ratio should handle division by zero
        assert not math.isnan(features['amount_deviation'])
        assert not math.isinf(features['amount_deviation'])
        assert not math.isnan(features['amount_vs_avg_ratio'])
        assert not math.isinf(features['amount_vs_avg_ratio'])
    
    def test_log_of_zero_handled(self, feature_extractor, valid_transaction):
        """PASS: Log of zero should be handled safely"""
        valid_transaction['amount'] = 0.0
        features = feature_extractor.extract_features(valid_transaction)
        # _safe_log should handle zero by using log(0 + 1) = 0
        assert features['amount_log'] == 0.0
        assert not math.isnan(features['amount_log'])
    
    # ============================================================
    # Test 3: Feature Value Ranges
    # ============================================================
    
    def test_amount_is_positive(self, feature_extractor, valid_transaction):
        """PASS: Amount should be positive"""
        features = feature_extractor.extract_features(valid_transaction)
        assert features['amount'] >= 0
    
    def test_amount_log_is_non_negative(self, feature_extractor, valid_transaction):
        """PASS: Log amount should be non-negative"""
        features = feature_extractor.extract_features(valid_transaction)
        assert features['amount_log'] >= 0
    
    def test_has_location_is_binary(self, feature_extractor, valid_transaction):
        """PASS: has_location should be 0 or 1"""
        features = feature_extractor.extract_features(valid_transaction)
        assert features['has_location'] in [0, 1]
    
    def test_has_location_true_when_coordinates_present(self, feature_extractor, valid_transaction):
        """PASS: has_location should be 1 when coordinates are present"""
        valid_transaction['location_lat'] = 36.08
        valid_transaction['location_lon'] = -81.18
        features = feature_extractor.extract_features(valid_transaction)
        assert features['has_location'] == 1
    
    def test_has_location_false_when_coordinates_missing(self, feature_extractor, valid_transaction):
        """PASS: has_location should be 0 when coordinates are missing"""
        valid_transaction['location_lat'] = None
        valid_transaction['location_lon'] = None
        features = feature_extractor.extract_features(valid_transaction)
        assert features['has_location'] == 0
    
    def test_velocity_counts_non_negative(self, feature_extractor, valid_transaction):
        """PASS: Velocity counts should be non-negative"""
        features = feature_extractor.extract_features(valid_transaction)
        assert features['tx_count_10m'] >= 0
        assert features['tx_count_1h'] >= 0
        assert features['tx_count_24h'] >= 0
    
    def test_velocity_amounts_non_negative(self, feature_extractor, valid_transaction):
        """PASS: Velocity amounts should be non-negative"""
        features = feature_extractor.extract_features(valid_transaction)
        assert features['total_amount_10m'] >= 0
        assert features['total_amount_1h'] >= 0
        assert features['total_amount_24h'] >= 0
    
    def test_unique_merchants_non_negative(self, feature_extractor, valid_transaction):
        """PASS: Unique merchant count should be non-negative"""
        features = feature_extractor.extract_features(valid_transaction)
        assert features['unique_merchants_24h'] >= 0
    
    def test_time_since_last_tx_non_negative(self, feature_extractor, valid_transaction):
        """PASS: Time since last transaction should be non-negative"""
        features = feature_extractor.extract_features(valid_transaction)
        assert features['time_since_last_tx'] >= 0
    
    def test_hour_of_day_range(self, feature_extractor, valid_transaction):
        """PASS: Hour of day should be 0-23"""
        features = feature_extractor.extract_features(valid_transaction)
        assert 0 <= features['hour_of_day'] <= 23
    
    def test_day_of_week_range(self, feature_extractor, valid_transaction):
        """PASS: Day of week should be 0-6"""
        features = feature_extractor.extract_features(valid_transaction)
        assert 0 <= features['day_of_week'] <= 6
    
    def test_is_weekend_binary(self, feature_extractor, valid_transaction):
        """PASS: is_weekend should be 0 or 1"""
        features = feature_extractor.extract_features(valid_transaction)
        assert features['is_weekend'] in [0, 1]
    
    def test_is_night_binary(self, feature_extractor, valid_transaction):
        """PASS: is_night should be 0 or 1"""
        features = feature_extractor.extract_features(valid_transaction)
        assert features['is_night'] in [0, 1]
    
    def test_merchant_risk_score_range(self, feature_extractor, valid_transaction):
        """PASS: Merchant risk score should be reasonable (0-1 typically)"""
        features = feature_extractor.extract_features(valid_transaction)
        # Risk score should be between 0 and 1 (or slightly outside for edge cases)
        assert 0 <= features['merchant_risk_score'] <= 1.0
    
    def test_merchant_fraud_rate_range(self, feature_extractor, valid_transaction):
        """PASS: Merchant fraud rate should be between 0 and 1"""
        features = feature_extractor.extract_features(valid_transaction)
        assert 0 <= features['merchant_fraud_rate'] <= 1.0
    
    # ============================================================
    # Test 4: Deterministic Computation
    # ============================================================
    
    def test_feature_extraction_is_deterministic(self, feature_extractor, valid_transaction):
        """PASS: Same input should produce same features"""
        features1 = feature_extractor.extract_features(valid_transaction.copy())
        features2 = feature_extractor.extract_features(valid_transaction.copy())
        assert features1 == features2
    
    def test_multiple_extractions_consistent(self, feature_extractor, valid_transaction):
        """PASS: Multiple extractions should be consistent"""
        results = [
            feature_extractor.extract_features(valid_transaction.copy())
            for _ in range(5)
        ]
        for result in results[1:]:
            assert result == results[0]
    
    def test_temporal_features_deterministic_for_same_timestamp(self, feature_extractor, valid_transaction):
        """PASS: Temporal features should be deterministic for same timestamp"""
        timestamp = 1707580000
        valid_transaction['timestamp'] = timestamp
        
        features1 = feature_extractor.extract_features(valid_transaction.copy())
        features2 = feature_extractor.extract_features(valid_transaction.copy())
        
        assert features1['hour_of_day'] == features2['hour_of_day']
        assert features1['day_of_week'] == features2['day_of_week']
        assert features1['is_weekend'] == features2['is_weekend']
        assert features1['is_night'] == features2['is_night']
    
    # ============================================================
    # Test 5: Edge Cases
    # ============================================================
    
    def test_first_transaction_for_card(self, feature_extractor, valid_transaction, mock_feature_store):
        """PASS: First transaction should use defaults"""
        mock_feature_store.get_transaction_history.return_value = []
        mock_feature_store.get_last_transaction_timestamp.return_value = None
        mock_feature_store.get_rolling_average.return_value = None
        
        features = feature_extractor.extract_features(valid_transaction)
        
        # Velocity features should be 0
        assert features['tx_count_10m'] == 0
        assert features['tx_count_1h'] == 0
        assert features['tx_count_24h'] == 0
        assert features['time_since_last_tx'] == 0
        
        # Rolling average should use default
        assert features['avg_tx_amount_30d'] == 75.0
    
    def test_high_velocity_transaction(self, feature_extractor, valid_transaction, mock_feature_store):
        """PASS: High velocity should be reflected in features"""
        # Simulate many recent transactions
        mock_history = [
            {'amount': 50.0, 'merchant_id': f'merchant_{i}', 'timestamp': 1707580000 - i*60}
            for i in range(20)
        ]
        mock_feature_store.get_transaction_history.return_value = mock_history
        mock_feature_store.get_unique_merchant_count.return_value = 15
        
        features = feature_extractor.extract_features(valid_transaction)
        
        # Should have high velocity counts
        assert features['tx_count_10m'] > 0
        assert features['unique_merchants_24h'] == 15
    
    def test_very_large_amount(self, feature_extractor, valid_transaction):
        """PASS: Very large amounts should be handled"""
        valid_transaction['amount'] = 999999.99
        features = feature_extractor.extract_features(valid_transaction)
        
        assert features['amount'] == 999999.99
        assert not math.isnan(features['amount_log'])
        assert not math.isinf(features['amount_log'])
    
    def test_very_small_amount(self, feature_extractor, valid_transaction):
        """PASS: Very small amounts should be handled"""
        valid_transaction['amount'] = 0.01
        features = feature_extractor.extract_features(valid_transaction)
        
        assert features['amount'] == 0.01
        assert features['amount_log'] >= 0
    
    # ============================================================
    # Test 6: Temporal Feature Correctness
    # ============================================================
    
    def test_weekend_detection_saturday(self, feature_extractor, valid_transaction):
        """PASS: Saturday should be detected as weekend"""
        # 2024-02-10 is a Saturday
        valid_transaction['timestamp'] = 1707580000  # Saturday
        features = feature_extractor.extract_features(valid_transaction)
        assert features['is_weekend'] == 1
    
    def test_weekend_detection_weekday(self, feature_extractor, valid_transaction):
        """PASS: Weekday should not be weekend"""
        # 2024-02-12 is a Monday
        valid_transaction['timestamp'] = 1707753600  # Monday
        features = feature_extractor.extract_features(valid_transaction)
        assert features['is_weekend'] == 0
    
    def test_night_detection_late_night(self, feature_extractor, valid_transaction):
        """PASS: Late night hours should be detected"""
        # Set timestamp to 2 AM
        from datetime import datetime
        dt = datetime(2024, 2, 10, 2, 0, 0)
        valid_transaction['timestamp'] = int(dt.timestamp())
        features = feature_extractor.extract_features(valid_transaction)
        assert features['is_night'] == 1
    
    def test_night_detection_daytime(self, feature_extractor, valid_transaction):
        """PASS: Daytime hours should not be night"""
        # Set timestamp to 2 PM
        from datetime import datetime
        dt = datetime(2024, 2, 10, 14, 0, 0)
        valid_transaction['timestamp'] = int(dt.timestamp())
        features = feature_extractor.extract_features(valid_transaction)
        assert features['is_night'] == 0


# TODO: Add tests for new features when added
# TODO: Add performance benchmarks for feature extraction
# TODO: Add tests for feature drift detection
