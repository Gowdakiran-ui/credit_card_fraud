"""
Test Suite: Input & Schema Validation
Tests that ensure data integrity at the pipeline entry point.

Validates:
- Required fields exist
- Correct data types
- Empty or malformed inputs handled safely
- Bad data is logged and skipped (never crashes)
"""
import pytest
from typing import Dict, Any
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "kafka" / "src"))

from pipeline.preprocessor import TransactionPreprocessor


class TestSchemaValidation:
    """Test schema validation for incoming transactions"""
    
    @pytest.fixture
    def preprocessor(self):
        """Create a preprocessor instance"""
        return TransactionPreprocessor()
    
    @pytest.fixture
    def valid_transaction(self):
        """Valid transaction for testing"""
        return {
            'transaction_id': 'tx_12345',
            'card_id': 'card_67890',
            'amount': 125.50,
            'merchant_id': 'merchant_abc',
            'timestamp': 1707580000,  # Valid Unix timestamp
            'merchant_category': 'grocery_pos',
            'location_lat': 36.08,
            'location_lon': -81.18,
        }
    
    # ============================================================
    # Test 1: Required Fields Validation
    # ============================================================
    
    def test_all_required_fields_present(self, preprocessor, valid_transaction):
        """PASS: Transaction with all required fields"""
        is_valid, error = preprocessor.validate_schema(valid_transaction)
        assert is_valid is True
        assert error == ""
    
    def test_missing_transaction_id(self, preprocessor, valid_transaction):
        """FAIL: Missing transaction_id should be caught"""
        del valid_transaction['transaction_id']
        is_valid, error = preprocessor.validate_schema(valid_transaction)
        assert is_valid is False
        assert "transaction_id" in error
    
    def test_missing_card_id(self, preprocessor, valid_transaction):
        """FAIL: Missing card_id should be caught"""
        del valid_transaction['card_id']
        is_valid, error = preprocessor.validate_schema(valid_transaction)
        assert is_valid is False
        assert "card_id" in error
    
    def test_missing_amount(self, preprocessor, valid_transaction):
        """FAIL: Missing amount should be caught"""
        del valid_transaction['amount']
        is_valid, error = preprocessor.validate_schema(valid_transaction)
        assert is_valid is False
        assert "amount" in error
    
    def test_missing_merchant_id(self, preprocessor, valid_transaction):
        """FAIL: Missing merchant_id should be caught"""
        del valid_transaction['merchant_id']
        is_valid, error = preprocessor.validate_schema(valid_transaction)
        assert is_valid is False
        assert "merchant_id" in error
    
    def test_missing_timestamp(self, preprocessor, valid_transaction):
        """FAIL: Missing timestamp should be caught"""
        del valid_transaction['timestamp']
        is_valid, error = preprocessor.validate_schema(valid_transaction)
        assert is_valid is False
        assert "timestamp" in error
    
    def test_multiple_missing_fields(self, preprocessor):
        """FAIL: Multiple missing fields should all be reported"""
        incomplete_tx = {
            'transaction_id': 'tx_123'
        }
        is_valid, error = preprocessor.validate_schema(incomplete_tx)
        assert is_valid is False
        assert "card_id" in error
        assert "amount" in error
        assert "merchant_id" in error
        assert "timestamp" in error
    
    def test_null_required_field(self, preprocessor, valid_transaction):
        """FAIL: Null values in required fields should be caught"""
        valid_transaction['amount'] = None
        is_valid, error = preprocessor.validate_schema(valid_transaction)
        assert is_valid is False
        assert "amount" in error
    
    # ============================================================
    # Test 2: Data Type Validation
    # ============================================================
    
    def test_correct_data_types(self, preprocessor, valid_transaction):
        """PASS: Correct data types should be accepted"""
        processed = preprocessor.preprocess(valid_transaction)
        assert isinstance(processed['transaction_id'], str)
        assert isinstance(processed['card_id'], str)
        assert isinstance(processed['amount'], float)
        assert isinstance(processed['merchant_id'], str)
        assert isinstance(processed['timestamp'], int)
    
    def test_amount_string_to_float_conversion(self, preprocessor, valid_transaction):
        """PASS: String amounts should be converted to float"""
        valid_transaction['amount'] = "125.50"
        processed = preprocessor.preprocess(valid_transaction)
        assert isinstance(processed['amount'], float)
        assert processed['amount'] == 125.50
    
    def test_amount_int_to_float_conversion(self, preprocessor, valid_transaction):
        """PASS: Integer amounts should be converted to float"""
        valid_transaction['amount'] = 125
        processed = preprocessor.preprocess(valid_transaction)
        assert isinstance(processed['amount'], float)
        assert processed['amount'] == 125.0
    
    def test_invalid_amount_type(self, preprocessor, valid_transaction):
        """FAIL: Invalid amount type should raise ValueError"""
        valid_transaction['amount'] = "not_a_number"
        with pytest.raises(ValueError, match="Type casting error"):
            preprocessor.preprocess(valid_transaction)
    
    def test_timestamp_float_to_int_conversion(self, preprocessor, valid_transaction):
        """PASS: Float timestamps should be converted to int"""
        valid_transaction['timestamp'] = 1707580000.5
        processed = preprocessor.preprocess(valid_transaction)
        assert isinstance(processed['timestamp'], int)
        assert processed['timestamp'] == 1707580000
    
    def test_timestamp_iso_string_parsing(self, preprocessor, valid_transaction):
        """PASS: ISO format timestamps should be parsed"""
        valid_transaction['timestamp'] = "2024-02-10T12:00:00Z"
        processed = preprocessor.preprocess(valid_transaction)
        assert isinstance(processed['timestamp'], int)
        assert processed['timestamp'] > 0
    
    def test_invalid_timestamp_format(self, preprocessor, valid_transaction):
        """FAIL: Invalid timestamp format should raise ValueError"""
        valid_transaction['timestamp'] = "invalid_timestamp"
        with pytest.raises(ValueError, match="Unable to parse timestamp"):
            preprocessor.preprocess(valid_transaction)
    
    def test_location_coordinates_type_conversion(self, preprocessor, valid_transaction):
        """PASS: String coordinates should be converted to float"""
        valid_transaction['location_lat'] = "36.08"
        valid_transaction['location_lon'] = "-81.18"
        processed = preprocessor.preprocess(valid_transaction)
        assert isinstance(processed['location_lat'], float)
        assert isinstance(processed['location_lon'], float)
    
    # ============================================================
    # Test 3: Empty or Malformed Input Handling
    # ============================================================
    
    def test_empty_dictionary(self, preprocessor):
        """FAIL: Empty dictionary should be rejected gracefully"""
        is_valid, error = preprocessor.validate_schema({})
        assert is_valid is False
        assert "Missing required fields" in error
    
    def test_non_dict_input(self, preprocessor):
        """FAIL: Non-dictionary input should be rejected"""
        is_valid, error = preprocessor.validate_schema("not a dict")
        assert is_valid is False
        assert "must be a dictionary" in error
    
    def test_none_input(self, preprocessor):
        """FAIL: None input should be rejected"""
        is_valid, error = preprocessor.validate_schema(None)
        assert is_valid is False
        assert "must be a dictionary" in error
    
    def test_list_input(self, preprocessor):
        """FAIL: List input should be rejected"""
        is_valid, error = preprocessor.validate_schema([1, 2, 3])
        assert is_valid is False
        assert "must be a dictionary" in error
    
    def test_empty_string_fields(self, preprocessor, valid_transaction):
        """PASS: Empty strings should be handled (converted to string type)"""
        valid_transaction['merchant_category'] = ""
        processed = preprocessor.preprocess(valid_transaction)
        assert processed['merchant_category'] == ""
    
    def test_negative_amount_conversion(self, preprocessor, valid_transaction):
        """PASS: Negative amounts should be converted to absolute value"""
        valid_transaction['amount'] = -125.50
        processed = preprocessor.preprocess(valid_transaction)
        assert processed['amount'] == 125.50
    
    def test_zero_amount(self, preprocessor, valid_transaction):
        """FAIL: Zero amount should be rejected"""
        valid_transaction['amount'] = 0
        with pytest.raises(ValueError, match="Amount must be positive"):
            preprocessor.preprocess(valid_transaction)
    
    def test_extremely_large_amount(self, preprocessor, valid_transaction):
        """PASS: Extremely large amounts should be clipped"""
        valid_transaction['amount'] = 999999999.99
        processed = preprocessor.preprocess(valid_transaction)
        # Should be clipped to the preprocessor's clip value
        assert processed['amount'] <= preprocessor.amount_clip_value
    
    # ============================================================
    # Test 4: Range Validation
    # ============================================================
    
    def test_timestamp_too_old(self, preprocessor, valid_transaction):
        """FAIL: Timestamp before 2000 should be rejected"""
        valid_transaction['timestamp'] = 946684799  # 1999-12-31
        with pytest.raises(ValueError, match="Timestamp out of range"):
            preprocessor.preprocess(valid_transaction)
    
    def test_timestamp_too_future(self, preprocessor, valid_transaction):
        """FAIL: Timestamp after 2100 should be rejected"""
        valid_transaction['timestamp'] = 4102444801  # 2100-01-02
        with pytest.raises(ValueError, match="Timestamp out of range"):
            preprocessor.preprocess(valid_transaction)
    
    def test_invalid_latitude_too_high(self, preprocessor, valid_transaction):
        """FAIL: Latitude > 90 should be rejected"""
        valid_transaction['location_lat'] = 91.0
        with pytest.raises(ValueError, match="Invalid latitude"):
            preprocessor.preprocess(valid_transaction)
    
    def test_invalid_latitude_too_low(self, preprocessor, valid_transaction):
        """FAIL: Latitude < -90 should be rejected"""
        valid_transaction['location_lat'] = -91.0
        with pytest.raises(ValueError, match="Invalid latitude"):
            preprocessor.preprocess(valid_transaction)
    
    def test_invalid_longitude_too_high(self, preprocessor, valid_transaction):
        """FAIL: Longitude > 180 should be rejected"""
        valid_transaction['location_lon'] = 181.0
        with pytest.raises(ValueError, match="Invalid longitude"):
            preprocessor.preprocess(valid_transaction)
    
    def test_invalid_longitude_too_low(self, preprocessor, valid_transaction):
        """FAIL: Longitude < -180 should be rejected"""
        valid_transaction['location_lon'] = -181.0
        with pytest.raises(ValueError, match="Invalid longitude"):
            preprocessor.preprocess(valid_transaction)
    
    def test_valid_coordinate_boundaries(self, preprocessor, valid_transaction):
        """PASS: Coordinates at valid boundaries should be accepted"""
        valid_transaction['location_lat'] = 90.0
        valid_transaction['location_lon'] = 180.0
        processed = preprocessor.preprocess(valid_transaction)
        assert processed['location_lat'] == 90.0
        assert processed['location_lon'] == 180.0
    
    # ============================================================
    # Test 5: Optional Fields Handling
    # ============================================================
    
    def test_missing_optional_fields_filled_with_defaults(self, preprocessor):
        """PASS: Missing optional fields should be filled with defaults"""
        minimal_tx = {
            'transaction_id': 'tx_123',
            'card_id': 'card_456',
            'amount': 100.0,
            'merchant_id': 'merchant_789',
            'timestamp': 1707580000
        }
        processed = preprocessor.preprocess(minimal_tx)
        assert processed['merchant_category'] == 'UNKNOWN'
        assert processed['location_lat'] is None
        assert processed['location_lon'] is None
    
    def test_optional_fields_preserved_when_present(self, preprocessor, valid_transaction):
        """PASS: Optional fields should be preserved when provided"""
        processed = preprocessor.preprocess(valid_transaction)
        assert processed['merchant_category'] == 'grocery_pos'
        assert processed['location_lat'] == 36.08
        assert processed['location_lon'] == -81.18
    
    # ============================================================
    # Test 6: No Silent Failures
    # ============================================================
    
    def test_preprocess_raises_on_invalid_schema(self, preprocessor):
        """FAIL: preprocess() should raise ValueError, not return None"""
        invalid_tx = {'transaction_id': 'tx_123'}  # Missing required fields
        with pytest.raises(ValueError):
            preprocessor.preprocess(invalid_tx)
    
    def test_preprocess_never_returns_none(self, preprocessor, valid_transaction):
        """PASS: preprocess() should always return a dict, never None"""
        result = preprocessor.preprocess(valid_transaction)
        assert result is not None
        assert isinstance(result, dict)
    
    def test_validate_schema_always_returns_tuple(self, preprocessor, valid_transaction):
        """PASS: validate_schema() should always return (bool, str)"""
        result = preprocessor.validate_schema(valid_transaction)
        assert isinstance(result, tuple)
        assert len(result) == 2
        assert isinstance(result[0], bool)
        assert isinstance(result[1], str)


# TODO: Add tests for new fields when schema evolves
# TODO: Add performance tests for schema validation latency
# TODO: Add tests for batch validation
