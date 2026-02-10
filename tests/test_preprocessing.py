"""
Test Suite: Preprocessing Validation
Tests that ensure data transformations preserve semantic meaning.

Validates:
- Input → output semantic preservation
- No empty strings after preprocessing (when not expected)
- Unicode, emojis, special characters handled
- Length constraints enforced
- Normalization is deterministic
"""
import pytest
from typing import Dict, Any
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "kafka" / "src"))

from pipeline.preprocessor import TransactionPreprocessor


class TestPreprocessingValidation:
    """Test preprocessing logic and transformations"""
    
    @pytest.fixture
    def preprocessor(self):
        """Create a preprocessor instance"""
        return TransactionPreprocessor(amount_clip_percentile=99.0)
    
    @pytest.fixture
    def valid_transaction(self):
        """Valid transaction for testing"""
        return {
            'transaction_id': 'tx_12345',
            'card_id': 'card_67890',
            'amount': 125.50,
            'merchant_id': 'merchant_abc',
            'timestamp': 1707580000,
            'merchant_category': 'grocery_pos',
        }
    
    # ============================================================
    # Test 1: Semantic Preservation
    # ============================================================
    
    def test_preprocessing_preserves_transaction_id(self, preprocessor, valid_transaction):
        """PASS: Transaction ID should remain unchanged"""
        original_id = valid_transaction['transaction_id']
        processed = preprocessor.preprocess(valid_transaction)
        assert processed['transaction_id'] == original_id
    
    def test_preprocessing_preserves_card_id(self, preprocessor, valid_transaction):
        """PASS: Card ID should remain unchanged"""
        original_card = valid_transaction['card_id']
        processed = preprocessor.preprocess(valid_transaction)
        assert processed['card_id'] == original_card
    
    def test_preprocessing_preserves_merchant_id(self, preprocessor, valid_transaction):
        """PASS: Merchant ID should remain unchanged"""
        original_merchant = valid_transaction['merchant_id']
        processed = preprocessor.preprocess(valid_transaction)
        assert processed['merchant_id'] == original_merchant
    
    def test_preprocessing_preserves_amount_value(self, preprocessor, valid_transaction):
        """PASS: Amount value should be preserved (within rounding)"""
        original_amount = valid_transaction['amount']
        processed = preprocessor.preprocess(valid_transaction)
        assert abs(processed['amount'] - original_amount) < 0.01
    
    def test_preprocessing_does_not_modify_original(self, preprocessor, valid_transaction):
        """PASS: Original transaction should not be modified"""
        original_copy = valid_transaction.copy()
        preprocessor.preprocess(valid_transaction)
        # Original should be unchanged (preprocessor creates a copy)
        assert valid_transaction == original_copy
    
    def test_amount_rounding_precision(self, preprocessor, valid_transaction):
        """PASS: Amount should be rounded to 2 decimal places"""
        valid_transaction['amount'] = 125.5555
        processed = preprocessor.preprocess(valid_transaction)
        # Check that amount has at most 2 decimal places
        assert processed['amount'] == round(processed['amount'], 2)
    
    # ============================================================
    # Test 2: String Handling (Unicode, Emojis, Special Chars)
    # ============================================================
    
    def test_unicode_in_transaction_id(self, preprocessor, valid_transaction):
        """PASS: Unicode characters in transaction_id should be preserved"""
        valid_transaction['transaction_id'] = 'tx_café_123'
        processed = preprocessor.preprocess(valid_transaction)
        assert processed['transaction_id'] == 'tx_café_123'
    
    def test_unicode_in_merchant_id(self, preprocessor, valid_transaction):
        """PASS: Unicode characters in merchant_id should be preserved"""
        valid_transaction['merchant_id'] = 'merchant_北京_店'
        processed = preprocessor.preprocess(valid_transaction)
        assert processed['merchant_id'] == 'merchant_北京_店'
    
    def test_emoji_in_merchant_category(self, preprocessor, valid_transaction):
        """PASS: Emojis in merchant_category should be preserved"""
        valid_transaction['merchant_category'] = 'food_🍕_delivery'
        processed = preprocessor.preprocess(valid_transaction)
        assert processed['merchant_category'] == 'food_🍕_delivery'
    
    def test_special_characters_in_ids(self, preprocessor, valid_transaction):
        """PASS: Special characters should be preserved"""
        valid_transaction['transaction_id'] = 'tx-2024_02/10#123'
        valid_transaction['card_id'] = 'card@domain.com'
        processed = preprocessor.preprocess(valid_transaction)
        assert processed['transaction_id'] == 'tx-2024_02/10#123'
        assert processed['card_id'] == 'card@domain.com'
    
    def test_whitespace_in_strings(self, preprocessor, valid_transaction):
        """PASS: Whitespace should be preserved"""
        valid_transaction['merchant_id'] = 'merchant with spaces'
        processed = preprocessor.preprocess(valid_transaction)
        assert processed['merchant_id'] == 'merchant with spaces'
    
    def test_empty_string_merchant_category(self, preprocessor, valid_transaction):
        """PASS: Empty merchant_category should be allowed (it's optional)"""
        valid_transaction['merchant_category'] = ''
        processed = preprocessor.preprocess(valid_transaction)
        assert processed['merchant_category'] == ''
    
    def test_very_long_string_ids(self, preprocessor, valid_transaction):
        """PASS: Very long strings should be preserved"""
        long_id = 'x' * 1000
        valid_transaction['transaction_id'] = long_id
        processed = preprocessor.preprocess(valid_transaction)
        assert processed['transaction_id'] == long_id
        assert len(processed['transaction_id']) == 1000
    
    # ============================================================
    # Test 3: Numeric Normalization
    # ============================================================
    
    def test_negative_amount_converted_to_positive(self, preprocessor, valid_transaction):
        """PASS: Negative amounts should be converted to absolute value"""
        valid_transaction['amount'] = -125.50
        processed = preprocessor.preprocess(valid_transaction)
        assert processed['amount'] == 125.50
        assert processed['amount'] > 0
    
    def test_amount_clipping_at_max_value(self, preprocessor, valid_transaction):
        """PASS: Amounts exceeding clip value should be clipped"""
        preprocessor.amount_clip_value = 5000.0
        valid_transaction['amount'] = 10000.0
        processed = preprocessor.preprocess(valid_transaction)
        assert processed['amount'] == 5000.0
    
    def test_amount_not_clipped_when_below_threshold(self, preprocessor, valid_transaction):
        """PASS: Amounts below clip value should not be modified"""
        preprocessor.amount_clip_value = 5000.0
        valid_transaction['amount'] = 100.0
        processed = preprocessor.preprocess(valid_transaction)
        assert processed['amount'] == 100.0
    
    def test_small_amount_precision(self, preprocessor, valid_transaction):
        """PASS: Small amounts should maintain precision"""
        valid_transaction['amount'] = 0.01
        processed = preprocessor.preprocess(valid_transaction)
        assert processed['amount'] == 0.01
    
    def test_fractional_cents_rounded(self, preprocessor, valid_transaction):
        """PASS: Fractional cents should be rounded"""
        valid_transaction['amount'] = 125.555
        processed = preprocessor.preprocess(valid_transaction)
        # Should be rounded to 2 decimal places
        assert processed['amount'] == 125.56
    
    # ============================================================
    # Test 4: Timestamp Normalization
    # ============================================================
    
    def test_timestamp_unix_epoch_preserved(self, preprocessor, valid_transaction):
        """PASS: Unix epoch timestamps should be preserved"""
        original_ts = 1707580000
        valid_transaction['timestamp'] = original_ts
        processed = preprocessor.preprocess(valid_transaction)
        assert processed['timestamp'] == original_ts
    
    def test_timestamp_float_truncated_to_int(self, preprocessor, valid_transaction):
        """PASS: Float timestamps should be truncated to int"""
        valid_transaction['timestamp'] = 1707580000.999
        processed = preprocessor.preprocess(valid_transaction)
        assert isinstance(processed['timestamp'], int)
        assert processed['timestamp'] == 1707580000
    
    def test_timestamp_iso_format_parsed(self, preprocessor, valid_transaction):
        """PASS: ISO format timestamps should be parsed to Unix epoch"""
        valid_transaction['timestamp'] = "2024-02-10T12:00:00Z"
        processed = preprocessor.preprocess(valid_transaction)
        assert isinstance(processed['timestamp'], int)
        assert processed['timestamp'] > 1700000000  # Reasonable timestamp
    
    def test_timestamp_standard_format_parsed(self, preprocessor, valid_transaction):
        """PASS: Standard datetime format should be parsed"""
        valid_transaction['timestamp'] = "2024-02-10 12:00:00"
        processed = preprocessor.preprocess(valid_transaction)
        assert isinstance(processed['timestamp'], int)
        assert processed['timestamp'] > 1700000000
    
    # ============================================================
    # Test 5: Deterministic Behavior
    # ============================================================
    
    def test_preprocessing_is_deterministic(self, preprocessor, valid_transaction):
        """PASS: Same input should always produce same output"""
        result1 = preprocessor.preprocess(valid_transaction.copy())
        result2 = preprocessor.preprocess(valid_transaction.copy())
        assert result1 == result2
    
    def test_multiple_runs_same_result(self, preprocessor, valid_transaction):
        """PASS: Running preprocessing multiple times should be consistent"""
        results = [
            preprocessor.preprocess(valid_transaction.copy())
            for _ in range(10)
        ]
        # All results should be identical
        for result in results[1:]:
            assert result == results[0]
    
    def test_order_independence_of_fields(self, preprocessor):
        """PASS: Field order should not affect preprocessing"""
        tx1 = {
            'transaction_id': 'tx_123',
            'card_id': 'card_456',
            'amount': 100.0,
            'merchant_id': 'merchant_789',
            'timestamp': 1707580000,
        }
        tx2 = {
            'timestamp': 1707580000,
            'merchant_id': 'merchant_789',
            'amount': 100.0,
            'card_id': 'card_456',
            'transaction_id': 'tx_123',
        }
        result1 = preprocessor.preprocess(tx1)
        result2 = preprocessor.preprocess(tx2)
        assert result1 == result2
    
    # ============================================================
    # Test 6: Edge Cases
    # ============================================================
    
    def test_very_small_amount(self, preprocessor, valid_transaction):
        """PASS: Very small amounts should be handled"""
        valid_transaction['amount'] = 0.01
        processed = preprocessor.preprocess(valid_transaction)
        assert processed['amount'] == 0.01
    
    def test_integer_amount_converted_to_float(self, preprocessor, valid_transaction):
        """PASS: Integer amounts should be converted to float"""
        valid_transaction['amount'] = 100
        processed = preprocessor.preprocess(valid_transaction)
        assert isinstance(processed['amount'], float)
        assert processed['amount'] == 100.0
    
    def test_null_optional_coordinates(self, preprocessor, valid_transaction):
        """PASS: Null coordinates should be handled"""
        valid_transaction['location_lat'] = None
        valid_transaction['location_lon'] = None
        processed = preprocessor.preprocess(valid_transaction)
        assert processed['location_lat'] is None
        assert processed['location_lon'] is None
    
    def test_zero_coordinates(self, preprocessor, valid_transaction):
        """PASS: Zero coordinates (valid location) should be preserved"""
        valid_transaction['location_lat'] = 0.0
        valid_transaction['location_lon'] = 0.0
        processed = preprocessor.preprocess(valid_transaction)
        assert processed['location_lat'] == 0.0
        assert processed['location_lon'] == 0.0
    
    # ============================================================
    # Test 7: Type Coercion Consistency
    # ============================================================
    
    def test_numeric_string_ids_converted_to_string(self, preprocessor, valid_transaction):
        """PASS: Numeric IDs should be converted to strings"""
        valid_transaction['transaction_id'] = 12345
        valid_transaction['card_id'] = 67890
        processed = preprocessor.preprocess(valid_transaction)
        assert isinstance(processed['transaction_id'], str)
        assert isinstance(processed['card_id'], str)
        assert processed['transaction_id'] == '12345'
        assert processed['card_id'] == '67890'
    
    def test_boolean_converted_to_string(self, preprocessor, valid_transaction):
        """PASS: Boolean values should be converted to strings"""
        valid_transaction['merchant_category'] = True
        processed = preprocessor.preprocess(valid_transaction)
        assert isinstance(processed['merchant_category'], str)
        assert processed['merchant_category'] == 'True'
    
    # ============================================================
    # Test 8: No Data Loss
    # ============================================================
    
    def test_all_input_fields_present_in_output(self, preprocessor, valid_transaction):
        """PASS: All input fields should be present in output"""
        processed = preprocessor.preprocess(valid_transaction)
        for key in valid_transaction.keys():
            assert key in processed
    
    def test_optional_fields_added_when_missing(self, preprocessor):
        """PASS: Optional fields should be added with defaults"""
        minimal_tx = {
            'transaction_id': 'tx_123',
            'card_id': 'card_456',
            'amount': 100.0,
            'merchant_id': 'merchant_789',
            'timestamp': 1707580000,
        }
        processed = preprocessor.preprocess(minimal_tx)
        # Optional fields should be added
        assert 'merchant_category' in processed
        assert 'location_lat' in processed
        assert 'location_lon' in processed


# TODO: Add tests for new preprocessing rules when added
# TODO: Add performance benchmarks for preprocessing latency
# TODO: Add tests for batch preprocessing
