"""
Unit Tests for Transaction Preprocessor
"""
import pytest
from datetime import datetime
from preprocessor import TransactionPreprocessor


class TestTransactionPreprocessor:
    """Test cases for TransactionPreprocessor"""
    
    def setup_method(self):
        """Setup test fixtures"""
        self.preprocessor = TransactionPreprocessor()
    
    def test_validate_schema_success(self):
        """Test schema validation with valid transaction"""
        transaction = {
            'transaction_id': 'tx_123',
            'card_id': 'card_456',
            'amount': 100.0,
            'merchant_id': 'merch_789',
            'timestamp': 1675890123
        }
        
        is_valid, error_msg = self.preprocessor.validate_schema(transaction)
        assert is_valid is True
        assert error_msg == ""
    
    def test_validate_schema_missing_fields(self):
        """Test schema validation with missing required fields"""
        transaction = {
            'transaction_id': 'tx_123',
            'amount': 100.0
        }
        
        is_valid, error_msg = self.preprocessor.validate_schema(transaction)
        assert is_valid is False
        assert "Missing required fields" in error_msg
    
    def test_preprocess_success(self):
        """Test full preprocessing pipeline"""
        transaction = {
            'transaction_id': 'tx_123',
            'card_id': 'card_456',
            'amount': 100.50,
            'merchant_id': 'merch_789',
            'timestamp': 1675890123,
            'merchant_category': 'GROCERY'
        }
        
        processed = self.preprocessor.preprocess(transaction)
        
        assert processed['transaction_id'] == 'tx_123'
        assert processed['amount'] == 100.50
        assert processed['timestamp'] == 1675890123
        assert processed['merchant_category'] == 'GROCERY'
    
    def test_normalize_amount_negative(self):
        """Test amount normalization with negative value"""
        amount = -50.0
        normalized = self.preprocessor._normalize_amount(amount)
        assert normalized == 50.0
    
    def test_normalize_amount_extreme(self):
        """Test amount clipping for extreme values"""
        self.preprocessor.amount_clip_value = 5000.0
        amount = 10000.0
        normalized = self.preprocessor._normalize_amount(amount)
        assert normalized == 5000.0
    
    def test_parse_timestamp_unix_epoch(self):
        """Test timestamp parsing from Unix epoch"""
        timestamp = 1675890123
        parsed = self.preprocessor._parse_timestamp(timestamp)
        assert parsed == 1675890123
    
    def test_handle_missing_values(self):
        """Test handling of missing optional fields"""
        transaction = {
            'transaction_id': 'tx_123',
            'card_id': 'card_456',
            'amount': 100.0,
            'merchant_id': 'merch_789',
            'timestamp': 1675890123
        }
        
        processed = self.preprocessor._handle_missing_values(transaction)
        assert processed['merchant_category'] == 'UNKNOWN'
        assert processed['location_lat'] is None
        assert processed['city'] == ''


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
