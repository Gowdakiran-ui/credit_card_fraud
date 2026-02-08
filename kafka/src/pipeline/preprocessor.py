"""
Transaction Data Preprocessor
Validates and cleans transaction data before feature extraction
"""
from typing import Dict, Any, Tuple
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class TransactionPreprocessor:
    """Validates and preprocesses transaction data"""
    
    # Required fields for a valid transaction
    REQUIRED_FIELDS = [
        'transaction_id',
        'card_id',
        'amount',
        'merchant_id',
        'timestamp'
    ]
    
    # Optional fields with defaults
    OPTIONAL_FIELDS = {
        'merchant_category': 'UNKNOWN',
        'location_lat': None,
        'location_lon': None,
        'city': '',
        'state': '',
        'user_id': '',
    }
    
    def __init__(self, amount_clip_percentile: float = 99.0):
        """
        Initialize preprocessor
        
        Args:
            amount_clip_percentile: Percentile to clip extreme amounts (default: 99)
        """
        self.amount_clip_percentile = amount_clip_percentile
        self.amount_clip_value = 10000.0  # Default max amount, updated dynamically
    
    def validate_schema(self, transaction: Dict[str, Any]) -> Tuple[bool, str]:
        """
        Validate that transaction has all required fields
        
        Args:
            transaction: Transaction dictionary
        
        Returns:
            Tuple of (is_valid, error_message)
        """
        if not isinstance(transaction, dict):
            return False, "Transaction must be a dictionary"
        
        # Check required fields
        missing_fields = [
            field for field in self.REQUIRED_FIELDS 
            if field not in transaction or transaction[field] is None
        ]
        
        if missing_fields:
            return False, f"Missing required fields: {', '.join(missing_fields)}"
        
        return True, ""
    
    def preprocess(self, transaction: Dict[str, Any]) -> Dict[str, Any]:
        """
        Clean and normalize transaction data
        
        Steps:
        1. Validate schema
        2. Handle missing optional fields
        3. Cast types to correct formats
        4. Normalize amounts
        5. Parse timestamps
        6. Validate ranges
        
        Args:
            transaction: Raw transaction dictionary
        
        Returns:
            Preprocessed transaction dictionary
        
        Raises:
            ValueError: If transaction fails validation
        """
        # Step 1: Validate schema
        is_valid, error_msg = self.validate_schema(transaction)
        if not is_valid:
            raise ValueError(f"Invalid transaction: {error_msg}")
        
        # Create a copy to avoid modifying original
        processed = transaction.copy()
        
        # Step 2: Handle missing optional fields
        processed = self._handle_missing_values(processed)
        
        # Step 3: Cast types
        processed = self._cast_types(processed)
        
        # Step 4: Normalize amount
        processed['amount'] = self._normalize_amount(processed['amount'])
        
        # Step 5: Parse timestamp (ensure it's Unix epoch)
        processed['timestamp'] = self._parse_timestamp(processed['timestamp'])
        
        # Step 6: Validate ranges
        self._validate_ranges(processed)
        
        return processed
    
    def _handle_missing_values(self, transaction: Dict[str, Any]) -> Dict[str, Any]:
        """
        Fill missing optional fields with defaults
        
        Args:
            transaction: Transaction dictionary
        
        Returns:
            Transaction with defaults filled
        """
        for field, default_value in self.OPTIONAL_FIELDS.items():
            if field not in transaction or transaction[field] is None:
                transaction[field] = default_value
        
        return transaction
    
    def _cast_types(self, transaction: Dict[str, Any]) -> Dict[str, Any]:
        """
        Cast fields to correct types
        
        Args:
            transaction: Transaction dictionary
        
        Returns:
            Transaction with correct types
        """
        try:
            # Ensure strings
            transaction['transaction_id'] = str(transaction['transaction_id'])
            transaction['card_id'] = str(transaction['card_id'])
            transaction['merchant_id'] = str(transaction['merchant_id'])
            transaction['merchant_category'] = str(transaction.get('merchant_category', 'UNKNOWN'))
            
            # Ensure numeric types
            transaction['amount'] = float(transaction['amount'])
            
            # Ensure timestamp is integer (Unix epoch)
            if isinstance(transaction['timestamp'], (int, float)):
                transaction['timestamp'] = int(transaction['timestamp'])
            
            # Handle optional numeric fields
            if transaction.get('location_lat') is not None:
                transaction['location_lat'] = float(transaction['location_lat'])
            if transaction.get('location_lon') is not None:
                transaction['location_lon'] = float(transaction['location_lon'])
            
        except (ValueError, TypeError) as e:
            raise ValueError(f"Type casting error: {e}")
        
        return transaction
    
    def _normalize_amount(self, amount: float) -> float:
        """
        Normalize transaction amount
        - Ensure positive
        - Clip extreme outliers
        
        Args:
            amount: Raw transaction amount
        
        Returns:
            Normalized amount
        """
        # Ensure positive
        if amount < 0:
            logger.warning(f"Negative amount detected: {amount}, converting to absolute value")
            amount = abs(amount)
        
        # Clip extreme values
        if amount > self.amount_clip_value:
            logger.warning(f"Amount {amount} exceeds clip value {self.amount_clip_value}, clipping")
            amount = self.amount_clip_value
        
        return round(amount, 2)
    
    def _parse_timestamp(self, timestamp: Any) -> int:
        """
        Parse timestamp to Unix epoch (seconds)
        
        Args:
            timestamp: Timestamp in various formats
        
        Returns:
            Unix epoch timestamp (seconds)
        """
        # Already Unix epoch
        if isinstance(timestamp, (int, float)):
            return int(timestamp)
        
        # String timestamp - try to parse
        if isinstance(timestamp, str):
            try:
                dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                return int(dt.timestamp())
            except ValueError:
                # Try other common formats
                try:
                    dt = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
                    return int(dt.timestamp())
                except ValueError:
                    raise ValueError(f"Unable to parse timestamp: {timestamp}")
        
        raise ValueError(f"Invalid timestamp type: {type(timestamp)}")
    
    def _validate_ranges(self, transaction: Dict[str, Any]):
        """
        Validate that values are in acceptable ranges
        
        Args:
            transaction: Transaction dictionary
        
        Raises:
            ValueError: If values are out of range
        """
        # Amount must be positive
        if transaction['amount'] <= 0:
            raise ValueError(f"Amount must be positive: {transaction['amount']}")
        
        # Timestamp must be reasonable (after 2000-01-01, before 2100-01-01)
        min_timestamp = 946684800  # 2000-01-01
        max_timestamp = 4102444800  # 2100-01-01
        
        if not (min_timestamp <= transaction['timestamp'] <= max_timestamp):
            raise ValueError(f"Timestamp out of range: {transaction['timestamp']}")
        
        # Validate coordinates if present
        if transaction.get('location_lat') is not None:
            if not (-90 <= transaction['location_lat'] <= 90):
                raise ValueError(f"Invalid latitude: {transaction['location_lat']}")
        
        if transaction.get('location_lon') is not None:
            if not (-180 <= transaction['location_lon'] <= 180):
                raise ValueError(f"Invalid longitude: {transaction['location_lon']}")
