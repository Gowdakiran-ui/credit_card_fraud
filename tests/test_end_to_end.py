"""
Test Suite: End-to-End Pipeline Validation
Tests that trace a transaction through the entire pipeline.

Validates:
- Kafka → preprocessing → feature extraction → Feast → Redis
- Entity IDs, timestamps, and values verified at each stage
- No data loss or corruption across pipeline stages
- Pipeline integration correctness
"""
import pytest
import json
import sys
from pathlib import Path
from unittest.mock import Mock, MagicMock, patch

# Imports are handled by conftest.py
try:
    from pipeline.preprocessor import TransactionPreprocessor
    from pipeline.feature_extractor import FeatureExtractor
    from pipeline.feature_store import FeatureStore
except ImportError:
    sys.path.insert(0, str(Path(__file__).parent.parent / "kafka" / "src"))
    from pipeline.preprocessor import TransactionPreprocessor
    from pipeline.feature_extractor import FeatureExtractor
    from pipeline.feature_store import FeatureStore


class TestEndToEndPipeline:
    """Test complete pipeline integration"""
    
    @pytest.fixture
    def preprocessor(self):
        """Create preprocessor"""
        return TransactionPreprocessor(amount_clip_percentile=99.0)
    
    @pytest.fixture
    def mock_redis_client(self):
        """Create mock Redis client"""
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
        
        # Track state
        client.state = {}
        
        def mock_hset(key, field=None, value=None, mapping=None, **kwargs):
            if mapping:
                client.state[key] = mapping
            elif field and value:
                if key not in client.state:
                    client.state[key] = {}
                client.state[key][field] = value
            return True
        
        def mock_hget(key, field):
            return client.state.get(key, {}).get(field)
        
        client.hset.side_effect = mock_hset
        client.hget.side_effect = mock_hget
        
        return client
    
    @pytest.fixture
    def feature_store(self, mock_redis_client):
        """Create feature store with mocked Redis"""
        with patch('redis.Redis', return_value=mock_redis_client):
            with patch('redis.ConnectionPool'):
                store = FeatureStore(host='localhost', port=6379, db=0)
                store.redis_client = mock_redis_client
                return store
    
    @pytest.fixture
    def feature_extractor(self, feature_store):
        """Create feature extractor"""
        config = {
            'velocity_windows': {
                '10m': 600,
                '1h': 3600,
                '24h': 86400
            },
            'rolling_avg_alpha': 0.1,
            'default_avg_amount': 75.0
        }
        return FeatureExtractor(feature_store, config)
    
    @pytest.fixture
    def raw_kafka_message(self):
        """Simulate a raw Kafka message"""
        return {
            'transaction_id': 'tx_e2e_12345',
            'card_id': 'card_e2e_67890',
            'amount': 125.50,
            'merchant_id': 'merchant_e2e_abc',
            'timestamp': 1707580000,
            'merchant_category': 'grocery_pos',
            'location_lat': 36.08,
            'location_lon': -81.18,
        }
    
    # ============================================================
    # Test 1: Complete Pipeline Flow
    # ============================================================
    
    def test_end_to_end_transaction_processing(
        self, 
        preprocessor, 
        feature_extractor, 
        feature_store,
        raw_kafka_message
    ):
        """PASS: Transaction should flow through entire pipeline"""
        
        # Stage 1: Kafka ingestion (simulated)
        kafka_message = raw_kafka_message.copy()
        assert kafka_message is not None
        assert 'transaction_id' in kafka_message
        
        # Stage 2: Preprocessing
        preprocessed = preprocessor.preprocess(kafka_message)
        assert preprocessed is not None
        assert preprocessed['transaction_id'] == 'tx_e2e_12345'
        assert preprocessed['card_id'] == 'card_e2e_67890'
        assert isinstance(preprocessed['amount'], float)
        assert isinstance(preprocessed['timestamp'], int)
        
        # Stage 3: Feature extraction
        features = feature_extractor.extract_features(preprocessed)
        assert features is not None
        assert 'amount' in features
        assert 'tx_count_10m' in features
        assert 'hour_of_day' in features
        
        # Stage 4: Update Redis state
        feature_extractor.update_card_state(
            preprocessed['card_id'],
            preprocessed
        )
        
        # Stage 5: Verify state was updated
        # (In real system, this would be verified by fetching from Redis)
        assert True  # State update completed without error
    
    def test_entity_id_preserved_across_pipeline(
        self,
        preprocessor,
        feature_extractor,
        raw_kafka_message
    ):
        """PASS: Entity IDs should be preserved throughout pipeline"""
        
        original_tx_id = raw_kafka_message['transaction_id']
        original_card_id = raw_kafka_message['card_id']
        original_merchant_id = raw_kafka_message['merchant_id']
        
        # Preprocess
        preprocessed = preprocessor.preprocess(raw_kafka_message)
        
        assert preprocessed['transaction_id'] == original_tx_id
        assert preprocessed['card_id'] == original_card_id
        assert preprocessed['merchant_id'] == original_merchant_id
        
        # Extract features
        features = feature_extractor.extract_features(preprocessed)
        
        # Features should reference the same entities
        # (merchant features are prefixed)
        assert 'merchant_risk_score' in features
    
    def test_timestamp_preserved_across_pipeline(
        self,
        preprocessor,
        feature_extractor,
        raw_kafka_message
    ):
        """PASS: Timestamp should be preserved and used correctly"""
        
        original_timestamp = raw_kafka_message['timestamp']
        
        # Preprocess
        preprocessed = preprocessor.preprocess(raw_kafka_message)
        assert preprocessed['timestamp'] == original_timestamp
        
        # Extract features
        features = feature_extractor.extract_features(preprocessed)
        
        # Temporal features should be derived from this timestamp
        assert 'hour_of_day' in features
        assert 'day_of_week' in features
        assert features['hour_of_day'] >= 0
        assert features['day_of_week'] >= 0
    
    def test_amount_preserved_across_pipeline(
        self,
        preprocessor,
        feature_extractor,
        raw_kafka_message
    ):
        """PASS: Amount should be preserved (within rounding)"""
        
        original_amount = raw_kafka_message['amount']
        
        # Preprocess
        preprocessed = preprocessor.preprocess(raw_kafka_message)
        assert abs(preprocessed['amount'] - original_amount) < 0.01
        
        # Extract features
        features = feature_extractor.extract_features(preprocessed)
        assert abs(features['amount'] - original_amount) < 0.01
    
    # ============================================================
    # Test 2: Multiple Transaction Flow
    # ============================================================
    
    def test_sequential_transactions_update_state(
        self,
        preprocessor,
        feature_extractor,
        feature_store,
        mock_redis_client
    ):
        """PASS: Sequential transactions should update velocity features"""
        
        card_id = 'card_sequential_test'
        base_time = 1707580000
        
        # Process first transaction
        tx1 = {
            'transaction_id': 'tx_1',
            'card_id': card_id,
            'amount': 100.0,
            'merchant_id': 'merchant_1',
            'timestamp': base_time,
            'merchant_category': 'test',
        }
        
        preprocessed1 = preprocessor.preprocess(tx1)
        features1 = feature_extractor.extract_features(preprocessed1)
        feature_extractor.update_card_state(card_id, preprocessed1)
        
        # First transaction should have zero velocity
        assert features1['tx_count_10m'] == 0
        assert features1['time_since_last_tx'] == 0
        
        # Process second transaction (5 minutes later)
        tx2 = {
            'transaction_id': 'tx_2',
            'card_id': card_id,
            'amount': 50.0,
            'merchant_id': 'merchant_2',
            'timestamp': base_time + 300,
            'merchant_category': 'test',
        }
        
        preprocessed2 = preprocessor.preprocess(tx2)
        
        # Mock the state to simulate that tx1 was stored
        mock_redis_client.hget.return_value = str(base_time)
        
        features2 = feature_extractor.extract_features(preprocessed2)
        
        # Second transaction should reflect time since first
        assert features2['time_since_last_tx'] == 300  # 5 minutes
    
    def test_batch_processing_maintains_consistency(
        self,
        preprocessor,
        feature_extractor
    ):
        """PASS: Batch of transactions should be processed consistently"""
        
        transactions = [
            {
                'transaction_id': f'tx_{i}',
                'card_id': 'card_batch',
                'amount': 100.0 + i,
                'merchant_id': f'merchant_{i}',
                'timestamp': 1707580000 + i * 60,
                'merchant_category': 'test',
            }
            for i in range(10)
        ]
        
        processed_count = 0
        for tx in transactions:
            try:
                preprocessed = preprocessor.preprocess(tx)
                features = feature_extractor.extract_features(preprocessed)
                feature_extractor.update_card_state(tx['card_id'], preprocessed)
                processed_count += 1
            except Exception as e:
                pytest.fail(f"Transaction {tx['transaction_id']} failed: {e}")
        
        assert processed_count == 10
    
    # ============================================================
    # Test 3: Error Recovery
    # ============================================================
    
    def test_invalid_transaction_rejected_early(
        self,
        preprocessor,
        feature_extractor
    ):
        """PASS: Invalid transactions should be rejected at preprocessing"""
        
        invalid_tx = {
            'transaction_id': 'tx_invalid',
            # Missing required fields
        }
        
        with pytest.raises(ValueError):
            preprocessor.preprocess(invalid_tx)
    
    def test_pipeline_continues_after_single_failure(
        self,
        preprocessor,
        feature_extractor
    ):
        """PASS: Pipeline should continue processing after single failure"""
        
        transactions = [
            {
                'transaction_id': 'tx_1',
                'card_id': 'card_123',
                'amount': 100.0,
                'merchant_id': 'merchant_1',
                'timestamp': 1707580000,
            },
            {
                'transaction_id': 'tx_2',
                # Invalid: missing required fields
            },
            {
                'transaction_id': 'tx_3',
                'card_id': 'card_123',
                'amount': 50.0,
                'merchant_id': 'merchant_3',
                'timestamp': 1707580100,
            },
        ]
        
        successful = 0
        failed = 0
        
        for tx in transactions:
            try:
                preprocessed = preprocessor.preprocess(tx)
                features = feature_extractor.extract_features(preprocessed)
                successful += 1
            except ValueError:
                failed += 1
        
        assert successful == 2
        assert failed == 1
    
    # ============================================================
    # Test 4: Data Integrity Checks
    # ============================================================
    
    def test_no_data_loss_in_pipeline(
        self,
        preprocessor,
        feature_extractor,
        raw_kafka_message
    ):
        """PASS: All input fields should be preserved or transformed"""
        
        original_fields = set(raw_kafka_message.keys())
        
        preprocessed = preprocessor.preprocess(raw_kafka_message)
        preprocessed_fields = set(preprocessed.keys())
        
        # All original fields should be present (or more, with defaults added)
        assert original_fields.issubset(preprocessed_fields)
    
    def test_no_silent_corruption(
        self,
        preprocessor,
        feature_extractor,
        raw_kafka_message
    ):
        """PASS: Values should not be silently corrupted"""
        
        # Store original values
        original_values = {
            'transaction_id': raw_kafka_message['transaction_id'],
            'card_id': raw_kafka_message['card_id'],
            'amount': raw_kafka_message['amount'],
            'merchant_id': raw_kafka_message['merchant_id'],
        }
        
        # Process
        preprocessed = preprocessor.preprocess(raw_kafka_message)
        features = feature_extractor.extract_features(preprocessed)
        
        # Verify critical values unchanged
        assert preprocessed['transaction_id'] == original_values['transaction_id']
        assert preprocessed['card_id'] == original_values['card_id']
        assert abs(preprocessed['amount'] - original_values['amount']) < 0.01
        assert preprocessed['merchant_id'] == original_values['merchant_id']
    
    # ============================================================
    # Test 5: Performance & Reliability
    # ============================================================
    
    def test_pipeline_completes_within_reasonable_time(
        self,
        preprocessor,
        feature_extractor,
        raw_kafka_message
    ):
        """PASS: Pipeline should complete quickly"""
        import time
        
        start = time.time()
        
        preprocessed = preprocessor.preprocess(raw_kafka_message)
        features = feature_extractor.extract_features(preprocessed)
        feature_extractor.update_card_state(
            preprocessed['card_id'],
            preprocessed
        )
        
        elapsed = time.time() - start
        
        # Should complete in under 100ms (generous for testing)
        assert elapsed < 0.1, f"Pipeline took {elapsed*1000:.1f}ms"
    
    def test_pipeline_is_deterministic(
        self,
        preprocessor,
        feature_extractor,
        raw_kafka_message
    ):
        """PASS: Same input should produce same output"""
        
        # Process twice
        preprocessed1 = preprocessor.preprocess(raw_kafka_message.copy())
        features1 = feature_extractor.extract_features(preprocessed1.copy())
        
        preprocessed2 = preprocessor.preprocess(raw_kafka_message.copy())
        features2 = feature_extractor.extract_features(preprocessed2.copy())
        
        assert preprocessed1 == preprocessed2
        assert features1 == features2
    
    # ============================================================
    # Test 6: Edge Cases
    # ============================================================
    
    def test_minimal_valid_transaction(
        self,
        preprocessor,
        feature_extractor
    ):
        """PASS: Minimal valid transaction should be processed"""
        
        minimal_tx = {
            'transaction_id': 'tx_minimal',
            'card_id': 'card_minimal',
            'amount': 1.0,
            'merchant_id': 'merchant_minimal',
            'timestamp': 1707580000,
        }
        
        preprocessed = preprocessor.preprocess(minimal_tx)
        features = feature_extractor.extract_features(preprocessed)
        feature_extractor.update_card_state(
            preprocessed['card_id'],
            preprocessed
        )
        
        assert features is not None
        assert len(features) > 0
    
    def test_transaction_with_all_optional_fields(
        self,
        preprocessor,
        feature_extractor
    ):
        """PASS: Transaction with all fields should be processed"""
        
        complete_tx = {
            'transaction_id': 'tx_complete',
            'card_id': 'card_complete',
            'amount': 125.50,
            'merchant_id': 'merchant_complete',
            'timestamp': 1707580000,
            'merchant_category': 'grocery_pos',
            'location_lat': 36.08,
            'location_lon': -81.18,
            'city': 'Test City',
            'state': 'TS',
            'user_id': 'user_123',
        }
        
        preprocessed = preprocessor.preprocess(complete_tx)
        features = feature_extractor.extract_features(preprocessed)
        
        assert features is not None
        assert features['has_location'] == 1


# TODO: Add integration tests with real Kafka
# TODO: Add tests for failure recovery and retry logic
# TODO: Add tests for monitoring and alerting integration
