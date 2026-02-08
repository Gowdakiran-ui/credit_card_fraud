"""
Kafka Consumer for Real-Time Feature Extraction
Orchestrates the feature extraction pipeline: consume → preprocess → extract → store
"""
import json
import signal
import sys
import time
from typing import Dict, Any
from kafka import KafkaConsumer
from kafka.errors import KafkaError

from src.utils.config import CONSUMER_CONFIG, TOPIC_NAME, REDIS_CONFIG, FEATURE_CONFIG
from src.pipeline.feature_store import FeatureStore
from src.pipeline.feature_extractor import FeatureExtractor
from src.pipeline.preprocessor import TransactionPreprocessor
from src.utils.logger import get_consumer_logger

logger = get_consumer_logger()


class FeatureExtractionConsumer:
    """Kafka consumer for real-time feature extraction and storage"""
    
    def __init__(self):
        """Initialize consumer and all services"""
        self.consumer = None
        self.feature_store = None
        self.feature_extractor = None
        self.preprocessor = None
        self.running = True
        
        # Metrics
        self.messages_processed = 0
        self.messages_failed = 0
        self.start_time = None
        self.total_latency = 0.0
        self.total_feature_extraction_time = 0.0
        self.total_redis_update_time = 0.0
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info("🛑 Shutdown signal received. Cleaning up...")
        self.running = False
    
    def _init_services(self):
        """Initialize all required services"""
        try:
            # Initialize Kafka consumer
            logger.info("Initializing Kafka consumer...")
            self.consumer = KafkaConsumer(
                TOPIC_NAME,
                **CONSUMER_CONFIG,
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            logger.info(f"✅ Kafka consumer subscribed to topic '{TOPIC_NAME}'")
            
            # Initialize feature store
            logger.info("Initializing Redis feature store...")
            self.feature_store = FeatureStore(**REDIS_CONFIG)
            
            # Initialize preprocessor
            logger.info("Initializing transaction preprocessor...")
            self.preprocessor = TransactionPreprocessor(
                amount_clip_percentile=FEATURE_CONFIG.get('amount_clip_percentile', 99.0)
            )
            
            # Initialize feature extractor
            logger.info("Initializing feature extractor...")
            self.feature_extractor = FeatureExtractor(
                feature_store=self.feature_store,
                feature_config=FEATURE_CONFIG
            )
            
            logger.info("✅ All services initialized successfully")
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize services: {e}")
            sys.exit(1)
    
    def _process_message(self, message: Dict[str, Any]) -> bool:
        """
        Process a single transaction message
        
        Pipeline:
        1. Validate and preprocess transaction
        2. Extract features
        3. Update Redis with features
        4. Log success
        
        Args:
            message: Transaction message from Kafka
        
        Returns:
            True if processed successfully, False otherwise
        """
        start_time = time.time()
        
        try:
            transaction_id = message.get('transaction_id', 'UNKNOWN')
            card_id = message.get('card_id', '')
            
            logger.debug(f"Processing transaction {transaction_id}")
            
            # Step 1: Validate and preprocess
            try:
                preprocessed_tx = self.preprocessor.preprocess(message)
            except ValueError as e:
                logger.error(f"❌ Validation failed for {transaction_id}: {e}")
                return False
            
            # Step 2: Extract features
            feature_start = time.time()
            features = self.feature_extractor.extract_features(preprocessed_tx)
            feature_time = (time.time() - feature_start) * 1000
            self.total_feature_extraction_time += feature_time
            
            # Step 3: Update Redis with new transaction state
            redis_start = time.time()
            self.feature_extractor.update_card_state(card_id, preprocessed_tx)
            redis_time = (time.time() - redis_start) * 1000
            self.total_redis_update_time += redis_time
            
            # Calculate total latency
            total_latency = (time.time() - start_time) * 1000
            self.total_latency += total_latency
            
            # Step 4: Log success
            logger.info(
                f"✅ {transaction_id} | Card: {card_id[:12]}... | "
                f"Features: {len(features)} | "
                f"Extract: {feature_time:.1f}ms | Redis: {redis_time:.1f}ms | "
                f"Total: {total_latency:.1f}ms"
            )
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error processing message: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def _print_stats(self):
        """Print current statistics"""
        if self.messages_processed == 0:
            return
        
        elapsed = time.time() - self.start_time
        rate = self.messages_processed / elapsed if elapsed > 0 else 0
        avg_latency = self.total_latency / self.messages_processed
        avg_feature_time = self.total_feature_extraction_time / self.messages_processed
        avg_redis_time = self.total_redis_update_time / self.messages_processed
        
        success_rate = (self.messages_processed / (self.messages_processed + self.messages_failed)) * 100
        
        logger.info(
            f"📊 Processed: {self.messages_processed:,} | Failed: {self.messages_failed} | "
            f"Success Rate: {success_rate:.1f}% | Rate: {rate:.2f} msg/sec | "
            f"Avg Latency: {avg_latency:.1f}ms (Feature: {avg_feature_time:.1f}ms, Redis: {avg_redis_time:.1f}ms)"
        )
    
    def consume(self):
        """Main consumption loop"""
        logger.info("\n🚀 Starting Feature Extraction Consumer")
        logger.info(f"📮 Topic: {TOPIC_NAME}")
        logger.info(f"🔧 Feature Windows: {FEATURE_CONFIG.get('velocity_windows', {})}")
        logger.info(f"💾 Redis: {REDIS_CONFIG.get('host')}:{REDIS_CONFIG.get('port')}\n")
        
        # Initialize all services
        self._init_services()
        
        try:
            self.start_time = time.time()
            stats_counter = 0
            
            logger.info("👂 Listening for messages...\n")
            
            for message in self.consumer:
                if not self.running:
                    break
                
                # Process the message
                transaction = message.value
                success = self._process_message(transaction)
                
                if success:
                    self.messages_processed += 1
                    stats_counter += 1
                else:
                    self.messages_failed += 1
                
                # Print stats every 100 messages
                if stats_counter >= 100:
                    self._print_stats()
                    stats_counter = 0
            
            # Final statistics
            logger.info("\n" + "="*80)
            logger.info("📊 Final Statistics")
            logger.info("="*80)
            self._print_stats()
            
            # Redis statistics
            logger.info(f"\n📈 Redis Feature Store Statistics:")
            logger.info(f"  Total Cards Processed: {self.messages_processed:,}")
            logger.info(f"  Average Feature Extraction Time: {self.total_feature_extraction_time / self.messages_processed:.1f}ms")
            logger.info(f"  Average Redis Update Time: {self.total_redis_update_time / self.messages_processed:.1f}ms")
            
        except KeyboardInterrupt:
            logger.info("\n🛑 Interrupted by user")
        except Exception as e:
            logger.error(f"\n❌ Error during consumption: {e}")
            import traceback
            traceback.print_exc()
        finally:
            self._cleanup()
    
    def _cleanup(self):
        """Clean up resources"""
        logger.info("\n🧹 Cleaning up resources...")
        
        if self.consumer:
            self.consumer.close()
            logger.info("✅ Kafka consumer closed")
        
        if self.feature_store:
            self.feature_store.close()
        
        logger.info("✅ Cleanup complete")


def main():
    """Main entry point"""
    consumer = FeatureExtractionConsumer()
    consumer.consume()


if __name__ == "__main__":
    main()
