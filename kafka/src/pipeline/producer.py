"""
Kafka Producer for Credit Card Transaction Data
Reads from fraudTrain.csv and publishes to Kafka topic
"""
import json
import time
import signal
import sys
from datetime import datetime
from typing import Dict, Any

import pandas as pd
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError

from src.utils.config import (
    PRODUCER_CONFIG,
    TOPIC_NAME,
    TOPIC_PARTITIONS,
    TOPIC_REPLICATION_FACTOR,
    DATASET_PATH,
    BATCH_SIZE,
    RATE_LIMIT
)


class TransactionProducer:
    """Kafka producer for credit card transactions"""
    
    def __init__(self):
        self.producer = None
        self.running = True
        self.messages_sent = 0
        self.start_time = None
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        print("\n\n🛑 Shutdown signal received. Cleaning up...")
        self.running = False
    
    def _create_topic(self):
        """Create Kafka topic if it doesn't exist"""
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=PRODUCER_CONFIG['bootstrap_servers']
            )
            
            topic = NewTopic(
                name=TOPIC_NAME,
                num_partitions=TOPIC_PARTITIONS,
                replication_factor=TOPIC_REPLICATION_FACTOR
            )
            
            admin_client.create_topics([topic], validate_only=False)
            print(f"✅ Topic '{TOPIC_NAME}' created successfully")
            
        except TopicAlreadyExistsError:
            print(f"ℹ️  Topic '{TOPIC_NAME}' already exists")
        except Exception as e:
            print(f"⚠️  Error creating topic: {e}")
        finally:
            admin_client.close()
    
    def _init_producer(self):
        """Initialize Kafka producer"""
        try:
            self.producer = KafkaProducer(
                **PRODUCER_CONFIG,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None
            )
            print(f"✅ Kafka producer connected to {PRODUCER_CONFIG['bootstrap_servers']}")
        except Exception as e:
            print(f"❌ Failed to connect to Kafka: {e}")
            sys.exit(1)
    
    def _transform_row(self, row: pd.Series) -> Dict[str, Any]:
        """Transform CSV row to transaction message format"""
        return {
            'transaction_id': str(row.get('trans_num', '')),
            'card_id': str(row.get('cc_num', '')),
            'user_id': str(row.get('cc_num', '')),  # Using cc_num as user_id
            'amount': float(row.get('amt', 0.0)),
            'merchant_id': str(row.get('merchant', '')),
            'merchant_category': str(row.get('category', '')),
            'timestamp': int(pd.to_datetime(row.get('trans_date_trans_time', datetime.now())).timestamp()),
            'location_lat': float(row.get('lat', 0.0)) if pd.notna(row.get('lat')) else None,
            'location_lon': float(row.get('long', 0.0)) if pd.notna(row.get('long')) else None,
            'city': str(row.get('city', '')),
            'state': str(row.get('state', '')),
            'zip': str(row.get('zip', '')),
            'job': str(row.get('job', '')),
            'dob': str(row.get('dob', '')),
            'is_fraud': int(row.get('is_fraud', 0)),
            'first_name': str(row.get('first', '')),
            'last_name': str(row.get('last', '')),
            'gender': str(row.get('gender', '')),
            'city_pop': int(row.get('city_pop', 0)) if pd.notna(row.get('city_pop')) else 0,
            'merchant_lat': float(row.get('merch_lat', 0.0)) if pd.notna(row.get('merch_lat')) else None,
            'merchant_lon': float(row.get('merch_long', 0.0)) if pd.notna(row.get('merch_long')) else None,
        }
    
    def _send_message(self, message: Dict[str, Any]):
        """Send message to Kafka with error handling"""
        try:
            # Use card_id as partition key for consistent routing
            key = message.get('card_id', '')
            
            future = self.producer.send(
                TOPIC_NAME,
                key=key,
                value=message
            )
            
            # Optional: Wait for acknowledgment (can slow down throughput)
            # future.get(timeout=10)
            
            self.messages_sent += 1
            
        except Exception as e:
            print(f"❌ Error sending message: {e}")
    
    def _print_stats(self):
        """Print current statistics"""
        elapsed = time.time() - self.start_time
        rate = self.messages_sent / elapsed if elapsed > 0 else 0
        
        print(f"\r📊 Sent: {self.messages_sent:,} messages | "
              f"Rate: {rate:.2f} msg/sec | "
              f"Elapsed: {elapsed:.1f}s", end='', flush=True)
    
    def produce_from_csv(self):
        """Read CSV and produce messages to Kafka"""
        print(f"\n🚀 Starting Kafka Producer")
        print(f"📁 Dataset: {DATASET_PATH}")
        print(f"📮 Topic: {TOPIC_NAME}")
        print(f"⚡ Rate Limit: {RATE_LIMIT if RATE_LIMIT > 0 else 'Unlimited'} msg/sec\n")
        
        # Create topic and initialize producer
        self._create_topic()
        self._init_producer()
        
        try:
            # Read CSV in chunks for memory efficiency
            print(f"📖 Reading dataset...")
            chunk_iter = pd.read_csv(DATASET_PATH, chunksize=BATCH_SIZE)
            
            self.start_time = time.time()
            batch_start = time.time()
            
            for chunk_num, chunk in enumerate(chunk_iter):
                if not self.running:
                    break
                
                # Process each row in the chunk
                for _, row in chunk.iterrows():
                    if not self.running:
                        break
                    
                    message = self._transform_row(row)
                    self._send_message(message)
                    
                    # Rate limiting
                    if RATE_LIMIT > 0:
                        expected_time = self.messages_sent / RATE_LIMIT
                        actual_time = time.time() - self.start_time
                        if actual_time < expected_time:
                            time.sleep(expected_time - actual_time)
                
                # Print stats every batch
                self._print_stats()
                
                # Flush producer periodically
                if chunk_num % 10 == 0:
                    self.producer.flush()
            
            # Final flush
            print("\n\n⏳ Flushing remaining messages...")
            self.producer.flush()
            
            # Final statistics
            elapsed = time.time() - self.start_time
            avg_rate = self.messages_sent / elapsed if elapsed > 0 else 0
            
            print(f"\n✅ Production Complete!")
            print(f"📊 Total Messages: {self.messages_sent:,}")
            print(f"⏱️  Total Time: {elapsed:.2f}s")
            print(f"📈 Average Rate: {avg_rate:.2f} msg/sec")
            
        except FileNotFoundError:
            print(f"❌ Dataset not found: {DATASET_PATH}")
            print(f"💡 Make sure fraudTrain.csv is in the parent directory")
        except Exception as e:
            print(f"\n❌ Error during production: {e}")
            import traceback
            traceback.print_exc()
        finally:
            self._cleanup()
    
    def _cleanup(self):
        """Clean up resources"""
        if self.producer:
            print("\n🧹 Closing producer...")
            self.producer.close()
            print("✅ Producer closed successfully")


def main():
    """Main entry point"""
    producer = TransactionProducer()
    producer.produce_from_csv()


if __name__ == "__main__":
    main()
