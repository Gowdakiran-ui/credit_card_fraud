"""
Redis Feature Populator
Utility script to populate Redis with sample features for testing
"""
import redis
import random
from typing import List
import argparse

def generate_card_features(card_id: str) -> dict:
    """Generate realistic card-level features"""
    return {
        'tx_count_10m': random.randint(0, 5),
        'tx_count_1h': random.randint(0, 15),
        'tx_count_24h': random.randint(0, 50),
        'total_amount_10m': round(random.uniform(0, 500), 2),
        'total_amount_1h': round(random.uniform(0, 1500), 2),
        'total_amount_24h': round(random.uniform(0, 5000), 2),
        'unique_merchants_24h': random.randint(1, 10),
        'avg_tx_amount_30d': round(random.uniform(30, 200), 2),
        'last_tx_timestamp': int(1675890123 - random.randint(0, 3600)),
        'is_new_card': random.choice([0, 0, 0, 1])  # 25% new cards
    }

def generate_merchant_features(merchant_id: str) -> dict:
    """Generate realistic merchant-level features"""
    # Most merchants are low risk
    risk_category = random.choices(
        ['low', 'medium', 'high'],
        weights=[0.8, 0.15, 0.05]
    )[0]
    
    if risk_category == 'low':
        risk_score = random.uniform(0.1, 0.4)
        fraud_rate = random.uniform(0.001, 0.005)
    elif risk_category == 'medium':
        risk_score = random.uniform(0.4, 0.7)
        fraud_rate = random.uniform(0.005, 0.02)
    else:  # high
        risk_score = random.uniform(0.7, 0.95)
        fraud_rate = random.uniform(0.02, 0.1)
    
    return {
        'risk_score': round(risk_score, 3),
        'fraud_rate': round(fraud_rate, 4),
        'total_transactions': random.randint(100, 10000)
    }

def populate_redis(
    host: str = 'localhost',
    port: int = 6379,
    num_cards: int = 1000,
    num_merchants: int = 500
):
    """
    Populate Redis with sample features
    
    Args:
        host: Redis host
        port: Redis port
        num_cards: Number of card features to generate
        num_merchants: Number of merchant features to generate
    """
    print(f"🔌 Connecting to Redis at {host}:{port}...")
    
    try:
        r = redis.Redis(host=host, port=port, decode_responses=True)
        r.ping()
        print("✅ Connected to Redis\n")
    except redis.ConnectionError as e:
        print(f"❌ Failed to connect to Redis: {e}")
        print("💡 Make sure Redis is running (docker-compose up)")
        return
    
    # Populate card features
    print(f"📝 Generating {num_cards} card features...")
    for i in range(num_cards):
        card_id = f"card_{i:08d}"
        features = generate_card_features(card_id)
        key = f"features:card:{card_id}"
        r.hset(key, mapping=features)
        r.expire(key, 2592000)  # 30 days TTL
        
        if (i + 1) % 100 == 0:
            print(f"  Progress: {i + 1}/{num_cards} cards")
    
    print(f"✅ Created {num_cards} card features\n")
    
    # Populate merchant features
    print(f"📝 Generating {num_merchants} merchant features...")
    for i in range(num_merchants):
        merchant_id = f"merchant_{i:06d}"
        features = generate_merchant_features(merchant_id)
        key = f"features:merchant:{merchant_id}"
        r.hset(key, mapping=features)
        r.expire(key, 2592000)  # 30 days TTL
        
        if (i + 1) % 100 == 0:
            print(f"  Progress: {i + 1}/{num_merchants} merchants")
    
    print(f"✅ Created {num_merchants} merchant features\n")
    
    # Verify
    total_keys = r.dbsize()
    print(f"📊 Total keys in Redis: {total_keys}")
    
    # Show sample
    print("\n📋 Sample card features:")
    sample_card = r.hgetall("features:card:card_00000000")
    for k, v in sample_card.items():
        print(f"  {k}: {v}")
    
    print("\n📋 Sample merchant features:")
    sample_merchant = r.hgetall("features:merchant:merchant_000000")
    for k, v in sample_merchant.items():
        print(f"  {k}: {v}")
    
    print("\n✅ Redis population complete!")

def main():
    parser = argparse.ArgumentParser(description='Populate Redis with sample features')
    parser.add_argument('--host', default='localhost', help='Redis host')
    parser.add_argument('--port', type=int, default=6379, help='Redis port')
    parser.add_argument('--cards', type=int, default=1000, help='Number of cards')
    parser.add_argument('--merchants', type=int, default=500, help='Number of merchants')
    
    args = parser.parse_args()
    
    populate_redis(
        host=args.host,
        port=args.port,
        num_cards=args.cards,
        num_merchants=args.merchants
    )

if __name__ == "__main__":
    main()
