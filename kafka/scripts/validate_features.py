"""
Feature Validation Script
Validates that features are correctly stored in Redis
"""
import redis
import json
import random
from typing import Dict, List


def connect_redis(host='localhost', port=6379, db=0):
    """Connect to Redis"""
    try:
        client = redis.Redis(host=host, port=port, db=db, decode_responses=True)
        client.ping()
        print(f"✅ Connected to Redis at {host}:{port}")
        return client
    except redis.ConnectionError as e:
        print(f"❌ Failed to connect to Redis: {e}")
        return None


def get_all_card_keys(client: redis.Redis) -> List[str]:
    """Get all card-related keys"""
    patterns = [
        'card:*:tx_history',
        'card:*:merchants:24h',
        'card:*:stats'
    ]
    
    all_keys = []
    for pattern in patterns:
        keys = client.keys(pattern)
        all_keys.extend(keys)
    
    return all_keys


def validate_transaction_history(client: redis.Redis, key: str) -> Dict:
    """Validate transaction history sorted set"""
    try:
        # Get all transactions
        tx_count = client.zcard(key)
        
        if tx_count == 0:
            return {'valid': True, 'count': 0, 'issues': []}
        
        # Get sample transactions
        sample_txs = client.zrange(key, 0, 4, withscores=True)
        
        issues = []
        for tx_data, score in sample_txs:
            try:
                tx = json.loads(tx_data)
                
                # Validate required fields
                if 'amount' not in tx:
                    issues.append(f"Missing 'amount' field")
                elif tx['amount'] <= 0:
                    issues.append(f"Invalid amount: {tx['amount']}")
                
                if 'timestamp' not in tx:
                    issues.append(f"Missing 'timestamp' field")
                elif tx['timestamp'] != score:
                    issues.append(f"Timestamp mismatch: {tx['timestamp']} != {score}")
                
            except json.JSONDecodeError:
                issues.append(f"Invalid JSON: {tx_data[:50]}")
        
        return {
            'valid': len(issues) == 0,
            'count': tx_count,
            'issues': issues
        }
    except Exception as e:
        return {'valid': False, 'count': 0, 'issues': [str(e)]}


def validate_merchant_set(client: redis.Redis, key: str) -> Dict:
    """Validate unique merchant set"""
    try:
        merchant_count = client.scard(key)
        
        return {
            'valid': True,
            'count': merchant_count,
            'issues': []
        }
    except Exception as e:
        return {'valid': False, 'count': 0, 'issues': [str(e)]}


def validate_card_stats(client: redis.Redis, key: str) -> Dict:
    """Validate card statistics hash"""
    try:
        stats = client.hgetall(key)
        
        issues = []
        
        # Check for expected fields
        if 'avg_amount' in stats:
            try:
                avg_amount = float(stats['avg_amount'])
                if avg_amount <= 0:
                    issues.append(f"Invalid avg_amount: {avg_amount}")
            except ValueError:
                issues.append(f"avg_amount is not a number: {stats['avg_amount']}")
        
        if 'last_tx_timestamp' in stats:
            try:
                timestamp = int(stats['last_tx_timestamp'])
                if timestamp < 946684800:  # Before 2000-01-01
                    issues.append(f"Invalid timestamp: {timestamp}")
            except ValueError:
                issues.append(f"last_tx_timestamp is not an integer: {stats['last_tx_timestamp']}")
        
        return {
            'valid': len(issues) == 0,
            'fields': stats,
            'issues': issues
        }
    except Exception as e:
        return {'valid': False, 'fields': {}, 'issues': [str(e)]}


def main():
    """Main validation function"""
    print("\n" + "="*80)
    print("Feature Validation Report")
    print("="*80 + "\n")
    
    # Connect to Redis
    client = connect_redis()
    if not client:
        return
    
    # Get all card keys
    all_keys = get_all_card_keys(client)
    print(f"\n📊 Total Keys Found: {len(all_keys)}")
    
    if len(all_keys) == 0:
        print("\n⚠️  No feature keys found in Redis!")
        print("💡 Make sure the consumer has processed some transactions.")
        return
    
    # Group keys by type
    tx_history_keys = [k for k in all_keys if ':tx_history' in k]
    merchant_keys = [k for k in all_keys if ':merchants:24h' in k]
    stats_keys = [k for k in all_keys if ':stats' in k]
    
    print(f"  - Transaction History Keys: {len(tx_history_keys)}")
    print(f"  - Merchant Set Keys: {len(merchant_keys)}")
    print(f"  - Stats Keys: {len(stats_keys)}")
    
    # Sample validation
    sample_size = min(10, len(all_keys))
    print(f"\n🔍 Validating {sample_size} random samples...\n")
    
    validation_results = {
        'tx_history': {'passed': 0, 'failed': 0},
        'merchants': {'passed': 0, 'failed': 0},
        'stats': {'passed': 0, 'failed': 0}
    }
    
    # Validate transaction history
    for key in random.sample(tx_history_keys, min(sample_size, len(tx_history_keys))):
        result = validate_transaction_history(client, key)
        if result['valid']:
            validation_results['tx_history']['passed'] += 1
        else:
            validation_results['tx_history']['failed'] += 1
            print(f"❌ {key}: {result['issues']}")
    
    # Validate merchant sets
    for key in random.sample(merchant_keys, min(sample_size, len(merchant_keys))):
        result = validate_merchant_set(client, key)
        if result['valid']:
            validation_results['merchants']['passed'] += 1
        else:
            validation_results['merchants']['failed'] += 1
            print(f"❌ {key}: {result['issues']}")
    
    # Validate stats
    for key in random.sample(stats_keys, min(sample_size, len(stats_keys))):
        result = validate_card_stats(client, key)
        if result['valid']:
            validation_results['stats']['passed'] += 1
        else:
            validation_results['stats']['failed'] += 1
            print(f"❌ {key}: {result['issues']}")
    
    # Print summary
    print("\n" + "="*80)
    print("Validation Summary")
    print("="*80)
    
    total_passed = sum(r['passed'] for r in validation_results.values())
    total_failed = sum(r['failed'] for r in validation_results.values())
    
    print(f"\n✅ Passed: {total_passed}")
    print(f"❌ Failed: {total_failed}")
    
    for key_type, results in validation_results.items():
        print(f"\n{key_type.upper()}:")
        print(f"  Passed: {results['passed']}")
        print(f"  Failed: {results['failed']}")
    
    if total_failed == 0:
        print("\n🎉 All validations passed!")
    else:
        print(f"\n⚠️  {total_failed} validations failed. Check logs above for details.")
    
    print("\n" + "="*80 + "\n")


if __name__ == '__main__':
    main()
