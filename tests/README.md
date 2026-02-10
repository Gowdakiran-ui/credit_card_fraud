# ML Feature Pipeline - Testing Suite

## Overview

Comprehensive testing and validation suite for the production ML feature pipeline. This suite ensures data integrity, prevents silent failures, and validates training-serving consistency.

## Test Structure

```
tests/
├── __init__.py
├── pytest.ini                      # Pytest configuration
├── requirements-test.txt           # Test dependencies
├── context.md                      # Pipeline context and assumptions
├── process.md                      # Test coverage and failure modes
├── test_schema_validation.py       # Layer 1: Input validation
├── test_preprocessing.py           # Layer 2: Data transformations
├── test_feature_validation.py      # Layer 3: Feature correctness
├── test_time_consistency.py        # Layer 4: Temporal correctness
├── test_offline_online_parity.py   # Layer 5: Store consistency
└── test_end_to_end.py             # Layer 6: Pipeline integration
```

## Quick Start

### Install Dependencies

```bash
pip install -r tests/requirements-test.txt
```

### Run All Tests

```bash
# From project root
pytest tests/ -v

# With coverage report
pytest tests/ --cov=kafka/src --cov-report=html

# Run specific test file
pytest tests/test_schema_validation.py -v

# Run specific test
pytest tests/test_schema_validation.py::TestSchemaValidation::test_all_required_fields_present -v
```

### Run by Category

```bash
# Fast unit tests only
pytest tests/ -m "not slow" -v

# Integration tests
pytest tests/ -m integration -v

# End-to-end tests
pytest tests/ -m e2e -v
```

## Test Layers

### Layer 1: Schema Validation
**File**: `test_schema_validation.py`  
**Purpose**: Validate input data at pipeline entry point

- ✅ Required fields exist
- ✅ Correct data types
- ✅ Empty/malformed inputs handled safely
- ✅ Bad data logged and rejected (never crashes)
- ✅ Range validation (timestamps, coordinates, amounts)

**Key Tests**: 40+ tests covering all validation scenarios

### Layer 2: Preprocessing
**File**: `test_preprocessing.py`  
**Purpose**: Ensure data transformations preserve semantic meaning

- ✅ Input → output semantic preservation
- ✅ No empty strings after preprocessing
- ✅ Unicode, emojis, special characters handled
- ✅ Length constraints enforced
- ✅ Deterministic transformations

**Key Tests**: 35+ tests covering normalization and edge cases

### Layer 3: Feature Validation
**File**: `test_feature_validation.py`  
**Purpose**: Validate extracted features are correct and safe

- ✅ Feature existence (no missing values)
- ✅ No NaNs or infinite values
- ✅ Feature value ranges are sane
- ✅ Feature computation is deterministic
- ✅ Edge cases (first transaction, high velocity)

**Key Tests**: 30+ tests covering all feature types

### Layer 4: Time & Consistency
**File**: `test_time_consistency.py`  
**Purpose**: Ensure temporal correctness and prevent data leakage

- ✅ Point-in-time correctness (no future leakage)
- ✅ Feature freshness checks
- ✅ Idempotency (duplicate inputs don't corrupt state)
- ✅ Timestamp ordering preserved
- ✅ Velocity windows strictly enforced

**Key Tests**: 20+ tests covering temporal edge cases

### Layer 5: Offline-Online Parity
**File**: `test_offline_online_parity.py`  
**Purpose**: Prevent training-serving skew

- ✅ Same entity from Feast offline and Redis online
- ✅ Values compared within tolerance
- ✅ Schema consistency across stores
- ✅ Type conversion consistency
- ✅ Error handling parity

**Key Tests**: 15+ tests covering store consistency

### Layer 6: End-to-End
**File**: `test_end_to_end.py`  
**Purpose**: Validate complete pipeline integration

- ✅ Kafka → preprocessing → features → Redis
- ✅ Entity IDs, timestamps, values verified
- ✅ No data loss or corruption
- ✅ Error recovery
- ✅ Performance validation

**Key Tests**: 15+ tests covering full pipeline flow

## Failure Modes Covered

### Silent Failures → Made Visible
- Missing required fields → `ValueError` raised
- Invalid data types → `ValueError` raised
- Out-of-range values → `ValueError` raised
- NaN/infinite values → Handled with safe defaults
- Redis failures → Return defaults, log error

### Data Corruption → Prevented
- Negative amounts → Converted to absolute
- Extreme amounts → Clipped to percentile
- Future timestamps → Rejected
- Invalid coordinates → Rejected
- Duplicate processing → Idempotent

### Training-Serving Skew → Eliminated
- Point-in-time correctness enforced
- No future data leakage
- Deterministic features
- Offline-online parity validated

## CI/CD Integration

### GitHub Actions Example

```yaml
name: Test ML Pipeline

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - uses: actions/setup-python@v2
        with:
          python-version: '3.9'
      - name: Install dependencies
        run: |
          pip install -r requirements.txt
          pip install -r tests/requirements-test.txt
      - name: Run tests
        run: pytest tests/ -v --cov=kafka/src --cov-report=xml
      - name: Upload coverage
        uses: codecov/codecov-action@v2
```

## Performance Benchmarks

- **Schema validation**: < 1ms per transaction
- **Preprocessing**: < 5ms per transaction
- **Feature extraction**: < 20ms per transaction (with Redis)
- **End-to-end pipeline**: < 100ms per transaction

## Adding New Tests

When adding new features to the pipeline:

1. **Update schema tests** if new required fields added
2. **Update preprocessing tests** if new transformations added
3. **Update feature tests** if new features computed
4. **Update time tests** if new temporal logic added
5. **Update parity tests** if new stores integrated
6. **Update e2e tests** if pipeline flow changes
7. **Update `context.md`** with new assumptions
8. **Update `process.md`** with new failure modes

## Troubleshooting

### Tests fail with import errors
```bash
# Ensure you're running from project root
cd c:\books\credit_card
pytest tests/ -v
```

### Tests fail with Redis connection errors
```bash
# Tests use mocked Redis by default
# If you see real Redis errors, check test fixtures
```

### Coverage report not generated
```bash
# Install pytest-cov
pip install pytest-cov

# Run with coverage
pytest tests/ --cov=kafka/src --cov-report=html
```

## Documentation

- **`context.md`**: Pipeline architecture, feature definitions, validation assumptions
- **`process.md`**: Test coverage summary, failure modes, execution instructions

## Contact

For questions about the testing suite, see the main project README or consult the team documentation.
