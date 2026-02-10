@echo off
REM Test Execution Script for ML Feature Pipeline
REM Run from project root: tests\run_tests.bat

echo ========================================
echo ML Feature Pipeline - Test Suite
echo ========================================
echo.

REM Check if pytest is installed
python -c "import pytest" 2>nul
if errorlevel 1 (
    echo [ERROR] pytest not installed
    echo Installing test dependencies...
    pip install -r tests\requirements-test.txt
    echo.
)

REM Run tests based on argument
if "%1"=="all" goto run_all
if "%1"=="schema" goto run_schema
if "%1"=="preprocessing" goto run_preprocessing
if "%1"=="features" goto run_features
if "%1"=="time" goto run_time
if "%1"=="parity" goto run_parity
if "%1"=="e2e" goto run_e2e
if "%1"=="coverage" goto run_coverage
if "%1"=="quick" goto run_quick

REM Default: show help
:show_help
echo Usage: run_tests.bat [option]
echo.
echo Options:
echo   all            - Run all tests
echo   schema         - Run schema validation tests
echo   preprocessing  - Run preprocessing tests
echo   features       - Run feature validation tests
echo   time           - Run time consistency tests
echo   parity         - Run offline-online parity tests
echo   e2e            - Run end-to-end tests
echo   coverage       - Run all tests with coverage report
echo   quick          - Run quick smoke tests
echo.
echo Example: run_tests.bat all
goto end

:run_all
echo Running all tests...
pytest tests\ -v
goto end

:run_schema
echo Running schema validation tests...
pytest tests\test_schema_validation.py -v
goto end

:run_preprocessing
echo Running preprocessing tests...
pytest tests\test_preprocessing.py -v
goto end

:run_features
echo Running feature validation tests...
pytest tests\test_feature_validation.py -v
goto end

:run_time
echo Running time consistency tests...
pytest tests\test_time_consistency.py -v
goto end

:run_parity
echo Running offline-online parity tests...
pytest tests\test_offline_online_parity.py -v
goto end

:run_e2e
echo Running end-to-end tests...
pytest tests\test_end_to_end.py -v
goto end

:run_coverage
echo Running all tests with coverage...
pytest tests\ --cov=kafka\src --cov-report=html --cov-report=term
echo.
echo Coverage report generated in htmlcov\index.html
goto end

:run_quick
echo Running quick smoke tests...
pytest tests\ -v -k "test_all_required_fields_present or test_preprocessing_preserves_transaction_id or test_no_nan_values or test_no_future_transactions_in_history or test_card_features_schema_consistency or test_entity_id_preserved_across_pipeline"
goto end

:end
echo.
echo ========================================
echo Test execution complete
echo ========================================
