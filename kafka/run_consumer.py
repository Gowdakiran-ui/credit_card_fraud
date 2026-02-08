"""
Kafka Consumer Entry Point
"""
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from src.pipeline.consumer import main

if __name__ == "__main__":
    main()
