"""
Pytest configuration and fixtures
"""
import sys
from pathlib import Path

# Add project paths to sys.path
project_root = Path(__file__).parent.parent
kafka_src = project_root / "kafka" / "src"
feature_repo = project_root / "feature_repo"

sys.path.insert(0, str(kafka_src))
sys.path.insert(0, str(feature_repo))
sys.path.insert(0, str(project_root))
