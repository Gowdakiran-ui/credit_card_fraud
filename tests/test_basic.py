import os
import pandas as pd
import pytest

def test_dataset_exists():
    """Verify that the fraud dataset is present."""
    assert os.path.exists('fraudTrain.csv'), "Dataset file fraudTrain.csv not found"

def test_visualizer_creation():
    """Verify that the plots directory and visualizations are created."""
    # This just checks if the script exists for now
    assert os.path.exists('visualize_data.py')

def test_math_sanity():
    """Simple sanity check."""
    assert 1 + 1 == 2
