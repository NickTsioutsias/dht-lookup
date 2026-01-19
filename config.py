"""
Project configuration and constants.

Centralizes all configurable parameters for the DHT implementation.
"""

import os

# -----------------------------------------------------------------------------
# Directory Paths
# -----------------------------------------------------------------------------

# Project root directory (where this file is located)
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))

# Data directory
DATA_DIR = os.path.join(PROJECT_ROOT, "data")

# Results directory (for plots and experiment outputs)
RESULTS_DIR = os.path.join(PROJECT_ROOT, "results")

# -----------------------------------------------------------------------------
# Dataset Configuration
# -----------------------------------------------------------------------------

# Dataset filename
DATASET_FILENAME = "data_movies_clean.csv"

# Full path to dataset
DATASET_PATH = os.path.join(DATA_DIR, DATASET_FILENAME)

# Column to use as DHT key
DATASET_KEY_COLUMN = "title"

# Columns to include as value (None means all columns)
DATASET_VALUE_COLUMNS = None

# -----------------------------------------------------------------------------
# DHT Common Configuration
# -----------------------------------------------------------------------------

# Hash bit size (SHA-1 produces 160 bits, but smaller is faster for testing)
# Using 160 for production, can reduce to 32 or 64 for faster testing
HASH_BIT_SIZE = 160

# Hash space size (2^HASH_BIT_SIZE)
HASH_SPACE_SIZE = 2 ** HASH_BIT_SIZE

# Default number of nodes in the network
DEFAULT_NODE_COUNT = 16

# -----------------------------------------------------------------------------
# Chord Configuration
# -----------------------------------------------------------------------------

# Finger table size (equals HASH_BIT_SIZE in standard Chord)
CHORD_FINGER_TABLE_SIZE = HASH_BIT_SIZE

# -----------------------------------------------------------------------------
# Pastry Configuration
# -----------------------------------------------------------------------------

# Base for Pastry (2^b where b is bits per digit)
# Base 16 means 4 bits per digit, which is standard
PASTRY_B = 4
PASTRY_BASE = 2 ** PASTRY_B

# Leaf set size (L/2 on each side)
# Total leaf set size is 2 * PASTRY_LEAF_SIZE
PASTRY_LEAF_SIZE = 8

# -----------------------------------------------------------------------------
# Logging Configuration
# -----------------------------------------------------------------------------

# Log level: DEBUG, INFO, WARNING, ERROR, CRITICAL
LOG_LEVEL = "INFO"

# Log to file (in addition to console)
LOG_TO_FILE = False

# Log file path (only used if LOG_TO_FILE is True)
LOG_FILE_PATH = os.path.join(PROJECT_ROOT, "dht.log")
