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

# Finger table size
# Theory: Need log2(N) fingers for N nodes. Using 20 supports up to 2^20 = 1M nodes.
# This is practical for real networks while avoiding the overhead of 160 fingers.
CHORD_FINGER_TABLE_SIZE = 20

# -----------------------------------------------------------------------------
# Pastry Configuration
# -----------------------------------------------------------------------------

# Base for Pastry (2^b where b is bits per digit)
# Base 16 means 4 bits per digit, which is standard
PASTRY_B = 4
PASTRY_BASE = 2 ** PASTRY_B

# Number of rows in routing table
# Theory: Need log_base(N) rows for N nodes. Using 5 rows supports up to 16^5 = 1M nodes.
# This is practical for real networks while avoiding 40 empty rows.
PASTRY_ROUTING_TABLE_ROWS = 5

# Leaf set size (L/2 on each side)
# Total leaf set size is 2 * PASTRY_LEAF_SIZE
PASTRY_LEAF_SIZE = 8

# -----------------------------------------------------------------------------
# B+ Tree Local Indexing Configuration
# -----------------------------------------------------------------------------

# B+ tree order (max children per internal node).
# Higher order = shallower tree but wider nodes.
# order=32 means each node holds up to 31 keys per node.
BPLUS_TREE_ORDER = 32

# -----------------------------------------------------------------------------
# Logging Configuration
# -----------------------------------------------------------------------------

# Log level: DEBUG, INFO, WARNING, ERROR, CRITICAL
LOG_LEVEL = "INFO"

# Log to file (in addition to console)
LOG_TO_FILE = False

# Log file path (only used if LOG_TO_FILE is True)
LOG_FILE_PATH = os.path.join(PROJECT_ROOT, "dht.log")
