# DHT Lookup: Chord and Pastry Implementation

Implementation and experimental evaluation of Chord and Pastry Distributed Hash Table (DHT) protocols for the course "Decentralized Data Engineering and Technologies".

## Project Overview

This project implements two fundamental DHT protocols:
- **Chord**: Ring-based DHT with finger table routing (O(log N) lookups)
- **Pastry**: Prefix-based DHT with routing table and leaf set (O(log₁₆ N) lookups)

Both implementations support all required operations: Build, Insert, Delete, Update, Lookup, Node Join, and Node Leave. The project includes a comprehensive benchmark system for performance comparison.

### Dataset

Uses the [Movies Metadata Dataset](https://www.kaggle.com/datasets/mustafasayed1181/movies-metadata-cleaned-dataset-19002025) from Kaggle containing ~946,000 movies. Movie titles are used as DHT keys, with full movie metadata as values.

## Quick Start

### Prerequisites

- Python 3.10+
- pandas

### Installation

```bash
# Clone the repository
git clone <repository-url>
cd dht-lookup

# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### Running the Demo

```bash
python main.py
```

This demonstrates all DHT operations with a small sample of movies.

### Running Benchmarks

```bash
# Quick benchmark (smaller parameters, ~3 seconds)
python -m evaluation.benchmark --quick

# Full benchmark (larger parameters, ~minutes)
python -m evaluation.benchmark
```

Results are saved to `results/benchmark_results.csv`.

## Project Structure

```
dht-lookup/
├── main.py                     # Demo entry point
├── config.py                   # Configuration parameters
├── requirements.txt            # Python dependencies
│
├── src/                        # Source code
│   ├── common/                 # Shared utilities
│   │   ├── hashing.py          # SHA-1 consistent hashing
│   │   ├── data_loader.py      # Movies dataset loading
│   │   └── logger.py           # Logging configuration
│   │
│   └── dht/                    # DHT implementations
│       ├── base_node.py        # Abstract base class for nodes
│       ├── base_network.py     # Abstract base class for networks
│       │
│       ├── chord/              # Chord protocol
│       │   ├── node.py         # ChordNode implementation
│       │   ├── chord_network.py # ChordNetwork manager
│       │   └── finger_table.py # Finger table data structure
│       │
│       └── pastry/             # Pastry protocol
│           ├── node.py         # PastryNode implementation
│           ├── pastry_network.py # PastryNetwork manager
│           └── routing_table.py # Routing table and leaf set
│
├── evaluation/                 # Benchmarking system
│   ├── benchmark.py            # Experimental evaluation
│   ├── visualize.py            # Plot generation
│   └── metrics.py              # Metrics collection
│
├── data/                       # Dataset
│   └── data_movies_clean.csv   # Movies dataset (~946K records)
│
├── results/                    # Benchmark outputs
│   └── benchmark_results.csv   # Generated results
│
├── tests/                      # Test suite
│   ├── test_refactoring.py     # Comprehensive tests
│   └── verify_concurrent_lookup.py # Concurrent lookup verification
│
└── docs/                       # Documentation
    └── implementation_notes.md # Technical notes
```

## Architecture

### Class Hierarchy

```
BaseNode (abstract)
├── ChordNode
└── PastryNode

BaseNetwork (abstract)
├── ChordNetwork
└── PastryNetwork
```

### BaseNode (Abstract)

Defines the interface for all DHT nodes:

**Concrete methods** (shared implementation):
- `insert(key, value)` → `(success, hops)`
- `lookup(key)` → `(value, hops)`
- `update(key, value)` → `(success, hops)`
- `delete(key)` → `(success, hops)`

**Abstract methods** (protocol-specific):
- `find_successor(key_id)` → `(node, hops)` - Core routing logic
- `join(existing_node)` → `hops` - Network join protocol
- `leave()` → `hops` - Network leave protocol

### BaseNetwork (Abstract)

Defines the interface for network management:

**Concrete methods**:
- `bulk_insert(items)` - Insert multiple items
- `bulk_lookup(keys)` - Look up multiple keys
- `bulk_delete(keys)` - Delete multiple keys
- `concurrent_lookup(keys)` - Parallel lookups using threads
- `concurrent_insert(items)` - Parallel inserts

**Abstract methods**:
- `create_node(identifier)` - Factory method
- `add_node(node)` - Add node to network (triggers join)
- `remove_node(identifier)` - Remove node (triggers leave)
- `build_network(num_nodes)` - Build complete network

## Chord Protocol

### Overview

Chord organizes nodes in a logical ring of size 2^m. Each node maintains a finger table for O(log N) routing.

### Key Components

**Finger Table**: Array of m entries where entry i points to `successor(n + 2^i)`.

**Routing**: To find a key, forward to the closest preceding node in the finger table until the responsible node is found.

**Key Responsibility**: A key K is stored at the first node whose ID ≥ K (successor of K).

### Configuration

```python
CHORD_FINGER_TABLE_SIZE = 20  # Supports up to 2^20 = 1M nodes
```

### Hop Counting

- **Join**: O(m × log N) - Initialize finger table + update others
- **Leave**: O(1) - Notify immediate neighbors only (lazy approach)
- **Lookup/Insert/Delete/Update**: O(log N) routing + 1 hop for operation

## Pastry Protocol

### Overview

Pastry uses prefix-based routing with hexadecimal node IDs. Routing converges by matching progressively longer prefixes.

### Key Components

**Routing Table**: 2D table where row i contains nodes sharing i-digit prefix with us. Column j contains node whose (i+1)th digit is j.

**Leaf Set**: L closest nodes by numeric ID (L/2 smaller, L/2 larger). Used for final routing step and key responsibility.

**Key Responsibility**: A key K is stored at the node with numerically closest ID to K.

### Configuration

```python
PASTRY_B = 4                    # 4 bits per digit (base 16)
PASTRY_ROUTING_TABLE_ROWS = 5   # Supports up to 16^5 = 1M nodes
PASTRY_LEAF_SIZE = 8            # 8 nodes on each side of leaf set
```

### Hop Counting

- **Join**: O(log₁₆ N) routing + notify neighbors
- **Leave**: O(L) - Notify all leaf set neighbors
- **Lookup/Insert/Delete/Update**: O(log₁₆ N) routing + 1 hop for operation

## Hop Counting Model

All implementations count **every network message** as a hop:

1. **Routing hops**: Messages forwarded through intermediate nodes
2. **Communication hops**: Direct messages to known nodes (e.g., final store request)

This provides a complete picture of network cost for fair comparison.

Example lookup:
```
Client at node_A:
  → Route to node_B (hop 1)
  → Route to node_C (hop 2)
  → node_C is responsible, send get request (hop 3 if node_C ≠ node_A)
Total: 3 hops
```

## Benchmark System

### Parameters

| Parameter | Full | Quick |
|-----------|------|-------|
| Network sizes | [8, 16, 32, 64, 128] | [8, 16, 32] |
| Movies to insert | 1000 | 100 |
| Operations per test | 500 | 50 |
| Nodes for join/leave | 10 | 5 |

### Metrics Collected

For each operation (build, insert, lookup, update, delete, node_join, node_leave):
- Count of operations
- Total hops
- Mean hops
- Min/Max hops
- Standard deviation

### Output

CSV file with columns:
```
operation, protocol, network_size, count, total_hops, min_hops, max_hops, mean_hops, std_hops
```

## Sample Results

From quick benchmark (8, 16, 32 nodes):

| Operation | Chord (mean) | Pastry (mean) | Winner |
|-----------|-------------|---------------|--------|
| Build (per node) | 91-339 | 5-12 | Pastry |
| Insert | 4.5-15.3 | 1.8-2.8 | Pastry |
| Lookup | 4.4-17.0 | 1.7-2.7 | Pastry |
| Update | 4.3-17.1 | 1.8-2.9 | Pastry |
| Delete | 4.4-17.5 | 1.7-2.8 | Pastry |
| Node Join | 226-718 | 12-15 | Pastry |
| Node Leave | 2 | 9-13 | Chord |

**Note**: Chord's higher hop counts in small networks are expected behavior. See [docs/implementation_notes.md](docs/implementation_notes.md) for detailed explanation.

## API Usage

### Basic Example

```python
from src.dht.chord.chord_network import ChordNetwork
from src.dht.pastry.pastry_network import PastryNetwork

# Create Chord network
chord = ChordNetwork()
chord.build_network(16)  # 16 nodes

# Insert data
success, hops = chord.insert("The Matrix", {"year": 1999, "rating": 8.7})

# Lookup
value, hops = chord.lookup("The Matrix")

# Update
success, hops = chord.update("The Matrix", {"year": 1999, "rating": 9.0})

# Delete
success, hops = chord.delete("The Matrix")

# Add node dynamically
new_node = chord.create_node("new_node")
join_hops = chord.add_node(new_node)

# Remove node
success, leave_hops = chord.remove_node("node_5")

# Cleanup
chord.clear()
```

### Concurrent Operations

```python
# Concurrent lookup of K movies
titles = ["Movie1", "Movie2", "Movie3", "Movie4", "Movie5"]
results = chord.concurrent_lookup(titles, max_workers=5)

print(f"Found: {results['found_count']}/{results['total_keys']}")
print(f"Average hops: {results['average_hops']:.2f}")

# Access individual results
for title, (value, hops) in results['results'].items():
    if value:
        print(f"{title}: popularity={value['popularity']}, hops={hops}")
```

### Loading Movie Data

```python
from src.common.data_loader import get_sample_movies, get_movie_by_title

# Get random sample
movies = get_sample_movies(100, seed=42)

# Insert into network
for movie in movies:
    chord.insert(movie.title, movie.to_dict())

# Get specific movie
movie = get_movie_by_title("The Matrix")
if movie:
    print(f"Budget: ${movie.budget:,}")
```

## Testing

```bash
# Run all tests
python tests/test_refactoring.py

# Verify concurrent lookup with movie data
python tests/verify_concurrent_lookup.py
```

## Configuration

All configurable parameters are in `config.py`:

```python
# Hash space
HASH_BIT_SIZE = 160              # SHA-1 produces 160 bits

# Chord
CHORD_FINGER_TABLE_SIZE = 20     # Finger table entries

# Pastry
PASTRY_B = 4                     # Bits per digit (base 16)
PASTRY_ROUTING_TABLE_ROWS = 5    # Routing table rows
PASTRY_LEAF_SIZE = 8             # Leaf set size per side

# Dataset
DATASET_PATH = "data/data_movies_clean.csv"
DATASET_KEY_COLUMN = "title"
```

## Known Limitations

1. **In-memory simulation**: No actual network sockets; uses method calls
2. **No failure handling**: Assumes nodes don't fail unexpectedly
3. **No persistence**: Data is lost when network is cleared
4. **Chord in small networks**: Performance degrades due to sparse finger tables (see docs/implementation_notes.md)

## References

- [Chord: A Scalable Peer-to-peer Lookup Service for Internet Applications](https://pdos.csail.mit.edu/papers/chord:sigcomm01/chord_sigcomm.pdf) - Stoica et al., 2001
- [Pastry: Scalable, decentralized object location and routing for large-scale peer-to-peer systems](https://rowstron.azurewebsites.net/PAST/pastry.pdf) - Rowstron & Druschel, 2001

## Authors

Developed for the course "Decentralized Data Engineering and Technologies" at University of Patras.

## License

Academic use only.
