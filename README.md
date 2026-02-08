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
- pandas, matplotlib


### One-Click Run (Demo + Benchmark + Plots)

```bash
# Full pipeline (demo, full benchmark, generate plots)
python run_all.py

# Quick pipeline (demo, quick benchmark, generate plots)
python run_all.py --quick
```

### Running Steps Individually

```bash
# 1. Demo: shows all DHT operations with a small sample of movies
python main.py

# 2. Benchmark: experimental evaluation across multiple network sizes
python -m evaluation.benchmark          # Full (network sizes: 8-128)
python -m evaluation.benchmark --quick  # Quick (network sizes: 8-32)

# 3. Visualization: generate comparison plots from benchmark results
python -m evaluation.visualize
```

Results are saved to `results/benchmark_results.csv`. Plots are saved to `results/`.

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
CHORD_FINGER_TABLE_SIZE = HASH_BIT_SIZE  # m=160 entries for full O(log N) routing
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

From full benchmark (8 to 128 nodes), mean hops per operation:

| Operation | Chord (8→128 nodes) | Pastry (8→128 nodes) | Notes |
|-----------|---------------------|----------------------|-------|
| Insert | 2.4 → 4.4 | 1.8 → 7.1 | Chord wins at large N |
| Lookup | 2.4 → 4.4 | 1.8 → 6.8 | Chord wins at large N |
| Update | 2.4 → 4.4 | 1.7 → 7.1 | Chord wins at large N |
| Delete | 2.4 → 4.4 | 1.8 → 7.2 | Chord wins at large N |
| Node Join | 6.6 → 7.1 | 13.7 → 17.8 | Chord always cheaper |
| Node Leave | 2.0 → 2.0 | 9.9 → 15.5 | Chord O(1) vs Pastry O(L) |

Both protocols exhibit O(log N) scaling for CRUD operations. Chord uses a 160-entry finger table for precise routing; Pastry benefits from its leaf set at small network sizes but grows faster at larger sizes due to routing table sparsity. Chord has significantly cheaper join/leave due to its lazy stabilization approach.

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
CHORD_FINGER_TABLE_SIZE = HASH_BIT_SIZE  # m=160 for full ring coverage

# Pastry
PASTRY_B = 4                     # Bits per digit (base 16)
PASTRY_ROUTING_TABLE_ROWS = 5    # Routing table rows
PASTRY_LEAF_SIZE = 8             # Leaf set size per side

# Dataset
DATASET_PATH = "data/data_movies_clean.csv"
DATASET_KEY_COLUMN = "title"
```

## Known Limitations

1. **In-memory simulation**: No actual network sockets; uses method calls between objects
2. **No failure handling**: Assumes nodes don't fail unexpectedly (graceful leave only)
3. **No persistence**: Data is lost when network is cleared

## References

- [Chord: A Scalable Peer-to-peer Lookup Service for Internet Applications](https://pdos.csail.mit.edu/papers/chord:sigcomm01/chord_sigcomm.pdf) - Stoica et al., 2001
- [Pastry: Scalable, decentralized object location and routing for large-scale peer-to-peer systems](https://rowstron.azurewebsites.net/PAST/pastry.pdf) - Rowstron & Druschel, 2001

## Authors

Developed for the course "Decentralized Data Engineering and Technologies" at University of Patras.

## License

Academic use only.
