"""
Benchmark system for DHT experimental evaluation.

Runs comprehensive experiments comparing Chord and Pastry DHTs across:
- Multiple network sizes
- All operations (insert, lookup, update, delete, join, leave)
- Multiple repetitions for statistical significance

Outputs results to CSV for visualization.

Usage:
    python -m evaluation.benchmark
    python -m evaluation.benchmark --quick  # Quick run with smaller parameters
"""

import argparse
import csv
import os
import sys
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Tuple
import statistics

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import config
from src.common.data_loader import get_sample_movies, clear_cache
from src.common.logger import get_logger
from src.dht.chord.chord_network import ChordNetwork
from src.dht.pastry.pastry_network import PastryNetwork

logger = get_logger(__name__)


# =============================================================================
# Configuration
# =============================================================================

@dataclass
class BenchmarkConfig:
    """Configuration for benchmark runs."""
    # Network sizes to test
    network_sizes: List[int] = field(default_factory=lambda: [8, 16, 32, 64, 128])

    # Number of movies to insert
    num_movies: int = 1000

    # Number of operations to measure for lookup/update/delete
    num_operations: int = 500

    # Number of nodes to add/remove for join/leave tests
    num_join_leave: int = 10

    # Random seed for reproducibility
    seed: int = 42

    # Output directory
    output_dir: str = config.RESULTS_DIR


@dataclass
class QuickBenchmarkConfig(BenchmarkConfig):
    """Smaller configuration for quick testing."""
    network_sizes: List[int] = field(default_factory=lambda: [8, 16, 32])
    num_movies: int = 100
    num_operations: int = 50
    num_join_leave: int = 5


# =============================================================================
# Statistics Helper
# =============================================================================

@dataclass
class OperationStats:
    """Statistics for a single operation type."""
    operation: str
    protocol: str
    network_size: int
    count: int
    total_hops: int
    min_hops: int
    max_hops: int
    mean_hops: float
    std_hops: float

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for CSV output."""
        return {
            "operation": self.operation,
            "protocol": self.protocol,
            "network_size": self.network_size,
            "count": self.count,
            "total_hops": self.total_hops,
            "min_hops": self.min_hops,
            "max_hops": self.max_hops,
            "mean_hops": round(self.mean_hops, 4),
            "std_hops": round(self.std_hops, 4),
        }


def compute_stats(
    operation: str,
    protocol: str,
    network_size: int,
    hops_list: List[int]
) -> OperationStats:
    """Compute statistics from a list of hop counts."""
    if not hops_list:
        return OperationStats(
            operation=operation,
            protocol=protocol,
            network_size=network_size,
            count=0,
            total_hops=0,
            min_hops=0,
            max_hops=0,
            mean_hops=0.0,
            std_hops=0.0,
        )

    return OperationStats(
        operation=operation,
        protocol=protocol,
        network_size=network_size,
        count=len(hops_list),
        total_hops=sum(hops_list),
        min_hops=min(hops_list),
        max_hops=max(hops_list),
        mean_hops=statistics.mean(hops_list),
        std_hops=statistics.stdev(hops_list) if len(hops_list) > 1 else 0.0,
    )


# =============================================================================
# Benchmark Runner
# =============================================================================

class BenchmarkRunner:
    """Runs benchmark experiments for Chord and Pastry DHTs."""

    def __init__(self, cfg: BenchmarkConfig):
        """Initialize the benchmark runner."""
        self.cfg = cfg
        self.results: List[OperationStats] = []
        self.movies = None

    def load_data(self) -> None:
        """Load movie data for benchmarking."""
        print(f"Loading {self.cfg.num_movies} movies from dataset...")
        self.movies = get_sample_movies(self.cfg.num_movies, seed=self.cfg.seed)
        print(f"Loaded {len(self.movies)} movies")

    def run_all(self) -> List[OperationStats]:
        """Run all benchmarks and return results."""
        self.load_data()

        for network_size in self.cfg.network_sizes:
            print()
            print("=" * 60)
            print(f"BENCHMARKING WITH {network_size} NODES")
            print("=" * 60)

            # Run for Chord
            self._benchmark_protocol("Chord", ChordNetwork, network_size)

            # Run for Pastry
            self._benchmark_protocol("Pastry", PastryNetwork, network_size)

        return self.results

    def _benchmark_protocol(
        self,
        protocol_name: str,
        network_class,
        network_size: int
    ) -> None:
        """Run all benchmarks for a single protocol."""
        print()
        print(f"--- {protocol_name} ---")

        # Create and build network
        network = network_class()

        # 1. Benchmark BUILD (node joins during network construction)
        print(f"  Building network ({network_size} nodes)...")
        join_hops = self._benchmark_build(network, network_size)
        self.results.append(compute_stats("build", protocol_name, network_size, join_hops))
        print(f"    Build complete: mean={statistics.mean(join_hops):.2f} hops/join")

        # Stabilize Chord finger tables after build (lazy protocol)
        if isinstance(network, ChordNetwork):
            network.stabilize_all(rounds=3)

        # 2. Benchmark INSERT
        print(f"  Inserting {self.cfg.num_movies} movies...")
        insert_hops = self._benchmark_insert(network)
        self.results.append(compute_stats("insert", protocol_name, network_size, insert_hops))
        print(f"    Insert complete: mean={statistics.mean(insert_hops):.2f} hops/insert")

        # 3. Benchmark LOOKUP
        print(f"  Looking up {self.cfg.num_operations} movies...")
        lookup_hops = self._benchmark_lookup(network)
        self.results.append(compute_stats("lookup", protocol_name, network_size, lookup_hops))
        print(f"    Lookup complete: mean={statistics.mean(lookup_hops):.2f} hops/lookup")

        # 4. Benchmark UPDATE
        print(f"  Updating {self.cfg.num_operations} movies...")
        update_hops = self._benchmark_update(network)
        self.results.append(compute_stats("update", protocol_name, network_size, update_hops))
        print(f"    Update complete: mean={statistics.mean(update_hops):.2f} hops/update")

        # 5. Benchmark DELETE
        print(f"  Deleting {self.cfg.num_operations} movies...")
        delete_hops = self._benchmark_delete(network)
        self.results.append(compute_stats("delete", protocol_name, network_size, delete_hops))
        print(f"    Delete complete: mean={statistics.mean(delete_hops):.2f} hops/delete")

        # 6. Benchmark NODE JOIN (adding more nodes)
        print(f"  Adding {self.cfg.num_join_leave} more nodes...")
        node_join_hops = self._benchmark_node_join(network)
        self.results.append(compute_stats("node_join", protocol_name, network_size, node_join_hops))
        print(f"    Node join complete: mean={statistics.mean(node_join_hops):.2f} hops/join")

        # 7. Benchmark NODE LEAVE
        print(f"  Removing {self.cfg.num_join_leave} nodes...")
        node_leave_hops = self._benchmark_node_leave(network)
        self.results.append(compute_stats("node_leave", protocol_name, network_size, node_leave_hops))
        print(f"    Node leave complete: mean={statistics.mean(node_leave_hops):.2f} hops/leave")

        # Cleanup
        network.clear()

    def _benchmark_build(self, network, network_size: int) -> List[int]:
        """Benchmark network building (node joins)."""
        hops_list = []
        for i in range(network_size):
            node = network.create_node(f"node_{i}")
            hops = network.add_node(node)
            hops_list.append(hops)
        return hops_list

    def _benchmark_insert(self, network) -> List[int]:
        """Benchmark insert operations."""
        hops_list = []
        for movie in self.movies:
            _, hops = network.insert(movie.title, movie.to_dict())
            hops_list.append(hops)
        return hops_list

    def _benchmark_lookup(self, network) -> List[int]:
        """Benchmark lookup operations."""
        hops_list = []
        # Use a subset of movies for lookup
        lookup_movies = self.movies[:self.cfg.num_operations]
        for movie in lookup_movies:
            _, hops = network.lookup(movie.title)
            hops_list.append(hops)
        return hops_list

    def _benchmark_update(self, network) -> List[int]:
        """Benchmark update operations."""
        hops_list = []
        update_movies = self.movies[:self.cfg.num_operations]
        for movie in update_movies:
            data = movie.to_dict()
            data["popularity"] = data["popularity"] + 1  # Modify something
            _, hops = network.update(movie.title, data)
            hops_list.append(hops)
        return hops_list

    def _benchmark_delete(self, network) -> List[int]:
        """Benchmark delete operations."""
        hops_list = []
        # Delete from the END of the movie list (so lookup/update still work on first N)
        delete_movies = self.movies[-self.cfg.num_operations:]
        for movie in delete_movies:
            _, hops = network.delete(movie.title)
            hops_list.append(hops)
        return hops_list

    def _benchmark_node_join(self, network) -> List[int]:
        """Benchmark adding new nodes to existing network."""
        hops_list = []
        base_count = network.node_count
        for i in range(self.cfg.num_join_leave):
            node = network.create_node(f"new_node_{i}")
            hops = network.add_node(node)
            hops_list.append(hops)

        # Stabilize Chord finger tables after joins (lazy protocol)
        if isinstance(network, ChordNetwork):
            network.stabilize_all(rounds=3)

        return hops_list

    def _benchmark_node_leave(self, network) -> List[int]:
        """Benchmark removing nodes from network."""
        hops_list = []
        # Remove the nodes we just added
        for i in range(self.cfg.num_join_leave):
            success, hops = network.remove_node(f"new_node_{i}")
            if success:
                hops_list.append(hops)
        return hops_list

    def save_results(self, filename: str = "benchmark_results.csv") -> str:
        """Save results to CSV file."""
        os.makedirs(self.cfg.output_dir, exist_ok=True)
        filepath = os.path.join(self.cfg.output_dir, filename)

        with open(filepath, "w", newline="") as f:
            if self.results:
                writer = csv.DictWriter(f, fieldnames=self.results[0].to_dict().keys())
                writer.writeheader()
                for result in self.results:
                    writer.writerow(result.to_dict())

        print(f"\nResults saved to: {filepath}")
        return filepath


# =============================================================================
# Result Printer
# =============================================================================

def print_summary(results: List[OperationStats]) -> None:
    """Print a summary table of results."""
    print()
    print("=" * 80)
    print("BENCHMARK SUMMARY")
    print("=" * 80)

    # Group by operation
    operations = ["build", "insert", "lookup", "update", "delete", "node_join", "node_leave"]

    for op in operations:
        print(f"\n{op.upper()}:")
        print("-" * 70)
        print(f"{'Network Size':<15} {'Chord (mean)':<15} {'Pastry (mean)':<15} {'Difference':<15}")
        print("-" * 70)

        # Get unique network sizes
        network_sizes = sorted(set(r.network_size for r in results))

        for size in network_sizes:
            chord_result = next((r for r in results if r.operation == op and r.protocol == "Chord" and r.network_size == size), None)
            pastry_result = next((r for r in results if r.operation == op and r.protocol == "Pastry" and r.network_size == size), None)

            chord_mean = f"{chord_result.mean_hops:.2f}" if chord_result else "N/A"
            pastry_mean = f"{pastry_result.mean_hops:.2f}" if pastry_result else "N/A"

            if chord_result and pastry_result:
                diff = chord_result.mean_hops - pastry_result.mean_hops
                diff_str = f"{diff:+.2f}"
            else:
                diff_str = "N/A"

            print(f"{size:<15} {chord_mean:<15} {pastry_mean:<15} {diff_str:<15}")


# =============================================================================
# Main
# =============================================================================

def main():
    """Main entry point for benchmark."""
    parser = argparse.ArgumentParser(description="DHT Benchmark System")
    parser.add_argument("--quick", action="store_true", help="Run quick benchmark with smaller parameters")
    parser.add_argument("--output", type=str, default="benchmark_results.csv", help="Output CSV filename")
    args = parser.parse_args()

    # Select configuration
    if args.quick:
        print("Running QUICK benchmark (smaller parameters)...")
        cfg = QuickBenchmarkConfig()
    else:
        print("Running FULL benchmark...")
        cfg = BenchmarkConfig()

    print(f"Configuration:")
    print(f"  Network sizes: {cfg.network_sizes}")
    print(f"  Movies to insert: {cfg.num_movies}")
    print(f"  Operations per test: {cfg.num_operations}")
    print(f"  Nodes for join/leave: {cfg.num_join_leave}")
    print(f"  Random seed: {cfg.seed}")

    # Run benchmarks
    start_time = time.time()

    runner = BenchmarkRunner(cfg)
    results = runner.run_all()

    elapsed = time.time() - start_time
    print(f"\nBenchmark completed in {elapsed:.1f} seconds")

    # Save results
    runner.save_results(args.output)

    # Print summary
    print_summary(results)

    # Clear cache
    clear_cache()

    return 0


if __name__ == "__main__":
    sys.exit(main())
