"""
DHT Lookup - Main Demo

Simple demonstration that the Chord and Pastry DHT implementations work correctly.
Shows all operations: build, insert, lookup, update, delete, node join, node leave.

For proper experimental evaluation, use the benchmark system (evaluation/benchmark.py).

Usage:
    python main.py
"""

import sys

from src.common.data_loader import get_sample_movies, get_dataset_stats
from src.common.logger import get_logger
from src.dht.chord.chord_network import ChordNetwork
from src.dht.pastry.pastry_network import PastryNetwork

logger = get_logger(__name__)


def print_header(title: str) -> None:
    """Print a section header."""
    print()
    print("=" * 60)
    print(title)
    print("=" * 60)


def print_comparison(operation: str, chord_hops: int, pastry_hops: int) -> None:
    """Print a comparison line for an operation."""
    print(f"  {operation:<20} | Chord: {chord_hops:>4} hops | Pastry: {pastry_hops:>4} hops")


def main():
    """
    Main demo function.

    Demonstrates all DHT operations with a small dataset.
    """
    print_header("DHT LOOKUP - DEMO")
    print("Demonstrating Chord and Pastry DHT implementations")
    print("For experimental evaluation, run: python -m evaluation.benchmark")

    # =========================================================================
    # 1. Dataset Info
    # =========================================================================
    print_header("1. DATASET")

    stats = get_dataset_stats()
    print(f"Total movies: {stats['total_rows']:,}")
    print(f"Unique titles: {stats['unique_titles']:,}")

    # Load sample movies
    num_movies = 50
    movies = get_sample_movies(num_movies, seed=42)
    print(f"Sample loaded: {len(movies)} movies")

    # =========================================================================
    # 2. Build Networks
    # =========================================================================
    print_header("2. BUILD NETWORKS")

    num_nodes = 8

    chord = ChordNetwork()
    chord_build = chord.build_network(num_nodes)
    print(f"Chord:  {chord.node_count} nodes (total join hops: {chord_build['total_join_hops']})")

    pastry = PastryNetwork()
    pastry_build = pastry.build_network(num_nodes)
    print(f"Pastry: {pastry.node_count} nodes (total join hops: {pastry_build['total_join_hops']})")

    # =========================================================================
    # 3. Insert Movies
    # =========================================================================
    print_header("3. INSERT MOVIES")

    items = [(movie.title, movie.to_dict()) for movie in movies]

    chord_insert = chord.bulk_insert(items)
    pastry_insert = pastry.bulk_insert(items)

    print(f"Inserted {num_movies} movies into each network:")
    print(f"  Chord:  avg {chord_insert['average_hops']:.2f} hops/insert (total: {chord_insert['total_hops']})")
    print(f"  Pastry: avg {pastry_insert['average_hops']:.2f} hops/insert (total: {pastry_insert['total_hops']})")

    # =========================================================================
    # 4. Lookup (Sequential)
    # =========================================================================
    print_header("4. LOOKUP (Sequential)")

    lookup_titles = [m.title for m in movies[:5]]

    print(f"Looking up {len(lookup_titles)} movies:")
    for title in lookup_titles:
        chord_val, chord_hops = chord.lookup(title)
        pastry_val, pastry_hops = pastry.lookup(title)
        status = "found" if chord_val else "NOT FOUND"
        print(f"  '{title[:35]:<35}' - {status} (Chord: {chord_hops} hops, Pastry: {pastry_hops} hops)")

    # =========================================================================
    # 5. Concurrent Lookup (K movies)
    # =========================================================================
    print_header("5. CONCURRENT LOOKUP (K movies)")

    K = 10
    k_titles = [m.title for m in movies[:K]]

    print(f"Concurrently looking up K={K} movies to get their popularity:")

    chord_concurrent = chord.concurrent_lookup(k_titles)
    pastry_concurrent = pastry.concurrent_lookup(k_titles)

    print(f"\nChord: found {chord_concurrent['found_count']}/{K}, "
          f"avg {chord_concurrent['average_hops']:.2f} hops")
    print(f"Pastry: found {pastry_concurrent['found_count']}/{K}, "
          f"avg {pastry_concurrent['average_hops']:.2f} hops")

    print(f"\nPopularities:")
    for title in k_titles[:5]:  # Show first 5
        chord_val, _ = chord_concurrent['results'].get(title, (None, 0))
        popularity = chord_val.get('popularity', 'N/A') if chord_val else 'N/A'
        print(f"  '{title[:40]:<40}' -> popularity: {popularity}")

    # =========================================================================
    # 6. Update
    # =========================================================================
    print_header("6. UPDATE")

    update_title = movies[0].title
    new_data = movies[0].to_dict()
    new_data['popularity'] = 999.99  # Modified value

    chord_success, chord_hops = chord.update(update_title, new_data)
    pastry_success, pastry_hops = pastry.update(update_title, new_data)

    print(f"Updated '{update_title[:40]}':")
    print(f"  Chord:  success={chord_success}, hops={chord_hops}")
    print(f"  Pastry: success={pastry_success}, hops={pastry_hops}")

    # Verify update
    chord_val, _ = chord.lookup(update_title)
    print(f"  Verified new popularity: {chord_val.get('popularity') if chord_val else 'N/A'}")

    # =========================================================================
    # 7. Delete
    # =========================================================================
    print_header("7. DELETE")

    delete_title = movies[1].title

    chord_success, chord_hops = chord.delete(delete_title)
    pastry_success, pastry_hops = pastry.delete(delete_title)

    print(f"Deleted '{delete_title[:40]}':")
    print(f"  Chord:  success={chord_success}, hops={chord_hops}")
    print(f"  Pastry: success={pastry_success}, hops={pastry_hops}")

    # Verify deletion
    chord_val, _ = chord.lookup(delete_title)
    print(f"  Verified deletion: {'not found (correct)' if chord_val is None else 'STILL EXISTS (error)'}")

    # =========================================================================
    # 8. Node Join
    # =========================================================================
    print_header("8. NODE JOIN")

    # Add a new node to each network
    chord_new = chord.create_node("new_chord_node")
    chord_join_hops = chord.add_node(chord_new)

    pastry_new = pastry.create_node("new_pastry_node")
    pastry_join_hops = pastry.add_node(pastry_new)

    print(f"Added new node to each network:")
    print(f"  Chord:  'new_chord_node' joined with {chord_join_hops} hops (now {chord.node_count} nodes)")
    print(f"  Pastry: 'new_pastry_node' joined with {pastry_join_hops} hops (now {pastry.node_count} nodes)")

    # =========================================================================
    # 9. Node Leave
    # =========================================================================
    print_header("9. NODE LEAVE")

    # Remove a node from each network
    chord_success, chord_leave_hops = chord.remove_node("node_3")
    pastry_success, pastry_leave_hops = pastry.remove_node("node_3")

    print(f"Removed 'node_3' from each network:")
    print(f"  Chord:  left with {chord_leave_hops} hops (now {chord.node_count} nodes)")
    print(f"  Pastry: left with {pastry_leave_hops} hops (now {pastry.node_count} nodes)")

    # =========================================================================
    # 10. Summary
    # =========================================================================
    print_header("10. SUMMARY")

    print("All operations completed successfully!")
    print()
    print("Operation Comparison (this demo):")
    print("-" * 50)
    print_comparison("Build (total)", chord_build['total_join_hops'], pastry_build['total_join_hops'])
    print_comparison("Insert (total)", chord_insert['total_hops'], pastry_insert['total_hops'])
    print_comparison("Concurrent Lookup", chord_concurrent['total_hops'], pastry_concurrent['total_hops'])
    print_comparison("Node Join", chord_join_hops, pastry_join_hops)
    print_comparison("Node Leave", chord_leave_hops, pastry_leave_hops)
    print("-" * 50)
    print()
    print("Note: This is a simple demo with small data.")
    print("For proper experimental evaluation with statistics, run:")
    print("  python -m evaluation.benchmark")

    # Cleanup
    chord.clear()
    pastry.clear()

    return 0


if __name__ == "__main__":
    sys.exit(main())
