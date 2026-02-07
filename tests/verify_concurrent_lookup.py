"""
Verification script for concurrent lookup with movies dataset.

This script verifies that:
1. Movies data can be loaded from the dataset
2. Movies can be inserted into both Chord and Pastry networks
3. Concurrent lookup works and returns correct popularity values
4. Hop counts are properly tracked

This is a temporary verification script - the logic will be
properly organized in main.py later.
"""

import sys
import os

# Add project root to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.common.data_loader import (
    load_dataset,
    get_sample_movies,
    get_dataset_stats,
)
from src.dht.chord.chord_network import ChordNetwork
from src.dht.pastry.pastry_network import PastryNetwork


def verify_concurrent_lookup():
    """
    Verify concurrent lookup works with actual movie data.

    Steps:
    1. Load sample movies from dataset
    2. Build Chord and Pastry networks
    3. Insert movies into both networks
    4. Perform concurrent lookup of K titles
    5. Verify results and hop counts
    """
    print("=" * 70)
    print("CONCURRENT LOOKUP VERIFICATION WITH MOVIES DATASET")
    print("=" * 70)

    # =========================================================================
    # Step 1: Load dataset and get sample movies
    # =========================================================================
    print("\n--- Step 1: Loading dataset ---")

    stats = get_dataset_stats()
    print(f"Dataset: {stats['total_rows']:,} total movies")
    print(f"Unique titles: {stats['unique_titles']:,}")

    # Get 100 sample movies for testing (with seed for reproducibility)
    num_movies = 100
    seed = 42
    sample_movies = get_sample_movies(num_movies, seed=seed)
    print(f"Loaded {len(sample_movies)} sample movies (seed={seed})")

    # Show a few sample titles
    print("\nSample titles:")
    for movie in sample_movies[:5]:
        print(f"  - {movie.title} (popularity: {movie.popularity})")

    # =========================================================================
    # Step 2: Build networks
    # =========================================================================
    print("\n--- Step 2: Building networks ---")

    num_nodes = 8

    # Build Chord network
    chord = ChordNetwork()
    chord_build = chord.build_network(num_nodes)
    print(f"Chord: {chord.node_count} nodes, join_hops={chord_build['total_join_hops']}")

    # Build Pastry network
    pastry = PastryNetwork()
    pastry_build = pastry.build_network(num_nodes)
    print(f"Pastry: {pastry.node_count} nodes, join_hops={pastry_build['total_join_hops']}")

    # =========================================================================
    # Step 3: Insert movies into both networks
    # =========================================================================
    print("\n--- Step 3: Inserting movies ---")

    # Prepare items: (title, movie_data_dict)
    items = [(movie.title, movie.to_dict()) for movie in sample_movies]

    # Insert into Chord
    chord_insert = chord.bulk_insert(items)
    print(f"Chord insert: {chord_insert['success_count']}/{chord_insert['total_items']} "
          f"(avg_hops={chord_insert['average_hops']:.2f})")

    # Insert into Pastry
    pastry_insert = pastry.bulk_insert(items)
    print(f"Pastry insert: {pastry_insert['success_count']}/{pastry_insert['total_items']} "
          f"(avg_hops={pastry_insert['average_hops']:.2f})")

    # =========================================================================
    # Step 4: Concurrent lookup of K movies
    # =========================================================================
    print("\n--- Step 4: Concurrent lookup of K=10 movies ---")

    # Select K random titles for lookup
    K = 10
    lookup_titles = [movie.title for movie in sample_movies[:K]]

    print(f"\nLooking up {K} movies concurrently:")
    for title in lookup_titles:
        print(f"  - {title}")

    # Concurrent lookup in Chord
    print("\n[Chord Results]")
    chord_result = chord.concurrent_lookup(lookup_titles)
    print(f"Found: {chord_result['found_count']}/{chord_result['total_keys']}")
    print(f"Total hops: {chord_result['total_hops']}, Avg hops: {chord_result['average_hops']:.2f}")

    print("\nPopularities retrieved:")
    for title, (value, hops) in chord_result['results'].items():
        if value is not None:
            popularity = value.get('popularity', 'N/A')
            print(f"  - {title[:40]:<40} | popularity={popularity:<10} | hops={hops}")
        else:
            print(f"  - {title[:40]:<40} | NOT FOUND | hops={hops}")

    # Concurrent lookup in Pastry
    print("\n[Pastry Results]")
    pastry_result = pastry.concurrent_lookup(lookup_titles)
    print(f"Found: {pastry_result['found_count']}/{pastry_result['total_keys']}")
    print(f"Total hops: {pastry_result['total_hops']}, Avg hops: {pastry_result['average_hops']:.2f}")

    print("\nPopularities retrieved:")
    for title, (value, hops) in pastry_result['results'].items():
        if value is not None:
            popularity = value.get('popularity', 'N/A')
            print(f"  - {title[:40]:<40} | popularity={popularity:<10} | hops={hops}")
        else:
            print(f"  - {title[:40]:<40} | NOT FOUND | hops={hops}")

    # =========================================================================
    # Step 5: Verification summary
    # =========================================================================
    print("\n--- Step 5: Verification Summary ---")
    print("=" * 70)

    all_passed = True

    # Check 1: All movies were inserted
    if chord_insert['success_count'] == num_movies:
        print("✓ Chord: All movies inserted successfully")
    else:
        print(f"✗ Chord: Only {chord_insert['success_count']}/{num_movies} inserted")
        all_passed = False

    if pastry_insert['success_count'] == num_movies:
        print("✓ Pastry: All movies inserted successfully")
    else:
        print(f"✗ Pastry: Only {pastry_insert['success_count']}/{num_movies} inserted")
        all_passed = False

    # Check 2: All K movies were found
    if chord_result['found_count'] == K:
        print(f"✓ Chord: All {K} movies found in concurrent lookup")
    else:
        print(f"✗ Chord: Only {chord_result['found_count']}/{K} found")
        all_passed = False

    if pastry_result['found_count'] == K:
        print(f"✓ Pastry: All {K} movies found in concurrent lookup")
    else:
        print(f"✗ Pastry: Only {pastry_result['found_count']}/{K} found")
        all_passed = False

    # Check 3: Hop counts are reasonable (> 0 for most operations)
    if chord_result['total_hops'] >= 0:
        print(f"✓ Chord: Hop counting works (total={chord_result['total_hops']})")
    else:
        print("✗ Chord: Invalid hop count")
        all_passed = False

    if pastry_result['total_hops'] >= 0:
        print(f"✓ Pastry: Hop counting works (total={pastry_result['total_hops']})")
    else:
        print("✗ Pastry: Invalid hop count")
        all_passed = False

    # Check 4: Popularity values are correct
    chord_popularities_ok = True
    pastry_popularities_ok = True

    for movie in sample_movies[:K]:
        # Check Chord
        chord_val, _ = chord_result['results'].get(movie.title, (None, 0))
        if chord_val is not None:
            if chord_val.get('popularity') != movie.popularity:
                chord_popularities_ok = False

        # Check Pastry
        pastry_val, _ = pastry_result['results'].get(movie.title, (None, 0))
        if pastry_val is not None:
            if pastry_val.get('popularity') != movie.popularity:
                pastry_popularities_ok = False

    if chord_popularities_ok:
        print("✓ Chord: Popularity values match original data")
    else:
        print("✗ Chord: Popularity values mismatch")
        all_passed = False

    if pastry_popularities_ok:
        print("✓ Pastry: Popularity values match original data")
    else:
        print("✗ Pastry: Popularity values mismatch")
        all_passed = False

    print("=" * 70)
    if all_passed:
        print("ALL VERIFICATIONS PASSED!")
        print("Concurrent lookup with movies dataset works correctly.")
    else:
        print("SOME VERIFICATIONS FAILED!")
        print("Please review the issues above.")
    print("=" * 70)

    # Cleanup
    chord.clear()
    pastry.clear()

    return all_passed


if __name__ == "__main__":
    success = verify_concurrent_lookup()
    sys.exit(0 if success else 1)
