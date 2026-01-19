"""
Comprehensive test for the refactoring of BaseNode and BaseNetwork.
Tests all shared methods for both Chord and Pastry implementations.
"""

import sys
import os

# Add project root to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def test_chord():
    """Test all refactored methods with Chord."""
    print('='*70)
    print('CHORD TESTS')
    print('='*70)
    
    from src.dht.chord.chord_network import ChordNetwork
    
    chord = ChordNetwork()
    
    # --- Protocol-specific: build_network ---
    print()
    print('--- ChordNetwork.build_network() ---')
    build_stats = chord.build_network(8)
    print(f'Nodes created: {chord.node_count}')
    print(f'Total join hops: {build_stats["total_join_hops"]}')
    assert chord.node_count == 8, 'FAILED: node count'
    print('PASSED')
    
    # --- BaseNetwork: get_node, get_random_node ---
    print()
    print('--- BaseNetwork.get_node(), get_random_node() ---')
    node = chord.get_node('node_0')
    print(f'get_node(node_0): {node.identifier if node else None}')
    assert node is not None and node.identifier == 'node_0', 'FAILED: get_node'
    random_node = chord.get_random_node()
    print(f'get_random_node(): {random_node.identifier if random_node else None}')
    assert random_node is not None, 'FAILED: get_random_node'
    print('PASSED')
    
    # --- BaseNode: insert ---
    print()
    print('--- BaseNode.insert() [via Chord] ---')
    success, hops = chord.insert('The Matrix', {'year': 1999})
    print(f'insert: success={success}, hops={hops}')
    assert success == True, 'FAILED: insert'
    print('PASSED')
    
    # --- BaseNode: lookup ---
    print()
    print('--- BaseNode.lookup() [via Chord] ---')
    value, hops = chord.lookup('The Matrix')
    print(f'lookup: found={value is not None}, hops={hops}')
    assert value is not None, 'FAILED: lookup'
    print('PASSED')
    
    # --- BaseNode: update ---
    print()
    print('--- BaseNode.update() [via Chord] ---')
    success, hops = chord.update('The Matrix', {'year': 1999, 'rating': 8.7})
    print(f'update: success={success}, hops={hops}')
    assert success == True, 'FAILED: update'
    print('PASSED')
    
    # --- BaseNode: delete ---
    print()
    print('--- BaseNode.delete() [via Chord] ---')
    success, hops = chord.delete('The Matrix')
    print(f'delete: success={success}, hops={hops}')
    assert success == True, 'FAILED: delete'
    print('PASSED')
    
    # --- BaseNetwork: bulk_insert ---
    print()
    print('--- BaseNetwork.bulk_insert() [via Chord] ---')
    items = [('ChordFilm1', {'id': 1}), ('ChordFilm2', {'id': 2}), ('ChordFilm3', {'id': 3})]
    result = chord.bulk_insert(items)
    print(f'bulk_insert: {result["success_count"]}/{result["total_items"]}, avg_hops={result["average_hops"]:.2f}')
    assert result['success_count'] == 3, 'FAILED: bulk_insert'
    print('PASSED')
    
    # --- BaseNetwork: bulk_lookup ---
    print()
    print('--- BaseNetwork.bulk_lookup() [via Chord] ---')
    result = chord.bulk_lookup(['ChordFilm1', 'ChordFilm2', 'ChordFilm3', 'NonExistent'])
    print(f'bulk_lookup: found={result["found_count"]}, not_found={result["not_found_count"]}')
    assert result['found_count'] == 3 and result['not_found_count'] == 1, 'FAILED: bulk_lookup'
    print('PASSED')
    
    # --- BaseNetwork: bulk_delete ---
    print()
    print('--- BaseNetwork.bulk_delete() [via Chord] ---')
    result = chord.bulk_delete(['ChordFilm1', 'NonExistent'])
    print(f'bulk_delete: success={result["success_count"]}, total={result["total_keys"]}')
    assert result['success_count'] == 1, 'FAILED: bulk_delete'
    print('PASSED')
    
    # --- BaseNetwork: concurrent_lookup ---
    print()
    print('--- BaseNetwork.concurrent_lookup() [via Chord] ---')
    result = chord.concurrent_lookup(['ChordFilm2', 'ChordFilm3', 'NonExistent'])
    print(f'concurrent_lookup: found={result["found_count"]}, not_found={result["not_found_count"]}')
    assert result['found_count'] == 2 and result['not_found_count'] == 1, 'FAILED: concurrent_lookup'
    print('PASSED')
    
    # --- BaseNetwork: concurrent_insert ---
    print()
    print('--- BaseNetwork.concurrent_insert() [via Chord] ---')
    items = [('ChordConcurrent1', {}), ('ChordConcurrent2', {})]
    result = chord.concurrent_insert(items)
    print(f'concurrent_insert: success={result["success_count"]}/{result["total_items"]}')
    assert result['success_count'] == 2, 'FAILED: concurrent_insert'
    print('PASSED')
    
    # --- BaseNetwork: get_network_stats ---
    print()
    print('--- BaseNetwork.get_network_stats() [via Chord] ---')
    stats = chord.get_network_stats()
    print(f'stats: {stats}')
    assert 'node_count' in stats and 'total_keys' in stats, 'FAILED: get_network_stats'
    print('PASSED')
    
    # --- Protocol-specific: remove_node (leave) ---
    print()
    print('--- ChordNetwork.remove_node() ---')
    success, hops = chord.remove_node('node_3')
    print(f'remove_node: success={success}, hops={hops}')
    assert success == True, 'FAILED: remove_node'
    assert chord.node_count == 7, 'FAILED: node count after remove'
    print('PASSED')
    
    # --- BaseNetwork: clear ---
    print()
    print('--- BaseNetwork.clear() [via Chord] ---')
    chord.clear()
    print(f'After clear: node_count={chord.node_count}')
    assert chord.node_count == 0, 'FAILED: clear'
    print('PASSED')
    
    print()
    print('CHORD: ALL TESTS PASSED')


def test_pastry():
    """Test all refactored methods with Pastry."""
    print()
    print('='*70)
    print('PASTRY TESTS')
    print('='*70)
    
    from src.dht.pastry.pastry_network import PastryNetwork
    
    pastry = PastryNetwork()
    
    # --- Protocol-specific: build_network ---
    print()
    print('--- PastryNetwork.build_network() ---')
    build_stats = pastry.build_network(8)
    print(f'Nodes created: {pastry.node_count}')
    print(f'Total join hops: {build_stats["total_join_hops"]}')
    assert pastry.node_count == 8, 'FAILED: node count'
    print('PASSED')
    
    # --- BaseNetwork: get_node, get_random_node ---
    print()
    print('--- BaseNetwork.get_node(), get_random_node() [via Pastry] ---')
    node = pastry.get_node('node_0')
    print(f'get_node(node_0): {node.identifier if node else None}')
    assert node is not None and node.identifier == 'node_0', 'FAILED: get_node'
    random_node = pastry.get_random_node()
    print(f'get_random_node(): {random_node.identifier if random_node else None}')
    assert random_node is not None, 'FAILED: get_random_node'
    print('PASSED')
    
    # --- BaseNode: insert ---
    print()
    print('--- BaseNode.insert() [via Pastry] ---')
    success, hops = pastry.insert('Inception', {'year': 2010})
    print(f'insert: success={success}, hops={hops}')
    assert success == True, 'FAILED: insert'
    print('PASSED')
    
    # --- BaseNode: lookup ---
    print()
    print('--- BaseNode.lookup() [via Pastry] ---')
    value, hops = pastry.lookup('Inception')
    print(f'lookup: found={value is not None}, hops={hops}')
    assert value is not None, 'FAILED: lookup'
    print('PASSED')
    
    # --- BaseNode: update ---
    print()
    print('--- BaseNode.update() [via Pastry] ---')
    success, hops = pastry.update('Inception', {'year': 2010, 'rating': 8.8})
    print(f'update: success={success}, hops={hops}')
    assert success == True, 'FAILED: update'
    print('PASSED')
    
    # --- BaseNode: delete ---
    print()
    print('--- BaseNode.delete() [via Pastry] ---')
    success, hops = pastry.delete('Inception')
    print(f'delete: success={success}, hops={hops}')
    assert success == True, 'FAILED: delete'
    print('PASSED')
    
    # --- BaseNetwork: bulk_insert ---
    print()
    print('--- BaseNetwork.bulk_insert() [via Pastry] ---')
    items = [('PastryFilm1', {'id': 1}), ('PastryFilm2', {'id': 2}), ('PastryFilm3', {'id': 3})]
    result = pastry.bulk_insert(items)
    print(f'bulk_insert: {result["success_count"]}/{result["total_items"]}, avg_hops={result["average_hops"]:.2f}')
    assert result['success_count'] == 3, 'FAILED: bulk_insert'
    print('PASSED')
    
    # --- BaseNetwork: bulk_lookup ---
    print()
    print('--- BaseNetwork.bulk_lookup() [via Pastry] ---')
    result = pastry.bulk_lookup(['PastryFilm1', 'PastryFilm2', 'PastryFilm3', 'NonExistent'])
    print(f'bulk_lookup: found={result["found_count"]}, not_found={result["not_found_count"]}')
    assert result['found_count'] == 3 and result['not_found_count'] == 1, 'FAILED: bulk_lookup'
    print('PASSED')
    
    # --- BaseNetwork: bulk_delete ---
    print()
    print('--- BaseNetwork.bulk_delete() [via Pastry] ---')
    result = pastry.bulk_delete(['PastryFilm1', 'NonExistent'])
    print(f'bulk_delete: success={result["success_count"]}, total={result["total_keys"]}')
    assert result['success_count'] == 1, 'FAILED: bulk_delete'
    print('PASSED')
    
    # --- BaseNetwork: concurrent_lookup ---
    print()
    print('--- BaseNetwork.concurrent_lookup() [via Pastry] ---')
    result = pastry.concurrent_lookup(['PastryFilm2', 'PastryFilm3', 'NonExistent'])
    print(f'concurrent_lookup: found={result["found_count"]}, not_found={result["not_found_count"]}')
    assert result['found_count'] == 2 and result['not_found_count'] == 1, 'FAILED: concurrent_lookup'
    print('PASSED')
    
    # --- BaseNetwork: concurrent_insert ---
    print()
    print('--- BaseNetwork.concurrent_insert() [via Pastry] ---')
    items = [('PastryConcurrent1', {}), ('PastryConcurrent2', {})]
    result = pastry.concurrent_insert(items)
    print(f'concurrent_insert: success={result["success_count"]}/{result["total_items"]}')
    assert result['success_count'] == 2, 'FAILED: concurrent_insert'
    print('PASSED')
    
    # --- BaseNetwork: get_network_stats ---
    print()
    print('--- BaseNetwork.get_network_stats() [via Pastry] ---')
    stats = pastry.get_network_stats()
    print(f'stats: {stats}')
    assert 'node_count' in stats and 'total_keys' in stats, 'FAILED: get_network_stats'
    print('PASSED')
    
    # --- Protocol-specific: remove_node (leave) ---
    print()
    print('--- PastryNetwork.remove_node() ---')
    success, hops = pastry.remove_node('node_3')
    print(f'remove_node: success={success}, hops={hops}')
    assert success == True, 'FAILED: remove_node'
    assert pastry.node_count == 7, 'FAILED: node count after remove'
    print('PASSED')
    
    # --- BaseNetwork: clear ---
    print()
    print('--- BaseNetwork.clear() [via Pastry] ---')
    pastry.clear()
    print(f'After clear: node_count={pastry.node_count}')
    assert pastry.node_count == 0, 'FAILED: clear'
    print('PASSED')
    
    print()
    print('PASTRY: ALL TESTS PASSED')


def print_summary():
    """Print test summary."""
    print()
    print('='*70)
    print('ALL TESTS PASSED!')
    print('='*70)
    print()
    print('Refactoring verified:')
    print('  BaseNode:')
    print('    - insert()    [Chord ✓] [Pastry ✓]')
    print('    - lookup()    [Chord ✓] [Pastry ✓]')
    print('    - update()    [Chord ✓] [Pastry ✓]')
    print('    - delete()    [Chord ✓] [Pastry ✓]')
    print('  BaseNetwork:')
    print('    - get_node()          [Chord ✓] [Pastry ✓]')
    print('    - get_random_node()   [Chord ✓] [Pastry ✓]')
    print('    - bulk_insert()       [Chord ✓] [Pastry ✓]')
    print('    - bulk_lookup()       [Chord ✓] [Pastry ✓]')
    print('    - bulk_delete()       [Chord ✓] [Pastry ✓]')
    print('    - concurrent_lookup() [Chord ✓] [Pastry ✓]')
    print('    - concurrent_insert() [Chord ✓] [Pastry ✓]')
    print('    - get_network_stats() [Chord ✓] [Pastry ✓]')
    print('    - clear()             [Chord ✓] [Pastry ✓]')
    print('  Protocol-specific:')
    print('    - build_network()     [Chord ✓] [Pastry ✓]')
    print('    - remove_node()       [Chord ✓] [Pastry ✓]')


if __name__ == '__main__':
    print('='*70)
    print('COMPREHENSIVE REFACTORING TEST')
    print('='*70)
    
    test_chord()
    test_pastry()
    print_summary()