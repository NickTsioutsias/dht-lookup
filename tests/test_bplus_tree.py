"""
Unit tests for the B+ Tree implementation.

Tests cover core operations, dict-like interface, range queries,
stress scenarios, various tree orders, and structural invariants.
"""

import random
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.indexing.bplus_tree import BPlusTree, BPlusLeafNode, BPlusInternalNode


# =========================================================================
# Tree Invariant Validator
# =========================================================================

def validate_tree(tree: BPlusTree) -> None:
    """
    Verify all B+ tree structural invariants hold.
    Raises AssertionError with a descriptive message if any invariant is violated.
    """
    if tree._root is None:
        assert tree._size == 0, "Root is None but size is non-zero"
        assert tree._leftmost_leaf is None, "Root is None but leftmost_leaf is set"
        return

    # 1. All leaves must be at the same depth.
    leaf_depths = []
    _collect_leaf_depths(tree._root, 0, leaf_depths)
    assert len(set(leaf_depths)) == 1, f"Leaves at different depths: {leaf_depths}"

    # 2. Keys within each node must be sorted.
    _check_sorted_keys(tree._root)

    # 3. Internal nodes must have len(children) == len(keys) + 1.
    _check_children_count(tree._root)

    # 4. Key ordering across children of internal nodes.
    _check_key_ordering(tree._root)

    # 5. Size matches actual count of keys in leaves.
    actual_count = 0
    leaf = tree._leftmost_leaf
    while leaf is not None:
        actual_count += len(leaf.keys)
        leaf = leaf.next
    assert actual_count == tree._size, (
        f"Size mismatch: _size={tree._size}, actual leaf key count={actual_count}"
    )

    # 6. Leftmost leaf is correct.
    leftmost = tree._root
    while isinstance(leftmost, BPlusInternalNode):
        leftmost = leftmost.children[0]
    assert leftmost is tree._leftmost_leaf, "Leftmost leaf pointer is incorrect"

    # 7. Leaf linked list covers all leaves.
    leaves_via_link = []
    leaf = tree._leftmost_leaf
    while leaf is not None:
        leaves_via_link.append(id(leaf))
        leaf = leaf.next
    leaves_via_tree = []
    _collect_leaves(tree._root, leaves_via_tree)
    assert len(leaves_via_link) == len(leaves_via_tree), (
        f"Linked list has {len(leaves_via_link)} leaves but tree has {len(leaves_via_tree)}"
    )

    # 8. Min occupancy (except root).
    if tree._size > 0:
        _check_min_occupancy(tree._root, tree._order, is_root=True)


def _collect_leaf_depths(node, depth, depths):
    if isinstance(node, BPlusLeafNode):
        depths.append(depth)
    elif isinstance(node, BPlusInternalNode):
        for child in node.children:
            _collect_leaf_depths(child, depth + 1, depths)


def _check_sorted_keys(node):
    for i in range(len(node.keys) - 1):
        assert node.keys[i] <= node.keys[i + 1], f"Keys not sorted: {node.keys}"
    if isinstance(node, BPlusInternalNode):
        for child in node.children:
            _check_sorted_keys(child)


def _check_children_count(node):
    if isinstance(node, BPlusInternalNode):
        assert len(node.children) == len(node.keys) + 1, (
            f"Internal node has {len(node.keys)} keys but {len(node.children)} children"
        )
        for child in node.children:
            _check_children_count(child)


def _check_key_ordering(node):
    if isinstance(node, BPlusInternalNode):
        for i, key in enumerate(node.keys):
            # All keys in children[i] must be < key (for internal) or <= key (for leaves).
            _check_all_keys_less(node.children[i], key)
            # All keys in children[i+1] must be >= key.
            _check_all_keys_gte(node.children[i + 1], key)
        for child in node.children:
            _check_key_ordering(child)


def _check_all_keys_less(node, bound):
    if isinstance(node, BPlusLeafNode):
        for k in node.keys:
            assert k < bound, f"Leaf key {k} not < parent separator {bound}"
    else:
        for k in node.keys:
            assert k < bound, f"Internal key {k} not < parent separator {bound}"
        for child in node.children:
            _check_all_keys_less(child, bound)


def _check_all_keys_gte(node, bound):
    if isinstance(node, BPlusLeafNode):
        for k in node.keys:
            assert k >= bound, f"Leaf key {k} not >= parent separator {bound}"
    else:
        for k in node.keys:
            assert k >= bound, f"Internal key {k} not >= parent separator {bound}"
        for child in node.children:
            _check_all_keys_gte(child, bound)


def _collect_leaves(node, leaves):
    if isinstance(node, BPlusLeafNode):
        leaves.append(node)
    elif isinstance(node, BPlusInternalNode):
        for child in node.children:
            _collect_leaves(child, leaves)


def _check_min_occupancy(node, order, is_root):
    min_keys = (order - 1) // 2
    if isinstance(node, BPlusLeafNode):
        if not is_root:
            assert len(node.keys) >= min_keys, (
                f"Leaf underflow: {len(node.keys)} keys, min is {min_keys}"
            )
    elif isinstance(node, BPlusInternalNode):
        if not is_root:
            assert len(node.keys) >= min_keys, (
                f"Internal underflow: {len(node.keys)} keys, min is {min_keys}"
            )
        for child in node.children:
            _check_min_occupancy(child, order, is_root=False)


# =========================================================================
# Tests: Core Operations
# =========================================================================

def test_insert_and_search_single():
    tree = BPlusTree(order=4)
    tree.insert("hello", 42)
    assert tree.search("hello") == 42
    validate_tree(tree)


def test_insert_and_search_multiple():
    tree = BPlusTree(order=4)
    data = {"alpha": 1, "beta": 2, "gamma": 3, "delta": 4, "epsilon": 5}
    for k, v in data.items():
        tree.insert(k, v)
    for k, v in data.items():
        assert tree.search(k) == v, f"Failed to find {k}"
    validate_tree(tree)


def test_insert_duplicate_key_updates_value():
    tree = BPlusTree(order=4)
    tree.insert("key", "old")
    tree.insert("key", "new")
    assert tree.search("key") == "new"
    assert len(tree) == 1
    validate_tree(tree)


def test_search_missing_key_returns_none():
    tree = BPlusTree(order=4)
    tree.insert("exists", 1)
    assert tree.search("missing") is None


def test_delete_single():
    tree = BPlusTree(order=4)
    tree.insert("key", "value")
    assert tree.delete("key") is True
    assert tree.search("key") is None
    assert len(tree) == 0
    validate_tree(tree)


def test_delete_missing_key_returns_false():
    tree = BPlusTree(order=4)
    tree.insert("exists", 1)
    assert tree.delete("missing") is False
    assert len(tree) == 1


def test_delete_from_empty_tree():
    tree = BPlusTree(order=4)
    assert tree.delete("anything") is False


def test_delete_all_keys():
    tree = BPlusTree(order=4)
    keys = ["a", "b", "c", "d", "e", "f", "g", "h"]
    for k in keys:
        tree.insert(k, k)
    for k in keys:
        assert tree.delete(k) is True
    assert len(tree) == 0
    assert tree._root is None
    validate_tree(tree)


def test_clear():
    tree = BPlusTree(order=4)
    for i in range(20):
        tree.insert(str(i), i)
    tree.clear()
    assert len(tree) == 0
    assert tree._root is None
    assert tree._leftmost_leaf is None
    validate_tree(tree)


# =========================================================================
# Tests: Dict-like Interface
# =========================================================================

def test_setitem_getitem():
    tree = BPlusTree(order=4)
    tree["key"] = "value"
    assert tree["key"] == "value"


def test_getitem_missing_raises_keyerror():
    tree = BPlusTree(order=4)
    try:
        _ = tree["missing"]
        assert False, "Should have raised KeyError"
    except KeyError:
        pass


def test_getitem_none_value():
    tree = BPlusTree(order=4)
    tree["key"] = None
    assert tree["key"] is None


def test_contains():
    tree = BPlusTree(order=4)
    tree["present"] = 1
    assert "present" in tree
    assert "absent" not in tree


def test_delitem():
    tree = BPlusTree(order=4)
    tree["key"] = "value"
    del tree["key"]
    assert "key" not in tree


def test_delitem_missing_raises_keyerror():
    tree = BPlusTree(order=4)
    try:
        del tree["missing"]
        assert False, "Should have raised KeyError"
    except KeyError:
        pass


def test_len():
    tree = BPlusTree(order=4)
    assert len(tree) == 0
    tree["a"] = 1
    assert len(tree) == 1
    tree["b"] = 2
    assert len(tree) == 2
    del tree["a"]
    assert len(tree) == 1


def test_bool():
    tree = BPlusTree(order=4)
    assert not tree
    tree["key"] = 1
    assert tree


def test_get_with_default():
    tree = BPlusTree(order=4)
    tree["exists"] = 42
    assert tree.get("exists") == 42
    assert tree.get("missing") is None
    assert tree.get("missing", "default") == "default"


def test_pop():
    tree = BPlusTree(order=4)
    tree["key"] = "value"
    result = tree.pop("key")
    assert result == "value"
    assert "key" not in tree
    validate_tree(tree)


def test_pop_missing_raises_keyerror():
    tree = BPlusTree(order=4)
    try:
        tree.pop("missing")
        assert False, "Should have raised KeyError"
    except KeyError:
        pass


def test_pop_missing_with_default():
    tree = BPlusTree(order=4)
    assert tree.pop("missing", "default") == "default"


def test_keys_returns_sorted():
    tree = BPlusTree(order=4)
    input_keys = ["delta", "alpha", "charlie", "bravo"]
    for k in input_keys:
        tree[k] = k
    assert list(tree.keys()) == sorted(input_keys)


def test_items_returns_sorted():
    tree = BPlusTree(order=4)
    pairs = [("c", 3), ("a", 1), ("b", 2)]
    for k, v in pairs:
        tree[k] = v
    assert list(tree.items()) == [("a", 1), ("b", 2), ("c", 3)]


def test_values():
    tree = BPlusTree(order=4)
    pairs = [("c", 3), ("a", 1), ("b", 2)]
    for k, v in pairs:
        tree[k] = v
    assert list(tree.values()) == [1, 2, 3]


def test_repr():
    tree = BPlusTree(order=8)
    tree["a"] = 1
    assert "order=8" in repr(tree)
    assert "size=1" in repr(tree)


# =========================================================================
# Tests: Range Queries
# =========================================================================

def test_range_query_full_range():
    tree = BPlusTree(order=4)
    for c in "abcdefghij":
        tree[c] = ord(c)
    result = tree.range_query("a", "j")
    assert len(result) == 10
    assert result[0] == ("a", ord("a"))
    assert result[-1] == ("j", ord("j"))


def test_range_query_partial():
    tree = BPlusTree(order=4)
    for c in "abcdefghij":
        tree[c] = ord(c)
    result = tree.range_query("c", "f")
    assert [k for k, v in result] == ["c", "d", "e", "f"]


def test_range_query_empty():
    tree = BPlusTree(order=4)
    for c in "abc":
        tree[c] = ord(c)
    result = tree.range_query("x", "z")
    assert result == []


def test_range_query_single_match():
    tree = BPlusTree(order=4)
    for c in "abcde":
        tree[c] = ord(c)
    result = tree.range_query("c", "c")
    assert result == [("c", ord("c"))]


def test_range_query_boundaries_inclusive():
    tree = BPlusTree(order=4)
    for c in "abcde":
        tree[c] = ord(c)
    result = tree.range_query("b", "d")
    keys = [k for k, v in result]
    assert "b" in keys and "d" in keys


def test_range_query_empty_tree():
    tree = BPlusTree(order=4)
    assert tree.range_query("a", "z") == []


# =========================================================================
# Tests: Stress / Large Scale
# =========================================================================

def test_large_insert_and_search():
    tree = BPlusTree(order=32)
    keys = [f"key_{i:05d}" for i in range(10000)]
    random.shuffle(keys)
    for k in keys:
        tree[k] = k
    assert len(tree) == 10000
    for k in keys:
        assert tree[k] == k, f"Failed to find {k}"
    validate_tree(tree)


def test_large_insert_delete_search():
    tree = BPlusTree(order=16)
    keys = [f"key_{i:05d}" for i in range(5000)]
    random.shuffle(keys)
    for k in keys:
        tree[k] = k

    # Delete half.
    to_delete = keys[:2500]
    to_keep = set(keys[2500:])
    for k in to_delete:
        assert tree.delete(k) is True

    assert len(tree) == 2500
    for k in to_keep:
        assert tree[k] == k
    for k in to_delete:
        assert k not in tree
    validate_tree(tree)


def test_insert_sorted_order():
    tree = BPlusTree(order=4)
    for i in range(100):
        tree[f"{i:03d}"] = i
    assert len(tree) == 100
    assert list(tree.keys()) == [f"{i:03d}" for i in range(100)]
    validate_tree(tree)


def test_insert_reverse_sorted_order():
    tree = BPlusTree(order=4)
    for i in range(99, -1, -1):
        tree[f"{i:03d}"] = i
    assert len(tree) == 100
    assert list(tree.keys()) == [f"{i:03d}" for i in range(100)]
    validate_tree(tree)


# =========================================================================
# Tests: Various Orders
# =========================================================================

def test_order_3_minimal():
    """Order 3 is the minimum. Frequent splits/merges."""
    tree = BPlusTree(order=3)
    for i in range(50):
        tree[f"k{i:02d}"] = i
    assert len(tree) == 50
    validate_tree(tree)

    # Delete all.
    for i in range(50):
        tree.delete(f"k{i:02d}")
    assert len(tree) == 0
    validate_tree(tree)


def test_order_4():
    tree = BPlusTree(order=4)
    keys = list(range(200))
    random.shuffle(keys)
    for k in keys:
        tree[str(k)] = k
    validate_tree(tree)
    for k in keys:
        tree.delete(str(k))
    validate_tree(tree)


def test_order_8():
    tree = BPlusTree(order=8)
    keys = [f"item_{i}" for i in range(500)]
    random.shuffle(keys)
    for k in keys:
        tree[k] = k
    validate_tree(tree)
    random.shuffle(keys)
    for k in keys[:250]:
        tree.delete(k)
    validate_tree(tree)


def test_order_64():
    tree = BPlusTree(order=64)
    for i in range(1000):
        tree[f"k{i:04d}"] = i
    validate_tree(tree)
    assert len(tree) == 1000


def test_invalid_order():
    try:
        BPlusTree(order=2)
        assert False, "Should have raised ValueError"
    except ValueError:
        pass


# =========================================================================
# Tests: Edge Cases
# =========================================================================

def test_single_element_delete():
    tree = BPlusTree(order=3)
    tree["only"] = 1
    tree.delete("only")
    assert len(tree) == 0
    assert tree._root is None
    validate_tree(tree)


def test_interleaved_insert_delete():
    """Insert and delete in alternating fashion."""
    tree = BPlusTree(order=4)
    for i in range(100):
        tree[f"k{i:03d}"] = i
        if i > 0 and i % 3 == 0:
            tree.delete(f"k{i - 1:03d}")
    validate_tree(tree)


def test_numeric_keys():
    """B+ tree works with integer keys too."""
    tree = BPlusTree(order=5)
    for i in range(50):
        tree.insert(i, f"value_{i}")
    for i in range(50):
        assert tree.search(i) == f"value_{i}"
    result = tree.range_query(10, 20)
    assert len(result) == 11
    assert result[0] == (10, "value_10")
    assert result[-1] == (20, "value_20")
    validate_tree(tree)


def test_dict_values_as_tree_values():
    """Store dict values (like movie records) in the tree."""
    tree = BPlusTree(order=4)
    movie = {"title": "The Matrix", "popularity": 85.5, "budget": 63000000}
    tree["The Matrix"] = movie
    result = tree["The Matrix"]
    assert result["popularity"] == 85.5
    assert result["budget"] == 63000000


# =========================================================================
# Main
# =========================================================================

if __name__ == "__main__":
    test_functions = [
        obj for name, obj in list(globals().items())
        if name.startswith("test_") and callable(obj)
    ]
    passed = 0
    failed = 0
    for test_fn in test_functions:
        try:
            test_fn()
            passed += 1
            print(f"  PASS: {test_fn.__name__}")
        except Exception as e:
            failed += 1
            print(f"  FAIL: {test_fn.__name__}: {e}")

    print(f"\n{'='*60}")
    print(f"Results: {passed} passed, {failed} failed out of {passed + failed}")
    if failed == 0:
        print("All tests passed!")
    else:
        print("Some tests failed!")
        sys.exit(1)
