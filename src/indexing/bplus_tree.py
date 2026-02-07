"""
B+ Tree implementation for local node storage in DHT.

Provides ordered storage with O(log n) insert, search, delete, and
efficient range queries via linked leaf nodes. Exposes a dict-like
interface so it can replace a plain dictionary as the local storage
engine in DHT nodes.
"""

from bisect import bisect_left, bisect_right
from typing import Any, Generator, List, Optional, Tuple, Union


class BPlusLeafNode:
    """Leaf node storing sorted key-value pairs with a next pointer."""

    __slots__ = ("keys", "values", "next", "parent")

    def __init__(self) -> None:
        self.keys: List[Any] = []
        self.values: List[Any] = []
        self.next: Optional["BPlusLeafNode"] = None
        self.parent: Optional["BPlusInternalNode"] = None


class BPlusInternalNode:
    """Internal node storing separator keys and child pointers."""

    __slots__ = ("keys", "children", "parent")

    def __init__(self) -> None:
        self.keys: List[Any] = []
        self.children: List[Union["BPlusInternalNode", BPlusLeafNode]] = []
        self.parent: Optional["BPlusInternalNode"] = None


class BPlusTree:
    """
    B+ Tree with dict-like interface.

    Args:
        order: Maximum number of children per internal node.
               A leaf holds at most order-1 keys. Minimum valid order is 3.
    """

    def __init__(self, order: int = 32) -> None:
        if order < 3:
            raise ValueError("B+ tree order must be >= 3")
        self._order = order
        self._root: Optional[Union[BPlusInternalNode, BPlusLeafNode]] = None
        self._size: int = 0
        self._leftmost_leaf: Optional[BPlusLeafNode] = None

    @property
    def order(self) -> int:
        return self._order

    # =========================================================================
    # Core Operations
    # =========================================================================

    def search(self, key: Any) -> Optional[Any]:
        """Search for a key. Returns the value or None if not found."""
        if self._root is None:
            return None
        leaf = self._find_leaf(key)
        idx = bisect_left(leaf.keys, key)
        if idx < len(leaf.keys) and leaf.keys[idx] == key:
            return leaf.values[idx]
        return None

    def insert(self, key: Any, value: Any) -> None:
        """Insert or update a key-value pair."""
        # Empty tree: create the first leaf.
        if self._root is None:
            leaf = BPlusLeafNode()
            leaf.keys.append(key)
            leaf.values.append(value)
            self._root = leaf
            self._leftmost_leaf = leaf
            self._size = 1
            return

        leaf = self._find_leaf(key)
        idx = bisect_left(leaf.keys, key)

        # Update existing key.
        if idx < len(leaf.keys) and leaf.keys[idx] == key:
            leaf.values[idx] = value
            return

        # Insert new key in sorted position.
        leaf.keys.insert(idx, key)
        leaf.values.insert(idx, value)
        self._size += 1

        # Split if the leaf overflows.
        if len(leaf.keys) >= self._order:
            self._split_leaf(leaf)

    def delete(self, key: Any) -> bool:
        """Delete a key. Returns True if deleted, False if not found."""
        if self._root is None:
            return False

        leaf = self._find_leaf(key)
        idx = bisect_left(leaf.keys, key)

        if idx >= len(leaf.keys) or leaf.keys[idx] != key:
            return False

        leaf.keys.pop(idx)
        leaf.values.pop(idx)
        self._size -= 1

        # If the tree is now empty, reset.
        if self._size == 0:
            self._root = None
            self._leftmost_leaf = None
            return True

        # If this leaf is the root, no rebalancing needed.
        if leaf is self._root:
            return True

        # Rebalance if underflow.
        min_keys = (self._order - 1) // 2  # ceil(order/2) - 1
        if len(leaf.keys) < min_keys:
            self._rebalance_leaf(leaf)

        return True

    def range_query(
        self, start_key: Any, end_key: Any
    ) -> List[Tuple[Any, Any]]:
        """
        Return all (key, value) pairs where start_key <= key <= end_key.

        Leverages the linked leaf list for efficient scanning.
        """
        if self._root is None:
            return []

        result: List[Tuple[Any, Any]] = []
        leaf = self._find_leaf(start_key)
        idx = bisect_left(leaf.keys, start_key)

        while leaf is not None:
            while idx < len(leaf.keys):
                if leaf.keys[idx] > end_key:
                    return result
                result.append((leaf.keys[idx], leaf.values[idx]))
                idx += 1
            leaf = leaf.next
            idx = 0

        return result

    # =========================================================================
    # Dict-like Interface
    # =========================================================================

    def __setitem__(self, key: Any, value: Any) -> None:
        self.insert(key, value)

    def __getitem__(self, key: Any) -> Any:
        result = self.search(key)
        if result is None:
            # Distinguish between "value is None" and "key not found".
            if self._root is not None:
                leaf = self._find_leaf(key)
                idx = bisect_left(leaf.keys, key)
                if idx < len(leaf.keys) and leaf.keys[idx] == key:
                    return None  # Key exists with None value.
            raise KeyError(key)
        return result

    def __contains__(self, key: Any) -> bool:
        if self._root is None:
            return False
        leaf = self._find_leaf(key)
        idx = bisect_left(leaf.keys, key)
        return idx < len(leaf.keys) and leaf.keys[idx] == key

    def __delitem__(self, key: Any) -> None:
        if not self.delete(key):
            raise KeyError(key)

    def __len__(self) -> int:
        return self._size

    def __bool__(self) -> bool:
        return self._size > 0

    def __repr__(self) -> str:
        return f"BPlusTree(order={self._order}, size={self._size})"

    def get(self, key: Any, default: Any = None) -> Any:
        """Get value for key, returning default if not found."""
        if key in self:
            return self[key]
        return default

    def pop(self, key: Any, *args: Any) -> Any:
        """Remove and return value for key. Raises KeyError if not found and no default."""
        if len(args) > 1:
            raise TypeError(f"pop expected at most 2 arguments, got {1 + len(args)}")

        if key in self:
            value = self[key]
            self.delete(key)
            return value

        if args:
            return args[0]
        raise KeyError(key)

    def keys(self) -> Generator[Any, None, None]:
        """Yield all keys in sorted order."""
        leaf = self._leftmost_leaf
        while leaf is not None:
            yield from leaf.keys
            leaf = leaf.next

    def values(self) -> Generator[Any, None, None]:
        """Yield all values in key-sorted order."""
        leaf = self._leftmost_leaf
        while leaf is not None:
            yield from leaf.values
            leaf = leaf.next

    def items(self) -> Generator[Tuple[Any, Any], None, None]:
        """Yield all (key, value) pairs in sorted order."""
        leaf = self._leftmost_leaf
        while leaf is not None:
            yield from zip(leaf.keys, leaf.values)
            leaf = leaf.next

    def clear(self) -> None:
        """Remove all entries."""
        self._root = None
        self._leftmost_leaf = None
        self._size = 0

    # =========================================================================
    # Private Helpers — Navigation
    # =========================================================================

    def _find_leaf(self, key: Any) -> BPlusLeafNode:
        """Navigate from root to the leaf that should contain the key."""
        node = self._root
        while isinstance(node, BPlusInternalNode):
            idx = bisect_right(node.keys, key)
            node = node.children[idx]
        return node  # type: ignore[return-value]

    # =========================================================================
    # Private Helpers — Splitting
    # =========================================================================

    def _split_leaf(self, leaf: BPlusLeafNode) -> None:
        """Split an overflowing leaf node."""
        mid = len(leaf.keys) // 2

        new_leaf = BPlusLeafNode()
        new_leaf.keys = leaf.keys[mid:]
        new_leaf.values = leaf.values[mid:]
        leaf.keys = leaf.keys[:mid]
        leaf.values = leaf.values[:mid]

        # Maintain linked list.
        new_leaf.next = leaf.next
        leaf.next = new_leaf

        # Push the first key of the new leaf up to the parent.
        push_up_key = new_leaf.keys[0]
        self._insert_into_parent(leaf, push_up_key, new_leaf)

    def _split_internal(self, node: BPlusInternalNode) -> None:
        """Split an overflowing internal node."""
        mid = len(node.keys) // 2
        push_up_key = node.keys[mid]

        new_node = BPlusInternalNode()
        new_node.keys = node.keys[mid + 1 :]
        new_node.children = node.children[mid + 1 :]
        node.keys = node.keys[:mid]
        node.children = node.children[: mid + 1]

        # Update parent pointers of migrated children.
        for child in new_node.children:
            child.parent = new_node

        self._insert_into_parent(node, push_up_key, new_node)

    def _insert_into_parent(
        self,
        left: Union[BPlusLeafNode, BPlusInternalNode],
        key: Any,
        right: Union[BPlusLeafNode, BPlusInternalNode],
    ) -> None:
        """Insert a key and right child into the parent of left, creating a new root if needed."""
        if left is self._root:
            new_root = BPlusInternalNode()
            new_root.keys = [key]
            new_root.children = [left, right]
            left.parent = new_root
            right.parent = new_root
            self._root = new_root
            return

        parent = left.parent
        assert parent is not None

        # Find position of left child in parent and insert key + right child.
        idx = parent.children.index(left)
        parent.keys.insert(idx, key)
        parent.children.insert(idx + 1, right)
        right.parent = parent

        # Split parent if it overflows.
        if len(parent.keys) >= self._order:
            self._split_internal(parent)

    # =========================================================================
    # Private Helpers — Deletion / Rebalancing
    # =========================================================================

    def _rebalance_leaf(self, leaf: BPlusLeafNode) -> None:
        """Rebalance an underflowing leaf by borrowing or merging."""
        parent = leaf.parent
        if parent is None:
            return

        idx = parent.children.index(leaf)

        # Try borrowing from right sibling.
        if idx + 1 < len(parent.children):
            right_sib = parent.children[idx + 1]
            if isinstance(right_sib, BPlusLeafNode):
                min_keys = (self._order - 1) // 2
                if len(right_sib.keys) > min_keys:
                    # Borrow the first key from the right sibling.
                    leaf.keys.append(right_sib.keys.pop(0))
                    leaf.values.append(right_sib.values.pop(0))
                    parent.keys[idx] = right_sib.keys[0]
                    return

        # Try borrowing from left sibling.
        if idx - 1 >= 0:
            left_sib = parent.children[idx - 1]
            if isinstance(left_sib, BPlusLeafNode):
                min_keys = (self._order - 1) // 2
                if len(left_sib.keys) > min_keys:
                    # Borrow the last key from the left sibling.
                    leaf.keys.insert(0, left_sib.keys.pop())
                    leaf.values.insert(0, left_sib.values.pop())
                    parent.keys[idx - 1] = leaf.keys[0]
                    return

        # Merge: prefer merging with the right sibling.
        if idx + 1 < len(parent.children):
            right_sib = parent.children[idx + 1]
            if isinstance(right_sib, BPlusLeafNode):
                self._merge_leaves(leaf, right_sib, parent, idx)
                return

        # Merge with the left sibling (current leaf is absorbed into left).
        if idx - 1 >= 0:
            left_sib = parent.children[idx - 1]
            if isinstance(left_sib, BPlusLeafNode):
                self._merge_leaves(left_sib, leaf, parent, idx - 1)

    def _merge_leaves(
        self,
        left: BPlusLeafNode,
        right: BPlusLeafNode,
        parent: BPlusInternalNode,
        key_idx: int,
    ) -> None:
        """Merge right leaf into left leaf, removing the separator key from parent."""
        left.keys.extend(right.keys)
        left.values.extend(right.values)
        left.next = right.next

        # Update leftmost leaf pointer if right was somehow leftmost (shouldn't happen
        # since we always merge right into left, but be safe).
        if self._leftmost_leaf is right:
            self._leftmost_leaf = left

        # Remove separator key and right child from parent.
        parent.keys.pop(key_idx)
        parent.children.pop(key_idx + 1)

        # If parent is root and now has only one child, that child becomes root.
        if parent is self._root and len(parent.keys) == 0:
            self._root = parent.children[0]
            self._root.parent = None
            return

        # Check if parent underflows.
        if parent is not self._root:
            min_keys = (self._order - 1) // 2
            if len(parent.keys) < min_keys:
                self._rebalance_internal(parent)

    def _rebalance_internal(self, node: BPlusInternalNode) -> None:
        """Rebalance an underflowing internal node by borrowing or merging."""
        parent = node.parent
        if parent is None:
            return

        idx = parent.children.index(node)

        # Try borrowing from right sibling.
        if idx + 1 < len(parent.children):
            right_sib = parent.children[idx + 1]
            if isinstance(right_sib, BPlusInternalNode):
                min_keys = (self._order - 1) // 2
                if len(right_sib.keys) > min_keys:
                    # Pull separator down from parent, push first key of right sib up.
                    node.keys.append(parent.keys[idx])
                    parent.keys[idx] = right_sib.keys.pop(0)
                    borrowed_child = right_sib.children.pop(0)
                    borrowed_child.parent = node
                    node.children.append(borrowed_child)
                    return

        # Try borrowing from left sibling.
        if idx - 1 >= 0:
            left_sib = parent.children[idx - 1]
            if isinstance(left_sib, BPlusInternalNode):
                min_keys = (self._order - 1) // 2
                if len(left_sib.keys) > min_keys:
                    # Pull separator down from parent, push last key of left sib up.
                    node.keys.insert(0, parent.keys[idx - 1])
                    parent.keys[idx - 1] = left_sib.keys.pop()
                    borrowed_child = left_sib.children.pop()
                    borrowed_child.parent = node
                    node.children.insert(0, borrowed_child)
                    return

        # Merge with right sibling.
        if idx + 1 < len(parent.children):
            right_sib = parent.children[idx + 1]
            if isinstance(right_sib, BPlusInternalNode):
                self._merge_internals(node, right_sib, parent, idx)
                return

        # Merge with left sibling.
        if idx - 1 >= 0:
            left_sib = parent.children[idx - 1]
            if isinstance(left_sib, BPlusInternalNode):
                self._merge_internals(left_sib, node, parent, idx - 1)

    def _merge_internals(
        self,
        left: BPlusInternalNode,
        right: BPlusInternalNode,
        parent: BPlusInternalNode,
        key_idx: int,
    ) -> None:
        """Merge right internal node into left, pulling separator down from parent."""
        # Pull the separator key down.
        left.keys.append(parent.keys[key_idx])
        left.keys.extend(right.keys)

        # Move children.
        for child in right.children:
            child.parent = left
        left.children.extend(right.children)

        # Remove separator and right child from parent.
        parent.keys.pop(key_idx)
        parent.children.pop(key_idx + 1)

        # If parent is root and now has only one child, that child becomes root.
        if parent is self._root and len(parent.keys) == 0:
            self._root = parent.children[0]
            self._root.parent = None
            return

        # Check if parent underflows.
        if parent is not self._root:
            min_keys = (self._order - 1) // 2
            if len(parent.keys) < min_keys:
                self._rebalance_internal(parent)
