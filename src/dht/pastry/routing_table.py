"""
Routing table and leaf set implementation for Pastry DHT.

Pastry uses two data structures for routing:
1. Routing Table: Organized by prefix matching, enables O(log_b N) routing
2. Leaf Set: L closest nodes by ID, used for final routing and key responsibility

Hop Counting Model:
    All network messages between different nodes are counted as hops.
"""

from typing import Optional, List, Dict, TYPE_CHECKING

import config
from src.common.hashing import get_id_hex
from src.common.logger import get_logger

if TYPE_CHECKING:
    from src.dht.pastry.node import PastryNode

logger = get_logger(__name__)


def get_shared_prefix_length(id1: str, id2: str) -> int:
    """
    Calculate the length of the shared prefix between two hex IDs.
    
    Args:
        id1: First hexadecimal ID string.
        id2: Second hexadecimal ID string.
    
    Returns:
        Number of matching hex digits from the start.
    
    Example:
        >>> get_shared_prefix_length("a1b2c3", "a1b4d5")
        3  # "a1b" matches
    """
    length = 0
    for c1, c2 in zip(id1, id2):
        if c1 == c2:
            length += 1
        else:
            break
    return length


def hex_digit_to_int(hex_char: str) -> int:
    """Convert a single hex character to integer (0-15)."""
    return int(hex_char, 16)


def int_to_hex_digit(value: int) -> str:
    """Convert an integer (0-15) to hex character."""
    return format(value, 'x')


class LeafSet:
    """
    Leaf set for Pastry routing.
    
    Contains the L closest nodes by numeric ID:
    - L/2 nodes with smaller IDs (left leaf set)
    - L/2 nodes with larger IDs (right leaf set)
    
    Used for:
    - Final routing step (when key is within leaf set range)
    - Determining key responsibility
    
    Attributes:
        owner_id: Numeric ID of the node that owns this leaf set.
        owner_hex: Hexadecimal representation of owner_id.
        size: Maximum size of each side (L/2).
    """
    
    def __init__(self, owner_id: int, size: int = None):
        """
        Initialize an empty leaf set.
        
        Args:
            owner_id: Numeric ID of the owning node.
            size: Size of each side (L/2). Defaults to config.PASTRY_LEAF_SIZE.
        """
        self._owner_id = owner_id
        self._owner_hex = get_id_hex(owner_id)
        self._size = size if size is not None else config.PASTRY_LEAF_SIZE
        
        # Left set: nodes with smaller IDs, sorted descending (closest first)
        self._left: List["PastryNode"] = []
        
        # Right set: nodes with larger IDs, sorted ascending (closest first)
        self._right: List["PastryNode"] = []
    
    @property
    def owner_id(self) -> int:
        """Numeric ID of the owning node."""
        return self._owner_id
    
    @property
    def size(self) -> int:
        """Maximum size of each side."""
        return self._size
    
    @property
    def left(self) -> List["PastryNode"]:
        """Nodes with smaller IDs (sorted by distance, closest first)."""
        return self._left.copy()
    
    @property
    def right(self) -> List["PastryNode"]:
        """Nodes with larger IDs (sorted by distance, closest first)."""
        return self._right.copy()
    
    def get_all_nodes(self) -> List["PastryNode"]:
        """Get all nodes in the leaf set."""
        return self._left + self._right
    
    def insert(self, node: "PastryNode") -> bool:
        """
        Insert a node into the leaf set if appropriate.
        
        Args:
            node: The node to potentially insert.
        
        Returns:
            True if the node was inserted, False otherwise.
        """
        if node.node_id == self._owner_id:
            return False
        
        if node.node_id < self._owner_id:
            # Goes in left set
            return self._insert_left(node)
        else:
            # Goes in right set
            return self._insert_right(node)
    
    def _insert_left(self, node: "PastryNode") -> bool:
        """Insert into left set (smaller IDs)."""
        # Check if already present
        for existing in self._left:
            if existing.node_id == node.node_id:
                return False
        
        # Add and sort by distance (closest = largest ID, so sort descending)
        self._left.append(node)
        self._left.sort(key=lambda n: n.node_id, reverse=True)
        
        # Trim to size
        if len(self._left) > self._size:
            self._left = self._left[:self._size]
            return node in self._left
        
        return True
    
    def _insert_right(self, node: "PastryNode") -> bool:
        """Insert into right set (larger IDs)."""
        # Check if already present
        for existing in self._right:
            if existing.node_id == node.node_id:
                return False
        
        # Add and sort by distance (closest = smallest ID, so sort ascending)
        self._right.append(node)
        self._right.sort(key=lambda n: n.node_id)
        
        # Trim to size
        if len(self._right) > self._size:
            self._right = self._right[:self._size]
            return node in self._right
        
        return True
    
    def remove(self, node: "PastryNode") -> bool:
        """
        Remove a node from the leaf set.
        
        Args:
            node: The node to remove.
        
        Returns:
            True if the node was found and removed, False otherwise.
        """
        for i, n in enumerate(self._left):
            if n.node_id == node.node_id:
                self._left.pop(i)
                return True
        
        for i, n in enumerate(self._right):
            if n.node_id == node.node_id:
                self._right.pop(i)
                return True
        
        return False
    
    def get_closest_node(self, key_id: int) -> Optional["PastryNode"]:
        """
        Find the node in the leaf set closest to the given key.
        
        Args:
            key_id: The key ID to find the closest node for.
        
        Returns:
            The closest node, or None if leaf set is empty.
        """
        all_nodes = self.get_all_nodes()
        if not all_nodes:
            return None
        
        closest = None
        min_distance = float('inf')
        
        for node in all_nodes:
            dist = abs(node.node_id - key_id)
            if dist < min_distance:
                min_distance = dist
                closest = node
        
        return closest
    
    def is_within_range(self, key_id: int) -> bool:
        """
        Check if a key falls within the range covered by the leaf set.
        
        Args:
            key_id: The key ID to check.
        
        Returns:
            True if the key is within leaf set range, False otherwise.
        """
        if not self._left and not self._right:
            return True  # We're the only node
        
        min_id = self._left[-1].node_id if self._left else self._owner_id
        max_id = self._right[-1].node_id if self._right else self._owner_id
        
        return min_id <= key_id <= max_id
    
    def get_successor(self) -> Optional["PastryNode"]:
        """Get the immediate successor (first node in right set)."""
        return self._right[0] if self._right else None
    
    def get_predecessor(self) -> Optional["PastryNode"]:
        """Get the immediate predecessor (first node in left set)."""
        return self._left[0] if self._left else None
    
    def __repr__(self) -> str:
        return f"LeafSet(left={len(self._left)}, right={len(self._right)})"


class RoutingTable:
    """
    Routing table for Pastry routing.
    
    Organized as a 2D table where:
    - Row i contains nodes sharing the first i hex digits with us
    - Column j contains a node whose (i+1)th digit is j
    
    This enables O(log_b N) routing where b is the base (16 for hex).
    
    Attributes:
        owner_id: Numeric ID of the owning node.
        owner_hex: Hexadecimal representation of owner_id.
        num_rows: Number of rows (hex digits in ID).
        num_cols: Number of columns (base, typically 16).
    """
    
    def __init__(self, owner_id: int):
        """
        Initialize an empty routing table.
        
        Args:
            owner_id: Numeric ID of the owning node.
        """
        self._owner_id = owner_id
        self._owner_hex = get_id_hex(owner_id)
        
        # Number of hex digits in the ID
        self._num_rows = config.HASH_BIT_SIZE // config.PASTRY_B
        
        # Base (number of possible values per digit)
        self._num_cols = config.PASTRY_BASE
        
        # Initialize empty table
        # table[row][col] = node or None
        self._table: List[List[Optional["PastryNode"]]] = [
            [None for _ in range(self._num_cols)]
            for _ in range(self._num_rows)
        ]
    
    @property
    def owner_id(self) -> int:
        """Numeric ID of the owning node."""
        return self._owner_id
    
    @property
    def owner_hex(self) -> str:
        """Hexadecimal ID of the owning node."""
        return self._owner_hex
    
    @property
    def num_rows(self) -> int:
        """Number of rows in the routing table."""
        return self._num_rows
    
    @property
    def num_cols(self) -> int:
        """Number of columns in the routing table."""
        return self._num_cols
    
    def get(self, row: int, col: int) -> Optional["PastryNode"]:
        """
        Get the node at the specified position.
        
        Args:
            row: Row index (prefix length).
            col: Column index (digit value).
        
        Returns:
            The node at that position, or None if empty.
        """
        if 0 <= row < self._num_rows and 0 <= col < self._num_cols:
            return self._table[row][col]
        return None
    
    def set(self, row: int, col: int, node: "PastryNode") -> None:
        """
        Set the node at the specified position.
        
        Args:
            row: Row index (prefix length).
            col: Column index (digit value).
            node: The node to store.
        """
        if 0 <= row < self._num_rows and 0 <= col < self._num_cols:
            self._table[row][col] = node
    
    def insert(self, node: "PastryNode") -> bool:
        """
        Insert a node into the appropriate position in the routing table.
        
        Args:
            node: The node to insert.
        
        Returns:
            True if the node was inserted, False if position was occupied.
        """
        if node.node_id == self._owner_id:
            return False
        
        node_hex = get_id_hex(node.node_id)
        prefix_len = get_shared_prefix_length(self._owner_hex, node_hex)
        
        if prefix_len >= self._num_rows:
            return False
        
        # The differing digit determines the column
        col = hex_digit_to_int(node_hex[prefix_len])
        
        # Check if position is empty or if new node is better
        existing = self._table[prefix_len][col]
        if existing is None:
            self._table[prefix_len][col] = node
            return True
        
        return False
    
    def remove(self, node: "PastryNode") -> bool:
        """
        Remove a node from the routing table.
        
        Args:
            node: The node to remove.
        
        Returns:
            True if the node was found and removed, False otherwise.
        """
        node_hex = get_id_hex(node.node_id)
        prefix_len = get_shared_prefix_length(self._owner_hex, node_hex)
        
        if prefix_len >= self._num_rows:
            return False
        
        col = hex_digit_to_int(node_hex[prefix_len])
        
        if self._table[prefix_len][col] == node:
            self._table[prefix_len][col] = None
            return True
        
        return False
    
    def get_node_for_key(self, key_id: int) -> Optional["PastryNode"]:
        """
        Find a node in the routing table that shares a longer prefix with the key.
        
        Args:
            key_id: The key ID to route towards.
        
        Returns:
            A node that shares a longer prefix with the key, or None.
        """
        key_hex = get_id_hex(key_id)
        prefix_len = get_shared_prefix_length(self._owner_hex, key_hex)
        
        if prefix_len >= self._num_rows:
            return None
        
        # Look for node at row=prefix_len with matching digit
        target_digit = hex_digit_to_int(key_hex[prefix_len])
        node = self._table[prefix_len][target_digit]
        
        return node
    
    def get_closest_node(self, key_id: int, current_prefix_len: int) -> Optional["PastryNode"]:
        """
        Find the closest node when no exact routing table match exists.
        
        Looks for a node with the same prefix length but numerically closer to the key.
        
        Args:
            key_id: The key ID to route towards.
            current_prefix_len: Current shared prefix length.
        
        Returns:
            A closer node, or None if none found.
        """
        key_hex = get_id_hex(key_id)
        
        # Look in the same row for a closer node
        if current_prefix_len < self._num_rows:
            min_distance = abs(key_id - self._owner_id)
            closest = None
            
            for col in range(self._num_cols):
                node = self._table[current_prefix_len][col]
                if node is not None:
                    dist = abs(node.node_id - key_id)
                    if dist < min_distance:
                        min_distance = dist
                        closest = node
            
            return closest
        
        return None
    
    def get_all_nodes(self) -> List["PastryNode"]:
        """Get all nodes in the routing table."""
        nodes = []
        for row in self._table:
            for node in row:
                if node is not None:
                    nodes.append(node)
        return nodes
    
    def get_row(self, row: int) -> List[Optional["PastryNode"]]:
        """Get all entries in a specific row."""
        if 0 <= row < self._num_rows:
            return self._table[row].copy()
        return []
    
    def get_filled_count(self) -> int:
        """Get the number of filled entries in the routing table."""
        count = 0
        for row in self._table:
            for node in row:
                if node is not None:
                    count += 1
        return count
    
    def __repr__(self) -> str:
        filled = self.get_filled_count()
        total = self._num_rows * self._num_cols
        return f"RoutingTable(filled={filled}/{total})"
    
    def print_compact(self, max_rows: int = 5) -> str:
        """
        Get a compact string representation for debugging.
        
        Args:
            max_rows: Maximum number of rows to display.
        
        Returns:
            Compact string representation.
        """
        lines = [f"RoutingTable for {self._owner_hex[:8]}...:"]
        
        for row in range(min(max_rows, self._num_rows)):
            row_entries = []
            for col in range(self._num_cols):
                node = self._table[row][col]
                if node is not None:
                    row_entries.append(f"{col:x}:{get_id_hex(node.node_id)[:4]}")
            
            if row_entries:
                lines.append(f"  Row {row}: {', '.join(row_entries)}")
            else:
                lines.append(f"  Row {row}: (empty)")
        
        if self._num_rows > max_rows:
            lines.append(f"  ... ({self._num_rows - max_rows} more rows)")
        
        return "\n".join(lines)
