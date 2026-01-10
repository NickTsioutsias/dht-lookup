"""
Finger table implementation for Chord DHT.

The finger table is the core routing structure that enables O(log N) lookups.
Each entry i points to the first node that succeeds (n + 2^i) mod 2^m,
where n is the current node's ID and m is the number of bits in the hash space.
"""

from typing import Optional, List, TYPE_CHECKING

import config
from src.common.logger import get_logger

if TYPE_CHECKING:
    from src.dht.chord.node import ChordNode

logger = get_logger(__name__)


class FingerTableEntry:
    """
    A single entry in the finger table.
    
    Attributes:
        start: The starting position this finger is responsible for (n + 2^i).
        node: The first node that succeeds the start position.
    """
    
    def __init__(self, start: int, node: Optional["ChordNode"] = None):
        """
        Initialize a finger table entry.
        
        Args:
            start: The starting position (n + 2^i) mod 2^m.
            node: The node responsible for this finger (can be set later).
        """
        self.start = start
        self.node = node
    
    def __repr__(self) -> str:
        node_info = f"node_id={self.node.node_id}" if self.node else "None"
        return f"FingerEntry(start={self.start}, {node_info})"


class FingerTable:
    """
    Finger table for Chord routing.
    
    Maintains m entries where entry i points to successor(n + 2^i).
    Enables O(log N) routing by allowing jumps that halve the distance
    to the target with each hop.
    
    Attributes:
        node_id: The ID of the node that owns this finger table.
        size: Number of entries (equals HASH_BIT_SIZE).
        entries: List of FingerTableEntry objects.
    """
    
    def __init__(self, node_id: int, size: int = None):
        """
        Initialize an empty finger table.
        
        Args:
            node_id: The ID of the node that owns this table.
            size: Number of entries. Defaults to CHORD_FINGER_TABLE_SIZE from config.
        """
        self._node_id = node_id
        self._size = size if size is not None else config.CHORD_FINGER_TABLE_SIZE
        self._entries: List[FingerTableEntry] = []
        
        # Initialize entries with correct start values
        for i in range(self._size):
            start = (self._node_id + (1 << i)) % config.HASH_SPACE_SIZE
            self._entries.append(FingerTableEntry(start=start, node=None))
    
    @property
    def node_id(self) -> int:
        """The ID of the node that owns this finger table."""
        return self._node_id
    
    @property
    def size(self) -> int:
        """Number of entries in the finger table."""
        return self._size
    
    def get_start(self, index: int) -> int:
        """
        Get the start value for finger entry at given index.
        
        Args:
            index: Finger table index (0 to size-1).
        
        Returns:
            The start position (n + 2^index) mod 2^m.
        
        Raises:
            IndexError: If index is out of range.
        """
        if index < 0 or index >= self._size:
            raise IndexError(f"Finger index {index} out of range [0, {self._size})")
        return self._entries[index].start
    
    def get_node(self, index: int) -> Optional["ChordNode"]:
        """
        Get the node stored at finger entry at given index.
        
        Args:
            index: Finger table index (0 to size-1).
        
        Returns:
            The node at this finger entry, or None if not set.
        
        Raises:
            IndexError: If index is out of range.
        """
        if index < 0 or index >= self._size:
            raise IndexError(f"Finger index {index} out of range [0, {self._size})")
        return self._entries[index].node
    
    def set_node(self, index: int, node: "ChordNode") -> None:
        """
        Set the node for finger entry at given index.
        
        Args:
            index: Finger table index (0 to size-1).
            node: The node to store at this entry.
        
        Raises:
            IndexError: If index is out of range.
        """
        if index < 0 or index >= self._size:
            raise IndexError(f"Finger index {index} out of range [0, {self._size})")
        self._entries[index].node = node
    
    def get_successor(self) -> Optional["ChordNode"]:
        """
        Get the immediate successor (finger[0]).
        
        The successor is the most important finger - it's the next node
        clockwise on the ring and handles queries we can't handle locally.
        
        Returns:
            The successor node, or None if not set.
        """
        return self._entries[0].node if self._entries else None
    
    def set_successor(self, node: "ChordNode") -> None:
        """
        Set the immediate successor (finger[0]).
        
        Args:
            node: The successor node.
        """
        if self._entries:
            self._entries[0].node = node
    
    def find_closest_preceding_node(self, target_id: int) -> Optional["ChordNode"]:
        """
        Find the closest preceding node to target_id from the finger table.
        
        This is the key routing function. We scan the finger table from
        highest to lowest, looking for a finger that falls between us
        and the target. This finger gets us closer to the target.
        
        Args:
            target_id: The ID we're trying to reach.
        
        Returns:
            The closest preceding node, or None if no suitable finger found.
        """
        from src.common.hashing import in_range
        
        # Scan from highest finger to lowest
        for i in range(self._size - 1, -1, -1):
            finger_node = self._entries[i].node
            if finger_node is None:
                continue
            
            # Check if this finger falls in (self.node_id, target_id)
            # We want a node that is between us and the target (exclusive)
            if in_range(
                finger_node.node_id,
                self._node_id,
                target_id,
                inclusive_start=False,
                inclusive_end=False
            ):
                return finger_node
        
        return None
    
    def get_all_unique_nodes(self) -> List["ChordNode"]:
        """
        Get all unique nodes referenced in the finger table.
        
        Multiple fingers often point to the same node (especially in
        small networks), so this returns deduplicated results.
        
        Returns:
            List of unique nodes in the finger table.
        """
        seen = set()
        unique_nodes = []
        
        for entry in self._entries:
            if entry.node is not None and entry.node.node_id not in seen:
                seen.add(entry.node.node_id)
                unique_nodes.append(entry.node)
        
        return unique_nodes
    
    def get_filled_count(self) -> int:
        """
        Get the number of finger entries that have nodes assigned.
        
        Returns:
            Count of non-None entries.
        """
        return sum(1 for entry in self._entries if entry.node is not None)
    
    def __repr__(self) -> str:
        filled = self.get_filled_count()
        return f"FingerTable(node_id={self._node_id}, entries={filled}/{self._size})"
    
    def __str__(self) -> str:
        """Detailed string representation for debugging."""
        lines = [f"FingerTable for node {self._node_id}:"]
        for i, entry in enumerate(self._entries):
            node_str = f"-> {entry.node.identifier} (id={entry.node.node_id})" if entry.node else "-> None"
            lines.append(f"  [{i:3}] start={entry.start} {node_str}")
        return "\n".join(lines)
    
    def print_compact(self, max_entries: int = 10) -> str:
        """
        Get a compact representation showing only first few entries.
        
        Useful for debugging without printing all 160 entries.
        
        Args:
            max_entries: Maximum number of entries to show.
        
        Returns:
            Compact string representation.
        """
        lines = [f"FingerTable for node {self._node_id} (showing first {max_entries}):"]
        for i in range(min(max_entries, self._size)):
            entry = self._entries[i]
            node_str = f"-> {entry.node.identifier}" if entry.node else "-> None"
            lines.append(f"  [{i}] start={entry.start} {node_str}")
        if self._size > max_entries:
            lines.append(f"  ... ({self._size - max_entries} more entries)")
        return "\n".join(lines)
