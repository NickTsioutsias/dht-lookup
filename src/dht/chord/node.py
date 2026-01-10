"""
Chord node implementation.

Implements the Chord DHT protocol with O(log N) routing.
Based on the original Chord paper by Stoica et al.
"""

from typing import Any, Dict, List, Optional, Tuple

import config
from src.common.hashing import hash_key, hash_node, in_range, distance
from src.common.logger import get_logger
from src.dht.base_node import BaseNode
from src.dht.chord.finger_table import FingerTable

logger = get_logger(__name__)


class ChordNode(BaseNode):
    """
    A node in the Chord DHT network.
    
    Implements the Chord protocol with finger table routing for O(log N) lookups.
    
    Attributes:
        identifier: Human-readable name for the node.
        node_id: Position in the hash space (0 to 2^m - 1).
        finger_table: Routing table with m entries.
        predecessor: The node immediately before this one on the ring.
    """
    
    def __init__(self, identifier: str, node_id: int = None):
        """
        Create a new Chord node.
        
        Args:
            identifier: Human-readable name (e.g., "node_1").
            node_id: Position in hash space. If None, computed from identifier.
        """
        if node_id is None:
            node_id = hash_node(identifier)
        
        super().__init__(identifier, node_id)
        
        self._finger_table = FingerTable(node_id)
        self._predecessor: Optional[ChordNode] = None
        self._is_active = False
    
    @property
    def finger_table(self) -> FingerTable:
        """The node's finger table for routing."""
        return self._finger_table
    
    @property
    def predecessor(self) -> Optional["ChordNode"]:
        """The node immediately before this one on the ring."""
        return self._predecessor
    
    @predecessor.setter
    def predecessor(self, node: Optional["ChordNode"]) -> None:
        """Set the predecessor node."""
        self._predecessor = node
    
    @property
    def successor(self) -> Optional["ChordNode"]:
        """The node immediately after this one on the ring (finger[0])."""
        return self._finger_table.get_successor()
    
    @successor.setter
    def successor(self, node: "ChordNode") -> None:
        """Set the successor node (finger[0])."""
        self._finger_table.set_successor(node)
    
    @property
    def is_active(self) -> bool:
        """Whether this node is currently part of the network."""
        return self._is_active
    
    def join(self, existing_node: Optional["ChordNode"] = None) -> None:
        """
        Join the Chord network.
        
        If existing_node is None, this node starts a new network.
        Otherwise, it joins through the existing node.
        
        Args:
            existing_node: A node already in the network, or None to start new.
        """
        if existing_node is None:
            # Starting a new network - we are our own successor and predecessor
            logger.info(f"{self.identifier} starting new Chord network")
            self._predecessor = self
            self.successor = self
            self._init_finger_table_single()
        else:
            # Joining existing network
            logger.info(f"{self.identifier} joining network via {existing_node.identifier}")
            self._init_finger_table(existing_node)
            self._update_others()
            self._migrate_keys_from_successor()
        
        self._is_active = True
        logger.debug(f"{self.identifier} joined with successor={self.successor.identifier}")
    
    def _init_finger_table_single(self) -> None:
        """Initialize finger table when we are the only node."""
        for i in range(self._finger_table.size):
            self._finger_table.set_node(i, self)
    
    def _init_finger_table(self, existing_node: "ChordNode") -> None:
        """
        Initialize finger table by querying the existing network.
        
        Args:
            existing_node: A node to query for finger information.
        """
        # Find our successor
        successor, _ = existing_node.find_successor(self._finger_table.get_start(0))
        self._finger_table.set_node(0, successor)
        
        # Set predecessor from successor's predecessor
        self._predecessor = successor.predecessor
        successor.predecessor = self
        
        # Initialize remaining fingers
        for i in range(self._finger_table.size - 1):
            finger_start = self._finger_table.get_start(i + 1)
            current_finger = self._finger_table.get_node(i)
            
            # If the next finger start falls between us and current finger,
            # we can reuse current finger (optimization)
            if current_finger and in_range(
                finger_start,
                self._node_id,
                current_finger.node_id,
                inclusive_start=True,
                inclusive_end=False
            ):
                self._finger_table.set_node(i + 1, current_finger)
            else:
                # Need to query the network
                node, _ = existing_node.find_successor(finger_start)
                self._finger_table.set_node(i + 1, node)
    
    def _update_others(self) -> None:
        """
        Update finger tables of other nodes that should point to us.
        
        Called when joining the network. Finds nodes whose finger tables
        should include this node and updates them.
        """
        for i in range(self._finger_table.size):
            # Find the node that might need to update finger i to point to us
            # This is the predecessor of (self.node_id - 2^i)
            target = (self._node_id - (1 << i)) % config.HASH_SPACE_SIZE
            predecessor = self._find_predecessor(target)
            
            if predecessor and predecessor.node_id != self._node_id:
                predecessor._update_finger_table(self, i)
    
    def _update_finger_table(self, node: "ChordNode", i: int) -> None:
        """
        Update finger i to point to node if appropriate.
        
        Args:
            node: The node that might be a better finger.
            i: The finger index to potentially update.
        """
        current_finger = self._finger_table.get_node(i)
        
        if current_finger is None:
            self._finger_table.set_node(i, node)
            if self._predecessor and self._predecessor.node_id != node.node_id:
                self._predecessor._update_finger_table(node, i)
            return
        
        # Check if node falls in [self.node_id, current_finger.node_id)
        if in_range(
            node.node_id,
            self._node_id,
            current_finger.node_id,
            inclusive_start=True,
            inclusive_end=False
        ):
            self._finger_table.set_node(i, node)
            if self._predecessor and self._predecessor.node_id != node.node_id:
                self._predecessor._update_finger_table(node, i)
    
    def _migrate_keys_from_successor(self) -> None:
        """
        Take over keys from successor that now belong to us.
        
        Called when joining. Keys in range (predecessor, self] should
        be moved from successor to us.
        """
        if self.successor is None or self.successor == self:
            return
        
        keys_to_migrate = []
        for key in list(self.successor.data.keys()):
            key_id = hash_key(key)
            # Key belongs to us if it's in (predecessor.node_id, self.node_id]
            if self._predecessor and in_range(
                key_id,
                self._predecessor.node_id,
                self._node_id,
                inclusive_start=False,
                inclusive_end=True
            ):
                keys_to_migrate.append(key)
        
        for key in keys_to_migrate:
            value = self.successor.data.pop(key)
            self._data[key] = value
            logger.debug(f"Migrated key '{key}' from {self.successor.identifier} to {self.identifier}")
    
    def leave(self) -> None:
        """
        Gracefully leave the Chord network.
        
        Transfers all keys to successor and updates predecessor/successor links.
        """
        if not self._is_active:
            logger.warning(f"{self.identifier} is not active, cannot leave")
            return
        
        logger.info(f"{self.identifier} leaving network")
        
        # Transfer all our keys to successor
        if self.successor and self.successor != self:
            for key, value in self._data.items():
                self.successor.store_local(key, value)
                logger.debug(f"Transferred key '{key}' to {self.successor.identifier}")
        
        # Update links
        if self.successor and self.successor != self:
            self.successor.predecessor = self._predecessor
        
        if self._predecessor and self._predecessor != self:
            self._predecessor.successor = self.successor
            # Update predecessor's finger table
            self._predecessor._remove_from_fingers(self)
        
        # Clear our state
        self._data.clear()
        self._is_active = False
        logger.info(f"{self.identifier} has left the network")
    
    def _remove_from_fingers(self, leaving_node: "ChordNode") -> None:
        """
        Update finger table to remove references to a leaving node.
        
        Args:
            leaving_node: The node that is leaving.
        """
        replacement = leaving_node.successor
        for i in range(self._finger_table.size):
            if self._finger_table.get_node(i) == leaving_node:
                self._finger_table.set_node(i, replacement)
    
    def find_successor(self, key_id: int) -> Tuple["ChordNode", int]:
        """
        Find the node responsible for key_id.
        
        Args:
            key_id: The hashed key ID to look up.
        
        Returns:
            Tuple of (responsible_node, hop_count).
        """
        hops = 0
        predecessor, pred_hops = self._find_predecessor_with_hops(key_id)
        hops += pred_hops
        
        if predecessor.successor:
            return predecessor.successor, hops
        return predecessor, hops
    
    def _find_predecessor(self, key_id: int) -> "ChordNode":
        """
        Find the predecessor node for key_id.
        
        Args:
            key_id: The key ID to find predecessor for.
        
        Returns:
            The predecessor node.
        """
        node, _ = self._find_predecessor_with_hops(key_id)
        return node
    
    def _find_predecessor_with_hops(self, key_id: int) -> Tuple["ChordNode", int]:
        """
        Find the predecessor node for key_id, counting hops.
        
        Args:
            key_id: The key ID to find predecessor for.
        
        Returns:
            Tuple of (predecessor_node, hop_count).
        """
        hops = 0
        current = self
        
        # Keep going until key_id is in (current, current.successor]
        while True:
            if current.successor is None:
                break
            
            if current == current.successor:
                # Single node network
                break
            
            if in_range(
                key_id,
                current.node_id,
                current.successor.node_id,
                inclusive_start=False,
                inclusive_end=True
            ):
                break
            
            # Find closest preceding finger
            next_node = current._closest_preceding_finger(key_id)
            
            if next_node == current:
                # No progress possible
                break
            
            current = next_node
            hops += 1
        
        return current, hops
    
    def _closest_preceding_finger(self, key_id: int) -> "ChordNode":
        """
        Find the closest preceding node to key_id from our finger table.
        
        Args:
            key_id: The target key ID.
        
        Returns:
            The closest preceding node, or self if none found.
        """
        result = self._finger_table.find_closest_preceding_node(key_id)
        return result if result else self
    
    def insert(self, key: str, value: Any) -> Tuple[bool, int]:
        """
        Insert a key-value pair into the DHT.
        
        Args:
            key: The key to insert.
            value: The value to store.
        
        Returns:
            Tuple of (success, hop_count).
        """
        key_id = hash_key(key)
        responsible_node, hops = self.find_successor(key_id)
        responsible_node.store_local(key, value)
        logger.debug(f"Inserted key '{key}' at {responsible_node.identifier} (hops: {hops})")
        return True, hops
    
    def lookup(self, key: str) -> Tuple[Optional[Any], int]:
        """
        Look up a value by key.
        
        Args:
            key: The key to look up.
        
        Returns:
            Tuple of (value or None, hop_count).
        """
        key_id = hash_key(key)
        responsible_node, hops = self.find_successor(key_id)
        value = responsible_node.get_local(key)
        logger.debug(f"Lookup key '{key}' at {responsible_node.identifier} (hops: {hops})")
        return value, hops
    
    def delete(self, key: str) -> Tuple[bool, int]:
        """
        Delete a key-value pair from the DHT.
        
        Args:
            key: The key to delete.
        
        Returns:
            Tuple of (success, hop_count).
        """
        key_id = hash_key(key)
        responsible_node, hops = self.find_successor(key_id)
        success = responsible_node.delete_local(key)
        logger.debug(f"Delete key '{key}' at {responsible_node.identifier}: {success} (hops: {hops})")
        return success, hops
    
    def update(self, key: str, value: Any) -> Tuple[bool, int]:
        """
        Update the value for an existing key.
        
        Args:
            key: The key to update.
            value: The new value.
        
        Returns:
            Tuple of (success, hop_count).
        """
        key_id = hash_key(key)
        responsible_node, hops = self.find_successor(key_id)
        
        if responsible_node.get_local(key) is not None:
            responsible_node.store_local(key, value)
            logger.debug(f"Updated key '{key}' at {responsible_node.identifier} (hops: {hops})")
            return True, hops
        else:
            logger.debug(f"Update failed - key '{key}' not found (hops: {hops})")
            return False, hops
    
    def get_routing_table_size(self) -> int:
        """Get the number of unique nodes in the finger table."""
        return len(self._finger_table.get_all_unique_nodes())
    
    def stabilize(self) -> None:
        """
        Periodic stabilization to fix successor pointer.
        
        Called periodically to handle concurrent joins/leaves.
        Checks if there's a node between us and our successor.
        """
        if not self._is_active or self.successor is None:
            return
        
        # Check if successor's predecessor should be our new successor
        x = self.successor.predecessor
        if x and x != self and in_range(
            x.node_id,
            self._node_id,
            self.successor.node_id,
            inclusive_start=False,
            inclusive_end=False
        ):
            self.successor = x
        
        # Notify successor about us
        self.successor._notify(self)
    
    def _notify(self, node: "ChordNode") -> None:
        """
        Handle notification from a potential predecessor.
        
        Args:
            node: The node claiming to be our predecessor.
        """
        if self._predecessor is None or in_range(
            node.node_id,
            self._predecessor.node_id,
            self._node_id,
            inclusive_start=False,
            inclusive_end=False
        ):
            self._predecessor = node
    
    def fix_fingers(self, index: int = None) -> None:
        """
        Refresh a finger table entry.
        
        Called periodically to keep finger table accurate.
        
        Args:
            index: The finger to fix. If None, fixes a random finger.
        """
        if not self._is_active:
            return
        
        import random
        if index is None:
            index = random.randint(0, self._finger_table.size - 1)
        
        finger_start = self._finger_table.get_start(index)
        node, _ = self.find_successor(finger_start)
        self._finger_table.set_node(index, node)
    
    def get_info(self) -> Dict[str, Any]:
        """
        Get information about this node for debugging.
        
        Returns:
            Dictionary with node information.
        """
        return {
            "identifier": self._identifier,
            "node_id": self._node_id,
            "is_active": self._is_active,
            "predecessor": self._predecessor.identifier if self._predecessor else None,
            "successor": self.successor.identifier if self.successor else None,
            "local_keys": self.get_local_key_count(),
            "finger_table_unique_nodes": self.get_routing_table_size(),
        }