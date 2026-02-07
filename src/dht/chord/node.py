"""
Chord node implementation.

Implements the Chord DHT protocol with O(log N) routing.
Based on the original Chord paper by Stoica et al.

Hop Counting Model:
    All network messages between different nodes are counted as hops,
    including both routing hops and communication hops.
"""

from typing import Any, Dict, List, Optional, Tuple

import config
from src.common.hashing import hash_key, hash_node, in_range
from src.common.logger import get_logger
from src.dht.base_node import BaseNode
from src.dht.chord.finger_table import FingerTable

logger = get_logger(__name__)


class ChordNode(BaseNode):
    """
    A node in the Chord DHT network.
    
    Implements the Chord protocol with finger table routing for O(log N) lookups.
    
    Inherits from BaseNode:
        - insert(), lookup(), delete(), update() - use find_successor() for routing
    
    Implements:
        - find_successor() - Chord's finger table routing
        - join() - Chord's join protocol
        - leave() - Chord's leave protocol
    
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
    
    # =========================================================================
    # Core DHT Operations (Chord-specific implementations)
    # =========================================================================
    
    def find_successor(self, key_id: int) -> Tuple["ChordNode", int]:
        """
        Find the node responsible for key_id using Chord's finger table routing.
        
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
    
    def join(self, existing_node: Optional["ChordNode"] = None) -> int:
        """
        Join the Chord network using the practical Chord protocol (SIGCOMM 2001).

        Uses the lazy/stabilization-based approach:
        - Join only sets successor and predecessor pointers (O(log N) hops)
        - Finger tables are populated lazily via fix_fingers()
        - stabilize() maintains predecessor/successor correctness

        Args:
            existing_node: A node already in the network, or None to start new.

        Returns:
            Total number of hops (network messages) used during join.
        """
        total_hops = 0

        if existing_node is None:
            # Starting a new network - we are our own successor and predecessor
            logger.info(f"{self.identifier} starting new Chord network")
            self._predecessor = self
            self.successor = self
            self._init_finger_table_single()
        else:
            # Joining existing network (practical Chord protocol)
            logger.info(f"{self.identifier} joining network via {existing_node.identifier}")

            # Step 1: Find our successor via routing (O(log N) hops)
            successor, hops = existing_node.find_successor(self._node_id)
            total_hops += hops

            # Step 2: Set our successor (finger[0])
            self.successor = successor

            # Step 3: Get predecessor from successor (1 hop)
            self._predecessor = successor.predecessor
            total_hops += 1

            # Step 4: Notify successor that we are its new predecessor (1 hop)
            successor.predecessor = self
            total_hops += 1

            # Step 5: Notify our predecessor to update its successor to us (1 hop)
            if self._predecessor and self._predecessor != self and self._predecessor != successor:
                self._predecessor.successor = self
                total_hops += 1

            # Step 6: Migrate keys from successor that now belong to us
            migrate_hops = self._migrate_keys_from_successor()
            total_hops += migrate_hops

            # Finger table is NOT initialized here — it is populated
            # lazily by fix_fingers() during stabilization rounds.

        self._is_active = True
        logger.debug(f"{self.identifier} joined with successor={self.successor.identifier}, hops={total_hops}")
        return total_hops
    
    def leave(self) -> int:
        """
        Gracefully leave the Chord network.
        
        Transfers all keys to successor and updates predecessor/successor links.
        Uses lazy approach: only notifies immediate neighbors, finger tables
        are repaired lazily by other nodes during stabilization.
        
        Hop counting:
            - 1 hop: notify successor (transfer keys + update predecessor pointer)
            - 1 hop: notify predecessor (update successor pointer)
            - Total: 2 hops for normal leave, 0 if only node in network
        
        Returns:
            Number of hops (network messages) used during leave.
        """
        total_hops = 0
        
        if not self._is_active:
            logger.warning(f"{self.identifier} is not active, cannot leave")
            return 0
        
        logger.info(f"{self.identifier} leaving network")
        
        # Check if we are the only node
        if self.successor == self and self._predecessor == self:
            logger.info(f"{self.identifier} was the only node, network is now empty")
            self._data.clear()
            self._is_active = False
            return 0
        
        # Step 1: Transfer keys and notify successor
        # 1 hop: message to successor with keys and new predecessor info
        if self.successor and self.successor != self:
            for key, value in self._data.items():
                self.successor.store_local(key, value)
                logger.debug(f"Transferred key '{key}' to {self.successor.identifier}")
            self.successor.predecessor = self._predecessor
            total_hops += 1
        
        # Step 2: Notify predecessor of new successor
        # 1 hop: message to predecessor with new successor info
        if self._predecessor and self._predecessor != self:
            self._predecessor.successor = self.successor
            total_hops += 1
        
        # Finger tables of other nodes are NOT updated here (lazy approach)
        # They will be repaired during stabilization or when lookups fail
        
        # Clear our state
        self._data.clear()
        self._is_active = False
        logger.info(f"{self.identifier} has left the network (hops: {total_hops})")
        
        return total_hops
    
    def get_routing_table_size(self) -> int:
        """Get the number of unique nodes in the finger table."""
        return len(self._finger_table.get_all_unique_nodes())
    
    # =========================================================================
    # Internal Helper Methods
    # =========================================================================
    
    def _init_finger_table_single(self) -> None:
        """Initialize finger table when we are the only node."""
        for i in range(self._finger_table.size):
            self._finger_table.set_node(i, self)
    
    def _migrate_keys_from_successor(self) -> int:
        """
        Take over keys from successor that now belong to us.
        
        Returns:
            Number of hops used (1 if keys migrated, 0 if no migration needed).
        """
        if self.successor is None or self.successor == self:
            return 0
        
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
        
        if keys_to_migrate:
            # 1 hop: message to successor to transfer keys
            for key in keys_to_migrate:
                value = self.successor.data.pop(key)
                self._data[key] = value
                logger.debug(f"Migrated key '{key}' from {self.successor.identifier} to {self.identifier}")
            return 1
        
        return 0
    
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
            hops += 1  # 1 hop: message to next node
        
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
    
    # =========================================================================
    # Stabilization Methods
    # =========================================================================
    
    def stabilize(self) -> None:
        """
        Periodic stabilization to fix successor pointer.
        
        Called periodically to handle concurrent joins/leaves.
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
    
    # =========================================================================
    # Debugging
    # =========================================================================
    
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
