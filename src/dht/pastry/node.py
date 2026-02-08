"""
Pastry node implementation.

Implements the Pastry DHT protocol with prefix-based routing for O(log_b N) lookups.
Based on the original Pastry paper by Rowstron and Druschel.

Hop Counting Model:
    All network messages between different nodes are counted as hops,
    including both routing hops and communication hops.
"""

from typing import Any, Dict, List, Optional, Tuple

import config
from src.common.hashing import hash_key, hash_node, get_id_hex
from src.common.logger import get_logger
from src.dht.base_node import BaseNode
from src.dht.pastry.routing_table import (
    RoutingTable,
    LeafSet,
    get_shared_prefix_length,
)

logger = get_logger(__name__)


class PastryNode(BaseNode):
    """
    A node in the Pastry DHT network.
    
    Implements the Pastry protocol with prefix-based routing for O(log_b N) lookups,
    where b is the base (typically 16 for hexadecimal).
    
    Inherits from BaseNode:
        - insert(), lookup(), delete(), update() - use find_successor() for routing
    
    Implements:
        - find_successor() - Pastry's prefix-based routing
        - join() - Pastry's join protocol
        - leave() - Pastry's leave protocol
    
    Attributes:
        identifier: Human-readable name for the node.
        node_id: Numeric position in the hash space (0 to 2^m - 1).
        node_hex: Hexadecimal representation of node_id.
        routing_table: Prefix-based routing table.
        leaf_set: L closest nodes by numeric ID.
    """
    
    def __init__(self, identifier: str, node_id: int = None):
        """
        Create a new Pastry node.
        
        Args:
            identifier: Human-readable name (e.g., "node_1").
            node_id: Position in hash space. If None, computed from identifier.
        """
        if node_id is None:
            node_id = hash_node(identifier)
        
        super().__init__(identifier, node_id)
        
        self._node_hex = get_id_hex(node_id)
        self._routing_table = RoutingTable(node_id)
        self._leaf_set = LeafSet(node_id)
        self._is_active = False
    
    @property
    def node_hex(self) -> str:
        """Hexadecimal representation of node_id."""
        return self._node_hex
    
    @property
    def routing_table(self) -> RoutingTable:
        """The node's routing table for prefix-based routing."""
        return self._routing_table
    
    @property
    def leaf_set(self) -> LeafSet:
        """The node's leaf set (L closest nodes)."""
        return self._leaf_set
    
    @property
    def is_active(self) -> bool:
        """Whether this node is currently part of the network."""
        return self._is_active
    
    @property
    def successor(self) -> Optional["PastryNode"]:
        """The immediate successor (first node in right leaf set)."""
        return self._leaf_set.get_successor()
    
    @property
    def predecessor(self) -> Optional["PastryNode"]:
        """The immediate predecessor (first node in left leaf set)."""
        return self._leaf_set.get_predecessor()
    
    # =========================================================================
    # Core DHT Operations (Pastry-specific implementations)
    # =========================================================================
    
    def find_successor(self, key_id: int) -> Tuple["PastryNode", int]:
        """
        Find the node responsible for key_id using Pastry's prefix-based routing.
        
        In Pastry, the responsible node is the one with numerically
        closest ID to the key.
        
        Args:
            key_id: The hashed key ID to look up.
        
        Returns:
            Tuple of (responsible_node, hop_count).
        """
        node, hops = self._route_to_key(key_id)
        
        if node is None:
            return self, hops
        
        return node, hops
    
    def join(self, existing_node: Optional["PastryNode"] = None) -> int:
        """
        Join the Pastry network.
        
        If existing_node is None, this node starts a new network.
        Otherwise, it joins through the existing node using Pastry's
        join protocol.
        
        Hop counting includes:
            - Routing hops to find position in network
            - Hops to collect routing table state from nodes along the path
            - Hops to notify leaf set neighbors
        
        Args:
            existing_node: A node already in the network, or None to start new.
        
        Returns:
            Total number of hops (network messages) used during join.
        """
        total_hops = 0
        
        if existing_node is None:
            # Starting a new network
            logger.info(f"{self.identifier} starting new Pastry network")
            # No other nodes to contact
        else:
            # Joining existing network
            logger.info(f"{self.identifier} joining network via {existing_node.identifier}")
            
            # Step 1: Route join message to node closest to our ID
            hops = self._route_join(existing_node)
            total_hops += hops
            
            # Step 2: Notify leaf set neighbors
            notify_hops = self._notify_neighbors()
            total_hops += notify_hops
            
            # Step 3: Get keys from neighbors that now belong to us
            migrate_hops = self._migrate_keys()
            total_hops += migrate_hops
        
        self._is_active = True
        logger.debug(f"{self.identifier} joined (hops: {total_hops})")
        return total_hops
    
    def leave(self) -> int:
        """
        Gracefully leave the Pastry network.
        
        Transfers keys to the closest remaining node and notifies neighbors.
        
        Hop counting:
            - 1 hop per neighbor notified
            - Keys transferred as part of notification
        
        Returns:
            Number of hops (network messages) used during leave.
        """
        total_hops = 0
        
        if not self._is_active:
            logger.warning(f"{self.identifier} is not active, cannot leave")
            return 0
        
        logger.info(f"{self.identifier} leaving network")
        
        neighbors = self._leaf_set.get_all_nodes()
        
        if not neighbors:
            # We're the only node
            logger.info(f"{self.identifier} was the only node, network is now empty")
            self._data.clear()
            self._is_active = False
            return 0
        
        # Find closest neighbor to transfer keys to
        closest = None
        min_dist = float('inf')
        for node in neighbors:
            dist = abs(node.node_id - self._node_id)
            if dist < min_dist:
                min_dist = dist
                closest = node
        
        # Transfer all keys to closest neighbor
        if closest and self._data:
            # 1 hop: transfer keys to closest neighbor
            for key, value in self._data.items():
                closest.store_local(key, value)
                logger.debug(f"Transferred key '{key}' to {closest.identifier}")
            total_hops += 1
        
        # Notify all neighbors to remove us from their state
        for node in neighbors:
            # 1 hop: notification message to each neighbor
            total_hops += 1
            node._remove_from_state(self)
        
        # Clear our state
        self._data.clear()
        self._is_active = False
        logger.info(f"{self.identifier} has left the network (hops: {total_hops})")
        
        return total_hops
    
    def get_routing_table_size(self) -> int:
        """Get the number of unique nodes in routing state."""
        rt_nodes = set(n.node_id for n in self._routing_table.get_all_nodes())
        leaf_nodes = set(n.node_id for n in self._leaf_set.get_all_nodes())
        return len(rt_nodes | leaf_nodes)
    
    # =========================================================================
    # Internal Routing Methods
    # =========================================================================
    
    def _route_to_key(self, key_id: int) -> Tuple[Optional["PastryNode"], int]:
        """
        Route towards the node responsible for a key.
        
        Uses Pastry's routing algorithm:
        1. If key is in leaf set range, forward to closest leaf
        2. Else, use routing table to find node with longer prefix match
        3. If no progress possible, forward to numerically closer node
        
        Args:
            key_id: The key ID to route towards.
        
        Returns:
            Tuple of (closest_node, hop_count).
        """
        hops = 0
        current = self
        visited = set()
        
        while current.node_id not in visited:
            visited.add(current.node_id)
            
            # Check if we're the closest node
            next_node = current._get_next_hop(key_id)
            
            if next_node is None:
                # We're the closest
                return current, hops
            
            if next_node.node_id == current.node_id:
                # No progress possible
                return current, hops
            
            # Check if next node is actually closer
            current_dist = abs(current.node_id - key_id)
            next_dist = abs(next_node.node_id - key_id)
            
            if next_dist >= current_dist:
                # We're already the closest
                return current, hops
            
            # Move to next node
            hops += 1  # 1 hop: forward message to next node
            current = next_node
        
        return current, hops
    
    def _get_next_hop(self, key_id: int) -> Optional["PastryNode"]:
        """
        Determine the next hop for routing to a key.
        
        Args:
            key_id: The key ID to route towards.
        
        Returns:
            The next node to forward to, or None if we're closest.
        """
        key_hex = get_id_hex(key_id)
        
        # Step 1: Check leaf set
        leaf_nodes = self._leaf_set.get_all_nodes()
        if leaf_nodes:
            # Find closest node (including ourselves)
            min_dist = abs(self._node_id - key_id)
            closest = None
            
            for node in leaf_nodes:
                dist = abs(node.node_id - key_id)
                if dist < min_dist:
                    min_dist = dist
                    closest = node
            
            if closest is not None:
                return closest
        
        # Step 2: Use routing table
        prefix_len = get_shared_prefix_length(self._node_hex, key_hex)
        
        # Try to find node with longer prefix match
        rt_node = self._routing_table.get_node_for_key(key_id)
        if rt_node is not None:
            return rt_node
        
        # Step 3: Find any node closer to the key
        rt_closer = self._routing_table.get_closest_node(key_id, prefix_len)
        if rt_closer is not None:
            return rt_closer
        
        # No better node found
        return None
    
    # =========================================================================
    # Join Protocol Helpers
    # =========================================================================
    
    def _route_join(self, entry_node: "PastryNode") -> int:
        """
        Route a join message through the network to find our position.
        
        Args:
            entry_node: The node to start routing from.
        
        Returns:
            Number of hops used.
        """
        total_hops = 0
        current = entry_node
        visited = set()
        
        while current is not None and current.node_id not in visited:
            visited.add(current.node_id)
            
            # 1 hop: message to current node asking for state
            total_hops += 1
            
            # Collect routing information from current node
            self._update_from_node(current)
            
            # Find next hop towards our ID
            next_node, _ = current._route_to_key(self._node_id)
            
            if next_node is None or next_node.node_id == current.node_id:
                # Reached the closest node
                break
            
            if next_node.node_id == self._node_id:
                # Found ourselves (shouldn't happen)
                break
            
            current = next_node
        
        return total_hops
    
    def _update_from_node(self, node: "PastryNode") -> None:
        """
        Update our routing state from another node.
        
        Args:
            node: The node to copy state from.
        """
        # Add the node itself to our state
        self._routing_table.insert(node)
        self._leaf_set.insert(node)
        
        # Copy relevant routing table entries
        prefix_len = get_shared_prefix_length(self._node_hex, node.node_hex)
        
        # Copy row at prefix_len from the other node
        if prefix_len < self._routing_table.num_rows:
            for col in range(self._routing_table.num_cols):
                other_node = node.routing_table.get(prefix_len, col)
                if other_node is not None and other_node.node_id != self._node_id:
                    self._routing_table.insert(other_node)
                    self._leaf_set.insert(other_node)
        
        # Copy leaf set entries
        for leaf_node in node.leaf_set.get_all_nodes():
            if leaf_node.node_id != self._node_id:
                self._leaf_set.insert(leaf_node)
                self._routing_table.insert(leaf_node)
    
    def _notify_neighbors(self) -> int:
        """
        Notify leaf set neighbors about our presence.
        
        Returns:
            Number of hops used.
        """
        total_hops = 0
        
        # Notify all nodes in our leaf set
        for node in self._leaf_set.get_all_nodes():
            # 1 hop: message to neighbor
            total_hops += 1
            node._add_to_state(self)
        
        return total_hops
    
    def _add_to_state(self, node: "PastryNode") -> None:
        """
        Add a node to our routing state.
        
        Args:
            node: The node to add.
        """
        self._routing_table.insert(node)
        self._leaf_set.insert(node)
    
    def _remove_from_state(self, node: "PastryNode") -> None:
        """
        Remove a node from our routing state.
        
        Args:
            node: The node to remove.
        """
        self._routing_table.remove(node)
        self._leaf_set.remove(node)
    
    def _migrate_keys(self) -> int:
        """
        Get keys from neighbors that should now belong to us.
        
        Returns:
            Number of hops used (1 if keys migrated, 0 otherwise).
        """
        neighbors = self._leaf_set.get_all_nodes()
        if not neighbors:
            return 0
        
        keys_migrated = False
        
        for neighbor in neighbors:
            keys_to_take = []
            for key in list(neighbor.data.keys()):
                key_id = hash_key(key)
                # Key belongs to us if we're closer
                if abs(key_id - self._node_id) < abs(key_id - neighbor.node_id):
                    keys_to_take.append(key)
            
            for key in keys_to_take:
                value = neighbor.data.pop(key)
                self._data[key] = value
                keys_migrated = True
                logger.debug(f"Migrated key '{key}' from {neighbor.identifier} to {self.identifier}")
        
        # Count as 1 hop if any keys were migrated (batch transfer)
        return 1 if keys_migrated else 0
    
