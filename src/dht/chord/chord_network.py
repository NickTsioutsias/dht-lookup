"""
Chord network manager.

Provides utilities for creating, managing, and testing Chord networks.
Handles node creation, joining, and bulk operations for benchmarking.
"""

from typing import Any, Dict, List, Optional, Tuple
import random

import config
from src.common.hashing import hash_node
from src.common.logger import get_logger
from src.dht.chord.node import ChordNode

logger = get_logger(__name__)


class ChordNetwork:
    """
    Manager for a Chord DHT network.
    
    Handles creation of nodes, network formation, and provides
    utilities for testing and benchmarking.
    
    Attributes:
        nodes: List of all nodes in the network.
        node_count: Number of nodes in the network.
    """
    
    def __init__(self):
        """Initialize an empty Chord network."""
        self._nodes: List[ChordNode] = []
        self._nodes_by_id: Dict[int, ChordNode] = {}
        self._nodes_by_identifier: Dict[str, ChordNode] = {}
    
    @property
    def nodes(self) -> List[ChordNode]:
        """List of all nodes in the network."""
        return self._nodes.copy()
    
    @property
    def node_count(self) -> int:
        """Number of nodes in the network."""
        return len(self._nodes)
    
    def create_node(self, identifier: str) -> ChordNode:
        """
        Create a new node (but don't add it to the network yet).
        
        Args:
            identifier: Human-readable name for the node.
        
        Returns:
            The created ChordNode.
        
        Raises:
            ValueError: If a node with this identifier already exists.
        """
        if identifier in self._nodes_by_identifier:
            raise ValueError(f"Node with identifier '{identifier}' already exists")
        
        node = ChordNode(identifier)
        return node
    
    def add_node(self, node: ChordNode) -> int:
        """
        Add an existing node to the network.
        
        The node will join through an existing node if the network
        is not empty, otherwise it starts a new network.
        
        Args:
            node: The node to add.
        
        Returns:
            Number of hops used during the join process.
        """
        if node.identifier in self._nodes_by_identifier:
            raise ValueError(f"Node '{node.identifier}' is already in the network")
        
        if len(self._nodes) == 0:
            # First node - start new network
            hops = node.join(None)
        else:
            # Join through a random existing node
            existing_node = random.choice(self._nodes)
            hops = node.join(existing_node)
        
        self._nodes.append(node)
        self._nodes_by_id[node.node_id] = node
        self._nodes_by_identifier[node.identifier] = node
        
        logger.info(f"Added {node.identifier} to network (total: {len(self._nodes)} nodes, join_hops: {hops})")
        return hops
    
    def create_and_add_node(self, identifier: str) -> Tuple[ChordNode, int]:
        """
        Create a new node and add it to the network.
        
        Convenience method combining create_node and add_node.
        
        Args:
            identifier: Human-readable name for the node.
        
        Returns:
            Tuple of (created_node, join_hops).
        """
        node = self.create_node(identifier)
        hops = self.add_node(node)
        return node, hops
    
    def remove_node(self, identifier: str) -> Tuple[bool, int]:
        """
        Remove a node from the network.
        
        The node will gracefully leave, transferring its keys.
        
        Args:
            identifier: Identifier of the node to remove.
        
        Returns:
            Tuple of (success, leave_hops).
        """
        if identifier not in self._nodes_by_identifier:
            logger.warning(f"Node '{identifier}' not found in network")
            return False, 0
        
        node = self._nodes_by_identifier[identifier]
        hops = node.leave()
        
        self._nodes.remove(node)
        del self._nodes_by_id[node.node_id]
        del self._nodes_by_identifier[identifier]
        
        logger.info(f"Removed {identifier} from network (total: {len(self._nodes)} nodes, leave_hops: {hops})")
        return True, hops
    
    def get_node(self, identifier: str) -> Optional[ChordNode]:
        """
        Get a node by its identifier.
        
        Args:
            identifier: The node's identifier.
        
        Returns:
            The node if found, None otherwise.
        """
        return self._nodes_by_identifier.get(identifier)
    
    def get_random_node(self) -> Optional[ChordNode]:
        """
        Get a random node from the network.
        
        Returns:
            A random node, or None if network is empty.
        """
        if not self._nodes:
            return None
        return random.choice(self._nodes)
    
    def build_network(self, num_nodes: int, identifier_prefix: str = "node_") -> Dict[str, Any]:
        """
        Build a network with the specified number of nodes.
        
        Creates nodes with identifiers like "node_0", "node_1", etc.
        
        Args:
            num_nodes: Number of nodes to create.
            identifier_prefix: Prefix for node identifiers.
        
        Returns:
            Dictionary with build statistics:
                - nodes: List of created nodes
                - total_join_hops: Total hops for all joins
                - average_join_hops: Average hops per join
                - join_hops_per_node: List of hops for each node join
        """
        logger.info(f"Building Chord network with {num_nodes} nodes")
        
        created_nodes = []
        join_hops_list = []
        
        for i in range(num_nodes):
            identifier = f"{identifier_prefix}{i}"
            node, hops = self.create_and_add_node(identifier)
            created_nodes.append(node)
            join_hops_list.append(hops)
        
        total_hops = sum(join_hops_list)
        avg_hops = total_hops / num_nodes if num_nodes > 0 else 0
        
        logger.info(f"Network built: {len(self._nodes)} nodes, total_join_hops: {total_hops}")
        
        return {
            "nodes": created_nodes,
            "total_join_hops": total_hops,
            "average_join_hops": avg_hops,
            "join_hops_per_node": join_hops_list,
        }
    
    def stabilize_all(self, rounds: int = 1) -> None:
        """
        Run stabilization on all nodes.
        
        Called to fix up the network after joins/leaves.
        
        Args:
            rounds: Number of stabilization rounds.
        """
        for _ in range(rounds):
            for node in self._nodes:
                node.stabilize()
                node.fix_fingers()
    
    def insert(self, key: str, value: Any, from_node: ChordNode = None) -> Tuple[bool, int]:
        """
        Insert a key-value pair into the network.
        
        Args:
            key: The key to insert.
            value: The value to store.
            from_node: Node to start the insert from. If None, uses random node.
        
        Returns:
            Tuple of (success, hop_count).
        """
        if not self._nodes:
            logger.error("Cannot insert: network is empty")
            return False, 0
        
        if from_node is None:
            from_node = random.choice(self._nodes)
        
        return from_node.insert(key, value)
    
    def lookup(self, key: str, from_node: ChordNode = None) -> Tuple[Optional[Any], int]:
        """
        Look up a value by key.
        
        Args:
            key: The key to look up.
            from_node: Node to start the lookup from. If None, uses random node.
        
        Returns:
            Tuple of (value or None, hop_count).
        """
        if not self._nodes:
            logger.error("Cannot lookup: network is empty")
            return None, 0
        
        if from_node is None:
            from_node = random.choice(self._nodes)
        
        return from_node.lookup(key)
    
    def delete(self, key: str, from_node: ChordNode = None) -> Tuple[bool, int]:
        """
        Delete a key from the network.
        
        Args:
            key: The key to delete.
            from_node: Node to start from. If None, uses random node.
        
        Returns:
            Tuple of (success, hop_count).
        """
        if not self._nodes:
            logger.error("Cannot delete: network is empty")
            return False, 0
        
        if from_node is None:
            from_node = random.choice(self._nodes)
        
        return from_node.delete(key)
    
    def bulk_insert(
        self,
        items: List[Tuple[str, Any]],
        from_node: ChordNode = None
    ) -> Dict[str, Any]:
        """
        Insert multiple key-value pairs.
        
        Args:
            items: List of (key, value) tuples to insert.
            from_node: Node to start from. If None, uses random node for each.
        
        Returns:
            Dictionary with statistics:
                - total_items: Number of items inserted
                - total_hops: Total hops across all inserts
                - average_hops: Average hops per insert
                - success_count: Number of successful inserts
        """
        total_hops = 0
        success_count = 0
        
        for key, value in items:
            node = from_node if from_node else random.choice(self._nodes)
            success, hops = node.insert(key, value)
            total_hops += hops
            if success:
                success_count += 1
        
        return {
            "total_items": len(items),
            "total_hops": total_hops,
            "average_hops": total_hops / len(items) if items else 0,
            "success_count": success_count,
        }
    
    def bulk_lookup(
        self,
        keys: List[str],
        from_node: ChordNode = None
    ) -> Dict[str, Any]:
        """
        Look up multiple keys.
        
        Args:
            keys: List of keys to look up.
            from_node: Node to start from. If None, uses random node for each.
        
        Returns:
            Dictionary with statistics:
                - total_keys: Number of keys looked up
                - total_hops: Total hops across all lookups
                - average_hops: Average hops per lookup
                - found_count: Number of keys found
                - not_found_count: Number of keys not found
        """
        total_hops = 0
        found_count = 0
        
        for key in keys:
            node = from_node if from_node else random.choice(self._nodes)
            value, hops = node.lookup(key)
            total_hops += hops
            if value is not None:
                found_count += 1
        
        return {
            "total_keys": len(keys),
            "total_hops": total_hops,
            "average_hops": total_hops / len(keys) if keys else 0,
            "found_count": found_count,
            "not_found_count": len(keys) - found_count,
        }
    
    def bulk_delete(
        self,
        keys: List[str],
        from_node: ChordNode = None
    ) -> Dict[str, Any]:
        """
        Delete multiple keys.
        
        Args:
            keys: List of keys to delete.
            from_node: Node to start from. If None, uses random node for each.
        
        Returns:
            Dictionary with statistics:
                - total_keys: Number of keys to delete
                - total_hops: Total hops across all deletes
                - average_hops: Average hops per delete
                - success_count: Number of successful deletes
        """
        total_hops = 0
        success_count = 0
        
        for key in keys:
            node = from_node if from_node else random.choice(self._nodes)
            success, hops = node.delete(key)
            total_hops += hops
            if success:
                success_count += 1
        
        return {
            "total_keys": len(keys),
            "total_hops": total_hops,
            "average_hops": total_hops / len(keys) if keys else 0,
            "success_count": success_count,
        }
    
    def get_network_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the network.
        
        Returns:
            Dictionary with network statistics.
        """
        if not self._nodes:
            return {"node_count": 0}
        
        total_keys = sum(node.get_local_key_count() for node in self._nodes)
        keys_per_node = [node.get_local_key_count() for node in self._nodes]
        
        return {
            "node_count": len(self._nodes),
            "total_keys": total_keys,
            "keys_per_node_min": min(keys_per_node),
            "keys_per_node_max": max(keys_per_node),
            "keys_per_node_avg": total_keys / len(self._nodes),
        }
    
    def get_ring_order(self) -> List[ChordNode]:
        """
        Get nodes sorted by their position on the ring.
        
        Returns:
            List of nodes sorted by node_id.
        """
        return sorted(self._nodes, key=lambda n: n.node_id)
    
    def print_ring(self) -> str:
        """
        Get a string representation of the ring structure.
        
        Returns:
            String showing nodes in ring order with their successors.
        """
        if not self._nodes:
            return "Empty network"
        
        lines = ["Chord Ring Structure:"]
        for node in self.get_ring_order():
            successor_id = node.successor.identifier if node.successor else "None"
            predecessor_id = node.predecessor.identifier if node.predecessor else "None"
            lines.append(
                f"  {node.identifier} (id={node.node_id}) "
                f"-> succ: {successor_id}, pred: {predecessor_id}, "
                f"keys: {node.get_local_key_count()}"
            )
        
        return "\n".join(lines)
    
    def clear(self) -> None:
        """Remove all nodes and reset the network."""
        for node in self._nodes:
            node._is_active = False
            node._data.clear()
        
        self._nodes.clear()
        self._nodes_by_id.clear()
        self._nodes_by_identifier.clear()
        
        logger.info("Network cleared")
