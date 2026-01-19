"""
Abstract base class for DHT networks.

Provides shared functionality for managing DHT networks, including:
- Node storage and access
- Bulk operations (insert, lookup, delete)
- Concurrent operations
- Network statistics

Hop Counting Model:
    All network messages between different nodes are counted as hops.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import random

from src.dht.base_node import BaseNode
from src.common.logger import get_logger

logger = get_logger(__name__)


class BaseNetwork(ABC):
    """
    Abstract base class for a DHT network.
    
    All DHT network implementations must inherit from this class.
    
    Concrete methods (shared by all DHTs):
        - bulk_insert(): Insert multiple key-value pairs
        - bulk_lookup(): Look up multiple keys
        - bulk_delete(): Delete multiple keys
        - concurrent_lookup(): Look up multiple keys concurrently
        - get_node(): Get node by identifier
        - get_random_node(): Get a random node
        - clear(): Clear the network
    
    Abstract methods (each DHT implements differently):
        - create_node(): Create a new node
        - add_node(): Add a node to the network
        - remove_node(): Remove a node from the network
        - build_network(): Build a network with multiple nodes
    
    Attributes:
        nodes: List of all nodes in the network.
        node_count: Number of nodes in the network.
    """
    
    def __init__(self):
        """Initialize an empty network."""
        self._nodes: List[BaseNode] = []
        self._nodes_by_id: Dict[int, BaseNode] = {}
        self._nodes_by_identifier: Dict[str, BaseNode] = {}
    
    # =========================================================================
    # Properties
    # =========================================================================
    
    @property
    def nodes(self) -> List[BaseNode]:
        """List of all nodes in the network."""
        return self._nodes.copy()
    
    @property
    def node_count(self) -> int:
        """Number of nodes in the network."""
        return len(self._nodes)
    
    # =========================================================================
    # Abstract Methods - Each DHT implements differently
    # =========================================================================
    
    @abstractmethod
    def create_node(self, identifier: str) -> BaseNode:
        """
        Create a new node (but don't add it to the network yet).
        
        Args:
            identifier: Human-readable name for the node.
        
        Returns:
            The created node.
        """
        pass
    
    @abstractmethod
    def add_node(self, node: BaseNode) -> int:
        """
        Add an existing node to the network.
        
        Args:
            node: The node to add.
        
        Returns:
            Number of hops used during the join process.
        """
        pass
    
    @abstractmethod
    def remove_node(self, identifier: str) -> Tuple[bool, int]:
        """
        Remove a node from the network.
        
        Args:
            identifier: Identifier of the node to remove.
        
        Returns:
            Tuple of (success, leave_hops).
        """
        pass
    
    @abstractmethod
    def build_network(self, num_nodes: int, identifier_prefix: str = "node_") -> Dict[str, Any]:
        """
        Build a network with the specified number of nodes.
        
        Args:
            num_nodes: Number of nodes to create.
            identifier_prefix: Prefix for node identifiers.
        
        Returns:
            Dictionary with build statistics.
        """
        pass
    
    # =========================================================================
    # Node Access Methods
    # =========================================================================
    
    def get_node(self, identifier: str) -> Optional[BaseNode]:
        """
        Get a node by its identifier.
        
        Args:
            identifier: The node's identifier.
        
        Returns:
            The node if found, None otherwise.
        """
        return self._nodes_by_identifier.get(identifier)
    
    def get_random_node(self) -> Optional[BaseNode]:
        """
        Get a random node from the network.
        
        Returns:
            A random node, or None if network is empty.
        """
        if not self._nodes:
            return None
        return random.choice(self._nodes)
    
    # =========================================================================
    # Single Operations
    # =========================================================================
    
    def insert(self, key: str, value: Any, from_node: BaseNode = None) -> Tuple[bool, int]:
        """
        Insert a key-value pair into the network.
        
        Args:
            key: The key to insert.
            value: The value to store.
            from_node: Node to start from. If None, uses random node.
        
        Returns:
            Tuple of (success, hop_count).
        """
        if not self._nodes:
            logger.error("Cannot insert: network is empty")
            return False, 0
        
        if from_node is None:
            from_node = random.choice(self._nodes)
        
        return from_node.insert(key, value)
    
    def lookup(self, key: str, from_node: BaseNode = None) -> Tuple[Optional[Any], int]:
        """
        Look up a value by key.
        
        Args:
            key: The key to look up.
            from_node: Node to start from. If None, uses random node.
        
        Returns:
            Tuple of (value or None, hop_count).
        """
        if not self._nodes:
            logger.error("Cannot lookup: network is empty")
            return None, 0
        
        if from_node is None:
            from_node = random.choice(self._nodes)
        
        return from_node.lookup(key)
    
    def delete(self, key: str, from_node: BaseNode = None) -> Tuple[bool, int]:
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
    
    def update(self, key: str, value: Any, from_node: BaseNode = None) -> Tuple[bool, int]:
        """
        Update a key's value in the network.
        
        Args:
            key: The key to update.
            value: The new value.
            from_node: Node to start from. If None, uses random node.
        
        Returns:
            Tuple of (success, hop_count).
        """
        if not self._nodes:
            logger.error("Cannot update: network is empty")
            return False, 0
        
        if from_node is None:
            from_node = random.choice(self._nodes)
        
        return from_node.update(key, value)
    
    # =========================================================================
    # Bulk Operations (Sequential)
    # =========================================================================
    
    def bulk_insert(
        self,
        items: List[Tuple[str, Any]],
        from_node: BaseNode = None
    ) -> Dict[str, Any]:
        """
        Insert multiple key-value pairs sequentially.
        
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
        if not self._nodes:
            logger.error("Cannot bulk_insert: network is empty")
            return {"total_items": 0, "total_hops": 0, "average_hops": 0, "success_count": 0}
        
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
        from_node: BaseNode = None
    ) -> Dict[str, Any]:
        """
        Look up multiple keys sequentially.
        
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
        if not self._nodes:
            logger.error("Cannot bulk_lookup: network is empty")
            return {"total_keys": 0, "total_hops": 0, "average_hops": 0, "found_count": 0, "not_found_count": 0}
        
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
        from_node: BaseNode = None
    ) -> Dict[str, Any]:
        """
        Delete multiple keys sequentially.
        
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
        if not self._nodes:
            logger.error("Cannot bulk_delete: network is empty")
            return {"total_keys": 0, "total_hops": 0, "average_hops": 0, "success_count": 0}
        
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
    
    # =========================================================================
    # Concurrent Operations
    # =========================================================================
    
    def concurrent_lookup(
        self,
        keys: List[str],
        max_workers: int = None
    ) -> Dict[str, Any]:
        """
        Look up multiple keys concurrently.
        
        Uses ThreadPoolExecutor to perform lookups in parallel.
        Each lookup starts from a random node.
        
        Args:
            keys: List of keys to look up.
            max_workers: Maximum number of concurrent threads.
                        If None, defaults to min(len(keys), 32).
        
        Returns:
            Dictionary with statistics:
                - total_keys: Number of keys looked up
                - total_hops: Total hops across all lookups
                - average_hops: Average hops per lookup
                - found_count: Number of keys found
                - not_found_count: Number of keys not found
                - results: Dict mapping key -> (value, hops)
        """
        if not self._nodes:
            logger.error("Cannot concurrent_lookup: network is empty")
            return {
                "total_keys": 0,
                "total_hops": 0,
                "average_hops": 0,
                "found_count": 0,
                "not_found_count": 0,
                "results": {},
            }
        
        if max_workers is None:
            max_workers = min(len(keys), 32)
        
        results = {}
        total_hops = 0
        found_count = 0
        
        def lookup_key(key: str) -> Tuple[str, Optional[Any], int]:
            """Worker function for concurrent lookup."""
            node = random.choice(self._nodes)
            value, hops = node.lookup(key)
            return key, value, hops
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(lookup_key, key): key for key in keys}
            
            for future in as_completed(futures):
                key, value, hops = future.result()
                results[key] = (value, hops)
                total_hops += hops
                if value is not None:
                    found_count += 1
        
        return {
            "total_keys": len(keys),
            "total_hops": total_hops,
            "average_hops": total_hops / len(keys) if keys else 0,
            "found_count": found_count,
            "not_found_count": len(keys) - found_count,
            "results": results,
        }
    
    def concurrent_insert(
        self,
        items: List[Tuple[str, Any]],
        max_workers: int = None
    ) -> Dict[str, Any]:
        """
        Insert multiple key-value pairs concurrently.
        
        Uses ThreadPoolExecutor to perform inserts in parallel.
        Each insert starts from a random node.
        
        Args:
            items: List of (key, value) tuples to insert.
            max_workers: Maximum number of concurrent threads.
                        If None, defaults to min(len(items), 32).
        
        Returns:
            Dictionary with statistics:
                - total_items: Number of items inserted
                - total_hops: Total hops across all inserts
                - average_hops: Average hops per insert
                - success_count: Number of successful inserts
        """
        if not self._nodes:
            logger.error("Cannot concurrent_insert: network is empty")
            return {"total_items": 0, "total_hops": 0, "average_hops": 0, "success_count": 0}
        
        if max_workers is None:
            max_workers = min(len(items), 32)
        
        total_hops = 0
        success_count = 0
        
        def insert_item(item: Tuple[str, Any]) -> Tuple[bool, int]:
            """Worker function for concurrent insert."""
            key, value = item
            node = random.choice(self._nodes)
            return node.insert(key, value)
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(insert_item, item) for item in items]
            
            for future in as_completed(futures):
                success, hops = future.result()
                total_hops += hops
                if success:
                    success_count += 1
        
        return {
            "total_items": len(items),
            "total_hops": total_hops,
            "average_hops": total_hops / len(items) if items else 0,
            "success_count": success_count,
        }
    
    # =========================================================================
    # Network Statistics
    # =========================================================================
    
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
    
    # =========================================================================
    # Utility Methods
    # =========================================================================
    
    def clear(self) -> None:
        """Remove all nodes and reset the network."""
        for node in self._nodes:
            node._is_active = False
            node._data.clear()
        
        self._nodes.clear()
        self._nodes_by_id.clear()
        self._nodes_by_identifier.clear()
        
        logger.info("Network cleared")
    
    def get_id_order(self) -> List[BaseNode]:
        """
        Get nodes sorted by their numeric ID.
        
        Returns:
            List of nodes sorted by node_id.
        """
        return sorted(self._nodes, key=lambda n: n.node_id)
