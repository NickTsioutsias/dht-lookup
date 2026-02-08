"""
Pastry network manager.

Provides Pastry-specific network management functionality.
Inherits shared functionality from BaseNetwork.
"""

from typing import Any, Dict, List, Optional, Tuple
import random

from src.common.logger import get_logger
from src.dht.base_network import BaseNetwork
from src.dht.pastry.node import PastryNode

logger = get_logger(__name__)


class PastryNetwork(BaseNetwork):
    """
    Manager for a Pastry DHT network.
    
    Inherits from BaseNetwork:
        - Node storage and access
        - Bulk operations (bulk_insert, bulk_lookup, bulk_delete)
        - Concurrent operations (concurrent_lookup, concurrent_insert)
        - Network statistics
    
    Implements:
        - create_node() - Creates PastryNode instances
        - add_node() - Pastry-specific join protocol
        - remove_node() - Pastry-specific leave protocol
        - build_network() - Build Pastry network
    """
    
    def create_node(self, identifier: str) -> PastryNode:
        """
        Create a new Pastry node (but don't add it to the network yet).
        
        Args:
            identifier: Human-readable name for the node.
        
        Returns:
            The created PastryNode.
        
        Raises:
            ValueError: If a node with this identifier already exists.
        """
        if identifier in self._nodes_by_identifier:
            raise ValueError(f"Node with identifier '{identifier}' already exists")
        
        return PastryNode(identifier)
    
    def add_node(self, node: PastryNode) -> int:
        """
        Add an existing Pastry node to the network.
        
        The node will join through an existing node if the network
        is not empty, otherwise it starts a new network.
        
        Args:
            node: The PastryNode to add.
        
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
        
        logger.info(f"Added {node.identifier} to Pastry network (total: {len(self._nodes)} nodes, join_hops: {hops})")
        return hops
    
    def remove_node(self, identifier: str) -> Tuple[bool, int]:
        """
        Remove a node from the Pastry network.
        
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
        
        logger.info(f"Removed {identifier} from Pastry network (total: {len(self._nodes)} nodes, leave_hops: {hops})")
        return True, hops
    
    def build_network(self, num_nodes: int, identifier_prefix: str = "node_") -> Dict[str, Any]:
        """
        Build a Pastry network with the specified number of nodes.
        
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
        logger.info(f"Building Pastry network with {num_nodes} nodes")
        
        created_nodes = []
        join_hops_list = []
        
        for i in range(num_nodes):
            identifier = f"{identifier_prefix}{i}"
            node = self.create_node(identifier)
            hops = self.add_node(node)
            created_nodes.append(node)
            join_hops_list.append(hops)
        
        total_hops = sum(join_hops_list)
        avg_hops = total_hops / num_nodes if num_nodes > 0 else 0
        
        logger.info(f"Pastry network built: {len(self._nodes)} nodes, total_join_hops: {total_hops}")
        
        return {
            "nodes": created_nodes,
            "total_join_hops": total_hops,
            "average_join_hops": avg_hops,
            "join_hops_per_node": join_hops_list,
        }
    
    # =========================================================================
    # Pastry-specific Methods
    # =========================================================================
    
    def get_network_stats(self) -> Dict[str, Any]:
        """
        Get Pastry-specific statistics about the network.
        
        Returns:
            Dictionary with network statistics.
        """
        # Get base stats
        stats = super().get_network_stats()
        
        if self._nodes:
            # Add Pastry-specific stats
            routing_table_sizes = [node.routing_table.get_filled_count() for node in self._nodes]
            leaf_set_sizes = [len(node.leaf_set.get_all_nodes()) for node in self._nodes]
            
            stats["routing_table_avg"] = sum(routing_table_sizes) / len(self._nodes)
            stats["leaf_set_avg"] = sum(leaf_set_sizes) / len(self._nodes)
        
        return stats
