"""
Abstract base class for DHT nodes.

Defines the interface that all DHT implementations (Chord, Pastry) must follow.
This ensures consistent behavior and enables fair comparison between protocols.

Hop Counting Model:
    All implementations count ALL network messages as hops, including:
    - Routing hops (messages forwarded through intermediate nodes)
    - Communication hops (direct messages to known nodes)
    This provides a complete picture of network cost for fair comparison.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple

from src.common.hashing import hash_key
from src.common.logger import get_logger

logger = get_logger(__name__)


class BaseNode(ABC):
    """
    Abstract base class for a DHT node.
    
    All DHT implementations must inherit from this class and implement
    the abstract methods defined here.
    
    Concrete methods (shared by all DHTs):
        - insert(): Insert a key-value pair
        - lookup(): Look up a value by key
        - delete(): Delete a key-value pair
        - update(): Update an existing key's value
        - store_local(): Store in local storage
        - get_local(): Retrieve from local storage
        - delete_local(): Delete from local storage
    
    Abstract methods (each DHT implements differently):
        - find_successor(): Core routing logic
        - join(): Network join protocol
        - leave(): Network leave protocol
        - get_routing_table_size(): Size of routing structure
    
    Attributes:
        identifier: Human-readable identifier for the node (e.g., "node_1").
        node_id: Numeric ID in the hash space (result of hashing identifier).
    """
    
    def __init__(self, identifier: str, node_id: int):
        """
        Initialize a DHT node.
        
        Args:
            identifier: Human-readable identifier for the node.
            node_id: Numeric ID in the hash space.
        """
        self._identifier = identifier
        self._node_id = node_id
        self._data: Dict[str, Any] = {}
    
    @property
    def identifier(self) -> str:
        """Human-readable identifier for the node."""
        return self._identifier
    
    @property
    def node_id(self) -> int:
        """Numeric ID in the hash space."""
        return self._node_id
    
    @property
    def data(self) -> Dict[str, Any]:
        """Local data storage (key-value pairs this node is responsible for)."""
        return self._data
    
    # =========================================================================
    # Abstract Methods - Each DHT implements differently
    # =========================================================================
    
    @abstractmethod
    def find_successor(self, key_id: int) -> Tuple["BaseNode", int]:
        """
        Find the node responsible for a given key ID.
        
        This is the core routing method that each DHT implements differently:
        - Chord: Uses finger table for O(log N) routing
        - Pastry: Uses prefix matching for O(log_b N) routing
        
        Args:
            key_id: The hashed key ID to look up.
        
        Returns:
            Tuple of (responsible_node, hop_count).
        """
        pass
    
    @abstractmethod
    def join(self, existing_node: Optional["BaseNode"] = None) -> int:
        """
        Join the DHT network.
        
        Args:
            existing_node: An existing node in the network to join through.
                          If None, this node starts a new network.
        
        Returns:
            Number of hops (network messages) used during the join process.
        """
        pass
    
    @abstractmethod
    def leave(self) -> int:
        """
        Gracefully leave the DHT network.
        
        Transfers responsibility for keys to other nodes.
        
        Returns:
            Number of hops (network messages) used during the leave process.
        """
        pass
    
    @abstractmethod
    def get_routing_table_size(self) -> int:
        """
        Get the size of the node's routing table.
        
        Returns:
            Number of entries in the routing table.
        """
        pass
    
    # =========================================================================
    # Concrete Methods - Shared by all DHTs
    # =========================================================================
    
    def insert(self, key: str, value: Any) -> Tuple[bool, int]:
        """
        Insert a key-value pair into the DHT.
        
        Routes to the responsible node and stores the value.
        
        Hop counting:
            - Routing hops to find responsible node
            - 1 hop to send store request (if not self)
        
        Args:
            key: The key to insert.
            value: The value to store.
        
        Returns:
            Tuple of (success, hop_count).
        """
        key_id = hash_key(key)
        responsible_node, hops = self.find_successor(key_id)
        
        # 1 hop: message to responsible node to store the key
        if responsible_node != self:
            hops += 1
        
        responsible_node.store_local(key, value)
        logger.debug(f"Inserted key '{key}' at {responsible_node.identifier} (hops: {hops})")
        return True, hops
    
    def lookup(self, key: str) -> Tuple[Optional[Any], int]:
        """
        Look up a value by key in the DHT.
        
        Routes to the responsible node and retrieves the value.
        
        Hop counting:
            - Routing hops to find responsible node
            - 1 hop to send lookup request (if not self)
        
        Args:
            key: The key to look up.
        
        Returns:
            Tuple of (value or None if not found, hop_count).
        """
        key_id = hash_key(key)
        responsible_node, hops = self.find_successor(key_id)
        
        # 1 hop: message to responsible node to get the key
        if responsible_node != self:
            hops += 1
        
        value = responsible_node.get_local(key)
        logger.debug(f"Lookup key '{key}' at {responsible_node.identifier} (hops: {hops})")
        return value, hops
    
    def delete(self, key: str) -> Tuple[bool, int]:
        """
        Delete a key-value pair from the DHT.
        
        Routes to the responsible node and deletes the key.
        
        Hop counting:
            - Routing hops to find responsible node
            - 1 hop to send delete request (if not self)
        
        Args:
            key: The key to delete.
        
        Returns:
            Tuple of (success, hop_count).
        """
        key_id = hash_key(key)
        responsible_node, hops = self.find_successor(key_id)
        
        # 1 hop: message to responsible node to delete the key
        if responsible_node != self:
            hops += 1
        
        success = responsible_node.delete_local(key)
        logger.debug(f"Delete key '{key}' at {responsible_node.identifier}: {success} (hops: {hops})")
        return success, hops
    
    def update(self, key: str, value: Any) -> Tuple[bool, int]:
        """
        Update the value for an existing key in the DHT.
        
        Routes to the responsible node and updates if key exists.
        
        Hop counting:
            - Routing hops to find responsible node
            - 1 hop to send update request (if not self)
        
        Args:
            key: The key to update.
            value: The new value to store.
        
        Returns:
            Tuple of (success, hop_count).
        """
        key_id = hash_key(key)
        responsible_node, hops = self.find_successor(key_id)
        
        # 1 hop: message to responsible node to update the key
        if responsible_node != self:
            hops += 1
        
        if responsible_node.get_local(key) is not None:
            responsible_node.store_local(key, value)
            logger.debug(f"Updated key '{key}' at {responsible_node.identifier} (hops: {hops})")
            return True, hops
        else:
            logger.debug(f"Update failed - key '{key}' not found (hops: {hops})")
            return False, hops
    
    # =========================================================================
    # Local Storage Methods
    # =========================================================================
    
    def store_local(self, key: str, value: Any) -> None:
        """
        Store a key-value pair in local storage.
        
        This is called when this node is responsible for the key.
        
        Args:
            key: The key to store.
            value: The value to store.
        """
        self._data[key] = value
    
    def get_local(self, key: str) -> Optional[Any]:
        """
        Retrieve a value from local storage.
        
        Args:
            key: The key to retrieve.
        
        Returns:
            The value if found, None otherwise.
        """
        return self._data.get(key)
    
    def delete_local(self, key: str) -> bool:
        """
        Delete a key-value pair from local storage.
        
        Args:
            key: The key to delete.
        
        Returns:
            True if the key was found and deleted, False otherwise.
        """
        if key in self._data:
            del self._data[key]
            return True
        return False
    
    def get_local_key_count(self) -> int:
        """
        Get the number of keys stored locally.
        
        Returns:
            Number of key-value pairs in local storage.
        """
        return len(self._data)
    
    # =========================================================================
    # Dunder Methods
    # =========================================================================
    
    def __repr__(self) -> str:
        """String representation of the node."""
        return f"{self.__class__.__name__}(id={self._identifier}, node_id={self._node_id})"
    
    def __eq__(self, other: object) -> bool:
        """Check equality based on node_id."""
        if not isinstance(other, BaseNode):
            return NotImplemented
        return self._node_id == other._node_id
    
    def __hash__(self) -> int:
        """Hash based on node_id."""
        return hash(self._node_id)
