"""
Abstract base class for DHT nodes.

Defines the interface that all DHT implementations (Chord, Pastry) must follow.
This ensures consistent behavior and enables fair comparison between protocols.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple


class BaseNode(ABC):
    """
    Abstract base class for a DHT node.
    
    All DHT implementations must inherit from this class and implement
    the abstract methods defined here.
    
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
    
    @abstractmethod
    def find_successor(self, key_id: int) -> Tuple["BaseNode", int]:
        """
        Find the node responsible for a given key ID.
        
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
            Number of hops used during the join process.
        """
        pass
    
    @abstractmethod
    def leave(self) -> int:
        """
        Gracefully leave the DHT network.
        
        Transfers responsibility for keys to other nodes.
        
        Returns:
            Number of hops used during the leave process.
        """
        pass
    
    @abstractmethod
    def insert(self, key: str, value: Any) -> Tuple[bool, int]:
        """
        Insert a key-value pair into the DHT.
        
        Args:
            key: The key to insert (will be hashed).
            value: The value to store.
        
        Returns:
            Tuple of (success, hop_count).
        """
        pass
    
    @abstractmethod
    def lookup(self, key: str) -> Tuple[Optional[Any], int]:
        """
        Look up a value by key in the DHT.
        
        Args:
            key: The key to look up (will be hashed).
        
        Returns:
            Tuple of (value or None if not found, hop_count).
        """
        pass
    
    @abstractmethod
    def delete(self, key: str) -> Tuple[bool, int]:
        """
        Delete a key-value pair from the DHT.
        
        Args:
            key: The key to delete (will be hashed).
        
        Returns:
            Tuple of (success, hop_count).
        """
        pass
    
    @abstractmethod
    def update(self, key: str, value: Any) -> Tuple[bool, int]:
        """
        Update the value for an existing key in the DHT.
        
        Args:
            key: The key to update (will be hashed).
            value: The new value to store.
        
        Returns:
            Tuple of (success, hop_count).
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
