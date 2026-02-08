"""
Consistent hashing utilities for DHT implementations.

Provides SHA-1 based hashing for mapping keys and node identifiers
to positions in the circular hash space.
"""

import hashlib
from typing import Union

import config


def hash_key(key: Union[str, bytes]) -> int:
    """
    Hash a key (e.g., movie title) to a position in the hash space.
    
    Uses SHA-1 to produce a 160-bit hash, then takes the configured
    number of bits to fit the hash space.
    
    Args:
        key: The key to hash (string or bytes).
    
    Returns:
        Integer position in the hash space [0, 2^HASH_BIT_SIZE).
    
    Example:
        >>> hash_key("The Matrix")
        123456789...  # Some large integer
    """
    if isinstance(key, str):
        key = key.encode("utf-8")
    
    # SHA-1 produces 160-bit (20 byte) hash
    sha1_hash = hashlib.sha1(key).digest()
    
    # Convert to integer
    hash_int = int.from_bytes(sha1_hash, byteorder="big")
    
    # Mask to configured bit size
    mask = (1 << config.HASH_BIT_SIZE) - 1
    
    return hash_int & mask


def hash_node(node_identifier: Union[str, int]) -> int:
    """
    Hash a node identifier to get its position in the hash space.
    
    Node identifiers can be strings (e.g., "node_1", IP addresses)
    or integers (which are converted to strings first).
    
    Args:
        node_identifier: Unique identifier for the node.
    
    Returns:
        Integer position in the hash space [0, 2^HASH_BIT_SIZE).
    
    Example:
        >>> hash_node("node_1")
        987654321...  # Some large integer
    """
    identifier_str = str(node_identifier)
    return hash_key(identifier_str)


def in_range(value: int, start: int, end: int, inclusive_start: bool = False, inclusive_end: bool = True) -> bool:
    """
    Check if a value falls within a range on the circular hash space.
    
    Handles wrap-around correctly. By default, the range is (start, end],
    meaning exclusive start and inclusive end. This is the standard
    convention for Chord's successor responsibility.
    
    Args:
        value: The value to check.
        start: Start of the range.
        end: End of the range.
        inclusive_start: If True, include start in the range.
        inclusive_end: If True, include end in the range.
    
    Returns:
        True if value is in the range, False otherwise.
    
    Example:
        # On a ring of size 8:
        # Range (6, 2] includes: 7, 0, 1, 2
        >>> in_range(7, 6, 2)  # True
        >>> in_range(4, 6, 2)  # False
    """
    # Normalize to hash space
    space_size = config.HASH_SPACE_SIZE
    value = value % space_size
    start = start % space_size
    end = end % space_size
    
    # Handle the case where start == end
    if start == end:
        if inclusive_start and inclusive_end:
            return value == start
        else:
            # Full ring (everything except possibly the endpoints)
            return True
    
    # Check boundaries based on inclusivity
    if value == start:
        return inclusive_start
    if value == end:
        return inclusive_end
    
    # Check if in range (handling wrap-around)
    if start < end:
        # Normal case: no wrap-around
        return start < value < end
    else:
        # Wrap-around case: range crosses 0
        return value > start or value < end

def get_id_hex(node_id: int, num_digits: int = None) -> str:
    """
    Get the hexadecimal representation of a node ID.
    
    Useful for Pastry's prefix-based routing.
    
    Args:
        node_id: The node ID.
        num_digits: Number of hex digits to display. 
                    Defaults to HASH_BIT_SIZE // 4.
    
    Returns:
        Hexadecimal string padded to the specified number of digits.
    
    Example:
        >>> get_id_hex(255, 4)
        "00ff"
    """
    if num_digits is None:
        num_digits = config.HASH_BIT_SIZE // 4
    
    return format(node_id % config.HASH_SPACE_SIZE, f"0{num_digits}x")
