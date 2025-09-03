"""Dictionary and JSON serialization utilities.

This module provides utilities for converting objects to and from JSON format,
handling special data types like datetime and MongoDB ObjectId.
"""

import json
from datetime import datetime
from typing import Any, Dict

from bson.objectid import ObjectId


def json2str(obj: Any) -> str:
    """Convert object to JSON string representation.
    
    Args:
        obj: Object to serialize to JSON string.
        
    Returns:
        JSON string representation of the object.
    """
    return json.dumps(to_dict(obj), sort_keys=True, ensure_ascii=False)


def str2json(str: str) -> Dict:
    """Convert JSON string to dictionary.
    
    Args:
        str: JSON string to deserialize.
        
    Returns:
        Dictionary representation of the JSON string.
    """
    return json.loads(str)


def to_dict(obj, classkey=None):
    """Convert object to dictionary with special handling for datetime and ObjectId.
    
    Recursively converts objects to dictionary format, handling special cases
    for datetime objects and MongoDB ObjectId instances.
    
    Args:
        obj: Object to convert to dictionary.
        classkey: Optional key for class information.
        
    Returns:
        Dictionary representation of the object.
    """
    if isinstance(obj, datetime):
        return obj.strftime("%Y-%m-%d %H:%M:%S")
    elif isinstance(obj, ObjectId):
        return str(obj)
    if isinstance(obj, dict):
        data = {}
        for (k, v) in obj.items():
            data[k] = to_dict(v, classkey)
        return data
    elif hasattr(obj, "_ast"):
        return to_dict(obj._ast())
    elif hasattr(obj, "__iter__") and not isinstance(obj, str):
        return [to_dict(v, classkey) for v in obj]
    elif hasattr(obj, "__dict__"):
        data = dict([(key, to_dict(value, classkey))
                     for key, value in obj.__dict__.items()
                     if not callable(value) and not key.startswith('_')])
        if classkey is not None and hasattr(obj, "__class__"):
            data[classkey] = obj.__class__.__name__
        return data
    else:
        return obj