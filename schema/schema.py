import json
import os
from typing import Dict, Any

# Module-level cache for schemas
_schema_cache: Dict[str, Dict[str, Any]] = {}

def get_schema(schema_name: str, use_cache: bool = True) -> Dict[str, Any]:
    """
    Get JSON schema by name from the schema directory.
    
    Args:
        schema_name: Name of the schema file without .json extension
        use_cache: Whether to use cached schema if available (default: True)
        
    Returns:
        Parsed JSON content as a dictionary
        
    Raises:
        FileNotFoundError: If schema file doesn't exist
        json.JSONDecodeError: If schema file contains invalid JSON
    """
    if use_cache and schema_name in _schema_cache:
        return _schema_cache[schema_name]
    schema_path = os.path.join(os.path.dirname(__file__), f"{schema_name}.json")
    if not os.path.exists(schema_path):
        raise FileNotFoundError(f"Schema file not found: {schema_path}")
        
    with open(schema_path, 'r', encoding='utf-8') as f:
        schema = json.load(f)
        _schema_cache[schema_name] = schema
        return schema
