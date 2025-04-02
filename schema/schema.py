import json
import os
from typing import Dict, Any

def get_schema(schema_name: str) -> Dict[str, Any]:
    """
    Get JSON schema by name from the schema directory.
    
    Args:
        schema_name: Name of the schema file without .json extension
        
    Returns:
        Parsed JSON content as a dictionary
        
    Raises:
        FileNotFoundError: If schema file doesn't exist
        json.JSONDecodeError: If schema file contains invalid JSON
    """
    schema_path = os.path.join(os.path.dirname(__file__), f"{schema_name}.json")
    if not os.path.exists(schema_path):
        raise FileNotFoundError(f"Schema file not found: {schema_path}")
        
    with open(schema_path, 'r', encoding='utf-8') as f:
        return json.load(f)
