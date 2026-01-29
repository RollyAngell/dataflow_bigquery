"""BigQuery schema utilities."""

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

DEFAULT_SCHEMA = [
    {"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "name", "type": "STRING", "mode": "REQUIRED"},
    {"name": "email", "type": "STRING", "mode": "REQUIRED"},
    {"name": "amount", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "created_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "is_active", "type": "BOOLEAN", "mode": "NULLABLE"},
    {"name": "category", "type": "STRING", "mode": "NULLABLE"},
    {"name": "_load_timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
    {"name": "_source_file", "type": "STRING", "mode": "REQUIRED"},
]


def load_schema_from_json(schema_path: str) -> List[Dict[str, Any]]:
    """Load BigQuery schema from a JSON file."""
    path = Path(schema_path)

    if not path.exists():
        logger.warning(f"Schema file not found: {schema_path}, using default schema")
        return DEFAULT_SCHEMA

    with open(path, "r", encoding="utf-8") as f:
        schema = json.load(f)

    logger.info(f"Loaded schema with {len(schema)} fields from {schema_path}")
    return schema


def get_table_schema(schema_path: Optional[str] = None) -> Dict[str, Any]:
    """Get schema as a dictionary for BigQuery."""
    if schema_path:
        fields = load_schema_from_json(schema_path)
    else:
        fields = DEFAULT_SCHEMA

    return {"fields": fields}


def get_field_types(schema_path: Optional[str] = None) -> Dict[str, str]:
    """Extract field name to type mapping from schema."""
    if schema_path:
        fields = load_schema_from_json(schema_path)
    else:
        fields = DEFAULT_SCHEMA

    return {field["name"]: field["type"] for field in fields}


def get_required_fields(schema_path: Optional[str] = None) -> List[str]:
    """Get list of required field names from schema."""
    if schema_path:
        fields = load_schema_from_json(schema_path)
    else:
        fields = DEFAULT_SCHEMA

    return [field["name"] for field in fields if field.get("mode") == "REQUIRED"]
