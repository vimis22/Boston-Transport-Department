"""Utility for Avro serialization"""

from typing import Any

def get_schema_field_names(schema: dict[str, Any]) -> set[str]:
    """
    Extract field names from an Avro schema.
    
    Args:
        schema: Avro schema dictionary
        
    Returns:
        Set of field names
    """
    if schema.get("type") != "record":
        return set()
    
    return {field["name"] for field in schema.get("fields", [])}


def filter_record_to_schema(record: dict[str, Any], schema: dict[str, Any]) -> dict[str, Any]:
    """
    Filter a record to only include fields defined in the schema.
    
    Args:
        record: Record dictionary
        schema: Avro schema dictionary
        
    Returns:
        Filtered record dictionary with only schema fields
    """
    field_names = get_schema_field_names(schema)
    return {k: v for k, v in record.items() if k in field_names}
