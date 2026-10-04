import json
from datetime import datetime


def format_json(data: dict, indent: int = 2) -> str:
    """
    Convert dictionary to pretty JSON string.
    Useful for printing Kafka / CDC events in CLI.
    """
    return json.dumps(data, indent=indent, ensure_ascii=False)


def safe_serialize(obj):
    """
    Helper to serialize non-serializable objects (datetime, etc.)
    """

    if isinstance(obj, datetime):
        return obj.isoformat()

    raise TypeError(f"Type not serializable: {type(obj)}")