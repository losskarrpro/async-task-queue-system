import re
from typing import Any, Dict, Optional, Union
from uuid import UUID
from datetime import datetime

from .exceptions import ValidationError


def validate_task_data(data: Dict[str, Any]) -> bool:
    """Valide les données d'une tâche"""
    required_fields = {"name", "payload", "priority"}
    if not all(field in data for field in required_fields):
        raise ValidationError(f"Missing required fields: {required_fields - set(data.keys())}")
    
    if not isinstance(data["name"], str) or not data["name"].strip():
        raise ValidationError("Task name must be a non-empty string")
    
    if not isinstance(data["payload"], dict):
        raise ValidationError("Task payload must be a dictionary")
    
    validate_priority(data["priority"])
    
    if "timeout" in data:
        validate_timeout(data["timeout"])
    
    if "metadata" in data and not isinstance(data["metadata"], dict):
        raise ValidationError("Task metadata must be a dictionary")
    
    return True


def validate_priority(priority: Union[str, int]) -> bool:
    """Valide la priorité d'une tâche"""
    valid_priorities = {"low", "medium", "high", "critical"}
    if isinstance(priority, str):
        if priority.lower() not in valid_priorities:
            raise ValidationError(f"Invalid priority: {priority}. Must be one of {valid_priorities}")
    elif isinstance(priority, int):
        if not 1 <= priority <= 4:
            raise ValidationError(f"Invalid priority: {priority}. Must be between 1 and 4")
    else:
        raise ValidationError(f"Priority must be string or integer, got {type(priority)}")
    return True


def validate_timeout(timeout: Union[int, float]) -> bool:
    """Valide le timeout d'une tâche"""
    if not isinstance(timeout, (int, float)):
        raise ValidationError(f"Timeout must be a number, got {type(timeout)}")
    if timeout <= 0:
        raise ValidationError(f"Timeout must be positive, got {timeout}")
    if timeout > 3600:
        raise ValidationError(f"Timeout cannot exceed 3600 seconds (1 hour), got {timeout}")
    return True


def validate_uuid(uuid_str: str) -> bool:
    """Valide un UUID"""
    uuid_pattern = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.I)
    if not uuid_pattern.match(uuid_str):
        raise ValidationError(f"Invalid UUID format: {uuid_str}")
    return True


def validate_date_format(date_str: str, format: str = "%Y-%m-%dT%H:%M:%S") -> bool:
    """Valide le format d'une date"""
    try:
        datetime.strptime(date_str, format)
        return True
    except ValueError:
        raise ValidationError(f"Invalid date format: {date_str}. Expected format: {format}")
