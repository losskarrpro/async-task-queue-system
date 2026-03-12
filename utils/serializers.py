import json
import pickle
from typing import Any, Dict, Optional, Union
from uuid import UUID
import msgpack

from core.task import Task, TaskPriority
from utils.logger import get_logger

logger = get_logger(__name__)

# Définir les classes localement pour éviter l'importation circulaire
class ResultStatus:
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMEOUT = "timeout"


class TaskResult:
    """Classe simplifiée pour la sérialisation"""
    def __init__(self, task_id, status=None, result=None, error=None, created_at=None, started_at=None, completed_at=None, execution_time=None, metadata=None):
        self.task_id = task_id
        self.status = status or ResultStatus.PENDING
        self.result = result
        self.error = error
        self.created_at = created_at
        self.started_at = started_at
        self.completed_at = completed_at
        self.execution_time = execution_time
        self.metadata = metadata or {}

    def to_dict(self):
        """Convertit le résultat en dictionnaire"""
        data = {
            'task_id': str(self.task_id),
            'status': self.status,
            'result': self.result,
            'error': self.error,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'started_at': self.started_at.isoformat() if self.started_at else None,
            'completed_at': self.completed_at.isoformat() if self.completed_at else None,
            'execution_time': self.execution_time,
            'metadata': self.metadata
        }
        return {k: v for k, v in data.items() if v is not None}

    @classmethod
    def from_dict(cls, data):
        """Crée un TaskResult à partir d'un dictionnaire"""
        return cls(
            task_id=UUID(data['task_id']) if isinstance(data.get('task_id'), str) else data.get('task_id'),
            status=data.get('status'),
            result=data.get('result'),
            error=data.get('error'),
            created_at=datetime.fromisoformat(data['created_at']) if data.get('created_at') else None,
            started_at=datetime.fromisoformat(data['started_at']) if data.get('started_at') else None,
            completed_at=datetime.fromisoformat(data['completed_at']) if data.get('completed_at') else None,
            execution_time=data.get('execution_time'),
            metadata=data.get('metadata', {})
        )


def json_serializer(obj: Any) -> bytes:
    """Serialize object to JSON bytes"""
    try:
        return json.dumps(obj, default=_json_default).encode('utf-8')
    except Exception as e:
        logger.error(f"JSON serialization error: {e}")
        raise


def json_deserializer(data: bytes) -> Any:
    """Deserialize JSON bytes to object"""
    try:
        return json.loads(data.decode('utf-8'), object_hook=_json_object_hook)
    except Exception as e:
        logger.error(f"JSON deserialization error: {e}")
        raise


def msgpack_serializer(obj: Any) -> bytes:
    """Serialize object to MessagePack bytes"""
    try:
        return msgpack.packb(obj, default=_msgpack_default)
    except Exception as e:
        logger.error(f"MessagePack serialization error: {e}")
        raise


def msgpack_deserializer(data: bytes) -> Any:
    """Deserialize MessagePack bytes to object"""
    try:
        return msgpack.unpackb(data, object_hook=_msgpack_object_hook)
    except Exception as e:
        logger.error(f"MessagePack deserialization error: {e}")
        raise


def serialize_task(task: Task) -> bytes:
    """Serialize a Task object"""
    task_dict = {
        'task_id': str(task.task_id),
        'name': task.name,
        'data': task.data,
        'priority': task.priority.value,
        'timeout': task.timeout,
        'created_at': task.created_at.isoformat() if task.created_at else None,
        'scheduled_for': task.scheduled_for.isoformat() if task.scheduled_for else None,
        'metadata': task.metadata
    }
    return json_serializer(task_dict)


def deserialize_task(data: bytes) -> Task:
    """Deserialize bytes to Task object"""
    task_dict = json_deserializer(data)
    
    # Convert priority string to enum
    if isinstance(task_dict.get('priority'), str):
        task_dict['priority'] = TaskPriority(task_dict['priority'])
    
    # Import datetime ici pour éviter les problèmes circulaires
    from datetime import datetime
    
    # Convert date strings back to datetime objects
    if task_dict.get('created_at'):
        task_dict['created_at'] = datetime.fromisoformat(task_dict['created_at'])
    if task_dict.get('scheduled_for'):
        task_dict['scheduled_for'] = datetime.fromisoformat(task_dict['scheduled_for'])
    
    return Task(**task_dict)


def serialize_result(result: TaskResult) -> bytes:
    """Serialize a TaskResult object"""
    return json_serializer(result.to_dict())


def deserialize_result(data: bytes) -> TaskResult:
    """Deserialize bytes to TaskResult object"""
    result_dict = json_deserializer(data)
    # Import datetime ici pour éviter les problèmes circulaires
    from datetime import datetime
    
    # Convert date strings back to datetime objects
    if result_dict.get('created_at'):
        result_dict['created_at'] = datetime.fromisoformat(result_dict['created_at'])
    if result_dict.get('started_at'):
        result_dict['started_at'] = datetime.fromisoformat(result_dict['started_at'])
    if result_dict.get('completed_at'):
        result_dict['completed_at'] = datetime.fromisoformat(result_dict['completed_at'])
    
    return TaskResult.from_dict(result_dict)


def _json_default(obj):
    """Default handler for JSON serialization"""
    if isinstance(obj, UUID):
        return str(obj)
    elif hasattr(obj, 'to_dict'):
        return obj.to_dict()
    elif hasattr(obj, 'isoformat'):  # For datetime objects
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


def _json_object_hook(obj):
    """Object hook for JSON deserialization"""
    # Handle UUID strings
    if 'task_id' in obj and isinstance(obj['task_id'], str):
        try:
            obj['task_id'] = UUID(obj['task_id'])
        except (ValueError, AttributeError):
            pass
    return obj


def _msgpack_default(obj):
    """Default handler for MessagePack serialization"""
    if isinstance(obj, UUID):
        return str(obj)
    elif hasattr(obj, 'isoformat'):  # For datetime objects
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj)} is not MessagePack serializable")


def _msgpack_object_hook(obj):
    """Object hook for MessagePack deserialization"""
    # Handle UUID strings
    if 'task_id' in obj and isinstance(obj['task_id'], str):
        try:
            obj['task_id'] = UUID(obj['task_id'])
        except (ValueError, AttributeError):
            pass
    return obj