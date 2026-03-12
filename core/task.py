import uuid
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional, List, Callable
import json
import pickle

from core.exceptions import TaskValidationError


class TaskStatus(Enum):
    """Statuts possibles d'une tâche"""
    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    RETRYING = "retrying"


class TaskPriority(Enum):
    """Priorités des tâches"""
    LOW = 1
    NORMAL = 2
    HIGH = 3
    CRITICAL = 4


@dataclass
class TaskMetadata:
    """Métadonnées d'une tâche"""
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    created_by: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    context: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convertit les métadonnées en dictionnaire"""
        return {
            'created_at': self.created_at,
            'updated_at': self.updated_at,
            'created_by': self.created_by,
            'tags': self.tags.copy(),
            'context': self.context.copy()
        }
    
    def update(self) -> None:
        """Met à jour le timestamp de modification"""
        self.updated_at = time.time()


@dataclass
class Task:
    """Représente une tâche à exécuter"""
    
    # Identifiants
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = "unnamed_task"
    version: str = "1.0.0"
    
    # Contenu de la tâche
    function_path: str = ""  # Chemin de la fonction à exécuter (module.function)
    args: List[Any] = field(default_factory=list)
    kwargs: Dict[str, Any] = field(default_factory=dict)
    
    # Configuration d'exécution
    queue_name: str = "default"
    priority: TaskPriority = TaskPriority.NORMAL
    timeout: Optional[float] = None  # Timeout en secondes
    max_retries: int = 3
    retry_delay: float = 1.0  # Délai entre les tentatives en secondes
    retry_backoff: bool = True  # Activation du backoff exponentiel
    
    # État d'exécution
    status: TaskStatus = TaskStatus.PENDING
    retries: int = 0
    result: Optional[Any] = None
    error: Optional[str] = None
    error_traceback: Optional[str] = None
    
    # Timestamps
    created_at: float = field(default_factory=time.time)
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    
    # Métadonnées
    metadata: TaskMetadata = field(default_factory=TaskMetadata)
    
    # Callbacks
    on_success: Optional[str] = None  # Chemin de fonction callback
    on_failure: Optional[str] = None  # Chemin de fonction callback
    on_complete: Optional[str] = None  # Chemin de fonction callback
    
    def __post_init__(self):
        """Validation après initialisation"""
        if not self.function_path:
            raise TaskValidationError("function_path cannot be empty")
        
    def to_dict(self) -> Dict[str, Any]:
        """Convertit la tâche en dictionnaire"""
        data = asdict(self)
        data['priority'] = self.priority.value
        data['status'] = self.status.value
        data['metadata'] = self.metadata.to_dict()
        return data
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Task':
        """Crée une tâche à partir d'un dictionnaire"""
        # Convertir les valeurs enum
        if 'priority' in data:
            data['priority'] = TaskPriority(data['priority'])
        if 'status' in data:
            data['status'] = TaskStatus(data['status'])
        if 'metadata' in data and isinstance(data['metadata'], dict):
            data['metadata'] = TaskMetadata(**data['metadata'])
        
        return cls(**data)
    
    def serialize(self) -> bytes:
        """Sérialise la tâche en bytes"""
        return pickle.dumps(self.to_dict())
    
    @classmethod
    def deserialize(cls, data: bytes) -> 'Task':
        """Désérialise une tâche à partir de bytes"""
        return cls.from_dict(pickle.loads(data))
    
    def mark_as_queued(self) -> None:
        """Marque la tâche comme mise en file d'attente"""
        self.status = TaskStatus.QUEUED
        self.metadata.update()
    
    def mark_as_running(self) -> None:
        """Marque la tâche comme en cours d'exécution"""
        self.status = TaskStatus.RUNNING
        self.started_at = time.time()
        self.metadata.update()
    
    def mark_as_completed(self, result: Any = None) -> None:
        """Marque la tâche comme terminée avec succès"""
        self.status = TaskStatus.COMPLETED
        self.result = result
        self.completed_at = time.time()
        self.metadata.update()
    
    def mark_as_failed(self, error: str, traceback: Optional[str] = None) -> None:
        """Marque la tâche comme échouée"""
        self.status = TaskStatus.FAILED
        self.error = error
        self.error_traceback = traceback
        self.completed_at = time.time()
        self.metadata.update()
    
    def mark_for_retry(self, error: str, traceback: Optional[str] = None) -> None:
        """Prépare la tâche pour une nouvelle tentative"""
        self.status = TaskStatus.RETRYING
        self.error = error
        self.error_traceback = traceback
        self.retries += 1
        self.metadata.update()
    
    def should_retry(self) -> bool:
        """Détermine si la tâche doit être réessayée"""
        return self.retries < self.max_retries and self.status in [TaskStatus.FAILED, TaskStatus.RETRYING]
    
    def get_retry_delay(self) -> float:
        """Calcule le délai avant la prochaine tentative"""
        if not self.retry_backoff:
            return self.retry_delay
        return self.retry_delay * (2 ** (self.retries - 1))
    
    def __repr__(self) -> str:
        return f"Task(id='{self.id}', name='{self.name}', status={self.status.value}, queue='{self.queue_name}')"
