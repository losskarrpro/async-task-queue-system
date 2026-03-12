import uuid
import asyncio
from abc import ABC, abstractmethod
from enum import Enum
from datetime import datetime
from typing import Any, Dict, Optional, List
from dataclasses import dataclass, field

from core.exceptions import TaskError, TaskTimeoutError


class TaskStatus(Enum):
    """États possibles d'une tâche"""
    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMEOUT = "timeout"


class TaskPriority(Enum):
    """Priorités des tâches"""
    LOW = 1
    NORMAL = 2
    HIGH = 3
    URGENT = 4


@dataclass
class TaskResult:
    """Résultat d'exécution d'une tâche"""
    task_id: str
    status: TaskStatus
    result: Optional[Any] = None
    error: Optional[str] = None
    execution_time: Optional[float] = None
    created_at: datetime = field(default_factory=datetime.now)
    completed_at: Optional[datetime] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "task_id": self.task_id,
            "status": self.status.value,
            "result": self.result,
            "error": self.error,
            "execution_time": self.execution_time,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "metadata": self.metadata
        }


class BaseTask(ABC):
    """Classe de base abstraite pour toutes les tâches"""
    
    def __init__(
        self,
        name: str,
        task_id: Optional[str] = None,
        priority: TaskPriority = TaskPriority.NORMAL,
        timeout: Optional[float] = None,
        retry_count: int = 0,
        max_retries: int = 3,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs
    ):
        self.id = task_id or str(uuid.uuid4())
        self.name = name
        self.priority = priority
        self.timeout = timeout
        self.retry_count = retry_count
        self.max_retries = max_retries
        self.metadata = metadata or {}
        self.kwargs = kwargs
        
        self.status = TaskStatus.PENDING
        self.created_at = datetime.now()
        self.started_at: Optional[datetime] = None
        self.completed_at: Optional[datetime] = None
        self.result: Optional[TaskResult] = None
        self._cancel_event = asyncio.Event()
        self._execution_lock = asyncio.Lock()
        
        self.callbacks: List[callable] = []
        self.dependencies: List['BaseTask'] = []
        
    def update_metadata(self, **updates: Any) -> None:
        """Met à jour les métadonnées de la tâche"""
        self.metadata.update(updates)
    
    def add_callback(self, callback: callable) -> None:
        """Ajoute un callback à appeler après l'exécution"""
        self.callbacks.append(callback)
    
    def add_dependency(self, task: 'BaseTask') -> None:
        """Ajoute une dépendance à la tâche"""
        self.dependencies.append(task)
    
    def can_execute(self) -> bool:
        """Vérifie si la tâche peut être exécutée"""
        if self.status in [TaskStatus.COMPLETED, TaskStatus.CANCELLED]:
            return False
            
        for dep in self.dependencies:
            if dep.status != TaskStatus.COMPLETED:
                return False
                
        return True
    
    def cancel(self) -> bool:
        """Annule la tâche si elle n'est pas déjà en cours ou terminée"""
        if self.status not in [TaskStatus.RUNNING, TaskStatus.COMPLETED, TaskStatus.FAILED]:
            self.status = TaskStatus.CANCELLED
            self._cancel_event.set()
            return True
        return False
    
    async def execute(self) -> TaskResult:
        """Exécute la tâche avec gestion d'erreurs et timeout"""
        async with self._execution_lock:
            if self.status in [TaskStatus.COMPLETED, TaskStatus.CANCELLED]:
                raise TaskError(f"Task {self.id} already in final state: {self.status}")
            
            self.status = TaskStatus.RUNNING
            self.started_at = datetime.now()
            
            try:
                if self.timeout:
                    result = await asyncio.wait_for(
                        self._execute_with_retry(),
                        timeout=self.timeout
                    )
                else:
                    result = await self._execute_with_retry()
                    
                self.result = result
                self.status = result.status
                self.completed_at = datetime.now()
                
            except asyncio.TimeoutError:
                self.status = TaskStatus.TIMEOUT
                self.result = TaskResult(
                    task_id=self.id,
                    status=TaskStatus.TIMEOUT,
                    error=f"Task timeout after {self.timeout} seconds",
                    created_at=self.created_at,
                    completed_at=datetime.now(),
                    metadata=self.metadata
                )
                raise TaskTimeoutError(f"Task {self.id} timed out after {self.timeout}s")
                
            except Exception as e:
                self.status = TaskStatus.FAILED
                self.result = TaskResult(
                    task_id=self.id,
                    status=TaskStatus.FAILED,
                    error=str(e),
                    created_at=self.created_at,
                    completed_at=datetime.now(),
                    metadata=self.metadata
                )
                raise
                
            finally:
                await self._execute_callbacks()
                
            return self.result
    
    async def _execute_with_retry(self) -> TaskResult:
        """Exécute la tâche avec mécanisme de retry"""
        last_error = None
        
        for attempt in range(self.retry_count, self.max_retries + 1):
            try:
                if self._cancel_event.is_set():
                    raise TaskError("Task cancelled")
                    
                start_time = datetime.now()
                result_data = await self._run()
                end_time = datetime.now()
                
                execution_time = (end_time - start_time).total_seconds()
                
                return TaskResult(
                    task_id=self.id,
                    status=TaskStatus.COMPLETED,
                    result=result_data,
                    execution_time=execution_time,
                    created_at=self.created_at,
                    completed_at=end_time,
                    metadata=self.metadata
                )
                
            except Exception as e:
                last_error = e
                self.retry_count = attempt
                
                if attempt < self.max_retries:
                    await asyncio.sleep(2 ** attempt)
                    continue
                else:
                    raise
        
        raise TaskError(f"Max retries exceeded: {last_error}")
    
    @abstractmethod
    async def _run(self) -> Any:
        """Méthode abstraite à implémenter par les sous-classes"""
        pass
    
    async def _execute_callbacks(self) -> None:
        """Exécute tous les callbacks enregistrés"""
        for callback in self.callbacks:
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback(self)
                else:
                    callback(self)
            except Exception as e:
                pass
    
    def to_dict(self) -> Dict[str, Any]:
        """Convertit la tâche en dictionnaire"""
        return {
            "id": self.id,
            "name": self.name,
            "status": self.status.value,
            "priority": self.priority.value,
            "timeout": self.timeout,
            "retry_count": self.retry_count,
            "max_retries": self.max_retries,
            "created_at": self.created_at.isoformat(),
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "metadata": self.metadata,
            "kwargs": self.kwargs,
            "result": self.result.to_dict() if self.result else None,
            "dependencies": [dep.id for dep in self.dependencies]
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'BaseTask':
        """Crée une tâche à partir d'un dictionnaire"""
        task = cls(
            name=data["name"],
            task_id=data["id"],
            priority=TaskPriority(data["priority"]),
            timeout=data["timeout"],
            retry_count=data["retry_count"],
            max_retries=data["max_retries"],
            metadata=data.get("metadata", {}),
            **data.get("kwargs", {})
        )
        
        task.status = TaskStatus(data["status"])
        task.created_at = datetime.fromisoformat(data["created_at"])
        
        if data["started_at"]:
            task.started_at = datetime.fromisoformat(data["started_at"])
        if data["completed_at"]:
            task.completed_at = datetime.fromisoformat(data["completed_at"])
        
        if data.get("result"):
            task.result = TaskResult(
                task_id=data["result"]["task_id"],
                status=TaskStatus(data["result"]["status"]),
                result=data["result"]["result"],
                error=data["result"]["error"],
                execution_time=data["result"]["execution_time"],
                created_at=datetime.fromisoformat(data["result"]["created_at"]),
                completed_at=datetime.fromisoformat(data["result"]["completed_at"]) if data["result"]["completed_at"] else None,
                metadata=data["result"]["metadata"]
            )
        
        return task
    
    def __str__(self) -> str:
        return f"Task(id={self.id}, name={self.name}, status={self.status.value})"
    
    def __repr__(self) -> str:
        return self.__str__()