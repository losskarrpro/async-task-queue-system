import asyncio
import json
import time
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Union
from uuid import UUID

import redis.asyncio as aioredis
from pydantic import BaseModel, Field

from utils.logger import get_logger

logger = get_logger(__name__)


class ResultStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMEOUT = "timeout"


class TaskResult(BaseModel):
    """Modèle pour stocker les résultats d'une tâche"""
    task_id: UUID
    status: ResultStatus = ResultStatus.PENDING
    result: Optional[Any] = None
    error: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.now)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    execution_time: Optional[float] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convertit le résultat en dictionnaire"""
        data = self.dict()
        data['task_id'] = str(self.task_id)
        data['created_at'] = self.created_at.isoformat()
        if self.started_at:
            data['started_at'] = self.started_at.isoformat()
        if self.completed_at:
            data['completed_at'] = self.completed_at.isoformat()
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TaskResult':
        """Crée un TaskResult à partir d'un dictionnaire"""
        if isinstance(data.get('task_id'), str):
            data['task_id'] = UUID(data['task_id'])
        if isinstance(data.get('created_at'), str):
            data['created_at'] = datetime.fromisoformat(data['created_at'])
        if data.get('started_at'):
            data['started_at'] = datetime.fromisoformat(data['started_at'])
        if data.get('completed_at'):
            data['completed_at'] = datetime.fromisoformat(data['completed_at'])
        return cls(**data)


class BaseResultStore(ABC):
    """Interface de base pour le stockage des résultats"""
    
    @abstractmethod
    async def initialize(self) -> None:
        """Initialise le store"""
        pass
    
    @abstractmethod
    async def shutdown(self) -> None:
        """Nettoie les ressources"""
        pass
    
    @abstractmethod
    async def store_result(self, result: TaskResult) -> None:
        """Stocke un résultat"""
        pass
    
    @abstractmethod
    async def get_result(self, task_id: UUID) -> Optional[TaskResult]:
        """Récupère un résultat"""
        pass
    
    @abstractmethod
    async def update_status(self, task_id: UUID, status: ResultStatus, 
                          error: Optional[str] = None) -> None:
        """Met à jour le statut d'une tâche"""
        pass
    
    @abstractmethod
    async def update_result(self, task_id: UUID, result: Any, 
                          execution_time: Optional[float] = None) -> None:
        """Met à jour le résultat d'une tâche"""
        pass
    
    @abstractmethod
    async def delete_result(self, task_id: UUID) -> None:
        """Supprime un résultat"""
        pass
    
    @abstractmethod
    async def cleanup_old_results(self, max_age_hours: int = 24) -> None:
        """Nettoie les résultats plus vieux que max_age_hours"""
        pass


class MemoryResultStore(BaseResultStore):
    """Stockage des résultats en mémoire"""
    
    def __init__(self):
        self._results: Dict[UUID, TaskResult] = {}
        self._lock = asyncio.Lock()
    
    async def initialize(self) -> None:
        """Initialise le store"""
        logger.info("Initializing MemoryResultStore")
    
    async def shutdown(self) -> None:
        """Nettoie les ressources"""
        async with self._lock:
            self._results.clear()
        logger.info("MemoryResultStore shutdown")
    
    async def store_result(self, result: TaskResult) -> None:
        """Stocke un résultat"""
        async with self._lock:
            self._results[result.task_id] = result
        logger.debug(f"Stored result for task {result.task_id}")
    
    async def get_result(self, task_id: UUID) -> Optional[TaskResult]:
        """Récupère un résultat"""
        async with self._lock:
            return self._results.get(task_id)
    
    async def update_status(self, task_id: UUID, status: ResultStatus, 
                          error: Optional[str] = None) -> None:
        """Met à jour le statut d'une tâche"""
        async with self._lock:
            if task_id in self._results:
                result = self._results[task_id]
                result.status = status
                result.error = error
                if status == ResultStatus.PROCESSING and not result.started_at:
                    result.started_at = datetime.now()
                elif status in [ResultStatus.COMPLETED, ResultStatus.FAILED, ResultStatus.CANCELLED, ResultStatus.TIMEOUT]:
                    result.completed_at = datetime.now()
                    if result.started_at:
                        result.execution_time = (result.completed_at - result.started_at).total_seconds()
                logger.debug(f"Updated status for task {task_id} to {status}")
            else:
                logger.warning(f"Task {task_id} not found in result store")
    
    async def update_result(self, task_id: UUID, result: Any, 
                          execution_time: Optional[float] = None) -> None:
        """Met à jour le résultat d'une tâche"""
        async with self._lock:
            if task_id in self._results:
                task_result = self._results[task_id]
                task_result.result = result
                task_result.status = ResultStatus.COMPLETED
                task_result.completed_at = datetime.now()
                if execution_time is not None:
                    task_result.execution_time = execution_time
                elif task_result.started_at:
                    task_result.execution_time = (task_result.completed_at - task_result.started_at).total_seconds()
                logger.debug(f"Updated result for task {task_id}")
            else:
                logger.warning(f"Task {task_id} not found in result store")
    
    async def delete_result(self, task_id: UUID) -> None:
        """Supprime un résultat"""
        async with self._lock:
            if task_id in self._results:
                del self._results[task_id]
                logger.debug(f"Deleted result for task {task_id}")
            else:
                logger.warning(f"Task {task_id} not found in result store")
    
    async def cleanup_old_results(self, max_age_hours: int = 24) -> None:
        """Nettoie les résultats plus vieux que max_age_hours"""
        cutoff_time = datetime.now() - timedelta(hours=max_age_hours)
        async with self._lock:
            to_delete = [
                task_id for task_id, result in self._results.items()
                if result.completed_at and result.completed_at < cutoff_time
            ]
            for task_id in to_delete:
                del self._results[task_id]
            if to_delete:
                logger.info(f"Cleaned up {len(to_delete)} old results")


class RedisResultStore(BaseResultStore):
    """Stockage des résultats dans Redis"""
    
    def __init__(self, redis_url: str = "redis://localhost:6379/0"):
        self.redis_url = redis_url
        self.redis: Optional[aioredis.Redis] = None
        self.key_prefix = "task_result:"
    
    async def initialize(self) -> None:
        """Initialise la connexion Redis"""
        self.redis = aioredis.from_url(self.redis_url, decode_responses=False)
        try:
            await self.redis.ping()
            logger.info(f"Connected to Redis at {self.redis_url}")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise
    
    async def shutdown(self) -> None:
        """Ferme la connexion Redis"""
        if self.redis:
            await self.redis.close()
            logger.info("RedisResultStore shutdown")
    
    async def store_result(self, result: TaskResult) -> None:
        """Stocke un résultat dans Redis"""
        if not self.redis:
            raise RuntimeError("Redis not initialized")
        
        key = f"{self.key_prefix}{result.task_id}"
        result_dict = result.to_dict()
        # Convert to JSON string for storage
        result_json = json.dumps(result_dict)
        
        await self.redis.set(key, result_json)
        # Set expiration to 7 days by default
        await self.redis.expire(key, 7 * 24 * 3600)
        logger.debug(f"Stored result for task {result.task_id} in Redis")
    
    async def get_result(self, task_id: UUID) -> Optional[TaskResult]:
        """Récupère un résultat depuis Redis"""
        if not self.redis:
            raise RuntimeError("Redis not initialized")
        
        key = f"{self.key_prefix}{task_id}"
        result_json = await self.redis.get(key)
        
        if result_json:
            result_dict = json.loads(result_json)
            return TaskResult.from_dict(result_dict)
        return None
    
    async def update_status(self, task_id: UUID, status: ResultStatus, 
                          error: Optional[str] = None) -> None:
        """Met à jour le statut d'une tâche dans Redis"""
        if not self.redis:
            raise RuntimeError("Redis not initialized")
        
        key = f"{self.key_prefix}{task_id}"
        result_json = await self.redis.get(key)
        
        if result_json:
            result_dict = json.loads(result_json)
            result_dict['status'] = status
            if error is not None:
                result_dict['error'] = error
            
            # Update timestamps
            now = datetime.now().isoformat()
            if status == ResultStatus.PROCESSING and not result_dict.get('started_at'):
                result_dict['started_at'] = now
            elif status in [ResultStatus.COMPLETED, ResultStatus.FAILED, ResultStatus.CANCELLED, ResultStatus.TIMEOUT]:
                result_dict['completed_at'] = now
                if result_dict.get('started_at'):
                    started_at = datetime.fromisoformat(result_dict['started_at'])
                    completed_at = datetime.fromisoformat(result_dict['completed_at'])
                    result_dict['execution_time'] = (completed_at - started_at).total_seconds()
            
            await self.redis.set(key, json.dumps(result_dict))
            logger.debug(f"Updated status for task {task_id} to {status} in Redis")
        else:
            logger.warning(f"Task {task_id} not found in Redis result store")
    
    async def update_result(self, task_id: UUID, result: Any, 
                          execution_time: Optional[float] = None) -> None:
        """Met à jour le résultat d'une tâche dans Redis"""
        if not self.redis:
            raise RuntimeError("Redis not initialized")
        
        key = f"{self.key_prefix}{task_id}"
        result_json = await self.redis.get(key)
        
        if result_json:
            result_dict = json.loads(result_json)
            result_dict['result'] = result
            result_dict['status'] = ResultStatus.COMPLETED
            result_dict['completed_at'] = datetime.now().isoformat()
            
            if execution_time is not None:
                result_dict['execution_time'] = execution_time
            elif result_dict.get('started_at'):
                started_at = datetime.fromisoformat(result_dict['started_at'])
                completed_at = datetime.fromisoformat(result_dict['completed_at'])
                result_dict['execution_time'] = (completed_at - started_at).total_seconds()
            
            await self.redis.set(key, json.dumps(result_dict))
            logger.debug(f"Updated result for task {task_id} in Redis")
        else:
            logger.warning(f"Task {task_id} not found in Redis result store")
    
    async def delete_result(self, task_id: UUID) -> None:
        """Supprime un résultat de Redis"""
        if not self.redis:
            raise RuntimeError("Redis not initialized")
        
        key = f"{self.key_prefix}{task_id}"
        deleted = await self.redis.delete(key)
        if deleted:
            logger.debug(f"Deleted result for task {task_id} from Redis")
        else:
            logger.warning(f"Task {task_id} not found in Redis result store")
    
    async def cleanup_old_results(self, max_age_hours: int = 24) -> None:
        """Nettoie les résultats plus vieux que max_age_hours dans Redis"""
        if not self.redis:
            raise RuntimeError("Redis not initialized")
        
        # This is a simplified implementation - in production you might want
        # to use Redis TTL or a more sophisticated cleanup strategy
        logger.info("Redis cleanup would be implemented with TTL or scheduled jobs")


class ResultStore:
    """Facade pour le stockage des résultats avec support multiple backends"""
    
    def __init__(self, store_type: str = "memory", **kwargs):
        self.store_type = store_type
        self.kwargs = kwargs
        self._store: Optional[BaseResultStore] = None
    
    async def initialize(self) -> None:
        """Initialise le store approprié"""
        if self.store_type == "redis":
            self._store = RedisResultStore(**self.kwargs)
        else:
            self._store = MemoryResultStore()
        
        await self._store.initialize()
        logger.info(f"Initialized ResultStore with {self.store_type} backend")
    
    async def shutdown(self) -> None:
        """Nettoie les ressources"""
        if self._store:
            await self._store.shutdown()
    
    async def store_result(self, result: TaskResult) -> None:
        """Stocke un résultat"""
        if not self._store:
            raise RuntimeError("ResultStore not initialized")
        await self._store.store_result(result)
    
    async def get_result(self, task_id: UUID) -> Optional[TaskResult]:
        """Récupère un résultat"""
        if not self._store:
            raise RuntimeError("ResultStore not initialized")
        return await self._store.get_result(task_id)
    
    async def update_status(self, task_id: UUID, status: ResultStatus, 
                          error: Optional[str] = None) -> None:
        """Met à jour le statut d'une tâche"""
        if not self._store:
            raise RuntimeError("ResultStore not initialized")
        await self._store.update_status(task_id, status, error)
    
    async def update_result(self, task_id: UUID, result: Any, 
                          execution_time: Optional[float] = None) -> None:
        """Met à jour le résultat d'une tâche"""
        if not self._store:
            raise RuntimeError("ResultStore not initialized")
        await self._store.update_result(task_id, result, execution_time)
    
    async def delete_result(self, task_id: UUID) -> None:
        """Supprime un résultat"""
        if not self._store:
            raise RuntimeError("ResultStore not initialized")
        await self._store.delete_result(task_id)
    
    async def cleanup_old_results(self, max_age_hours: int = 24) -> None:
        """Nettoie les résultats plus vieux que max_age_hours"""
        if not self._store:
            raise RuntimeError("ResultStore not initialized")
        await self._store.cleanup_old_results(max_age_hours)