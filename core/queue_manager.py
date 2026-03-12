import asyncio
from typing import Any, Dict, List, Optional, Tuple, Union
from enum import Enum
import heapq
from dataclasses import dataclass
from datetime import datetime

from core.exceptions import QueueNotFoundError, QueueFullError, TaskNotFoundError
from core.task import Task


class QueueType(Enum):
    FIFO = "fifo"
    LIFO = "lifo"
    PRIORITY = "priority"


@dataclass
class QueueStats:
    name: str
    queue_type: QueueType
    size: int
    max_size: int
    tasks_processed: int
    tasks_failed: int
    created_at: datetime
    last_updated: datetime


class BaseQueue:
    """Base class for all queue types"""
    
    def __init__(self, name: str, max_size: int = 0):
        self.name = name
        self.max_size = max_size
        self._queue = []
        self.tasks_processed = 0
        self.tasks_failed = 0
        self.created_at = datetime.now()
        self.last_updated = datetime.now()
    
    async def put(self, task: Task) -> None:
        if self.max_size > 0 and len(self._queue) >= self.max_size:
            raise QueueFullError(f"Queue '{self.name}' is full (max_size: {self.max_size})")
        self._queue.append(task)
        self.last_updated = datetime.now()
    
    async def get(self) -> Optional[Task]:
        if not self._queue:
            return None
        self.last_updated = datetime.now()
        return self._queue.pop(0)
    
    def qsize(self) -> int:
        return len(self._queue)
    
    def empty(self) -> bool:
        return len(self._queue) == 0
    
    def contains(self, task_id: str) -> bool:
        return any(task.id == task_id for task in self._queue)
    
    def remove(self, task_id: str) -> bool:
        for i, task in enumerate(self._queue):
            if task.id == task_id:
                self._queue.pop(i)
                self.last_updated = datetime.now()
                return True
        return False
    
    def get_stats(self) -> QueueStats:
        return QueueStats(
            name=self.name,
            queue_type=QueueType.FIFO,
            size=self.qsize(),
            max_size=self.max_size,
            tasks_processed=self.tasks_processed,
            tasks_failed=self.tasks_failed,
            created_at=self.created_at,
            last_updated=self.last_updated
        )
    
    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name='{self.name}', size={self.qsize()})"


class FIFOQueue(BaseQueue):
    """First-In-First-Out queue implementation"""
    
    def __init__(self, name: str, max_size: int = 0):
        super().__init__(name, max_size)
        self.queue_type = QueueType.FIFO
    
    async def put(self, task: Task) -> None:
        await super().put(task)
    
    async def get(self) -> Optional[Task]:
        return await super().get()


class LIFOQueue(BaseQueue):
    """Last-In-First-Out queue implementation"""
    
    def __init__(self, name: str, max_size: int = 0):
        super().__init__(name, max_size)
        self.queue_type = QueueType.LIFO
    
    async def put(self, task: Task) -> None:
        if self.max_size > 0 and len(self._queue) >= self.max_size:
            raise QueueFullError(f"Queue '{self.name}' is full (max_size: {self.max_size})")
        self._queue.append(task)
        self.last_updated = datetime.now()
    
    async def get(self) -> Optional[Task]:
        if not self._queue:
            return None
        self.last_updated = datetime.now()
        return self._queue.pop()


class PriorityQueue(BaseQueue):
    """Priority queue implementation"""
    
    def __init__(self, name: str, max_size: int = 0):
        super().__init__(name, max_size)
        self.queue_type = QueueType.PRIORITY
        self._queue = []  # heap list
    
    async def put(self, task: Task) -> None:
        if self.max_size > 0 and len(self._queue) >= self.max_size:
            raise QueueFullError(f"Queue '{self.name}' is full (max_size: {self.max_size})")
        # Use negative priority for max-heap behavior (higher priority = smaller number)
        heapq.heappush(self._queue, (-task.priority.value, task.created_at, task.id, task))
        self.last_updated = datetime.now()
    
    async def get(self) -> Optional[Task]:
        if not self._queue:
            return None
        self.last_updated = datetime.now()
        _, _, _, task = heapq.heappop(self._queue)
        return task
    
    def qsize(self) -> int:
        return len(self._queue)
    
    def empty(self) -> bool:
        return len(self._queue) == 0
    
    def contains(self, task_id: str) -> bool:
        return any(task.id == task_id for _, _, _, task in self._queue)
    
    def remove(self, task_id: str) -> bool:
        for i, (priority, created_at, tid, task) in enumerate(self._queue):
            if task.id == task_id:
                self._queue.pop(i)
                heapq.heapify(self._queue)
                self.last_updated = datetime.now()
                return True
        return False


class QueueManager:
    """Manages multiple queues and provides high-level operations"""
    
    def __init__(self):
        self._queues: Dict[str, BaseQueue] = {}
        self._queue_types: Dict[str, QueueType] = {}
        self._lock = asyncio.Lock()
    
    async def create_queue(self, name: str, queue_type: QueueType = QueueType.FIFO, max_size: int = 0) -> BaseQueue:
        """Create a new queue"""
        async with self._lock:
            if name in self._queues:
                raise QueueError(f"Queue '{name}' already exists")
            
            if queue_type == QueueType.FIFO:
                queue = FIFOQueue(name, max_size)
            elif queue_type == QueueType.LIFO:
                queue = LIFOQueue(name, max_size)
            elif queue_type == QueueType.PRIORITY:
                queue = PriorityQueue(name, max_size)
            else:
                raise ValueError(f"Invalid queue type: {queue_type}")
            
            self._queues[name] = queue
            self._queue_types[name] = queue_type
            return queue
    
    async def delete_queue(self, name: str) -> bool:
        """Delete a queue"""
        async with self._lock:
            if name not in self._queues:
                return False
            
            del self._queues[name]
            del self._queue_types[name]
            return True
    
    def get_queue(self, name: str) -> BaseQueue:
        """Get a queue by name"""
        if name not in self._queues:
            raise QueueNotFoundError(f"Queue '{name}' not found")
        return self._queues[name]
    
    async def enqueue_task(self, task: Task) -> None:
        """Enqueue a task to its specified queue"""
        queue = self.get_queue(task.queue_name)
        await queue.put(task)
        task.mark_as_queued()
    
    async def dequeue_task(self, queue_name: str) -> Optional[Task]:
        """Dequeue a task from the specified queue"""
        queue = self.get_queue(queue_name)
        task = await queue.get()
        if task:
            queue.tasks_processed += 1
        return task
    
    async def remove_task(self, queue_name: str, task_id: str) -> bool:
        """Remove a specific task from a queue"""
        queue = self.get_queue(queue_name)
        return queue.remove(task_id)
    
    def get_queue_stats(self, queue_name: str) -> QueueStats:
        """Get statistics for a queue"""
        queue = self.get_queue(queue_name)
        return queue.get_stats()
    
    def list_queues(self) -> List[str]:
        """List all queue names"""
        return list(self._queues.keys())
    
    def queue_exists(self, name: str) -> bool:
        """Check if a queue exists"""
        return name in self._queues
    
    def get_queue_type(self, name: str) -> QueueType:
        """Get the type of a queue"""
        if name not in self._queue_types:
            raise QueueNotFoundError(f"Queue '{name}' not found")
        return self._queue_types[name]
    
    async def clear_queue(self, queue_name: str) -> None:
        """Clear all tasks from a queue"""
        queue = self.get_queue(queue_name)
        queue._queue.clear()
        queue.last_updated = datetime.now()
    
    def __repr__(self) -> str:
        return f"QueueManager(queues={list(self._queues.keys())})"
