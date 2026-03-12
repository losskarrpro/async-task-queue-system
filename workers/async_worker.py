import asyncio
import time
from typing import Any, Dict, Optional, Callable
from abc import ABC, abstractmethod
import uuid

from core.task import Task
from core.result_store import ResultStore
from utils.logger import get_logger
from .base_worker import BaseWorker

logger = get_logger(__name__)


class AsyncWorker(BaseWorker):
    """Asynchronous worker implementation"""

    def __init__(self, worker_id: str, result_store: ResultStore):
        super().__init__(worker_id, result_store)
        self._running = False
        self._current_task: Optional[Task] = None

    async def process_task(self, task: Task) -> Dict[str, Any]:
        """Process a single task asynchronously"""
        self._current_task = task
        task.status = "processing"
        task.started_at = time.time()
        
        logger.info(f"Worker {self.worker_id} processing task {task.task_id}")
        
        try:
            # Simulate some async work
            await asyncio.sleep(0.1)
            
            # Execute the task function
            if task.func:
                if asyncio.iscoroutinefunction(task.func):
                    result = await task.func(*task.args, **task.kwargs)
                else:
                    # Run synchronous function in thread pool
                    loop = asyncio.get_event_loop()
                    result = await loop.run_in_executor(
                        None, task.func, *task.args, **task.kwargs
                    )
            else:
                # Default behavior for testing
                result = {"processed_by": self.worker_id, "task_id": task.task_id}
            
            task.status = "completed"
            task.completed_at = time.time()
            task.result = result
            
            # Store result
            await self.result_store.store_result(task.task_id, result)
            
            logger.info(f"Worker {self.worker_id} completed task {task.task_id}")
            return result
            
        except Exception as e:
            task.status = "failed"
            task.completed_at = time.time()
            task.error = str(e)
            
            logger.error(f"Worker {self.worker_id} failed task {task.task_id}: {e}")
            raise
        finally:
            self._current_task = None

    async def run(self, task_queue: asyncio.Queue) -> None:
        """Main worker loop to process tasks from queue"""
        self._running = True
        logger.info(f"Worker {self.worker_id} started")
        
        while self._running:
            try:
                # Get task from queue with timeout
                try:
                    task = await asyncio.wait_for(task_queue.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                
                # Process the task
                await self.process_task(task)
                
                # Mark task as done
                task_queue.task_done()
                
            except asyncio.CancelledError:
                logger.info(f"Worker {self.worker_id} cancelled")
                break
            except Exception as e:
                logger.error(f"Worker {self.worker_id} error: {e}")
                
        logger.info(f"Worker {self.worker_id} stopped")

    def stop(self) -> None:
        """Stop the worker"""
        self._running = False
        logger.info(f"Worker {self.worker_id} stopping")

    @property
    def is_running(self) -> bool:
        """Check if worker is running"""
        return self._running

    @property
    def current_task(self) -> Optional[Task]:
        """Get current task being processed"""
        return self._current_task