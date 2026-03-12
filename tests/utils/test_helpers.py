import asyncio
import json
import uuid
from unittest.mock import AsyncMock, Mock, MagicMock
from datetime import datetime, timedelta
import pytest

from core.task import Task, TaskStatus, Priority
from core.queue_manager import QueueManager, QueueType
from core.result_store import ResultStore
from workers.async_worker import AsyncWorker


class TestHelpers:
    """Classe utilitaire pour les tests"""

    @staticmethod
    def create_test_task(
        task_id: str = None,
        task_type: str = "test_task",
        data: dict = None,
        priority: Priority = Priority.MEDIUM,
        metadata: dict = None
    ) -> Task:
        """Crée une tâche de test"""
        return Task(
            task_id=task_id or str(uuid.uuid4()),
            task_type=task_type,
            data=data or {"message": "test"},
            priority=priority,
            metadata=metadata or {"source": "test"}
        )

    @staticmethod
    def create_completed_task(
        task_id: str = None,
        result: dict = None,
        execution_time: float = 0.1
    ) -> Task:
        """Crée une tâche complétée avec résultat"""
        task = TestHelpers.create_test_task(task_id=task_id)
        task.status = TaskStatus.COMPLETED
        task.result = result or {"success": True, "data": "completed"}
        task.execution_time = execution_time
        task.completed_at = datetime.now()
        return task

    @staticmethod
    def create_failed_task(
        task_id: str = None,
        error: str = "Test error",
        execution_time: float = 0.05
    ) -> Task:
        """Crée une tâche échouée"""
        task = TestHelpers.create_test_task(task_id=task_id)
        task.status = TaskStatus.FAILED
        task.error = error
        task.execution_time = execution_time
        task.completed_at = datetime.now()
        return task

    @staticmethod
    async def setup_queue_manager(
        queue_type: QueueType = QueueType.FIFO,
        max_size: int = 100
    ) -> QueueManager:
        """Configure un QueueManager pour les tests"""
        queue_manager = QueueManager()
        await queue_manager.create_queue("test_queue", queue_type, max_size)
        return queue_manager

    @staticmethod
    async def setup_result_store(
        use_redis: bool = False,
        redis_url: str = "redis://localhost:6379/0"
    ) -> ResultStore:
        """Configure un ResultStore pour les tests"""
        if use_redis:
            try:
                import redis.asyncio as redis
                client = redis.from_url(redis_url)
                await client.ping()
                return ResultStore(backend="redis", redis_client=client)
            except Exception:
                pytest.skip("Redis non disponible")
        
        return ResultStore(backend="memory")

    @staticmethod
    def create_mock_async_worker(
        worker_id: str = None,
        process_time: float = 0.01
    ) -> AsyncWorker:
        """Crée un worker mock pour les tests"""
        worker = AsyncMock(spec=AsyncWorker)
        worker.worker_id = worker_id or f"worker-{uuid.uuid4()}"
        worker.is_running = True
        worker.process_task = AsyncMock()
        worker.stop = AsyncMock()
        worker.join = AsyncMock()
        
        # Simuler le traitement d'une tâche
        async def mock_process(task):
            await asyncio.sleep(process_time)
            task.status = TaskStatus.COMPLETED
            task.result = {"processed_by": worker.worker_id}
            return task
        
        worker.process_task.side_effect = mock_process
        
        return worker

    @staticmethod
    def mock_redis_client():
        """Crée un client Redis mock"""
        redis_mock = Mock()
        redis_mock.set = AsyncMock()
        redis_mock.get = AsyncMock()
        redis_mock.delete = AsyncMock()
        redis_mock.exists = AsyncMock()
        redis_mock.keys = AsyncMock()
        redis_mock.expire = AsyncMock()
        redis_mock.close = AsyncMock()
        return redis_mock

    @staticmethod
    def create_task_batch(count: int = 10) -> list[Task]:
        """Crée un lot de tâches pour les tests"""
        return [
            TestHelpers.create_test_task(
                task_id=f"test-task-{i}",
                task_type=f"task_type_{i % 3}",
                data={"index": i, "data": f"test_data_{i}"},
                priority=Priority(i % 3 + 1)
            )
            for i in range(count)
        ]

    @staticmethod
    async def wait_for_condition(
        condition_func,
        timeout: float = 5.0,
        interval: float = 0.1
    ) -> bool:
        """Attend qu'une condition soit vraie"""
        start_time = asyncio.get_event_loop().time()
        
        while asyncio.get_event_loop().time() - start_time < timeout:
            if await condition_func():
                return True
            await asyncio.sleep(interval)
        
        return False

    @staticmethod
    def assert_task_equal(task1: Task, task2: Task, ignore_timestamps: bool = True):
        """Vérifie que deux tâches sont égales (optionnellement sans les timestamps)"""
        assert task1.task_id == task2.task_id
        assert task1.task_type == task2.task_type
        assert task1.data == task2.data
        assert task1.priority == task2.priority
        assert task1.metadata == task2.metadata
        assert task1.status == task2.status
        
        if not ignore_timestamps:
            assert task1.created_at == task2.created_at
            assert task1.started_at == task2.started_at
            assert task1.completed_at == task2.completed_at
        
        if task1.status == TaskStatus.COMPLETED:
            assert task1.result == task2.result
        elif task1.status == TaskStatus.FAILED:
            assert task1.error == task2.error

    @staticmethod
    def serialize_task_for_test(task: Task) -> dict:
        """Sérialise une tâche pour les tests API"""
        return {
            "task_id": task.task_id,
            "task_type": task.task_type,
            "data": task.data,
            "priority": task.priority.value,
            "metadata": task.metadata,
            "status": task.status.value,
            "created_at": task.created_at.isoformat() if task.created_at else None,
            "started_at": task.started_at.isoformat() if task.started_at else None,
            "completed_at": task.completed_at.isoformat() if task.completed_at else None,
            "result": task.result,
            "error": task.error,
            "execution_time": task.execution_time
        }


@pytest.fixture
def test_helpers():
    """Fixture pytest pour les helpers de test"""
    return TestHelpers


@pytest.fixture
async def queue_manager_fixture():
    """Fixture pour un QueueManager"""
    manager = await TestHelpers.setup_queue_manager()
    yield manager
    await manager.clear_all_queues()


@pytest.fixture
async def result_store_fixture():
    """Fixture pour un ResultStore en mémoire"""
    store = await TestHelpers.setup_result_store(use_redis=False)
    yield store
    await store.clear_all()


@pytest.fixture
def mock_redis_fixture():
    """Fixture pour un client Redis mock"""
    return TestHelpers.mock_redis_client()


@pytest.fixture
def mock_worker_fixture():
    """Fixture pour un worker mock"""
    return TestHelpers.create_mock_async_worker()


@pytest.fixture
def task_batch_fixture():
    """Fixture pour un lot de tâches"""
    return TestHelpers.create_task_batch(count=5)


@pytest.fixture
def completed_task_fixture():
    """Fixture pour une tâche complétée"""
    return TestHelpers.create_completed_task()


@pytest.fixture
def failed_task_fixture():
    """Fixture pour une tâche échouée"""
    return TestHelpers.create_failed_task()


class AsyncContextManagerMock:
    """Mock pour un context manager asynchrone"""
    
    def __init__(self, return_value=None, side_effect=None):
        self.return_value = return_value
        self.side_effect = side_effect
    
    async def __aenter__(self):
        if self.side_effect:
            return await self.side_effect()
        return self.return_value
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass


def create_async_mock_iterator(items):
    """Crée un itérateur asynchrone mock"""
    async def async_gen():
        for item in items:
            yield item
    return async_gen()