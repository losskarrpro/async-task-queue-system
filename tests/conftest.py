import asyncio
import pytest
import tempfile
import sys
import os
from unittest.mock import AsyncMock, MagicMock, patch
from typing import Dict, Any, AsyncGenerator, Generator

# Add project root to sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.queue_manager import QueueManager
from core.result_store import ResultStore
from core.task import Task
from workers.worker_pool import WorkerPool
from workers.async_worker import AsyncWorker
from config.settings import QueueType, TaskStatus
# Import create_app directly to avoid circular import
from api.app import create_app


@pytest.fixture
def temp_config_file() -> Generator[str, None, None]:
    """Create temporary configuration file for testing."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
        f.write("""
QUEUE_TYPE = "fifo"
MAX_WORKERS = 2
RESULT_STORE_TYPE = "memory"
REDIS_URL = "redis://localhost:6379/0"
LOG_LEVEL = "INFO"
API_PORT = 7501
WORKER_POLL_INTERVAL = 0.1
MAX_RETRIES = 3
        """)
        temp_path = f.name
    
    yield temp_path
    os.unlink(temp_path)


@pytest.fixture(scope="session")
def event_loop() -> Generator[asyncio.AbstractEventLoop, None, None]:
    """Create event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def mock_redis() -> Generator[MagicMock, None, None]:
    """Mock Redis client for tests."""
    with patch('redis.Redis') as mock_redis_class:
        mock_client = MagicMock()
        mock_client.ping.return_value = True
        mock_client.get.return_value = None
        mock_client.set.return_value = True
        mock_client.delete.return_value = True
        mock_client.hgetall.return_value = {}
        mock_client.hset.return_value = True
        mock_client.hdel.return_value = True
        mock_client.expire.return_value = True
        mock_redis_class.return_value = mock_client
        yield mock_client


@pytest.fixture(params=["memory", "redis"])
def result_store(request: pytest.FixtureRequest, mock_redis: MagicMock) -> ResultStore:
    """Provide ResultStore with different backends."""
    if request.param == "redis":
        # Patch the Redis import in result_store module
        with patch('core.result_store.redis.Redis', return_value=mock_redis):
            store = ResultStore(store_type="redis")
    else:
        store = ResultStore(store_type="memory")
    
    yield store
    
    # Cleanup
    if request.param == "memory":
        store._results.clear()


@pytest.fixture(params=[QueueType.FIFO, QueueType.LIFO, QueueType.PRIORITY])
def queue_manager(request: pytest.FixtureRequest) -> QueueManager:
    """Provide QueueManager with different queue types."""
    return QueueManager(queue_type=request.param)


@pytest.fixture
def sample_task_data() -> Dict[str, Any]:
    """Provide sample task data for testing."""
    return {
        "task_id": "test_task_123",
        "name": "test_task",
        "queue": "default",
    }