import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
import uuid

from core.task import Task
from core.exceptions import TaskError, QueueError
from workers.async_worker import AsyncWorker
from workers.base_worker import WorkerStatus
from core.result_store import ResultStore


@pytest.fixture
def mock_queue():
    """Mock queue manager."""
    queue = AsyncMock()
    queue.get_task = AsyncMock()
    queue.task_done = AsyncMock()
    return queue


@pytest.fixture
def mock_result_store():
    """Mock result store."""
    store = AsyncMock()
    store.set_result = AsyncMock()
    return store


@pytest.fixture
def sample_task():
    """Create a sample task."""
    return Task(
        id=str(uuid.uuid4()),
        name="test_task",
        func_name="tasks.example_tasks.add",
        args=[2, 3],
        kwargs={},
        queue_type="fifo",
        priority=1,
        timeout=10,
        retries=0
    )


@pytest.mark.asyncio
async def test_async_worker_initialization():
    """Test AsyncWorker initialization."""
    worker = AsyncWorker(
        worker_id="test_worker",
        queue=AsyncMock(),
        result_store=AsyncMock(),
        poll_interval=0.1
    )
    assert worker.worker_id == "test_worker"
    assert worker.status == WorkerStatus.IDLE
    assert worker.poll_interval == 0.1
    assert worker.current_task is None


@pytest.mark.asyncio
async def test_worker_start_stop(mock_queue, mock_result_store):
    """Test worker start and stop."""
    worker = AsyncWorker(
        worker_id="test_worker",
        queue=mock_queue,
        result_store=mock_result_store,
        poll_interval=0.01
    )
    
    # Mock get_task to return None to exit loop
    mock_queue.get_task.return_value = None
    
    # Start worker in background
    task = asyncio.create_task(worker.start())
    
    # Wait a bit for worker to start
    await asyncio.sleep(0.02)
    assert worker.status == WorkerStatus.RUNNING
    
    # Stop worker
    worker.stop()
    await asyncio.sleep(0.02)
    assert worker.status == WorkerStatus.STOPPED
    
    # Wait for worker task to complete
    await task
    mock_queue.get_task.assert_called()


@pytest.mark.asyncio
async def test_worker_process_task_success(mock_queue, mock_result_store, sample_task):
    """Test successful task processing."""
    worker = AsyncWorker(
        worker_id="test_worker",
        queue=mock_queue,
        result_store=mock_result_store,
        poll_interval=0.1
    )
    
    # Mock task execution
    with patch('workers.async_worker.execute_task', new_callable=AsyncMock) as mock_execute:
        mock_execute.return_value = 5
        
        result = await worker._process_task(sample_task)
        
        assert result == 5
        mock_execute.assert_called_once_with(sample_task)
        mock_result_store.set_result.assert_called_once_with(
            sample_task.id, 
            {"status": "completed", "result": 5}
        )


@pytest.mark.asyncio
async def test_worker_process_task_failure(mock_queue, mock_result_store, sample_task):
    """Test task processing with failure."""
    worker = AsyncWorker(
        worker_id="test_worker",
        queue=mock_queue,
        result_store=mock_result_store,
        poll_interval=0.1
    )
    
    # Mock task execution to raise exception
    with patch('workers.async_worker.execute_task', new_callable=AsyncMock) as mock_execute:
        mock_execute.side_effect = TaskError("Task failed")
        
        result = await worker._process_task(sample_task)
        
        assert result is None
        mock_execute.assert_called_once_with(sample_task)
        mock_result_store.set_result.assert_called_once_with(
            sample_task.id, 
            {"status": "failed", "error": "TaskError: Task failed"}
        )


@pytest.mark.asyncio
async def test_worker_process_task_timeout(mock_queue, mock_result_store, sample_task):
    """Test task processing timeout."""
    worker = AsyncWorker(
        worker_id="test_worker",
        queue=mock_queue,
        result_store=mock_result_store,
        poll_interval=0.1
    )
    
    # Mock task execution to timeout
    async def slow_execute(task):
        await asyncio.sleep(0.2)  # Longer than timeout
        return 5
    
    sample_task.timeout = 0.1  # Set timeout to 0.1 seconds
    
    with patch('workers.async_worker.execute_task', new_callable=AsyncMock) as mock_execute:
        mock_execute.side_effect = slow_execute
        
        result = await worker._process_task(sample_task)
        
        assert result is None
        mock_result_store.set_result.assert_called_once_with(
            sample_task.id, 
            {"status": "timeout", "error": "Task timed out after 0.1 seconds"}
        )


@pytest.mark.asyncio
async def test_worker_run_loop(mock_queue, mock_result_store, sample_task):
    """Test worker run loop with task processing."""
    worker = AsyncWorker(
        worker_id="test_worker",
        queue=mock_queue,
        result_store=mock_result_store,
        poll_interval=0.01
    )
    
    # Setup mocks
    mock_queue.get_task.side_effect = [sample_task, None]  # One task then stop
    worker._process_task = AsyncMock(return_value=5)
    
    # Run worker
    task = asyncio.create_task(worker.start())
    await asyncio.sleep(0.05)
    worker.stop()
    
    # Wait for worker to finish
    await task
    
    # Verify interactions
    assert mock_queue.get_task.call_count >= 2
    worker._process_task.assert_called_once_with(sample_task)
    mock_queue.task_done.assert_called_once_with(sample_task.id)


@pytest.mark.asyncio
async def test_worker_handle_queue_error(mock_queue, mock_result_store):
    """Test worker handling queue errors."""
    worker = AsyncWorker(
        worker_id="test_worker",
        queue=mock_queue,
        result_store=mock_result_store,
        poll_interval=0.01
    )
    
    # Mock queue to raise error
    mock_queue.get_task.side_effect = QueueError("Queue unavailable")
    
    # Run worker briefly
    task = asyncio.create_task(worker.start())
    await asyncio.sleep(0.05)
    worker.stop()
    
    await task
    
    # Worker should still be stopped without crashing
    assert worker.status == WorkerStatus.STOPPED


@pytest.mark.asyncio
async def test_worker_status_transitions(mock_queue, mock_result_store, sample_task):
    """Test worker status transitions during task processing."""
    worker = AsyncWorker(
        worker_id="test_worker",
        queue=mock_queue,
        result_store=mock_result_store,
        poll_interval=0.1
    )
    
    assert worker.status == WorkerStatus.IDLE
    
    # Mock a task that takes time to process
    async def slow_process(task):
        await asyncio.sleep(0.05)
        return 5
    
    worker._process_task = AsyncMock(side_effect=slow_process)
    
    # Get task and process it
    worker.status = WorkerStatus.RUNNING
    process_task = asyncio.create_task(worker._process_task(sample_task))
    
    # Status should be PROCESSING while task is running
    await asyncio.sleep(0.01)
    assert worker.status == WorkerStatus.PROCESSING
    
    # Wait for task completion
    await process_task
    
    # Status should return to RUNNING (or IDLE if no more tasks)
    # In this test, we're just checking the transition happened
    assert worker.status == WorkerStatus.RUNNING or worker.status == WorkerStatus.IDLE


@pytest.mark.asyncio
async def test_worker_statistics(mock_queue, mock_result_store, sample_task):
    """Test worker statistics tracking."""
    worker = AsyncWorker(
        worker_id="test_worker",
        queue=mock_queue,
        result_store=mock_result_store,
        poll_interval=0.1
    )
    
    # Process multiple tasks
    for i in range(3):
        await worker._process_task(sample_task)
    
    stats = worker.get_statistics()
    
    assert stats["worker_id"] == "test_worker"
    assert stats["tasks_processed"] == 3
    assert "uptime" in stats
    assert "status" in stats


@pytest.mark.asyncio
async def test_worker_with_retries(mock_queue, mock_result_store):
    """Test worker task retry mechanism."""
    task = Task(
        id=str(uuid.uuid4()),
        name="test_retry_task",
        func_name="tasks.example_tasks.failing_task",
        args=[],
        kwargs={},
        queue_type="fifo",
        priority=1,
        timeout=5,
        retries=2
    )
    
    worker = AsyncWorker(
        worker_id="test_worker",
        queue=mock_queue,
        result_store=mock_result_store,
        poll_interval=0.1
    )
    
    # Mock execute_task to fail twice then succeed
    with patch('workers.async_worker.execute_task', new_callable=AsyncMock) as mock_execute:
        mock_execute.side_effect = [TaskError("Fail 1"), TaskError("Fail 2"), "Success"]
        
        # Mock queue to reschedule task
        mock_queue.reschedule_task = AsyncMock()
        
        result = await worker._process_task(task)
        
        # Task should be rescheduled twice
        assert mock_queue.reschedule_task.call_count == 2
        # Final result should be None because task was rescheduled
        assert result is None


def test_worker_sync_methods():
    """Test synchronous methods of AsyncWorker."""
    worker = AsyncWorker(
        worker_id="test_worker",
        queue=AsyncMock(),
        result_store=AsyncMock(),
        poll_interval=0.1
    )
    
    # Test get_info
    info = worker.get_info()
    assert info["worker_id"] == "test_worker"
    assert info["status"] == WorkerStatus.IDLE.value
    
    # Test __repr__
    repr_str = repr(worker)
    assert "AsyncWorker" in repr_str
    assert "test_worker" in repr_str


@pytest.mark.asyncio
async def test_worker_graceful_shutdown(mock_queue, mock_result_store, sample_task):
    """Test worker graceful shutdown during task processing."""
    worker = AsyncWorker(
        worker_id="test_worker",
        queue=mock_queue,
        result_store=mock_result_store,
        poll_interval=0.1
    )
    
    # Create an event to control task execution
    task_started = asyncio.Event()
    task_completed = asyncio.Event()
    
    async def long_running_task(task):
        task_started.set()
        await asyncio.sleep(0.2)  # Simulate long task
        task_completed.set()
        return "result"
    
    worker._process_task = AsyncMock(side_effect=long_running_task)
    
    # Start worker and begin processing
    worker.status = WorkerStatus.RUNNING
    process_task = asyncio.create_task(worker._process_task(sample_task))
    
    # Wait for task to start
    await task_started.wait()
    
    # Stop worker while task is running
    worker.stop()
    
    # Task should continue to completion
    await task_completed.wait()
    await process_task
    
    assert worker.status == WorkerStatus.STOPPED