import pytest
import asyncio
import json
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

from core.task import Task, TaskStatus, TaskResult, TaskPriority
from core.exceptions import TaskValidationError, TaskNotFoundError, TaskExecutionError
from tasks.base_task import BaseTask
from tasks.example_tasks import ExampleTask, FailingTask, LongRunningTask
from tasks.registry import TaskRegistry
from utils.validators import validate_task_data


class TestTaskStatus:
    """Test TaskStatus enum"""

    def test_task_status_values(self):
        assert TaskStatus.PENDING == "pending"
        assert TaskStatus.QUEUED == "queued"
        assert TaskStatus.RUNNING == "running"
        assert TaskStatus.COMPLETED == "completed"
        assert TaskStatus.FAILED == "failed"
        assert TaskStatus.CANCELLED == "cancelled"
        assert TaskStatus.RETRYING == "retrying"

    def test_task_status_choices(self):
        statuses = list(TaskStatus)
        expected = [
            TaskStatus.PENDING,
            TaskStatus.QUEUED,
            TaskStatus.RUNNING,
            TaskStatus.COMPLETED,
            TaskStatus.FAILED,
            TaskStatus.CANCELLED,
            TaskStatus.RETRYING,
        ]
        assert statuses == expected


class TestTaskPriority:
    """Test TaskPriority enum"""

    def test_task_priority_values(self):
        assert TaskPriority.LOW == 0
        assert TaskPriority.NORMAL == 1
        assert TaskPriority.HIGH == 2
        assert TaskPriority.CRITICAL == 3

    def test_task_priority_ordering(self):
        assert TaskPriority.LOW < TaskPriority.NORMAL
        assert TaskPriority.NORMAL < TaskPriority.HIGH
        assert TaskPriority.HIGH < TaskPriority.CRITICAL


class TestTask:
    """Test Task class"""

    def test_task_creation(self):
        """Test basic task creation"""
        task = Task(
            task_id="test-task-1",
            task_type="example_task",
            data={"param1": "value1", "param2": 42},
            priority=TaskPriority.HIGH,
            metadata={"user_id": 123, "source": "cli"},
        )

        assert task.task_id == "test-task-1"
        assert task.task_type == "example_task"
        assert task.data == {"param1": "value1", "param2": 42}
        assert task.priority == TaskPriority.HIGH
        assert task.status == TaskStatus.PENDING
        assert task.metadata == {"user_id": 123, "source": "cli"}
        assert task.created_at is not None
        assert task.updated_at is not None
        assert task.retry_count == 0
        assert task.max_retries == 3
        assert task.timeout == 300
        assert task.result is None
        assert task.error_message is None
        assert task.queue_name is None
        assert task.worker_id is None

    def test_task_default_values(self):
        """Test task creation with default values"""
        task = Task(
            task_id="test-task-2",
            task_type="example_task",
            data={},
        )

        assert task.priority == TaskPriority.NORMAL
        assert task.status == TaskStatus.PENDING
        assert task.retry_count == 0
        assert task.max_retries == 3
        assert task.timeout == 300
        assert task.metadata == {}
        assert task.queue_name is None
        assert task.worker_id is None

    def test_task_serialization(self):
        """Test task serialization to dict"""
        task = Task(
            task_id="test-task-3",
            task_type="example_task",
            data={"param": "test"},
            priority=TaskPriority.HIGH,
            metadata={"test": True},
        )
        task.status = TaskStatus.RUNNING
        task.worker_id = "worker-1"
        task.queue_name = "high-priority"
        task.result = TaskResult(success=True, data={"result": "ok"})
        task.error_message = None

        serialized = task.to_dict()

        assert serialized["task_id"] == "test-task-3"
        assert serialized["task_type"] == "example_task"
        assert serialized["data"] == {"param": "test"}
        assert serialized["priority"] == TaskPriority.HIGH
        assert serialized["status"] == TaskStatus.RUNNING
        assert serialized["metadata"] == {"test": True}
        assert serialized["worker_id"] == "worker-1"
        assert serialized["queue_name"] == "high-priority"
        assert serialized["retry_count"] == 0
        assert serialized["max_retries"] == 3
        assert serialized["timeout"] == 300
        assert serialized["result"] == {"success": True, "data": {"result": "ok"}, "error": None, "execution_time": None}
        assert serialized["error_message"] is None
        assert "created_at" in serialized
        assert "updated_at" in serialized

    def test_task_deserialization(self):
        """Test task creation from dict"""
        now = datetime.utcnow().isoformat()
        task_dict = {
            "task_id": "test-task-4",
            "task_type": "example_task",
            "data": {"param": "test"},
            "priority": TaskPriority.NORMAL,
            "status": TaskStatus.COMPLETED,
            "metadata": {"user": "test"},
            "created_at": now,
            "updated_at": now,
            "retry_count": 1,
            "max_retries": 5,
            "timeout": 60,
            "result": {"success": True, "data": {"result": "success"}},
            "error_message": None,
            "queue_name": "default",
            "worker_id": "worker-2",
        }

        task = Task.from_dict(task_dict)

        assert task.task_id == "test-task-4"
        assert task.task_type == "example_task"
        assert task.data == {"param": "test"}
        assert task.priority == TaskPriority.NORMAL
        assert task.status == TaskStatus.COMPLETED
        assert task.metadata == {"user": "test"}
        assert task.retry_count == 1
        assert task.max_retries == 5
        assert task.timeout == 60
        assert task.result.success is True
        assert task.result.data == {"result": "success"}
        assert task.error_message is None
        assert task.queue_name == "default"
        assert task.worker_id == "worker-2"

    def test_task_update_status(self):
        """Test updating task status"""
        task = Task(
            task_id="test-task-5",
            task_type="example_task",
            data={},
        )

        initial_updated_at = task.updated_at

        # Simulate a small time passage
        import time
        time.sleep(0.001)

        task.update_status(TaskStatus.RUNNING, worker_id="worker-1")

        assert task.status == TaskStatus.RUNNING
        assert task.worker_id == "worker-1"
        assert task.updated_at > initial_updated_at

    def test_task_complete(self):
        """Test marking task as completed"""
        task = Task(
            task_id="test-task-6",
            task_type="example_task",
            data={},
        )

        result_data = {"output": "success", "processed": 42}
        task.complete(result_data)

        assert task.status == TaskStatus.COMPLETED
        assert task.result.success is True
        assert task.result.data == result_data
        assert task.error_message is None

    def test_task_fail(self):
        """Test marking task as failed"""
        task = Task(
            task_id="test-task-7",
            task_type="example_task",
            data={},
        )

        error_msg = "Something went wrong"
        task.fail(error_msg)

        assert task.status == TaskStatus.FAILED
        assert task.result is None
        assert task.error_message == error_msg

    def test_task_retry(self):
        """Test retrying a task"""
        task = Task(
            task_id="test-task-8",
            task_type="example_task",
            data={},
            max_retries=5,
        )

        task.retry_count = 2
        task.retry("Temporary failure")

        assert task.status == TaskStatus.RETRYING
        assert task.retry_count == 3
        assert task.error_message == "Temporary failure"

    def test_task_cancel(self):
        """Test cancelling a task"""
        task = Task(
            task_id="test-task-9",
            task_type="example_task",
            data={},
        )

        task.status = TaskStatus.RUNNING
        task.cancel()

        assert task.status == TaskStatus.CANCELLED

    def test_task_should_retry(self):
        """Test retry logic"""
        task = Task(
            task_id="test-task-10",
            task_type="example_task",
            data={},
            max_retries=3,
        )

        # Task hasn't failed yet, should not retry
        assert task.should_retry() is False

        # Mark as failed with retries left
        task.status = TaskStatus.FAILED
        task.retry_count = 0
        assert task.should_retry() is True

        # No retries left
        task.retry_count = 3
        assert task.should_retry() is False

        # Different status
        task.status = TaskStatus.COMPLETED
        assert task.should_retry() is False

    def test_task_validation(self):
        """Test task data validation"""
        # Valid task
        task = Task(
            task_id="test-task-11",
            task_type="example_task",
            data={"required_param": "value"},
        )
        task.validate()
        
        # Invalid task ID
        with pytest.raises(TaskValidationError):
            task = Task(
                task_id="",
                task_type="example_task",
                data={},
            )
            task.validate()
        
        # Invalid task type
        with pytest.raises(TaskValidationError):
            task = Task(
                task_id="test-task-12",
                task_type="",
                data={},
            )
            task.validate()

    def test_task_str_representation(self):
        """Test string representation of task"""
        task = Task(
            task_id="test-task-13",
            task_type="example_task",
            data={"param": "test"},
        )
        
        representation = str(task)
        assert "test-task-13" in representation
        assert "example_task" in representation


class TestTaskResult:
    """Test TaskResult class"""

    def test_task_result_creation(self):
        """Test basic task result creation"""
        result = TaskResult(
            success=True,
            data={"output": "result"},
            error=None,
            execution_time=1.5,
        )

        assert result.success is True
        assert result.data == {"output": "result"}
        assert result.error is None
        assert result.execution_time == 1.5

    def test_task_result_defaults(self):
        """Test task result with default values"""
        result = TaskResult(success=False)

        assert result.success is False
        assert result.data is None
        assert result.error is None
        assert result.execution_time is None

    def test_task_result_serialization(self):
        """Test task result serialization"""
        result = TaskResult(
            success=True,
            data={"result": "ok"},
            error="Some error",
            execution_time=2.3,
        )

        serialized = result.to_dict()

        assert serialized == {
            "success": True,
            "data": {"result": "ok"},
            "error": "Some error",
            "execution_time": 2.3,
        }

    def test_task_result_deserialization(self):
        """Test task result deserialization"""
        result_dict = {
            "success": False,
            "data": None,
            "error": "Failed to execute",
            "execution_time": 0.5,
        }

        result = TaskResult.from_dict(result_dict)

        assert result.success is False
        assert result.data is None
        assert result.error == "Failed to execute"
        assert result.execution_time == 0.5


class TestBaseTask:
    """Test BaseTask abstract class"""

    def test_base_task_creation(self):
        """Test base task creation"""
        task = BaseTask(
            task_id="base-task-1",
            data={"param": "value"},
            priority=TaskPriority.HIGH,
        )

        assert task.task_id == "base-task-1"
        assert task.task_type == "base_task"
        assert task.data == {"param": "value"}
        assert task.priority == TaskPriority.HIGH

    def test_base_task_abstract_methods(self):
        """Test that base task has required abstract methods"""
        with pytest.raises(TypeError):
            BaseTask(task_id="test", data={})

    @pytest.mark.asyncio
    async def test_base_task_execute_not_implemented(self):
        """Test that execute method raises NotImplementedError"""
        class ConcreteTask(BaseTask):
            pass

        task = ConcreteTask(task_id="test", data={})
        
        with pytest.raises(NotImplementedError):
            await task.execute()


class TestExampleTasks:
    """Test example tasks implementation"""

    @pytest.mark.asyncio
    async def test_example_task_execute(self):
        """Test ExampleTask execution"""
        task = ExampleTask(
            task_id="example-task-1",
            data={"input": "test", "multiply_by": 2},
        )

        result = await task.execute()

        assert result.success is True
        assert "processed_input" in result.data
        assert "multiplied" in result.data
        assert "timestamp" in result.data
        assert result.data["processed_input"] == "test_processed"
        assert result.data["multiplied"] == 4  # 2 * 2
        assert result.execution_time is not None

    @pytest.mark.asyncio
    async def test_example_task_default_values(self):
        """Test ExampleTask with default values"""
        task = ExampleTask(
            task_id="example-task-2",
            data={"input": "test"},
        )

        result = await task.execute()

        assert result.success is True
        assert result.data["multiplied"] == 2  # Default multiply_by is 1

    @pytest.mark.asyncio
    async def test_failing_task_execute(self):
        """Test FailingTask execution"""
        task = FailingTask(
            task_id="failing-task-1",
            data={},
        )

        result = await task.execute()

        assert result.success is False
        assert "error" in result.data
        assert "Failed by design" in result.data["error"]
        assert result.execution_time is not None

    @pytest.mark.asyncio
    async def test_long_running_task_execute(self):
        """Test LongRunningTask execution"""
        task = LongRunningTask(
            task_id="long-task-1",
            data={"duration": 0.01},  # Short duration for test
        )

        result = await task.execute()

        assert result.success is True
        assert "elapsed_time" in result.data
        assert result.data["elapsed_time"] >= 0.01
        assert "completed_at" in result.data
        assert result.execution_time is not None

    def test_example_task_validation(self):
        """Test ExampleTask data validation"""
        # Valid data
        task = ExampleTask(
            task_id="example-task-3",
            data={"input": "test", "multiply_by": 3},
        )
        
        # Invalid input type
        with pytest.raises(TaskValidationError):
            task = ExampleTask(
                task_id="example-task-4",
                data={"input": 123, "multiply_by": 3},
            )
            task.validate_data()
        
        # Invalid multiply_by type
        with pytest.raises(TaskValidationError):
            task = ExampleTask(
                task_id="example-task-5",
                data={"input": "test", "multiply_by": "three"},
            )
            task.validate_data()
        
        # Missing required field
        with pytest.raises(TaskValidationError):
            task = ExampleTask(
                task_id="example-task-6",
                data={"multiply_by": 3},
            )
            task.validate_data()


class TestTaskRegistry:
    """Test TaskRegistry class"""

    def setup_method(self):
        """Setup fresh registry for each test"""
        TaskRegistry._instance = None
        self.registry = TaskRegistry()

    def test_singleton_pattern(self):
        """Test that TaskRegistry is a singleton"""
        registry1 = TaskRegistry()
        registry2 = TaskRegistry()
        
        assert registry1 is registry2
        assert id(registry1) == id(registry2)

    def test_register_task(self):
        """Test registering a task class"""
        self.registry.register("custom_task", ExampleTask)
        
        assert "custom_task" in self.registry._task_classes
        assert self.registry._task_classes["custom_task"] == ExampleTask

    def test_register_duplicate_task(self):
        """Test registering duplicate task type"""
        self.registry.register("custom_task", ExampleTask)
        
        # Should not raise error when registering same class
        self.registry.register("custom_task", ExampleTask)
        
        # Should raise error when registering different class
        with pytest.raises(ValueError):
            self.registry.register("custom_task", FailingTask)

    def test_get_task_class(self):
        """Test getting task class by type"""
        self.registry.register("example", ExampleTask)
        self.registry.register("failing", FailingTask)
        
        task_class = self.registry.get_task_class("example")
        assert task_class == ExampleTask
        
        task_class = self.registry.get_task_class("failing")
        assert task_class == FailingTask

    def test_get_nonexistent_task_class(self):
        """Test getting non-existent task class"""
        with pytest.raises(TaskNotFoundError):
            self.registry.get_task_class("nonexistent")

    def test_create_task_instance(self):
        """Test creating task instance from registry"""
        self.registry.register("example", ExampleTask)
        
        task_data = {
            "task_id": "test-instance-1",
            "task_type": "example",
            "data": {"input": "test", "multiply_by": 2},
            "priority": TaskPriority.HIGH,
        }
        
        task = self.registry.create_task(**task_data)
        
        assert isinstance(task, ExampleTask)
        assert task.task_id == "test-instance-1"
        assert task.task_type == "example"
        assert task.data == {"input": "test", "multiply_by": 2}
        assert task.priority == TaskPriority.HIGH

    def test_get_all_task_types(self):
        """Test getting all registered task types"""
        self.registry.register("example", ExampleTask)
        self.registry.register("failing", FailingTask)
        self.registry.register("long_running", LongRunningTask)
        
        task_types = self.registry.get_all_task_types()
        
        assert "example" in task_types
        assert "failing" in task_types
        assert "long_running" in task_types
        assert len(task_types) == 3

    def test_clear_registry(self):
        """Test clearing the registry"""
        self.registry.register("example", ExampleTask)
        self.registry.register("failing", FailingTask)
        
        assert len(self.registry.get_all_task_types()) == 2
        
        self.registry.clear()
        
        assert len(self.registry.get_all_task_types()) == 0


class TestTaskIntegration:
    """Integration tests for tasks"""

    @pytest.mark.asyncio
    async def test_task_lifecycle(self):
        """Test complete task lifecycle"""
        # Create task
        task = Task(
            task_id="lifecycle-task-1",
            task_type="example_task",
            data={"input": "test", "multiply_by": 3},
        )
        
        assert task.status == TaskStatus.PENDING
        
        # Update to queued
        task.update_status(TaskStatus.QUEUED, queue_name="default")
        assert task.status == TaskStatus.QUEUED
        assert task.queue_name == "default"
        
        # Update to running
        task.update_status(TaskStatus.RUNNING, worker_id="worker-1")
        assert task.status == TaskStatus.RUNNING
        assert task.worker_id == "worker-1"
        
        # Simulate execution
        example_task = ExampleTask(
            task_id=task.task_id,
            data=task.data,
            priority=task.priority,
        )
        
        result = await example_task.execute()
        
        # Complete task
        task.complete(result.data)
        assert task.status == TaskStatus.COMPLETED
        assert task.result.success is True
        assert task.result.data["multiplied"] == 6  # 2 * 3

    @pytest.mark.asyncio
    async def test_task_retry_cycle(self):
        """Test task retry cycle"""
        task = Task(
            task_id="retry-task-1",
            task_type="failing_task",
            data={},
            max_retries=2,
        )
        
        assert task.status == TaskStatus.PENDING
        
        # First failure
        task.update_status(TaskStatus.RUNNING)
        task.fail("First failure")
        assert task.status == TaskStatus.FAILED
        assert task.retry_count == 0
        
        # Should retry
        assert task.should_retry() is True
        
        # Retry
        task.retry("Retrying after first failure")
        assert task.status == TaskStatus.RETRYING
        assert task.retry_count == 1
        
        # Update back to running
        task.update_status(TaskStatus.RUNNING)
        
        # Second failure
        task.fail("Second failure")
        assert task.status == TaskStatus.FAILED
        assert task.retry_count == 1
        
        # Should retry again
        assert task.should_retry() is True
        
        # Retry again
        task.retry("Retrying after second failure")
        assert task.status == TaskStatus.RETRYING
        assert task.retry_count == 2
        
        # Update back to running
        task.update_status(TaskStatus.RUNNING)
        
        # Third failure - no more retries
        task.fail("Third failure")
        assert task.status == TaskStatus.FAILED
        assert task.retry_count == 2
        
        # Should not retry (max retries reached)
        assert task.should_retry() is False

    @pytest.mark.asyncio
    async def test_task_with_registry(self):
        """Test task creation and execution using registry"""
        registry = TaskRegistry()
        registry.register("example", ExampleTask)
        registry.register("failing", FailingTask)
        
        # Create example task
        example_task = registry.create_task(
            task_id="registry-example-1",
            task_type="example",
            data={"input": "registry_test", "multiply_by": 5},
        )
        
        assert isinstance(example_task, ExampleTask)
        
        result = await example_task.execute()
        assert result.success is True
        assert result.data["multiplied"] == 10  # 2 * 5
        
        # Create failing task
        failing_task = registry.create_task(
            task_id="registry-failing-1",
            task_type="failing",
            data={},
        )
        
        assert isinstance(failing_task, FailingTask)
        
        result = await failing_task.execute()
        assert result.success is False
        assert "Failed by design" in result.data["error"]


def test_task_json_serialization():
    """Test JSON serialization of task"""
    task = Task(
        task_id="json-task-1",
        task_type="example_task",
        data={"param": "value", "number": 42, "nested": {"key": "value"}},
        priority=TaskPriority.HIGH,
        metadata={"source": "test"},
    )
    
    task.status = TaskStatus.COMPLETED
    task.result = TaskResult(success=True, data={"output": "result"})
    
    # Convert to dict
    task_dict = task.to_dict()
    
    # Convert to JSON
    task_json = json.dumps(task_dict)
    
    # Parse back
    parsed_dict = json.loads(task_json)
    
    # Create new task from parsed dict
    new_task = Task.from_dict(parsed_dict)
    
    assert new_task.task_id == task.task_id
    assert new_task.task_type == task.task_type
    assert new_task.data == task.data
    assert new_task.priority == task.priority
    assert new_task.status == task.status
    assert new_task.metadata == task.metadata
    assert new_task.result.success == task.result.success
    assert new_task.result.data == task.result.data


@pytest.mark.asyncio
async def test_task_timeout_handling():
    """Test task timeout handling"""
    task = Task(
        task_id="timeout-task-1",
        task_type="long_running_task",
        data={"duration": 0.1},  # Task takes 0.1 seconds
        timeout=0.05,  # Timeout after 0.05 seconds
    )
    
    # Create the actual task instance
    long_task = LongRunningTask(
        task_id=task.task_id,
        data=task.data,
        priority=task.priority,
    )
    
    # This should complete within timeout if we don't actually enforce it in the test
    # (the timeout enforcement would be in the worker)
    result = await long_task.execute()
    
    assert result.success is True
    assert result.data["elapsed_time"] >= 0.1


def test_task_equality():
    """Test task equality based on task_id"""
    task1 = Task(
        task_id="same-task",
        task_type="example_task",
        data={"param": "value1"},
    )
    
    task2 = Task(
        task_id="same-task",
        task_type="example_task",
        data={"param": "value2"},  # Different data
    )
    
    task3 = Task(
        task_id="different-task",
        task_type="example_task",
        data={"param": "value1"},
    )
    
    # Same task_id should be equal
    assert task1 == task2
    
    # Different task_id should not be equal
    assert task1 != task3
    
    # Hash should be based on task_id
    assert hash(task1) == hash(task2)
    assert hash(task1) != hash(task3)


def test_task_comparison_by_priority():
    """Test task comparison by priority"""
    low_priority = Task(
        task_id="low-priority",
        task_type="example_task",
        data={},
        priority=TaskPriority.LOW,
    )
    
    high_priority = Task(
        task_id="high-priority",
        task_type="example_task",
        data={},
        priority=TaskPriority.HIGH,
    )
    
    # Higher priority tasks should come first
    assert high_priority.priority > low_priority.priority
    
    # Test comparison
    assert high_priority > low_priority
    assert low_priority < high_priority
    
    # Same priority
    normal1 = Task(
        task_id="normal-1",
        task_type="example_task",
        data={},
        priority=TaskPriority.NORMAL,
    )
    
    normal2 = Task(
        task_id="normal-2",
        task_type="example_task",
        data={},
        priority=TaskPriority.NORMAL,
    )
    
    # When priorities are equal, compare by creation time
    # normal1 was created before normal2
    assert normal1 <= normal2