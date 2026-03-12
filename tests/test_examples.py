import asyncio
import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from tasks.example_tasks import (
    add_task,
    multiply_task,
    failing_task,
    long_running_task,
    exception_task,
    process_data_task,
    generate_report_task
)
from tasks.registry import TASK_REGISTRY
from core.exceptions import TaskExecutionError


class TestExampleTasks:
    """Tests for example tasks"""

    @pytest.mark.asyncio
    async def test_add_task_success(self):
        """Test add_task returns correct sum"""
        result = await add_task(5, 3)
        assert result == 8
        result = await add_task(-2, 2)
        assert result == 0
        result = await add_task(0, 0)
        assert result == 0

    @pytest.mark.asyncio
    async def test_add_task_with_floats(self):
        """Test add_task with float numbers"""
        result = await add_task(2.5, 3.5)
        assert result == 6.0
        result = await add_task(-1.5, 1.5)
        assert result == 0.0

    @pytest.mark.asyncio
    async def test_multiply_task_success(self):
        """Test multiply_task returns correct product"""
        result = await multiply_task(4, 3)
        assert result == 12
        result = await multiply_task(-2, 5)
        assert result == -10
        result = await multiply_task(0, 100)
        assert result == 0

    @pytest.mark.asyncio
    async def test_multiply_task_with_floats(self):
        """Test multiply_task with float numbers"""
        result = await multiply_task(2.5, 4.0)
        assert result == 10.0
        result = await multiply_task(-1.5, 2.0)
        assert result == -3.0

    @pytest.mark.asyncio
    async def test_failing_task_raises_exception(self):
        """Test failing_task raises TaskExecutionError"""
        with pytest.raises(TaskExecutionError) as exc_info:
            await failing_task("test reason")
        assert "Simulated failure" in str(exc_info.value)
        assert "test reason" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_failing_task_different_reasons(self):
        """Test failing_task with different failure reasons"""
        with pytest.raises(TaskExecutionError) as exc_info:
            await failing_task("network error")
        assert "network error" in str(exc_info.value)

        with pytest.raises(TaskExecutionError) as exc_info:
            await failing_task("database timeout")
        assert "database timeout" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_long_running_task_completes(self):
        """Test long_running_task completes successfully"""
        result = await long_running_task("test_data", delay=0.001)
        assert result == "Processed: test_data"

    @pytest.mark.asyncio
    async def test_long_running_task_with_delay(self):
        """Test long_running_task respects delay parameter"""
        start_time = asyncio.get_event_loop().time()
        result = await long_running_task("test", delay=0.01)
        end_time = asyncio.get_event_loop().time()
        
        elapsed = end_time - start_time
        assert elapsed >= 0.01
        assert result == "Processed: test"

    @pytest.mark.asyncio
    async def test_exception_task_raises_value_error(self):
        """Test exception_task raises ValueError"""
        with pytest.raises(ValueError) as exc_info:
            await exception_task("invalid")
        assert "Invalid input" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_exception_task_with_valid_input(self):
        """Test exception_task with valid input"""
        result = await exception_task("valid")
        assert result == "Processed: valid"

    @pytest.mark.asyncio
    async def test_process_data_task_success(self):
        """Test process_data_task processes data correctly"""
        test_data = [1, 2, 3, 4, 5]
        result = await process_data_task(test_data)
        
        assert "sum" in result
        assert "average" in result
        assert "count" in result
        assert result["sum"] == 15
        assert result["average"] == 3.0
        assert result["count"] == 5

    @pytest.mark.asyncio
    async def test_process_data_task_empty_list(self):
        """Test process_data_task with empty list"""
        result = await process_data_task([])
        assert result["sum"] == 0
        assert result["average"] == 0.0
        assert result["count"] == 0

    @pytest.mark.asyncio
    async def test_process_data_task_with_negative_numbers(self):
        """Test process_data_task with negative numbers"""
        test_data = [-1, -2, -3, -4, -5]
        result = await process_data_task(test_data)
        assert result["sum"] == -15
        assert result["average"] == -3.0
        assert result["count"] == 5

    @pytest.mark.asyncio
    async def test_generate_report_task_success(self):
        """Test generate_report_task generates correct report"""
        data = {"metric1": 100, "metric2": 200}
        result = await generate_report_task(data)
        
        assert "report_id" in result
        assert "timestamp" in result
        assert "data" in result
        assert "summary" in result
        assert result["data"] == data
        assert isinstance(result["report_id"], str)
        assert len(result["report_id"]) > 0
        assert "Total metrics: 2" in result["summary"]

    @pytest.mark.asyncio
    async def test_generate_report_task_empty_data(self):
        """Test generate_report_task with empty data"""
        result = await generate_report_task({})
        assert result["data"] == {}
        assert "Total metrics: 0" in result["summary"]

    @pytest.mark.asyncio
    async def test_tasks_registered_in_registry(self):
        """Test that example tasks are registered in the task registry"""
        assert "add_task" in TASK_REGISTRY
        assert "multiply_task" in TASK_REGISTRY
        assert "failing_task" in TASK_REGISTRY
        assert "long_running_task" in TASK_REGISTRY
        assert "exception_task" in TASK_REGISTRY
        assert "process_data_task" in TASK_REGISTRY
        assert "generate_report_task" in TASK_REGISTRY
        
        # Verify the functions match
        assert TASK_REGISTRY["add_task"] == add_task
        assert TASK_REGISTRY["multiply_task"] == multiply_task

    @pytest.mark.asyncio
    async def test_task_concurrent_execution(self):
        """Test that tasks can be executed concurrently"""
        tasks = [
            add_task(1, 2),
            multiply_task(3, 4),
            process_data_task([1, 2, 3])
        ]
        
        results = await asyncio.gather(*tasks)
        
        assert results[0] == 3
        assert results[1] == 12
        assert results[2]["sum"] == 6

    @pytest.mark.asyncio
    async def test_task_with_invalid_arguments(self):
        """Test tasks with invalid arguments raise appropriate errors"""
        # add_task with non-numeric arguments should raise TypeError
        with pytest.raises(TypeError):
            await add_task("not", "numbers")

    @pytest.mark.asyncio
    async def test_task_timeout_handling(self):
        """Test that long running task can be cancelled"""
        task = asyncio.create_task(long_running_task("test", delay=1.0))
        
        # Cancel after short delay
        await asyncio.sleep(0.001)
        task.cancel()
        
        # Should raise CancelledError
        with pytest.raises(asyncio.CancelledError):
            await task

    @pytest.mark.parametrize("a,b,expected", [
        (1, 2, 3),
        (0, 0, 0),
        (-1, 1, 0),
        (100, 200, 300),
    ])
    @pytest.mark.asyncio
    async def test_add_task_parameterized(self, a, b, expected):
        """Parameterized test for add_task"""
        result = await add_task(a, b)
        assert result == expected

    @pytest.mark.parametrize("data,expected_sum", [
        ([1, 2, 3], 6),
        ([], 0),
        ([-1, 0, 1], 0),
        ([10, 20, 30, 40], 100),
    ])
    @pytest.mark.asyncio
    async def test_process_data_task_parameterized(self, data, expected_sum):
        """Parameterized test for process_data_task"""
        result = await process_data_task(data)
        assert result["sum"] == expected_sum
        assert result["count"] == len(data)

    def test_task_function_signatures(self):
        """Test that tasks have correct function signatures"""
        import inspect
        
        # Check add_task signature
        sig = inspect.signature(add_task)
        params = list(sig.parameters.keys())
        assert params == ['a', 'b']
        
        # Check process_data_task signature
        sig = inspect.signature(process_data_task)
        params = list(sig.parameters.keys())
        assert params == ['data']

    @pytest.mark.asyncio
    async def test_task_serialization_compatibility(self):
        """Test that task results can be serialized"""
        import json
        
        # Test with add_task
        result = await add_task(5, 3)
        json_str = json.dumps(result)
        assert json_str == "8"
        
        # Test with process_data_task
        result = await process_data_task([1, 2, 3])
        json_str = json.dumps(result)
        parsed = json.loads(json_str)
        assert parsed["sum"] == 6
        assert parsed["count"] == 3
        
        # Test with generate_report_task
        result = await generate_report_task({"test": 123})
        json_str = json.dumps(result)
        parsed = json.loads(json_str)
        assert "report_id" in parsed
        assert "data" in parsed