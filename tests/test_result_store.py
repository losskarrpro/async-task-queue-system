import asyncio
import pytest
import time
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, timedelta

from core.result_store import InMemoryResultStore, RedisResultStore
from core.exceptions import ResultNotFoundError, ResultExpiredError
from core.task import TaskResult


class TestInMemoryResultStore:
    """Tests for InMemoryResultStore"""
    
    @pytest.fixture
    def store(self):
        return InMemoryResultStore()
    
    @pytest.fixture
    def sample_result(self):
        return TaskResult(
            task_id="test_task_123",
            status="completed",
            result={"data": "test"},
            error=None,
            created_at=datetime.now(),
            completed_at=datetime.now(),
            execution_time=1.5
        )
    
    @pytest.mark.asyncio
    async def test_store_and_get_result(self, store, sample_result):
        """Test storing and retrieving a result"""
        await store.store_result(sample_result)
        
        retrieved = await store.get_result("test_task_123")
        
        assert retrieved.task_id == sample_result.task_id
        assert retrieved.status == sample_result.status
        assert retrieved.result == sample_result.result
    
    @pytest.mark.asyncio
    async def test_get_nonexistent_result(self, store):
        """Test retrieving a non-existent result raises exception"""
        with pytest.raises(ResultNotFoundError):
            await store.get_result("nonexistent")
    
    @pytest.mark.asyncio
    async def test_delete_result(self, store, sample_result):
        """Test deleting a result"""
        await store.store_result(sample_result)
        
        await store.delete_result("test_task_123")
        
        with pytest.raises(ResultNotFoundError):
            await store.get_result("test_task_123")
    
    @pytest.mark.asyncio
    async def test_delete_nonexistent_result(self, store):
        """Test deleting non-existent result does nothing"""
        await store.delete_result("nonexistent")  # Should not raise
    
    @pytest.mark.asyncio
    async def test_cleanup_expired_results(self, store):
        """Test cleanup of expired results"""
        # Create results with different expiration times
        result1 = TaskResult(
            task_id="expired_task",
            status="completed",
            result={},
            created_at=datetime.now() - timedelta(hours=25),
            completed_at=datetime.now() - timedelta(hours=24),
            execution_time=1.0
        )
        
        result2 = TaskResult(
            task_id="valid_task",
            status="completed",
            result={},
            created_at=datetime.now(),
            completed_at=datetime.now(),
            execution_time=1.0
        )
        
        await store.store_result(result1, ttl=3600)  # 1 hour TTL
        await store.store_result(result2, ttl=86400)  # 24 hour TTL
        
        # Fast-forward time by 2 hours
        with patch('time.time', return_value=time.time() + 7200):
            await store.cleanup_expired()
        
        # Expired result should be gone
        with pytest.raises(ResultNotFoundError):
            await store.get_result("expired_task")
        
        # Valid result should still exist
        retrieved = await store.get_result("valid_task")
        assert retrieved.task_id == "valid_task"
    
    @pytest.mark.asyncio
    async def test_exists_result(self, store, sample_result):
        """Test checking if result exists"""
        await store.store_result(sample_result)
        
        assert await store.exists("test_task_123") is True
        assert await store.exists("nonexistent") is False
    
    @pytest.mark.asyncio
    async def test_get_all_results(self, store):
        """Test retrieving all results"""
        results = []
        for i in range(3):
            result = TaskResult(
                task_id=f"task_{i}",
                status="completed",
                result={"index": i},
                created_at=datetime.now(),
                completed_at=datetime.now(),
                execution_time=i
            )
            results.append(result)
            await store.store_result(result)
        
        all_results = await store.get_all_results()
        
        assert len(all_results) == 3
        task_ids = {r.task_id for r in all_results}
        assert task_ids == {"task_0", "task_1", "task_2"}
    
    @pytest.mark.asyncio
    async def test_clear_all_results(self, store, sample_result):
        """Test clearing all results"""
        await store.store_result(sample_result)
        
        assert await store.exists("test_task_123") is True
        
        await store.clear_all()
        
        assert await store.exists("test_task_123") is False
        assert len(await store.get_all_results()) == 0


class TestRedisResultStore:
    """Tests for RedisResultStore with mocked Redis"""
    
    @pytest.fixture
    def mock_redis(self):
        mock_redis = AsyncMock()
        mock_redis.set = AsyncMock()
        mock_redis.get = AsyncMock()
        mock_redis.delete = AsyncMock()
        mock_redis.exists = AsyncMock()
        mock_redis.keys = AsyncMock()
        mock_redis.flushdb = AsyncMock()
        mock_redis.expire = AsyncMock()
        return mock_redis
    
    @pytest.fixture
    def store(self, mock_redis):
        with patch('core.result_store.aioredis.Redis', return_value=mock_redis):
            store = RedisResultStore(redis_url="redis://localhost:6379")
            store.redis = mock_redis
            return store
    
    @pytest.fixture
    def sample_result(self):
        return TaskResult(
            task_id="test_task_123",
            status="completed",
            result={"data": "test"},
            error=None,
            created_at=datetime.now(),
            completed_at=datetime.now(),
            execution_time=1.5
        )
    
    @pytest.mark.asyncio
    async def test_store_result(self, store, mock_redis, sample_result):
        """Test storing result in Redis"""
        await store.store_result(sample_result, ttl=3600)
        
        # Verify Redis.set was called with serialized result
        mock_redis.set.assert_called_once()
        args, kwargs = mock_redis.set.call_args
        
        # Check that key and value were passed
        assert "result:test_task_123" in args or "result:test_task_123" == args[0]
        assert "expire" in kwargs or "ex" in kwargs
        if "ex" in kwargs:
            assert kwargs["ex"] == 3600
    
    @pytest.mark.asyncio
    async def test_get_result_success(self, store, mock_redis, sample_result):
        """Test successfully retrieving result from Redis"""
        # Mock Redis.get to return serialized result
        serialized = store.serialize_result(sample_result)
        mock_redis.get.return_value = serialized
        
        retrieved = await store.get_result("test_task_123")
        
        mock_redis.get.assert_called_once_with("result:test_task_123")
        assert retrieved.task_id == sample_result.task_id
        assert retrieved.status == sample_result.status
    
    @pytest.mark.asyncio
    async def test_get_result_not_found(self, store, mock_redis):
        """Test retrieving non-existent result from Redis"""
        mock_redis.get.return_value = None
        
        with pytest.raises(ResultNotFoundError):
            await store.get_result("nonexistent")
    
    @pytest.mark.asyncio
    async def test_get_result_invalid_data(self, store, mock_redis):
        """Test retrieving invalid/corrupted result data"""
        mock_redis.get.return_value = b"invalid json data"
        
        with pytest.raises(ValueError):
            await store.get_result("corrupted_task")
    
    @pytest.mark.asyncio
    async def test_delete_result(self, store, mock_redis):
        """Test deleting result from Redis"""
        mock_redis.delete.return_value = 1
        
        await store.delete_result("test_task_123")
        
        mock_redis.delete.assert_called_once_with("result:test_task_123")
    
    @pytest.mark.asyncio
    async def test_exists_result(self, store, mock_redis):
        """Test checking if result exists in Redis"""
        mock_redis.exists.return_value = 1
        
        exists = await store.exists("test_task_123")
        
        mock_redis.exists.assert_called_once_with("result:test_task_123")
        assert exists is True
    
    @pytest.mark.asyncio
    async def test_cleanup_expired(self, store, mock_redis):
        """Test cleanup of expired results (Redis handles TTL automatically)"""
        await store.cleanup_expired()
        
        # Redis handles TTL automatically, so cleanup might just be a no-op
        # or could scan for expired keys. We'll verify the method doesn't raise
        assert True
    
    @pytest.mark.asyncio
    async def test_get_all_results(self, store, mock_redis, sample_result):
        """Test retrieving all results from Redis"""
        # Mock keys and get operations
        mock_redis.keys.return_value = [b"result:task_1", b"result:task_2"]
        
        serialized = store.serialize_result(sample_result)
        mock_redis.get.side_effect = [serialized, serialized]
        
        results = await store.get_all_results()
        
        assert len(results) == 2
        mock_redis.keys.assert_called_once_with("result:*")
        assert mock_redis.get.call_count == 2
    
    @pytest.mark.asyncio
    async def test_clear_all_results(self, store, mock_redis):
        """Test clearing all results from Redis"""
        await store.clear_all()
        
        mock_redis.flushdb.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_connection_error_handling(self):
        """Test handling of Redis connection errors"""
        with patch('core.result_store.aioredis.Redis', side_effect=ConnectionError):
            store = RedisResultStore(redis_url="redis://localhost:6379")
            
            # Should handle connection errors gracefully
            with pytest.raises(ConnectionError):
                await store.get_result("test_task")


@pytest.mark.integration
class TestResultStoreIntegration:
    """Integration tests for ResultStore with actual Redis"""
    
    @pytest.fixture
    async def redis_store(self):
        """Create RedisResultStore with test Redis instance"""
        store = RedisResultStore(redis_url="redis://localhost:7501")
        await store.initialize()
        yield store
        await store.clear_all()
        await store.close()
    
    @pytest.mark.asyncio
    @pytest.mark.skipif(True, reason="Requires Redis server running")
    async def test_redis_integration(self, redis_store):
        """Integration test with actual Redis server"""
        result = TaskResult(
            task_id="integration_test",
            status="completed",
            result={"integration": "test"},
            created_at=datetime.now(),
            completed_at=datetime.now(),
            execution_time=2.5
        )
        
        # Store result
        await redis_store.store_result(result, ttl=10)
        
        # Verify it exists
        assert await redis_store.exists("integration_test") is True
        
        # Retrieve and verify
        retrieved = await redis_store.get_result("integration_test")
        assert retrieved.task_id == "integration_test"
        assert retrieved.result == {"integration": "test"}
        
        # Delete
        await redis_store.delete_result("integration_test")
        assert await redis_store.exists("integration_test") is False


class TestResultSerialization:
    """Tests for result serialization/deserialization"""
    
    def test_serialize_deserialize_taskresult(self):
        """Test serialization and deserialization of TaskResult"""
        from core.result_store import InMemoryResultStore
        
        store = InMemoryResultStore()
        
        original = TaskResult(
            task_id="serialize_test",
            status="failed",
            result=None,
            error="Test error",
            created_at=datetime(2024, 1, 1, 12, 0, 0),
            completed_at=datetime(2024, 1, 1, 12, 1, 0),
            execution_time=60.0
        )
        
        # Serialize
        serialized = store.serialize_result(original)
        
        # Should be JSON string
        assert isinstance(serialized, str)
        
        # Deserialize
        deserialized = store.deserialize_result(serialized)
        
        assert deserialized.task_id == original.task_id
        assert deserialized.status == original.status
        assert deserialized.error == original.error
        assert deserialized.execution_time == original.execution_time
    
    def test_serialize_invalid_object(self):
        """Test serialization of invalid object"""
        from core.result_store import InMemoryResultStore
        
        store = InMemoryResultStore()
        
        # Should raise TypeError for non-serializable object
        with pytest.raises(TypeError):
            store.serialize_result({"complex": object()})