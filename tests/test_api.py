import pytest
import asyncio
import json
from httpx import AsyncClient
from unittest.mock import AsyncMock, patch, MagicMock

from core.queue_manager import QueueManager
from core.task import Task
from core.result_store import ResultStore
from workers.worker_pool import WorkerPool
from api.app import app


@pytest.fixture
async def async_client():
    """Fixture pour le client HTTP asynchrone."""
    async with AsyncClient(app=app, base_url="http://test") as ac:
        yield ac


@pytest.fixture
def mock_queue_manager():
    """Fixture pour un mock de QueueManager."""
    with patch("api.routes.tasks.queue_manager", new_callable=AsyncMock) as mock:
        yield mock


@pytest.fixture
def mock_worker_pool():
    """Fixture pour un mock de WorkerPool."""
    with patch("api.routes.monitoring.worker_pool", new_callable=AsyncMock) as mock:
        yield mock


@pytest.fixture
def mock_result_store():
    """Fixture pour un mock de ResultStore."""
    with patch("api.routes.tasks.result_store", new_callable=MagicMock) as mock:
        yield mock


@pytest.fixture
def sample_task_data():
    """Données de test pour une tâche."""
    return {
        "task_id": "test_task_123",
        "task_type": "example_task",
        "priority": 5,
        "queue_type": "fifo",
        "parameters": {"arg1": "value1", "arg2": 42}
    }


class TestTaskRoutes:
    """Tests pour les routes de tâches."""

    async def test_submit_task_success(self, async_client, mock_queue_manager, sample_task_data):
        """Test de soumission réussie d'une tâche."""
        mock_queue_manager.submit_task.return_value = "test_task_123"
        
        response = await async_client.post("/api/tasks/submit", json=sample_task_data)
        
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["task_id"] == "test_task_123"
        mock_queue_manager.submit_task.assert_called_once()

    async def test_submit_task_invalid_data(self, async_client, mock_queue_manager):
        """Test de soumission avec des données invalides."""
        invalid_data = {"task_type": "invalid_task"}
        
        response = await async_client.post("/api/tasks/submit", json=invalid_data)
        
        assert response.status_code == 422

    async def test_get_task_status(self, async_client, mock_result_store):
        """Test de récupération du statut d'une tâche."""
        mock_result_store.get_result.return_value = {
            "status": "completed",
            "result": {"output": "test_result"},
            "error": None
        }
        
        response = await async_client.get("/api/tasks/status/test_task_123")
        
        assert response.status_code == 200
        data = response.json()
        assert data["task_id"] == "test_task_123"
        assert data["status"] == "completed"
        mock_result_store.get_result.assert_called_once_with("test_task_123")

    async def test_get_task_status_not_found(self, async_client, mock_result_store):
        """Test de récupération du statut d'une tâche inexistante."""
        mock_result_store.get_result.return_value = None
        
        response = await async_client.get("/api/tasks/status/nonexistent_task")
        
        assert response.status_code == 404
        data = response.json()
        assert data["detail"] == "Task not found"

    async def test_cancel_task_success(self, async_client, mock_queue_manager):
        """Test d'annulation réussie d'une tâche."""
        mock_queue_manager.cancel_task.return_value = True
        
        response = await async_client.post("/api/tasks/test_task_123/cancel")
        
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        mock_queue_manager.cancel_task.assert_called_once_with("test_task_123")

    async def test_cancel_task_not_found(self, async_client, mock_queue_manager):
        """Test d'annulation d'une tâche inexistante."""
        mock_queue_manager.cancel_task.return_value = False
        
        response = await async_client.post("/api/tasks/nonexistent_task/cancel")
        
        assert response.status_code == 404
        data = response.json()
        assert data["detail"] == "Task not found or already completed"

    async def test_list_tasks(self, async_client, mock_result_store):
        """Test de liste des tâches avec filtres."""
        mock_result_store.list_results.return_value = [
            {"task_id": "task1", "status": "completed"},
            {"task_id": "task2", "status": "pending"}
        ]
        
        response = await async_client.get("/api/tasks/list?status=completed")
        
        assert response.status_code == 200
        data = response.json()
        assert len(data["tasks"]) == 2
        mock_result_store.list_results.assert_called_once()


class TestQueueRoutes:
    """Tests pour les routes de file d'attente."""

    async def test_get_queue_stats(self, async_client, mock_queue_manager):
        """Test de récupération des statistiques de la file."""
        mock_queue_manager.get_queue_stats.return_value = {
            "fifo": {"pending": 5, "processing": 2},
            "priority": {"pending": 3, "processing": 1}
        }
        
        response = await async_client.get("/api/queue/stats")
        
        assert response.status_code == 200
        data = response.json()
        assert "fifo" in data
        assert "priority" in data

    async def test_purge_queue(self, async_client, mock_queue_manager):
        """Test de purge d'une file d'attente."""
        mock_queue_manager.purge_queue.return_value = 10
        
        response = await async_client.post("/api/queue/purge", json={"queue_type": "fifo"})
        
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["purged_count"] == 10

    async def test_purge_queue_invalid_type(self, async_client, mock_queue_manager):
        """Test de purge avec type de file invalide."""
        response = await async_client.post("/api/queue/purge", json={"queue_type": "invalid"})
        
        assert response.status_code == 422

    async def test_get_queue_contents(self, async_client, mock_queue_manager):
        """Test de récupération du contenu d'une file."""
        mock_queue_manager.get_queue_contents.return_value = [
            {"task_id": "task1", "task_type": "example_task"},
            {"task_id": "task2", "task_type": "example_task"}
        ]
        
        response = await async_client.get("/api/queue/contents/fifo?limit=10")
        
        assert response.status_code == 200
        data = response.json()
        assert len(data["tasks"]) == 2
        mock_queue_manager.get_queue_contents.assert_called_once_with("fifo", 10)


class TestMonitoringRoutes:
    """Tests pour les routes de monitoring."""

    async def test_health_check(self, async_client, mock_queue_manager, mock_worker_pool):
        """Test du endpoint de santé."""
        mock_queue_manager.health_check.return_value = {"status": "healthy"}
        mock_worker_pool.get_worker_stats.return_value = {"active_workers": 3, "total_workers": 5}
        
        response = await async_client.get("/api/monitoring/health")
        
        assert response.status_code == 200
        data = response.json()
        assert data["queue_manager"] == {"status": "healthy"}
        assert "worker_pool" in data

    async def test_get_system_stats(self, async_client, mock_queue_manager, mock_worker_pool):
        """Test de récupération des statistiques système."""
        mock_queue_manager.get_system_stats.return_value = {
            "total_tasks_processed": 100,
            "average_processing_time": 2.5
        }
        mock_worker_pool.get_worker_stats.return_value = {
            "active_workers": 3,
            "idle_workers": 2
        }
        
        response = await async_client.get("/api/monitoring/stats")
        
        assert response.status_code == 200
        data = response.json()
        assert "queue_manager" in data
        assert "worker_pool" in data

    async def test_get_worker_details(self, async_client, mock_worker_pool):
        """Test de récupération des détails des workers."""
        mock_worker_pool.get_worker_details.return_value = [
            {"worker_id": 1, "status": "active", "current_task": "task_123"},
            {"worker_id": 2, "status": "idle", "current_task": None}
        ]
        
        response = await async_client.get("/api/monitoring/workers")
        
        assert response.status_code == 200
        data = response.json()
        assert len(data["workers"]) == 2
        assert data["workers"][0]["worker_id"] == 1


class TestWebRoutes:
    """Tests pour les routes web (HTML)."""

    async def test_dashboard(self, async_client, mock_queue_manager, mock_worker_pool):
        """Test de la page dashboard."""
        mock_queue_manager.get_queue_stats.return_value = {
            "fifo": {"pending": 5, "processing": 2}
        }
        mock_worker_pool.get_worker_stats.return_value = {
            "active_workers": 3, "total_workers": 5
        }
        
        response = await async_client.get("/dashboard")
        
        assert response.status_code == 200
        assert "text/html" in response.headers["content-type"]

    async def test_task_detail(self, async_client, mock_result_store):
        """Test de la page de détail d'une tâche."""
        mock_result_store.get_result.return_value = {
            "task_id": "test_task_123",
            "status": "completed",
            "result": {"output": "test"},
            "created_at": "2023-01-01T00:00:00"
        }
        
        response = await async_client.get("/tasks/test_task_123")
        
        assert response.status_code == 200
        assert "text/html" in response.headers["content-type"]

    async def test_queue_status(self, async_client, mock_queue_manager):
        """Test de la page de statut des files."""
        mock_queue_manager.get_queue_contents.return_value = [
            {"task_id": "task1", "task_type": "example_task"}
        ]
        
        response = await async_client.get("/queues/fifo")
        
        assert response.status_code == 200
        assert "text/html" in response.headers["content-type"]