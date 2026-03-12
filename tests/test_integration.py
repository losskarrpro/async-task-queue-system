import asyncio
import json
import time
from unittest.mock import patch, MagicMock

import pytest
from fastapi.testclient import TestClient
import redis

from core.queue_manager import QueueManager
from core.task import Task, TaskPriority, TaskStatus
from core.result_store import ResultStore, RedisResultStore
from workers.async_worker import AsyncWorker
from workers.worker_pool import WorkerPool
from api.app import create_app
from cli.main import app as cli_app
from tasks.example_tasks import add_task, multiply_task, slow_task
from config.settings import settings
from utils.logger import setup_logger


class TestIntegrationBasic:
    """Tests d'intégration basiques du système de file d'attente"""

    @pytest.fixture
    def queue_manager(self):
        """Fixture pour créer un QueueManager"""
        manager = QueueManager()
        yield manager
        manager.clear_all_queues()

    @pytest.fixture
    def result_store(self):
        """Fixture pour créer un ResultStore en mémoire"""
        return ResultStore()

    @pytest.fixture
    def worker_pool(self, queue_manager, result_store):
        """Fixture pour créer un WorkerPool"""
        pool = WorkerPool(
            queue_manager=queue_manager,
            result_store=result_store,
            num_workers=2
        )
        yield pool
        pool.stop()

    def test_end_to_end_task_execution(self, queue_manager, result_store, worker_pool):
        """Test d'exécution complète d'une tâche de bout en bout"""
        # Créer une tâche
        task_id = "test-task-1"
        task = Task(
            id=task_id,
            func_name="add_task",
            args=(5, 3),
            kwargs={},
            priority=TaskPriority.NORMAL
        )

        # Soumettre la tâche
        queue_manager.submit_task("default", task)
        assert queue_manager.get_queue_size("default") == 1

        # Démarrer les workers
        worker_pool.start()

        # Attendre l'exécution
        time.sleep(0.5)

        # Vérifier le résultat
        result = result_store.get_result(task_id)
        assert result is not None
        assert result["status"] == TaskStatus.COMPLETED
        assert result["result"] == 8  # 5 + 3
        assert queue_manager.get_queue_size("default") == 0

    def test_multiple_tasks_execution(self, queue_manager, result_store, worker_pool):
        """Test d'exécution de plusieurs tâches"""
        tasks = []
        for i in range(5):
            task_id = f"test-task-{i}"
            task = Task(
                id=task_id,
                func_name="add_task",
                args=(i, i * 2),
                kwargs={},
                priority=TaskPriority.NORMAL
            )
            tasks.append(task)
            queue_manager.submit_task("default", task)

        assert queue_manager.get_queue_size("default") == 5

        worker_pool.start()
        time.sleep(1)

        # Vérifier que toutes les tâches sont terminées
        for task in tasks:
            result = result_store.get_result(task.id)
            assert result is not None
            assert result["status"] == TaskStatus.COMPLETED
            expected = task.args[0] + (task.args[0] * 2)
            assert result["result"] == expected

        assert queue_manager.get_queue_size("default") == 0

    def test_task_with_exception(self, queue_manager, result_store, worker_pool):
        """Test d'exécution d'une tâche qui génère une exception"""
        task_id = "test-error-task"
        task = Task(
            id=task_id,
            func_name="divide_task",  # Fonction non définie
            args=(10, 0),
            kwargs={},
            priority=TaskPriority.NORMAL
        )

        queue_manager.submit_task("default", task)
        worker_pool.start()
        time.sleep(0.5)

        result = result_store.get_result(task_id)
        assert result is not None
        assert result["status"] == TaskStatus.FAILED
        assert "error" in result

    def test_task_priority_execution(self, queue_manager, result_store, worker_pool):
        """Test d'exécution avec priorités"""
        # Tâche basse priorité
        low_task = Task(
            id="low-priority",
            func_name="slow_task",
            args=(0.1,),
            kwargs={},
            priority=TaskPriority.LOW
        )

        # Tâche haute priorité (soumise après)
        high_task = Task(
            id="high-priority",
            func_name="add_task",
            args=(1, 1),
            kwargs={},
            priority=TaskPriority.HIGH
        )

        queue_manager.submit_task("priority", low_task)
        queue_manager.submit_task("priority", high_task)

        worker_pool.start()
        time.sleep(0.3)

        # La tâche haute priorité devrait être exécutée en premier
        high_result = result_store.get_result("high-priority")
        low_result = result_store.get_result("low-priority")

        assert high_result is not None
        assert high_result["status"] == TaskStatus.COMPLETED
        # La tâche basse priorité pourrait être en cours ou terminée
        assert low_result is not None


class TestIntegrationRedis:
    """Tests d'intégration avec Redis"""

    @pytest.fixture
    def redis_client(self):
        """Fixture pour créer un client Redis de test"""
        try:
            client = redis.Redis(
                host=settings.REDIS_HOST,
                port=settings.REDIS_PORT,
                db=settings.REDIS_TEST_DB,
                decode_responses=True
            )
            client.ping()
            yield client
            client.flushdb()
            client.close()
        except (redis.ConnectionError, ConnectionRefusedError):
            pytest.skip("Redis n'est pas disponible")

    @pytest.fixture
    def redis_result_store(self, redis_client):
        """Fixture pour créer un RedisResultStore"""
        return RedisResultStore(redis_client=redis_client)

    def test_redis_result_storage(self, redis_result_store):
        """Test de stockage des résultats dans Redis"""
        task_id = "redis-test-task"
        result_data = {
            "status": TaskStatus.COMPLETED,
            "result": 42,
            "execution_time": 0.1
        }

        # Stocker le résultat
        redis_result_store.store_result(task_id, result_data)

        # Récupérer le résultat
        retrieved = redis_result_store.get_result(task_id)
        assert retrieved is not None
        assert retrieved["status"] == TaskStatus.COMPLETED
        assert retrieved["result"] == 42

        # Tester la suppression
        redis_result_store.delete_result(task_id)
        assert redis_result_store.get_result(task_id) is None

    def test_redis_with_queue_manager(self, redis_client, redis_result_store):
        """Test d'intégration QueueManager avec Redis"""
        queue_manager = QueueManager()
        worker_pool = WorkerPool(
            queue_manager=queue_manager,
            result_store=redis_result_store,
            num_workers=1
        )

        try:
            # Créer et soumettre une tâche
            task = Task(
                id="redis-integration-test",
                func_name="multiply_task",
                args=(6, 7),
                kwargs={},
                priority=TaskPriority.NORMAL
            )

            queue_manager.submit_task("default", task)

            # Exécuter la tâche
            worker_pool.start()
            time.sleep(0.5)

            # Vérifier le résultat dans Redis
            result = redis_result_store.get_result("redis-integration-test")
            assert result is not None
            assert result["status"] == TaskStatus.COMPLETED
            assert result["result"] == 42  # 6 * 7

            # Vérifier que la tâche a été supprimée de la queue
            assert queue_manager.get_queue_size("default") == 0
        finally:
            worker_pool.stop()
            queue_manager.clear_all_queues()


class TestIntegrationAPI:
    """Tests d'intégration de l'API"""

    @pytest.fixture
    def api_client(self):
        """Fixture pour créer un client de test API"""
        app = create_app()
        return TestClient(app)

    def test_api_submit_and_monitor_task(self, api_client):
        """Test de soumission et monitoring de tâche via API"""
        # Soumettre une tâche
        task_data = {
            "queue_name": "api-test",
            "func_name": "add_task",
            "args": [10, 20],
            "kwargs": {},
            "priority": "normal"
        }

        response = api_client.post("/api/tasks/submit", json=task_data)
        assert response.status_code == 200
        task_info = response.json()
        task_id = task_info["task_id"]
        assert task_id is not None

        # Vérifier le statut de la tâche
        response = api_client.get(f"/api/tasks/{task_id}/status")
        assert response.status_code == 200
        status_info = response.json()
        assert "status" in status_info

        # Attendre l'exécution
        time.sleep(0.5)

        # Récupérer le résultat
        response = api_client.get(f"/api/tasks/{task_id}/result")
        assert response.status_code == 200
        result_info = response.json()
        assert result_info["result"] == 30  # 10 + 20

    def test_api_queue_management(self, api_client):
        """Test de gestion des files via API"""
        # Créer une file
        queue_data = {
            "queue_name": "test-queue",
            "queue_type": "fifo",
            "max_size": 100
        }

        response = api_client.post("/api/queues/create", json=queue_data)
        assert response.status_code == 200

        # Lister les files
        response = api_client.get("/api/queues")
        assert response.status_code == 200
        queues = response.json()
        assert "test-queue" in queues

        # Obtenir les stats de la file
        response = api_client.get("/api/queues/test-queue/stats")
        assert response.status_code == 200
        stats = response.json()
        assert "size" in stats

        # Supprimer la file
        response = api_client.delete("/api/queues/test-queue")
        assert response.status_code == 200

    def test_api_monitoring_endpoints(self, api_client):
        """Test des endpoints de monitoring"""
        # Stats du système
        response = api_client.get("/api/monitoring/system-stats")
        assert response.status_code == 200
        stats = response.json()
        assert "queues" in stats
        assert "workers" in stats

        # Health check
        response = api_client.get("/api/monitoring/health")
        assert response.status_code == 200
        health = response.json()
        assert health["status"] == "healthy"

        # Métriques
        response = api_client.get("/api/monitoring/metrics")
        assert response.status_code == 200


class TestIntegrationCLI:
    """Tests d'intégration de l'interface CLI"""

    @pytest.fixture
    def cli_runner(self):
        """Fixture pour créer un runner CLI"""
        from click.testing import CliRunner
        return CliRunner()

    def test_cli_submit_task(self, cli_runner, monkeypatch):
        """Test de soumission de tâche via CLI"""
        # Mock du QueueManager pour éviter les dépendances
        mock_manager = MagicMock()
        mock_manager.submit_task.return_value = "test-task-123"
        
        monkeypatch.setattr(
            "cli.commands.submit.QueueManager",
            MagicMock(return_value=mock_manager)
        )

        result = cli_runner.invoke(
            cli_app,
            ["submit", "--queue", "default", "--func", "add_task", "--args", "[2,3]"]
        )

        assert result.exit_code == 0
        assert "test-task-123" in result.output
        mock_manager.submit_task.assert_called_once()

    def test_cli_monitor_tasks(self, cli_runner, monkeypatch):
        """Test de monitoring via CLI"""
        # Mock du ResultStore
        mock_store = MagicMock()
        mock_store.get_all_results.return_value = {
            "task1": {"status": "completed", "result": 5},
            "task2": {"status": "failed", "error": "Division by zero"}
        }
        
        monkeypatch.setattr(
            "cli.commands.monitor.ResultStore",
            MagicMock(return_value=mock_store)
        )

        result = cli_runner.invoke(cli_app, ["monitor", "--all"])
        assert result.exit_code == 0
        assert "completed" in result.output
        assert "failed" in result.output

    def test_cli_queue_operations(self, cli_runner, monkeypatch):
        """Test d'opérations sur les files via CLI"""
        mock_manager = MagicMock()
        mock_manager.get_queue_stats.return_value = {
            "size": 5,
            "type": "fifo",
            "tasks": []
        }
        
        monkeypatch.setattr(
            "cli.commands.queue.QueueManager",
            MagicMock(return_value=mock_manager)
        )

        result = cli_runner.invoke(cli_app, ["queue", "stats", "default"])
        assert result.exit_code == 0
        assert "size" in result.output


class TestIntegrationWebInterface:
    """Tests d'intégration de l'interface web"""

    @pytest.fixture
    def web_client(self):
        """Fixture pour créer un client de test web"""
        from web.routes import create_web_app
        app = create_web_app()
        return TestClient(app)

    def test_web_dashboard(self, web_client):
        """Test du dashboard web"""
        response = web_client.get("/")
        assert response.status_code == 200
        assert "Task Queue System" in response.text

    def test_web_queue_status(self, web_client, monkeypatch):
        """Test de la page de statut des files"""
        # Mock des données
        mock_data = {
            "default": {"size": 3, "type": "fifo"},
            "priority": {"size": 1, "type": "priority"}
        }
        
        monkeypatch.setattr(
            "web.routes.QueueManager",
            MagicMock(return_value=MagicMock(
                get_all_queues=MagicMock(return_value=mock_data)
            ))
        )

        response = web_client.get("/queues")
        assert response.status_code == 200
        assert "default" in response.text
        assert "priority" in response.text

    def test_web_task_detail(self, web_client, monkeypatch):
        """Test de la page de détail des tâches"""
        mock_result = {
            "status": TaskStatus.COMPLETED,
            "result": 42,
            "execution_time": 0.15,
            "created_at": "2024-01-01T00:00:00"
        }
        
        monkeypatch.setattr(
            "web.routes.ResultStore",
            MagicMock(return_value=MagicMock(
                get_result=MagicMock(return_value=mock_result)
            ))
        )

        response = web_client.get("/tasks/test-task-123")
        assert response.status_code == 200
        assert "42" in response.text
        assert "completed" in response.text


class TestIntegrationFailureScenarios:
    """Tests d'intégration des scénarios d'échec"""

    def test_worker_failure_recovery(self, queue_manager, result_store):
        """Test de récupération après échec d'un worker"""
        worker_pool = WorkerPool(
            queue_manager=queue_manager,
            result_store=result_store,
            num_workers=2
        )

        try:
            # Soumettre plusieurs tâches
            for i in range(10):
                task = Task(
                    id=f"task-{i}",
                    func_name="slow_task",
                    args=(0.1,),
                    kwargs={},
                    priority=TaskPriority.NORMAL
                )
                queue_manager.submit_task("default", task)

            # Démarrer les workers
            worker_pool.start()
            
            # Simuler un crash après 0.2 secondes
            time.sleep(0.2)
            worker_pool.stop()

            # Redémarrer les workers
            worker_pool.start()
            time.sleep(1)

            # Vérifier que toutes les tâches ont été traitées
            for i in range(10):
                result = result_store.get_result(f"task-{i}")
                assert result is not None
                assert result["status"] == TaskStatus.COMPLETED

            assert queue_manager.get_queue_size("default") == 0
        finally:
            worker_pool.stop()

    def test_queue_overflow_handling(self, queue_manager):
        """Test de gestion du débordement de file"""
        # Configurer une petite file
        queue_manager.create_queue("small_queue", "fifo", max_size=2)

        # Soumettre plus de tâches que la capacité
        tasks = []
        for i in range(4):
            task = Task(
                id=f"overflow-{i}",
                func_name="add_task",
                args=(i, i),
                kwargs={},
                priority=TaskPriority.NORMAL
            )
            tasks.append(task)
            success = queue_manager.submit_task("small_queue", task)
            
            # Les deux premières devraient réussir, les autres échouer
            if i < 2:
                assert success is True
            else:
                assert success is False

        assert queue_manager.get_queue_size("small_queue") == 2

    def test_network_partition_simulation(self, redis_client, redis_result_store):
        """Test de simulation de partition réseau avec Redis"""
        try:
            # Tester la connexion initiale
            assert redis_client.ping() is True

            # Simuler une perte de connexion
            with patch.object(redis_client, 'set', side_effect=redis.ConnectionError):
                # Tentative de stockage pendant la panne
                with pytest.raises(redis.ConnectionError):
                    redis_result_store.store_result(
                        "partition-test",
                        {"status": "completed", "result": 100}
                    )

            # Vérifier que le système continue de fonctionner (mode dégradé)
            # après restauration de la connexion
            result_data = {"status": TaskStatus.COMPLETED, "result": 200}
            redis_result_store.store_result("recovery-test", result_data)
            
            retrieved = redis_result_store.get_result("recovery-test")
            assert retrieved is not None
            assert retrieved["result"] == 200
        except (redis.ConnectionError, ConnectionRefusedError):
            pytest.skip("Redis n'est pas disponible")


@pytest.mark.asyncio
class TestIntegrationAsync:
    """Tests d'intégration asynchrones"""

    async def test_async_task_execution(self):
        """Test d'exécution asynchrone de tâches"""
        queue_manager = QueueManager()
        result_store = ResultStore()
        
        worker = AsyncWorker(
            queue_manager=queue_manager,
            result_store=result_store,
            queue_name="default"
        )

        try:
            # Démarrer le worker
            worker_task = asyncio.create_task(worker.start())

            # Soumettre une tâche asynchrone
            task = Task(
                id="async-test",
                func_name="slow_task",
                args=(0.1,),
                kwargs={},
                priority=TaskPriority.NORMAL
            )

            queue_manager.submit_task("default", task)

            # Attendre l'exécution
            await asyncio.sleep(0.3)

            # Vérifier le résultat
            result = result_store.get_result("async-test")
            assert result is not None
            assert result["status"] == TaskStatus.COMPLETED
        finally:
            worker.stop()
            await worker_task

    async def test_concurrent_task_processing(self):
        """Test de traitement concurrent de tâches"""
        queue_manager = QueueManager()
        result_store = ResultStore()
        
        # Créer plusieurs workers
        workers = []
        for i in range(3):
            worker = AsyncWorker(
                queue_manager=queue_manager,
                result_store=result_store,
                queue_name="default"
            )
            workers.append(worker)

        try:
            # Démarrer tous les workers
            worker_tasks = [asyncio.create_task(w.start()) for w in workers]

            # Soumettre plusieurs tâches
            task_count = 10
            for i in range(task_count):
                task = Task(
                    id=f"concurrent-{i}",
                    func_name="slow_task",
                    args=(0.05,),
                    kwargs={},
                    priority=TaskPriority.NORMAL
                )
                queue_manager.submit_task("default", task)

            # Attendre l'exécution
            await asyncio.sleep(1)

            # Vérifier que toutes les tâches sont terminées
            for i in range(task_count):
                result = result_store.get_result(f"concurrent-{i}")
                assert result is not None
                assert result["status"] == TaskStatus.COMPLETED

            assert queue_manager.get_queue_size("default") == 0
        finally:
            for worker in workers:
                worker.stop()
            for task in worker_tasks:
                await task


def test_integration_performance():
    """Test de performance du système complet"""
    queue_manager = QueueManager()
    result_store = ResultStore()
    
    worker_pool = WorkerPool(
        queue_manager=queue_manager,
        result_store=result_store,
        num_workers=4
    )

    try:
        # Mesurer le temps d'exécution
        start_time = time.time()

        # Soumettre 50 tâches
        task_count = 50
        for i in range(task_count):
            task = Task(
                id=f"perf-{i}",
                func_name="add_task",
                args=(i, i * 2),
                kwargs={},
                priority=TaskPriority.NORMAL
            )
            queue_manager.submit_task("default", task)

        # Démarrer les workers
        worker_pool.start()

        # Attendre la fin de toutes les tâches
        max_wait_time = 5
        elapsed = 0
        while elapsed < max_wait_time:
            if queue_manager.get_queue_size("default") == 0:
                break
            time.sleep(0.1)
            elapsed += 0.1

        end_time = time.time()
        total_time = end_time - start_time

        # Vérifier que toutes les tâches sont terminées
        completed_count = 0
        for i in range(task_count):
            result = result_store.get_result(f"perf-{i}")
            if result and result["status"] == TaskStatus.COMPLETED:
                completed_count += 1

        # Vérifier les performances
        assert completed_count == task_count
        assert total_time < 3  # Doit traiter 50 tâches en moins de 3 secondes
        
        # Calculer le débit
        throughput = task_count / total_time
        print(f"\nPerformance: {throughput:.2f} tâches/seconde")
        assert throughput > 15  # Au moins 15 tâches par seconde
    finally:
        worker_pool.stop()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])