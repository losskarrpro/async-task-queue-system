```python
import asyncio
import pytest
from unittest.mock import AsyncMock, Mock, patch, call
import sys
import time
from datetime import datetime

sys.path.insert(0, '.')

from core.queue_manager import QueueManager
from core.task import Task, TaskStatus, TaskPriority
from core.result_store import ResultStore
from core.exceptions import TaskError, WorkerError
from workers.base_worker import BaseWorker
from workers.async_worker import AsyncWorker
from workers.worker_pool import WorkerPool


class TestWorkerPool:
    """Tests unitaires pour la classe WorkerPool"""
    
    @pytest.fixture
    def queue_manager(self):
        """Fixture pour créer un QueueManager"""
        return QueueManager()
    
    @pytest.fixture
    def result_store(self):
        """Fixture pour créer un ResultStore"""
        return ResultStore()
    
    @pytest.fixture
    def sample_task(self):
        """Fixture pour créer une tâche de test"""
        return Task(
            task_id="test_task_1",
            task_type="test_task",
            data={"param1": "value1"},
            priority=TaskPriority.NORMAL
        )
    
    @pytest.fixture
    def async_worker_mock(self):
        """Fixture pour créer un mock d'AsyncWorker"""
        worker = Mock(spec=AsyncWorker)
        worker.id = "worker_1"
        worker.is_running = False
        worker.start = AsyncMock()
        worker.stop = AsyncMock()
        worker.process_task = AsyncMock()
        return worker
    
    @pytest.fixture
    def worker_pool(self, queue_manager, result_store):
        """Fixture pour créer un WorkerPool"""
        return WorkerPool(
            queue_manager=queue_manager,
            result_store=result_store,
            worker_class=AsyncWorker,
            min_workers=2,
            max_workers=5
        )
    
    @pytest.mark.asyncio
    async def test_worker_pool_initialization(self, worker_pool):
        """Test l'initialisation du WorkerPool"""
        assert worker_pool.min_workers == 2
        assert worker_pool.max_workers == 5
        assert worker_pool.worker_class == AsyncWorker
        assert isinstance(worker_pool.queue_manager, QueueManager)
        assert isinstance(worker_pool.result_store, ResultStore)
        assert len(worker_pool.workers) == 0
        assert worker_pool.is_running is False
        assert worker_pool.worker_counter == 0
    
    @pytest.mark.asyncio
    async def test_worker_pool_start_stop(self, worker_pool):
        """Test le démarrage et l'arrêt du pool de workers"""
        # Mock de la méthode create_worker pour éviter de créer de vrais workers
        with patch.object(worker_pool, 'create_worker', new_callable=AsyncMock) as mock_create:
            mock_create.return_value = Mock(spec=AsyncWorker)
            
            # Démarrage du pool
            await worker_pool.start()
            assert worker_pool.is_running is True
            # Vérifie que create_worker a été appelé min_workers fois
            assert mock_create.call_count == worker_pool.min_workers
            
            # Arrêt du pool
            await worker_pool.stop()
            assert worker_pool.is_running is False
    
    @pytest.mark.asyncio
    async def test_create_worker(self, worker_pool):
        """Test la création d'un worker"""
        # Mock de AsyncWorker.__init__ pour éviter l'initialisation complète
        with patch('workers.worker_pool.AsyncWorker') as mock_worker_class:
            mock_worker_instance = Mock(spec=AsyncWorker)
            mock_worker_instance.id = "worker_1"
            mock_worker_instance.start = AsyncMock()
            mock_worker_class.return_value = mock_worker_instance
            
            # Création d'un worker
            worker = await worker_pool.create_worker()
            
            # Vérifications
            assert worker is mock_worker_instance
            assert worker.id in worker_pool.workers
            assert worker_pool.workers[worker.id] == worker
            mock_worker_instance.start.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_create_worker_exceeds_max(self, worker_pool):
        """Test que la création échoue si on dépasse max_workers"""
        worker_pool.is_running = True
        
        # Remplir le pool avec le nombre maximum de workers
        for i in range(worker_pool.max_workers):
            worker = Mock(spec=AsyncWorker)
            worker.id = f"worker_{i}"
            worker_pool.workers[worker.id] = worker
            worker_pool.worker_counter = worker_pool.max_workers
        
        # Tentative de création d'un worker supplémentaire
        with pytest.raises(WorkerError, match="Maximum number of workers reached"):
            await worker_pool.create_worker()
    
    @pytest.mark.asyncio
    async def test_remove_worker(self, worker_pool, async_worker_mock):
        """Test la suppression d'un worker"""
        # Ajouter un worker mocké
        worker_pool.workers[async_worker_mock.id] = async_worker_mock
        
        # Supprimer le worker
        await worker_pool.remove_worker(async_worker_mock.id)
        
        # Vérifications
        assert async_worker_mock.id not in worker_pool.workers
        async_worker_mock.stop.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_remove_worker_not_found(self, worker_pool):
        """Test la suppression d'un worker inexistant"""
        with pytest.raises(WorkerError, match="Worker not found"):
            await worker_pool.remove_worker("non_existent_worker")
    
    @pytest.mark.asyncio
    async def test_scale_up(self, worker_pool):
        """Test l'augmentation du nombre de workers"""
        worker_pool.is_running = True
        worker_pool.workers = {"worker_1": Mock(), "worker_2": Mock()}
        
        # Mock de create_worker
        with patch.object(worker_pool, 'create_worker', new_callable=AsyncMock) as mock_create:
            mock_create.return_value = Mock()
            
            # Scale up de 1 worker
            await worker_pool.scale_up(1)
            
            # Vérification
            mock_create.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_scale_down(self, worker_pool):
        """Test la réduction du nombre de workers"""
        worker_pool.is_running = True
        
        # Créer plusieurs workers mockés
        workers = {}
        for i in range(3):
            worker = Mock(spec=AsyncWorker)
            worker.id = f"worker_{i}"
            workers[worker.id] = worker
        
        worker_pool.workers = workers
        
        # Mock de remove_worker
        with patch.object(worker_pool, 'remove_worker', new_callable=AsyncMock) as mock_remove:
            # Scale down de 2 workers
            await worker_pool.scale_down(2)
            
            # Vérification
            assert mock_remove.call_count == 2
    
    @pytest.mark.asyncio
    async def test_scale_down_below_min(self, worker_pool):
        """Test qu'on ne peut pas descendre en dessous du minimum de workers"""
        worker_pool.is_running = True
        
        # Créer exactement min_workers
        workers = {}
        for i in range(worker_pool.min_workers):
            worker = Mock(spec=AsyncWorker)
            worker.id = f"worker_{i}"
            workers[worker.id] = worker
        
        worker_pool.workers = workers
        
        # Tentative de scale down de 1 worker
        with pytest.raises(WorkerError, match="Cannot scale below minimum workers"):
            await worker_pool.scale_down(1)
    
    @pytest.mark.asyncio
    async def test_get_worker_stats(self, worker_pool):
        """Test la récupération des statistiques des workers"""
        # Créer des workers mockés avec différents états
        worker1 = Mock(spec=AsyncWorker)
        worker1.id = "worker_1"
        worker1.is_running = True
        worker1.current_task = Mock()
        worker1.current_task.task_id = "task_1"
        worker1.processed_count = 5
        
        worker2 = Mock(spec=AsyncWorker)
        worker2.id = "worker_2"
        worker2.is_running = False
        worker2.current_task = None
        worker2.processed_count = 3
        
        worker_pool.workers = {
            worker1.id: worker1,
            worker2.id: worker2
        }
        
        # Récupérer les stats
        stats = worker_pool.get_worker_stats()
        
        # Vérifications
        assert len(stats) == 2
        assert stats[0]["worker_id"] == "worker_1"
        assert stats[0]["is_running"] is True
        assert stats[0]["current_task"] == "task_1"
        assert stats[0]["processed_count"] == 5
        assert stats[1]["worker_id"] == "worker_2"
        assert stats[1]["is_running"] is False
        assert stats[1]["current_task"] is None
        assert stats[1]["processed_count"] == 3
    
    @pytest.mark.asyncio
    async def test_auto_scale_based_on_queue(self, worker_pool):
        """Test l'auto-scaling basé sur la taille de la file d'attente"""
        worker_pool.is_running = True
        
        # Configurer la file avec 10 tâches
        for i in range(10):
            task = Task(
                task_id=f"task_{i}",
                task_type="test",
                data={"index": i}
            )
            worker_pool.queue_manager.enqueue(task, "default")
        
        # Mock pour scale_up et scale_down
        with patch.object(worker_pool, 'scale_up', new_callable=AsyncMock) as mock_scale_up:
            with patch.object(worker_pool, 'scale_down', new_callable=AsyncMock) as mock_scale_down:
                # Configurer le pool avec 2 workers (min_workers)
                workers = {}
                for i in range(worker_pool.min_workers):
                    worker = Mock(spec=AsyncWorker)
                    worker.id = f"worker_{i}"
                    workers[worker.id] = worker
                worker_pool.workers = workers
                
                # Exécuter l'auto-scale
                await worker_pool.auto_scale_based_on_queue()
                
                # Avec 10 tâches et 2 workers, on devrait scale up
                # (10 tâches / 2 = 5 par worker > threshold 4)
                mock_scale_up.assert_called()
                mock_scale_down.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_auto_scale_idle_workers(self, worker_pool):
        """Test l'auto-scaling pour réduire les workers inactifs"""
        worker_pool.is_running = True
        
        # Configurer le pool avec plus que min_workers
        workers = {}
        for i in range(4):  # 4 workers, min_workers=2
            worker = Mock(spec=AsyncWorker)
            worker.id = f"worker_{i}"
            worker.current_task = None  # Tous inactifs
            worker.idle_time = 120  # 2 minutes d'inactivité
            workers[worker.id] = worker
        worker_pool.workers = workers
        
        # Mock pour scale_down
        with patch.object(worker_pool, 'scale_down', new_callable=AsyncMock) as mock_scale_down:
            # Exécuter l'auto-scale
            await worker_pool.auto_scale_idle_workers()
            
            # Avec 4 workers inactifs et min_workers=2, on devrait scale down de 2
            mock_scale_down.assert_called()
    
    @pytest.mark.asyncio
    async def test_handle_worker_crash(self, worker_pool):
        """Test la gestion d'un crash de worker"""
        worker_pool.is_running = True
        
        # Créer un worker mocké qui va "crasher"
        crashed_worker = Mock(spec=AsyncWorker)
        crashed_worker.id = "crashed_worker"
        crashed_worker.is_running = False
        
        # Créer un worker normal
        normal_worker = Mock(spec=AsyncWorker)
        normal_worker.id = "normal_worker"
        normal_worker.is_running = True
        
        worker_pool.workers = {
            crashed_worker.id: crashed_worker,
            normal_worker.id: normal_worker
        }
        
        # Mock pour remove_worker et create_worker
        with patch.object(worker_pool, 'remove_worker', new_callable=AsyncMock) as mock_remove:
            with patch.object(worker_pool, 'create_worker', new_callable=AsyncMock) as mock_create:
                # Gérer les crashes
                await worker_pool.handle_worker_crashes()
                
                # Vérifications
                mock_remove.assert_called_once_with("crashed_worker")
                mock_create.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_monitor_and_scale(self, worker_pool):
        """Test la surveillance et le scaling automatique"""
        worker_pool.is_running = True
        
        # Mock des méthodes d'auto-scale
        with patch.object(worker_pool, 'auto_scale_based_on_queue', new_callable=AsyncMock) as mock_queue_scale:
            with patch.object(worker_pool, 'auto_scale_idle_workers', new_callable=AsyncMock) as mock_idle_scale:
                with patch.object(worker_pool, 'handle_worker_crashes', new_callable=AsyncMock) as mock_handle_crashes:
                    # Exécuter la surveillance
                    await worker_pool.monitor_and_scale()
                    
                    # Vérifier que toutes les méthodes ont été appelées
                    mock_queue_scale.assert_called_once()
                    mock_idle_scale.assert_called_once()
                    mock_handle_crashes.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_pool_stats(self, worker_pool):
        """Test la récupération des statistiques du pool"""
        # Configurer le pool
        worker1 = Mock(spec=AsyncWorker)
        worker1.id = "worker_1"
        worker1.is_running = True
        worker1.processed_count = 10
        
        worker2 = Mock(spec=AsyncWorker)
        worker2.id = "worker_2"
        worker2.is_running = True
        worker2.processed_count = 5
        
        worker_pool.workers = {
            worker1.id: worker1,
            worker2.id: worker2
        }
        
        worker_pool.is_running = True
        worker_pool.worker_counter = 2
        
        # Ajouter des tâches à la file
        for i in range(3):
            task = Task(
                task_id=f"task_{i}",
                task_type="test",
                data={"index": i}
            )
            worker_pool.queue_manager.enqueue(task, "default")
        
        # Récupérer les stats
        stats = worker_pool.get_pool_stats()
        
        # Vérifications
        assert stats["is_running"] is True
        assert stats["worker_count"] == 2
        assert stats["queue_size"] == 3
        assert stats["total_processed"] == 15  # 10 + 5
        assert "avg_processed_per_worker" in stats
        assert "uptime" in stats
    
    @pytest.mark.asyncio
    async def test_worker_pool_context_manager(self, worker_pool):
        """Test l'utilisation du WorkerPool comme context manager"""
        # Mock des méthodes start et stop
        with patch.object(worker_pool, 'start', new_callable=AsyncMock) as mock_start:
            with patch.object(worker_pool, 'stop', new_callable=AsyncMock) as mock_stop:
                # Utiliser le context manager
                async with worker_pool:
                    mock_start.assert_called_once()
                
                # Vérifier que stop a été appelé à la sortie du contexte
                mock_stop.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_worker_pool_with_real_workers(self, queue_manager, result_store):
        """Test d'intégration avec de vrais workers"""
        # Créer un pool avec un mock de worker class simplifié
        class SimpleWorker(BaseWorker):
            def __init__(self, worker_id, queue_manager, result_store):
                super().__init__(worker_id, queue_manager, result_store)
                self.processed_count = 0
            
            async def process_task(self, task):
                self.processed_count += 1
                await asyncio.sleep(0.01)  # Petit délai pour simuler le traitement
                return {"result": f"Processed {task.task_id}"}
        
        pool = WorkerPool(
            queue_manager=queue_manager,
            result_store=result_store,
            worker_class=SimpleWorker,
            min_workers=1,
            max_workers=3
        )
        
        # Ajouter quelques tâches à la file
        for i in range(5):
            task = Task(
                task_id=f"task_{i}",
                task_type="test",
                data={"index": i}
            )
            queue_manager.enqueue(task, "default")
        
        # Démarrer le pool
        await pool.start()
        
        # Attendre un peu pour que les workers traitent les tâches
        await asyncio.sleep(0.1)
        
        # Vérifier que certaines tâches ont été traitées
        stats = pool.get_pool_stats()
        assert stats["worker_count"] >= 1
        assert stats["total_processed"] > 0
        
        # Arrêter le pool
        await pool.stop()
    
    @pytest.mark.asyncio
    async def test_worker_pool_task_distribution(self, queue_manager, result_store):
        """Test la distribution des tâches entre les workers"""
        # Suivi des tâches traitées par chaque worker
        worker_tasks = {}
        
        class TrackingWorker(BaseWorker):
            def __init__(self, worker_id, queue_manager, result_store):
                super().__init__(worker_id, queue_manager, result_store)
                self.processed_tasks = []
            
            async def process_task(self, task):
                self.processed_tasks.append(task.task_id)
                await asyncio.sleep(0.01)
                return {"worker": self.id, "task": task.task_id}
        
        pool = WorkerPool(
            queue_manager=queue_manager,
            result_store=result_store,
            worker_class=TrackingWorker,
            min_workers=3,
            max_workers=3
        )
        
        # Ajouter des tâches
        task_count = 9
        for i in range(task_count):
            task = Task(
                task_id=f"task_{i}",
                task_type="test",
                data={"index": i}
            )
            queue_manager.enqueue(task, "default")
        
        # Démarrer le pool
        await pool.start()
        
        # Attendre que toutes les tâches soient traitées
        max_wait = 2.0
        start_time = time.time()
        while queue_manager.get_queue_size("default") > 0 and time.time() - start_time < max_wait:
            await asyncio.sleep(0.1)
        
        # Arrêter le pool
        await pool.stop()
        
        # Vérifier la distribution
        total_processed = 0
        for worker_id, worker in pool.workers.items():
            processed_count = len(worker.processed_tasks)
            total_processed += processed_count
            print(f"Worker {worker_id} processed {processed_count} tasks")
        
        # Toutes les tâches devraient avoir été traitées
        assert total_processed == task_count
        
        # La distribution ne devrait pas être trop déséquilibrée
        # (dans un système idéal, chaque worker traite environ task_count/3 tâches)
        worker_counts = [len(w.processed_tasks) for w in pool.workers.values()]
        max_diff = max(worker_counts) - min(worker_counts)
        # La différence ne devrait pas être trop grande
        assert max_diff <= 2  # Permettre un petit déséquilibre

    def test_worker_pool_repr(self, worker_pool):
        """Test la représentation textuelle du WorkerPool"""
        representation = repr(worker_pool)
        assert "WorkerPool" in representation
        assert "min_workers=2" in representation
        assert "max_workers=5" in representation
        assert "workers=0" in representation
    
    @pytest.mark.asyncio
    async def test_worker_pool_with_custom_config(self):
        """Test le WorkerPool avec une configuration personnalisée"""
        queue_manager = QueueManager()
        result_store = ResultStore()
        
        pool = WorkerPool(
            queue_manager=queue_manager,
            result_store=result_store,
            worker_class=AsyncWorker,
            min_workers=1,
            max_workers=10,
            scaling_threshold=10,  # Changer le seuil par défaut
            idle_timeout=300  # 5 minutes
        )
        
        assert pool.scaling_threshold == 10
        assert pool.idle_timeout == 300
        assert pool.min_workers == 1
        assert pool.max_workers == 10
    
    @pytest.mark.asyncio
    async def test_worker_pool_scale_with_no_tasks(self, worker_pool):
        """Test le scaling avec une file vide"""
        worker_pool.is_running = True
        
        # Configurer le pool avec plus que min_workers
        workers = {}
        for i in range(4):  # 4 workers, min_workers=2
            worker = Mock(spec=AsyncWorker)
            worker.id = f"worker_{i}"
            worker.current_task = None
            worker.idle_time = 30  # 30 secondes seulement
            workers[worker.id] = worker
        
        worker_pool.workers = workers
        
        # Mock de scale_down
        with patch.object(worker_pool, 'scale_down', new_callable=AsyncMock) as mock_scale_down:
            # File vide, workers pas assez inactifs pour être supprimés
            await worker_pool.auto_scale_idle_workers()
            
            # Ne devrait pas scale down car idle_time < idle_timeout
            mock_scale_down.assert_not_called()