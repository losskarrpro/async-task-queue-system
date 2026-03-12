import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from core.queue_manager import QueueManager, QueueType
from core.exceptions import QueueEmptyError, InvalidQueueTypeError, TaskNotFoundError
from core.task import Task
from datetime import datetime
import uuid


@pytest.fixture
def sample_task():
    """Fixture pour créer une tâche de test."""
    return Task(
        id=str(uuid.uuid4()),
        name="test_task",
        queue_type=QueueType.FIFO,
        data={"test": "data"},
        priority=1
    )


@pytest.fixture
def queue_manager():
    """Fixture pour créer un QueueManager."""
    return QueueManager()


@pytest.fixture
def fifo_queue_manager():
    """Fixture pour un QueueManager avec une file FIFO."""
    qm = QueueManager()
    qm.create_queue("test_fifo", QueueType.FIFO)
    return qm


@pytest.fixture
def lifo_queue_manager():
    """Fixture pour un QueueManager avec une file LIFO."""
    qm = QueueManager()
    qm.create_queue("test_lifo", QueueType.LIFO)
    return qm


@pytest.fixture
def priority_queue_manager():
    """Fixture pour un QueueManager avec une file de priorité."""
    qm = QueueManager()
    qm.create_queue("test_priority", QueueType.PRIORITY)
    return qm


class TestQueueManager:
    """Tests pour la classe QueueManager."""

    def test_create_queue(self, queue_manager):
        """Test de création de file."""
        # Création d'une file FIFO
        queue_manager.create_queue("fifo_queue", QueueType.FIFO)
        assert "fifo_queue" in queue_manager.queues
        assert queue_manager.queues["fifo_queue"].queue_type == QueueType.FIFO

        # Création d'une file LIFO
        queue_manager.create_queue("lifo_queue", QueueType.LIFO)
        assert queue_manager.queues["lifo_queue"].queue_type == QueueType.LIFO

        # Création d'une file de priorité
        queue_manager.create_queue("priority_queue", QueueType.PRIORITY)
        assert queue_manager.queues["priority_queue"].queue_type == QueueType.PRIORITY

        # Test avec un type de file invalide
        with pytest.raises(InvalidQueueTypeError):
            queue_manager.create_queue("invalid_queue", "INVALID")

        # Test de création d'une file existante (ne devrait pas lever d'erreur, juste ignorer)
        queue_manager.create_queue("fifo_queue", QueueType.FIFO)
        assert queue_manager.queues["fifo_queue"].queue_type == QueueType.FIFO

    def test_delete_queue(self, queue_manager):
        """Test de suppression de file."""
        queue_manager.create_queue("test_queue", QueueType.FIFO)
        assert "test_queue" in queue_manager.queues

        queue_manager.delete_queue("test_queue")
        assert "test_queue" not in queue_manager.queues

        # Suppression d'une file inexistante (ne devrait pas lever d'erreur)
        queue_manager.delete_queue("nonexistent")

    def test_list_queues(self, queue_manager):
        """Test de liste des files."""
        assert queue_manager.list_queues() == []

        queue_manager.create_queue("queue1", QueueType.FIFO)
        queue_manager.create_queue("queue2", QueueType.LIFO)

        queues = queue_manager.list_queues()
        assert len(queues) == 2
        assert "queue1" in queues
        assert "queue2" in queues

    def test_get_queue_stats(self, queue_manager, sample_task):
        """Test de récupération des statistiques d'une file."""
        queue_manager.create_queue("stats_queue", QueueType.FIFO)
        
        # File vide
        stats = queue_manager.get_queue_stats("stats_queue")
        assert stats["queue_type"] == QueueType.FIFO
        assert stats["size"] == 0
        assert stats["pending"] == 0
        assert stats["processing"] == 0
        assert stats["completed"] == 0
        assert stats["failed"] == 0

        # Ajout de tâches
        queue_manager.enqueue("stats_queue", sample_task)
        stats = queue_manager.get_queue_stats("stats_queue")
        assert stats["size"] == 1
        assert stats["pending"] == 1

        # File inexistante
        with pytest.raises(KeyError):
            queue_manager.get_queue_stats("nonexistent")

    @pytest.mark.asyncio
    async def test_enqueue_fifo(self, fifo_queue_manager, sample_task):
        """Test d'ajout de tâche dans une file FIFO."""
        await fifo_queue_manager.enqueue("test_fifo", sample_task)
        assert fifo_queue_manager.queues["test_fifo"].size() == 1

        # Ajout d'une seconde tâche
        task2 = Task(
            id=str(uuid.uuid4()),
            name="test_task2",
            queue_type=QueueType.FIFO,
            data={"test": "data2"},
            priority=1
        )
        await fifo_queue_manager.enqueue("test_fifo", task2)
        assert fifo_queue_manager.queues["test_fifo"].size() == 2

        # File inexistante
        with pytest.raises(KeyError):
            await fifo_queue_manager.enqueue("nonexistent", sample_task)

    @pytest.mark.asyncio
    async def test_dequeue_fifo(self, fifo_queue_manager, sample_task):
        """Test de récupération de tâche depuis une file FIFO (ordre FIFO)."""
        task2 = Task(
            id=str(uuid.uuid4()),
            name="test_task2",
            queue_type=QueueType.FIFO,
            data={"test": "data2"},
            priority=1
        )

        await fifo_queue_manager.enqueue("test_fifo", sample_task)
        await fifo_queue_manager.enqueue("test_fifo", task2)

        # Premier élément sorti doit être le premier entré
        dequeued = await fifo_queue_manager.dequeue("test_fifo")
        assert dequeued.id == sample_task.id

        # Deuxième élément sorti doit être le deuxième entré
        dequeued = await fifo_queue_manager.dequeue("test_fifo")
        assert dequeued.id == task2.id

        # File vide
        with pytest.raises(QueueEmptyError):
            await fifo_queue_manager.dequeue("test_fifo")

        # File inexistante
        with pytest.raises(KeyError):
            await fifo_queue_manager.dequeue("nonexistent")

    @pytest.mark.asyncio
    async def test_enqueue_dequeue_lifo(self, lifo_queue_manager, sample_task):
        """Test d'ajout et récupération dans une file LIFO (ordre LIFO)."""
        task2 = Task(
            id=str(uuid.uuid4()),
            name="test_task2",
            queue_type=QueueType.LIFO,
            data={"test": "data2"},
            priority=1
        )

        await lifo_queue_manager.enqueue("test_lifo", sample_task)
        await lifo_queue_manager.enqueue("test_lifo", task2)

        # Premier élément sorti doit être le dernier entré
        dequeued = await lifo_queue_manager.dequeue("test_lifo")
        assert dequeued.id == task2.id

        # Deuxième élément sorti doit être le premier entré
        dequeued = await lifo_queue_manager.dequeue("test_lifo")
        assert dequeued.id == sample_task.id

    @pytest.mark.asyncio
    async def test_enqueue_dequeue_priority(self, priority_queue_manager):
        """Test d'ajout et récupération dans une file de priorité."""
        task_low = Task(
            id=str(uuid.uuid4()),
            name="low_task",
            queue_type=QueueType.PRIORITY,
            data={"test": "low"},
            priority=3  # Priorité basse (plus grand nombre = plus basse priorité)
        )

        task_high = Task(
            id=str(uuid.uuid4()),
            name="high_task",
            queue_type=QueueType.PRIORITY,
            data={"test": "high"},
            priority=1  # Priorité haute
        )

        task_medium = Task(
            id=str(uuid.uuid4()),
            name="medium_task",
            queue_type=QueueType.PRIORITY,
            data={"test": "medium"},
            priority=2  # Priorité moyenne
        )

        # Ajout dans un ordre quelconque
        await priority_queue_manager.enqueue("test_priority", task_low)
        await priority_queue_manager.enqueue("test_priority", task_high)
        await priority_queue_manager.enqueue("test_priority", task_medium)

        # Doit sortir par ordre de priorité: haute, moyenne, basse
        dequeued = await priority_queue_manager.dequeue("test_priority")
        assert dequeued.id == task_high.id

        dequeued = await priority_queue_manager.dequeue("test_priority")
        assert dequeued.id == task_medium.id

        dequeued = await priority_queue_manager.dequeue("test_priority")
        assert dequeued.id == task_low.id

    def test_queue_size(self, fifo_queue_manager, sample_task):
        """Test de récupération de la taille d'une file."""
        assert fifo_queue_manager.queue_size("test_fifo") == 0

        # Ajout asynchrone nécessite d'exécuter la coroutine
        asyncio.run(fifo_queue_manager.enqueue("test_fifo", sample_task))
        assert fifo_queue_manager.queue_size("test_fifo") == 1

        # File inexistante
        with pytest.raises(KeyError):
            fifo_queue_manager.queue_size("nonexistent")

    def test_is_empty(self, fifo_queue_manager, sample_task):
        """Test de vérification si une file est vide."""
        assert fifo_queue_manager.is_empty("test_fifo") is True

        asyncio.run(fifo_queue_manager.enqueue("test_fifo", sample_task))
        assert fifo_queue_manager.is_empty("test_fifo") is False

        # File inexistante
        with pytest.raises(KeyError):
            fifo_queue_manager.is_empty("nonexistent")

    @pytest.mark.asyncio
    async def test_peek(self, fifo_queue_manager, sample_task):
        """Test de visualisation de la prochaine tâche sans la retirer."""
        # File vide
        with pytest.raises(QueueEmptyError):
            await fifo_queue_manager.peek("test_fifo")

        await fifo_queue_manager.enqueue("test_fifo", sample_task)
        
        task2 = Task(
            id=str(uuid.uuid4()),
            name="test_task2",
            queue_type=QueueType.FIFO,
            data={"test": "data2"},
            priority=1
        )
        await fifo_queue_manager.enqueue("test_fifo", task2)

        # Peek doit retourner la première tâche sans modifier la file
        peeked = await fifo_queue_manager.peek("test_fifo")
        assert peeked.id == sample_task.id
        assert fifo_queue_manager.queue_size("test_fifo") == 2

        # File inexistante
        with pytest.raises(KeyError):
            await fifo_queue_manager.peek("nonexistent")

    @pytest.mark.asyncio
    async def test_remove_task(self, fifo_queue_manager, sample_task):
        """Test de suppression d'une tâche spécifique."""
        task2 = Task(
            id=str(uuid.uuid4()),
            name="test_task2",
            queue_type=QueueType.FIFO,
            data={"test": "data2"},
            priority=1
        )

        await fifo_queue_manager.enqueue("test_fifo", sample_task)
        await fifo_queue_manager.enqueue("test_fifo", task2)

        # Suppression d'une tâche existante
        removed = await fifo_queue_manager.remove_task("test_fifo", sample_task.id)
        assert removed.id == sample_task.id
        assert fifo_queue_manager.queue_size("test_fifo") == 1

        # La tâche restante doit être task2
        dequeued = await fifo_queue_manager.dequeue("test_fifo")
        assert dequeued.id == task2.id

        # Suppression d'une tâche inexistante
        with pytest.raises(TaskNotFoundError):
            await fifo_queue_manager.remove_task("test_fifo", "nonexistent_id")

        # File inexistante
        with pytest.raises(KeyError):
            await fifo_queue_manager.remove_task("nonexistent", sample_task.id)

    @pytest.mark.asyncio
    async def test_clear_queue(self, fifo_queue_manager, sample_task):
        """Test de vidage d'une file."""
        await fifo_queue_manager.enqueue("test_fifo", sample_task)
        assert fifo_queue_manager.queue_size("test_fifo") == 1

        await fifo_queue_manager.clear_queue("test_fifo")
        assert fifo_queue_manager.queue_size("test_fifo") == 0
        assert fifo_queue_manager.is_empty("test_fifo") is True

        # Vidage d'une file inexistante (ne devrait pas lever d'erreur)
        await fifo_queue_manager.clear_queue("nonexistent")

    @pytest.mark.asyncio
    async def test_get_task_status(self, fifo_queue_manager, sample_task):
        """Test de récupération du statut d'une tâche."""
        await fifo_queue_manager.enqueue("test_fifo", sample_task)
        
        status = fifo_queue_manager.get_task_status("test_fifo", sample_task.id)
        assert status == "pending"

        # Marquer la tâche comme en cours de traitement
        await fifo_queue_manager.dequeue("test_fifo")
        status = fifo_queue_manager.get_task_status("test_fifo", sample_task.id)
        assert status == "processing"

        # Tâche inexistante
        with pytest.raises(TaskNotFoundError):
            fifo_queue_manager.get_task_status("test_fifo", "nonexistent_id")

        # File inexistante
        with pytest.raises(KeyError):
            fifo_queue_manager.get_task_status("nonexistent", sample_task.id)

    def test_get_all_tasks(self, fifo_queue_manager, sample_task):
        """Test de récupération de toutes les tâches d'une file."""
        # File vide
        tasks = fifo_queue_manager.get_all_tasks("test_fifo")
        assert tasks == []

        # Ajout de tâches
        asyncio.run(fifo_queue_manager.enqueue("test_fifo", sample_task))
        
        task2 = Task(
            id=str(uuid.uuid4()),
            name="test_task2",
            queue_type=QueueType.FIFO,
            data={"test": "data2"},
            priority=1
        )
        asyncio.run(fifo_queue_manager.enqueue("test_fifo", task2))

        tasks = fifo_queue_manager.get_all_tasks("test_fifo")
        assert len(tasks) == 2
        task_ids = [task.id for task in tasks]
        assert sample_task.id in task_ids
        assert task2.id in task_ids

        # File inexistante
        with pytest.raises(KeyError):
            fifo_queue_manager.get_all_tasks("nonexistent")

    @pytest.mark.asyncio
    async def test_concurrent_enqueue_dequeue(self, fifo_queue_manager):
        """Test d'opérations concurrentes sur la file."""
        tasks = []
        for i in range(10):
            task = Task(
                id=str(uuid.uuid4()),
                name=f"task_{i}",
                queue_type=QueueType.FIFO,
                data={"index": i},
                priority=1
            )
            tasks.append(task)

        # Ajout concurrent
        enqueue_tasks = [fifo_queue_manager.enqueue("test_fifo", task) for task in tasks]
        await asyncio.gather(*enqueue_tasks)

        assert fifo_queue_manager.queue_size("test_fifo") == 10

        # Récupération concurrente
        dequeue_tasks = [fifo_queue_manager.dequeue("test_fifo") for _ in range(10)]
        results = await asyncio.gather(*dequeue_tasks)

        assert len(results) == 10
        assert fifo_queue_manager.is_empty("test_fifo") is True

        # Vérifier que l'ordre FIFO est préservé malgré la concurrence
        # (les IDs doivent être dans le même ordre que l'ajout)
        for i, result in enumerate(results):
            assert result.id == tasks[i].id