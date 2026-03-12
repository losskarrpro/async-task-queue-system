import asyncio
import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from datetime import datetime
import signal
import sys

from workers.async_worker import AsyncWorker
from core.queue_manager import QueueManager
from core.result_store import ResultStore
from utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class WorkerStatus:
    """État d'un worker individuel"""
    worker_id: str
    status: str  # idle, busy, stopped, error
    current_task: Optional[str] = None
    task_start_time: Optional[datetime] = None
    processed_count: int = 0
    error_count: int = 0
    last_error: Optional[str] = None


class WorkerPool:
    """Gestionnaire de pool de workers asynchrones"""
    
    def __init__(
        self,
        queue_manager: QueueManager,
        result_store: ResultStore,
        num_workers: int = 3,
        queue_name: str = "default",
        worker_name_prefix: str = "worker"
    ):
        """
        Initialise le pool de workers
        
        Args:
            queue_manager: Gestionnaire de file d'attente
            result_store: Store pour les résultats
            num_workers: Nombre de workers à démarrer
            queue_name: Nom de la file à consommer
            worker_name_prefix: Préfixe pour les noms des workers
        """
        self.queue_manager = queue_manager
        self.result_store = result_store
        self.num_workers = num_workers
        self.queue_name = queue_name
        self.worker_name_prefix = worker_name_prefix
        
        self.workers: Dict[str, AsyncWorker] = {}
        self.worker_status: Dict[str, WorkerStatus] = {}
        self.running = False
        self.tasks: List[asyncio.Task] = []
        
        # Configuration du logging
        self.logger = logging.getLogger(__name__)
        
        # Gestion des signaux pour arrêt propre
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)
    
    def _handle_signal(self, signum, frame):
        """Gestion des signaux d'arrêt"""
        self.logger.info(f"Signal {signum} reçu, arrêt du pool...")
        asyncio.create_task(self.stop())
    
    async def start(self) -> None:
        """Démarre le pool de workers"""
        if self.running:
            self.logger.warning("Pool déjà en cours d'exécution")
            return
        
        self.logger.info(f"Démarrage du pool avec {self.num_workers} workers")
        self.running = True
        
        # Création et démarrage des workers
        for i in range(self.num_workers):
            worker_id = f"{self.worker_name_prefix}-{i+1:03d}"
            await self._add_worker(worker_id)
        
        # Démarrage de la boucle de surveillance
        self.tasks.append(asyncio.create_task(self._monitor_loop()))
        
        self.logger.info(f"Pool démarré avec {len(self.workers)} workers")
    
    async def _add_worker(self, worker_id: str) -> None:
        """Ajoute un worker au pool"""
        worker = AsyncWorker(
            worker_id=worker_id,
            queue_manager=self.queue_manager,
            result_store=self.result_store,
            queue_name=self.queue_name
        )
        self.workers[worker_id] = worker
        self.worker_status[worker_id] = WorkerStatus(
            worker_id=worker_id,
            status="idle"
        )
        
        # Démarrer le worker
        task = asyncio.create_task(worker.start())
        self.tasks.append(task)
        self.logger.info(f"Worker {worker_id} ajouté et démarré")
    
    async def _monitor_loop(self) -> None:
        """Boucle de surveillance des workers"""
        while self.running:
            try:
                for worker_id, worker in self.workers.items():
                    status = self.worker_status[worker_id]
                    # Mettre à jour le statut basé sur l'état du worker
                    if worker.is_busy():
                        status.status = "busy"
                        status.current_task = worker.current_task_id
                    else:
                        status.status = "idle"
                        status.current_task = None
                    
                    # Mettre à jour les compteurs
                    status.processed_count = worker.processed_count
                    status.error_count = worker.error_count
                    
                    if worker.last_error:
                        status.last_error = str(worker.last_error)
                
                await asyncio.sleep(1)  # Surveiller toutes les secondes
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Erreur dans la boucle de surveillance: {e}")
                await asyncio.sleep(5)
    
    async def stop(self) -> None:
        """Arrête le pool de workers"""
        if not self.running:
            return
        
        self.logger.info("Arrêt du pool de workers...")
        self.running = False
        
        # Arrêter tous les workers
        for worker_id, worker in self.workers.items():
            await worker.stop()
            self.worker_status[worker_id].status = "stopped"
        
        # Annuler toutes les tâches
        for task in self.tasks:
            task.cancel()
        
        # Attendre que toutes les tâches soient terminées
        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)
        
        self.workers.clear()
        self.tasks.clear()
        self.logger.info("Pool de workers arrêté")
    
    def get_status(self) -> Dict[str, Any]:
        """Retourne le statut du pool"""
        return {
            "running": self.running,
            "num_workers": len(self.workers),
            "workers": {
                worker_id: {
                    "status": status.status,
                    "current_task": status.current_task,
                    "processed_count": status.processed_count,
                    "error_count": status.error_count,
                    "last_error": status.last_error
                }
                for worker_id, status in self.worker_status.items()
            }
        }
