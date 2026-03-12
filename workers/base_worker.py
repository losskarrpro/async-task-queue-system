import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Any, Optional, Dict
from enum import Enum

from core.task import Task
from core.result_store import ResultStore
from utils.logger import get_logger


class WorkerStatus(Enum):
    IDLE = "idle"
    RUNNING = "running"
    PAUSED = "paused"
    STOPPED = "stopped"


class BaseWorker(ABC):
    """Classe de base abstraite pour tous les workers."""
    
    def __init__(
        self,
        worker_id: str,
        result_store: Optional[ResultStore] = None,
        max_retries: int = 3,
        health_check_interval: int = 30
    ):
        self.worker_id = worker_id
        self.status = WorkerStatus.IDLE
        self.result_store = result_store
        self.max_retries = max_retries
        self.health_check_interval = health_check_interval
        self._stop_event = asyncio.Event()
        self._current_task: Optional[Task] = None
        self._stats = {
            "processed_tasks": 0,
            "failed_tasks": 0,
            "retried_tasks": 0,
            "total_processing_time": 0.0
        }
        self.logger = get_logger(f"worker.{worker_id}")
    
    @abstractmethod
    async def process_task(self, task: Task) -> Any:
        """Méthode abstraite pour traiter une tâche."""
        pass
    
    async def execute_task(self, task: Task) -> Dict[str, Any]:
        """Exécute une tâche avec gestion des retries et stockage du résultat."""
        retry_count = 0
        start_time = asyncio.get_event_loop().time()
        
        while retry_count <= self.max_retries:
            try:
                self._current_task = task
                self.logger.info(f"Début du traitement de la tâche {task.task_id}")
                
                result = await self.process_task(task)
                processing_time = asyncio.get_event_loop().time() - start_time
                
                # Stocker le résultat
                if self.result_store:
                    await self.result_store.store_result(
                        task.task_id,
                        {
                            "status": "completed",
                            "result": result,
                            "worker_id": self.worker_id,
                            "processing_time": processing_time,
                            "retry_count": retry_count
                        }
                    )
                
                # Mettre à jour les statistiques
                self._stats["processed_tasks"] += 1
                self._stats["total_processing_time"] += processing_time
                
                self.logger.info(f"Tâche {task.task_id} traitée avec succès")
                self._current_task = None
                
                return {
                    "status": "completed",
                    "result": result,
                    "processing_time": processing_time,
                    "retry_count": retry_count
                }
                
            except Exception as e:
                retry_count += 1
                self._stats["retried_tasks"] += 1
                
                if retry_count <= self.max_retries:
                    self.logger.warning(
                        f"Échec du traitement de la tâche {task.task_id}, "
                        f"retry {retry_count}/{self.max_retries}: {str(e)}"
                    )
                    await asyncio.sleep(2 ** retry_count)  # Backoff exponentiel
                else:
                    processing_time = asyncio.get_event_loop().time() - start_time
                    self._stats["failed_tasks"] += 1
                    
                    # Stocker l'échec
                    if self.result_store:
                        await self.result_store.store_result(
                            task.task_id,
                            {
                                "status": "failed",
                                "error": str(e),
                                "worker_id": self.worker_id,
                                "processing_time": processing_time,
                                "retry_count": retry_count
                            }
                        )
                    
                    self.logger.error(f"Tâche {task.task_id} a échoué après {self.max_retries} retries: {str(e)}")
                    self._current_task = None
                    
                    return {
                        "status": "failed",
                        "error": str(e),
                        "processing_time": processing_time,
                        "retry_count": retry_count
                    }
    
    async def start(self):
        """Démarre le worker."""
        self.status = WorkerStatus.RUNNING
        self._stop_event.clear()
        self.logger.info(f"Worker {self.worker_id} démarré")
    
    async def stop(self):
        """Arrête le worker."""
        self.status = WorkerStatus.STOPPED
        self._stop_event.set()
        self.logger.info(f"Worker {self.worker_id} arrêté")
    
    async def pause(self):
        """Met le worker en pause."""
        self.status = WorkerStatus.PAUSED
        self.logger.info(f"Worker {self.worker_id} en pause")
    
    async def resume(self):
        """Reprend l'exécution du worker."""
        self.status = WorkerStatus.RUNNING
        self.logger.info(f"Worker {self.worker_id} repris")
    
    async def health_check(self) -> Dict[str, Any]:
        """Vérifie l'état de santé du worker."""
        return {
            "worker_id": self.worker_id,
            "status": self.status.value,
            "current_task": self._current_task.task_id if self._current_task else None,
            "stats": self._stats.copy(),
            "uptime": self._stats.get("uptime", 0),
            "memory_usage": self._get_memory_usage(),
            "cpu_usage": self._get_cpu_usage()
        }
    
    def get_stats(self) -> Dict[str, Any]:
        """Retourne les statistiques du worker."""
        return self._stats.copy()
    
    def _get_memory_usage(self) -> float:
        """Récupère l'utilisation mémoire du processus."""
        try:
            import psutil
            process = psutil.Process()
            return process.memory_percent()
        except ImportError:
            return 0.0
        except Exception:
            return 0.0
    
    def _get_cpu_usage(self) -> float:
        """Récupère l'utilisation CPU du processus."""
        try:
            import psutil
            process = psutil.Process()
            return process.cpu_percent(interval=0.1)
        except ImportError:
            return 0.0
        except Exception:
            return 0.0
    
    async def run_health_check_loop(self):
        """Boucle de vérification de santé périodique."""
        while not self._stop_event.is_set():
            try:
                health = await self.health_check()
                self.logger.debug(f"Health check: {health}")
            except Exception as e:
                self.logger.error(f"Erreur lors du health check: {str(e)}")
            
            await asyncio.sleep(self.health_check_interval)
    
    def __repr__(self) -> str:
        return f"<Worker {self.worker_id} - {self.status.value}>"