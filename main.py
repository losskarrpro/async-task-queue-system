import asyncio
import sys
import argparse
from typing import Optional

from core.queue_manager import QueueManager
from core.result_store import ResultStore
from workers.worker_pool import WorkerPool
from utils.logger import setup_logger
from config.settings import settings

logger = setup_logger(__name__)


async def start_api():
    """Démarre le serveur API web"""
    try:
        from api.app import app
        import uvicorn
        
        logger.info(f"Démarrage de l'API sur http://{settings.API_HOST}:{settings.API_PORT}")
        config = uvicorn.Config(
            app,
            host=settings.API_HOST,
            port=settings.API_PORT,
            log_level=settings.LOG_LEVEL.lower(),
            reload=settings.DEBUG
        )
        server = uvicorn.Server(config)
        await server.serve()
    except ImportError as e:
        logger.error(f"Impossible d'importer l'API: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Erreur lors du démarrage de l'API: {e}")
        sys.exit(1)


async def start_workers(num_workers: int = None):
    """Démarre le pool de workers"""
    try:
        if num_workers is None:
            num_workers = settings.WORKER_COUNT
        
        queue_manager = QueueManager()
        result_store = ResultStore()
        
        worker_pool = WorkerPool(
            queue_manager=queue_manager,
            result_store=result_store,
            worker_count=num_workers
        )
        
        logger.info(f"Démarrage de {num_workers} workers...")
        await worker_pool.start()
        
        # Attendre indéfiniment (ou jusqu'à interruption)
        while True:
            await asyncio.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("Arrêt des workers...")
        if 'worker_pool' in locals():
            await worker_pool.stop()
    except Exception as e:
        logger.error(f"Erreur lors du démarrage des workers: {e}")
        sys.exit(1)


async def start_all():
    """Démarre à la fois l'API et les workers"""
    import threading
    
    # Démarrer les workers dans un thread séparé
    worker_thread = threading.Thread(
        target=lambda: asyncio.run(start_workers()),
        daemon=True
    )
    worker_thread.start()
    
    # Démarrer l'API dans le thread principal
    await start_api()


def start_cli():
    """Démarre l'interface CLI"""
    try:
        from cli.main import cli
        cli()
    except ImportError as e:
        logger.error(f"Impossible d'importer le CLI: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Erreur lors du démarrage du CLI: {e}")
        sys.exit(1)


def main():
    """Point d'entrée principal de l'application"""
    parser = argparse.ArgumentParser(
        description="Système de file de tâche asynchrone"
    )
    
    parser.add_argument(
        'mode',
        choices=['api', 'workers', 'all', 'cli'],
        help="Mode de démarrage: api (serveur web), workers (traitement), all (api + workers), cli (interface ligne de commande)"
    )
    
    parser.add_argument(
        '--workers',
        type=int,
        default=None,
        help="Nombre de workers à démarrer (par défaut: valeur de la configuration)"
    )
    
    parser.add_argument(
        '--config',
        type=str,
        default=None,
        help="Chemin vers un fichier de configuration"
    )
    
    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        default=None,
        help="Niveau de log"
    )
    
    args = parser.parse_args()
    
    # Configurer le niveau de log si spécifié
    if args.log_level:
        import logging
        logging.getLogger().setLevel(args.log_level)
        logger.setLevel(args.log_level)
    
    # Charger la configuration personnalisée si spécifiée
    if args.config:
        try:
            settings.load_from_file(args.config)
            logger.info(f"Configuration chargée depuis {args.config}")
        except Exception as e:
            logger.error(f"Erreur lors du chargement de la configuration: {e}")
            sys.exit(1)
    
    # Démarrer le mode sélectionné
    if args.mode == 'api':
        asyncio.run(start_api())
    elif args.mode == 'workers':
        asyncio.run(start_workers(args.workers))
    elif args.mode == 'all':
        asyncio.run(start_all())
    elif args.mode == 'cli':
        start_cli()


if __name__ == "__main__":
    main()