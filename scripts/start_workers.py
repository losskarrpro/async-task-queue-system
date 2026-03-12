#!/usr/bin/env python3

import asyncio
import argparse
import signal
import sys
from typing import Optional

from workers.worker_pool import WorkerPool
from config.settings import get_settings
from utils.logger import setup_logger

logger = setup_logger(__name__)

class WorkerManager:
    def __init__(self, num_workers: int = 2, queue_name: str = "default"):
        self.settings = get_settings()
        self.num_workers = num_workers
        self.queue_name = queue_name
        self.worker_pool: Optional[WorkerPool] = None

    async def start(self):
        """Start the worker pool"""
        logger.info(f"Starting {self.num_workers} workers for queue '{self.queue_name}'")
        
        self.worker_pool = WorkerPool(
            num_workers=self.num_workers,
            queue_name=self.queue_name
        )
        
        try:
            await self.worker_pool.start()
        except KeyboardInterrupt:
            logger.info("Received shutdown signal")
        except Exception as e:
            logger.error(f"Worker pool error: {e}", exc_info=True)
            raise
        finally:
            await self.shutdown()

    async def shutdown(self):
        """Gracefully shutdown the worker pool"""
        if self.worker_pool:
            logger.info("Shutting down worker pool...")
            await self.worker_pool.stop()
            logger.info("Worker pool stopped")

def parse_args():
    parser = argparse.ArgumentParser(description="Start async task workers")
    parser.add_argument(
        "-w", "--workers",
        type=int,
        default=2,
        help="Number of worker processes (default: 2)"
    )
    parser.add_argument(
        "-q", "--queue",
        type=str,
        default="default",
        help="Queue name to consume tasks from (default: 'default')"
    )
    parser.add_argument(
        "--host",
        type=str,
        help="Redis host (overrides config)"
    )
    parser.add_argument(
        "--port",
        type=int,
        help="Redis port (overrides config)"
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging"
    )
    parser.add_argument(
        "--memory-only",
        action="store_true",
        help="Use in-memory backend only (no Redis)"
    )
    return parser.parse_args()

async def main():
    args = parse_args()
    
    # Override settings if provided
    if args.host or args.port or args.memory_only:
        import os
        if args.host:
            os.environ["REDIS_HOST"] = args.host
        if args.port:
            os.environ["REDIS_PORT"] = str(args.port)
        if args.memory_only:
            os.environ["RESULT_BACKEND"] = "memory"
    
    # Configure logging level
    if args.debug:
        import logging
        logging.getLogger().setLevel(logging.DEBUG)
        logger.setLevel(logging.DEBUG)
    
    worker_manager = WorkerManager(
        num_workers=args.workers,
        queue_name=args.queue
    )
    
    # Setup signal handlers
    loop = asyncio.get_event_loop()
    
    def signal_handler():
        logger.info("Received shutdown signal")
        asyncio.create_task(worker_manager.shutdown())
    
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, signal_handler)
    
    try:
        await worker_manager.start()
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logger.error(f"Failed to start workers: {e}", exc_info=True)
        return 1
    finally:
        # Cleanup signal handlers
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.remove_signal_handler(sig)
    
    return 0

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)