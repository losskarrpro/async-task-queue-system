import argparse
import asyncio
import logging
import sys
import signal

from scripts.start_api import start_api_server
from scripts.start_workers import start_worker_pool
from config.settings import settings
from utils.logger import setup_logging


def signal_handler(sig, frame):
    """Handle shutdown signals gracefully"""
    logging.info("Shutdown signal received, shutting down...")
    sys.exit(0)


async def run_api(host: str, port: int, debug: bool):
    """Run the API server"""
    logging.info(f"Starting API server on {host}:{port}")
    await start_api_server(host=host, port=port, debug=debug)


async def run_workers(num_workers: int, queue_type: str):
    """Run worker pool"""
    logging.info(f"Starting {num_workers} workers for queue type: {queue_type}")
    await start_worker_pool(num_workers=num_workers, queue_type=queue_type)


async def run_all(host: str, port: int, num_workers: int, queue_type: str, debug: bool):
    """Run both API server and workers concurrently"""
    api_task = asyncio.create_task(run_api(host, port, debug))
    workers_task = asyncio.create_task(run_workers(num_workers, queue_type))
    
    await asyncio.gather(api_task, workers_task)


def main():
    """Main entry point for the application"""
    parser = argparse.ArgumentParser(
        description="Async Task Queue System",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")
    
    # API command
    api_parser = subparsers.add_parser("api", help="Start the API server")
    api_parser.add_argument(
        "--host",
        type=str,
        default="0.0.0.0",
        help="Host to bind the API server (default: 0.0.0.0)"
    )
    api_parser.add_argument(
        "--port",
        type=int,
        default=7500,
        help="Port to bind the API server (default: 7500)"
    )
    api_parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug mode"
    )
    
    # Workers command
    workers_parser = subparsers.add_parser("workers", help="Start worker pool")
    workers_parser.add_argument(
        "--num-workers",
        type=int,
        default=4,
        help="Number of workers to start (default: 4)"
    )
    workers_parser.add_argument(
        "--queue-type",
        type=str,
        choices=["fifo", "lifo", "priority"],
        default="fifo",
        help="Type of queue to process (default: fifo)"
    )
    
    # All command (API + Workers)
    all_parser = subparsers.add_parser("all", help="Start both API server and workers")
    all_parser.add_argument(
        "--host",
        type=str,
        default="0.0.0.0",
        help="Host to bind the API server (default: 0.0.0.0)"
    )
    all_parser.add_argument(
        "--port",
        type=int,
        default=7500,
        help="Port to bind the API server (default: 7500)"
    )
    all_parser.add_argument(
        "--num-workers",
        type=int,
        default=4,
        help="Number of workers to start (default: 4)"
    )
    all_parser.add_argument(
        "--queue-type",
        type=str,
        choices=["fifo", "lifo", "priority"],
        default="fifo",
        help="Type of queue to process (default: fifo)"
    )
    all_parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug mode"
    )
    
    # CLI command
    subparsers.add_parser("cli", help="Start the CLI interface")
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(level=logging.DEBUG if getattr(args, "debug", False) else logging.INFO)
    
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    if args.command == "api":
        asyncio.run(run_api(args.host, args.port, args.debug))
    elif args.command == "workers":
        asyncio.run(run_workers(args.num_workers, args.queue_type))
    elif args.command == "all":
        asyncio.run(run_all(args.host, args.port, args.num_workers, args.queue_type, args.debug))
    elif args.command == "cli":
        from cli.main import main as cli_main
        cli_main()
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()