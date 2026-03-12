import click
import asyncio
from typing import Dict, Any, Optional
from datetime import datetime

from core.queue_manager import QueueManager
from core.exceptions import QueueError, QueueNotFoundError
from utils.logger import setup_logger
from utils.helpers import format_bytes, format_timedelta
from config.settings import get_settings

logger = setup_logger(__name__)

@click.group(name="queue", help="Manage task queues")
def queue_group():
    """Queue management commands."""
    pass

async def _get_queue_manager() -> QueueManager:
    """Get initialized QueueManager instance."""
    settings = get_settings()
    return QueueManager(settings)

@queue_group.command(name="list", help="List all available queues")
@click.option("--verbose", "-v", is_flag=True, help="Show detailed queue information")
@click.option("--active-only", "-a", is_flag=True, help="Show only active queues")
def list_queues(verbose: bool, active_only: bool):
    """List all task queues."""
    try:
        async def run():
            qm = await _get_queue_manager()
            queues = await qm.list_queues()
            
            if not queues:
                click.echo("No queues found.")
                return
            
            if active_only:
                queues = {name: info for name, info in queues.items() if info.get("active", False)}
            
            if verbose:
                click.echo(f"{'Queue Name':<30} {'Type':<15} {'Size':<8} {'Capacity':<10} {'Active':<8} {'Pending':<8} {'Processing':<10}")
                click.echo("-" * 100)
                for name, info in sorted(queues.items()):
                    click.echo(
                        f"{name:<30} "
                        f"{info.get('type', 'UNKNOWN'):<15} "
                        f"{info.get('size', 0):<8} "
                        f"{info.get('capacity', 'unlimited'):<10} "
                        f"{info.get('active', False):<8} "
                        f"{info.get('pending', 0):<8} "
                        f"{info.get('processing', 0):<10}"
                    )
            else:
                click.echo(f"{'Queue Name':<30} {'Type':<15} {'Size':<8} {'Active':<8}")
                click.echo("-" * 70)
                for name, info in sorted(queues.items()):
                    click.echo(
                        f"{name:<30} "
                        f"{info.get('type', 'UNKNOWN'):<15} "
                        f"{info.get('size', 0):<8} "
                        f"{info.get('active', False):<8}"
                    )
            
            click.echo(f"\nTotal queues: {len(queues)}")
        
        asyncio.run(run())
    except Exception as e:
        logger.error(f"Error listing queues: {e}")
        click.echo(f"Error: {e}", err=True)

@queue_group.command(name="info", help="Show detailed information about a queue")
@click.argument("queue_name")
@click.option("--show-tasks", "-t", is_flag=True, help="Show tasks in the queue")
@click.option("--limit", default=20, help="Limit number of tasks shown")
def queue_info(queue_name: str, show_tasks: bool, limit: int):
    """Display detailed information about a specific queue."""
    try:
        async def run():
            qm = await _get_queue_manager()
            
            try:
                info = await qm.get_queue_info(queue_name)
            except QueueNotFoundError:
                click.echo(f"Queue '{queue_name}' not found.", err=True)
                return
            
            # Display basic queue information
            click.echo(f"\nQueue: {queue_name}")
            click.echo("=" * 50)
            click.echo(f"Type: {info.get('type', 'UNKNOWN')}")
            click.echo(f"Status: {'ACTIVE' if info.get('active') else 'PAUSED'}")
            click.echo(f"Size: {info.get('size', 0)}")
            click.echo(f"Capacity: {info.get('capacity', 'unlimited')}")
            click.echo(f"Pending tasks: {info.get('pending', 0)}")
            click.echo(f"Processing tasks: {info.get('processing', 0)}")
            click.echo(f"Completed tasks: {info.get('completed', 0)}")
            click.echo(f"Failed tasks: {info.get('failed', 0)}")
            
            # Display statistics if available
            stats = info.get('statistics', {})
            if stats:
                click.echo("\nStatistics:")
                click.echo(f"  Average processing time: {format_timedelta(stats.get('avg_processing_time', 0))}")
                click.echo(f"  Success rate: {stats.get('success_rate', 0):.2%}")
                click.echo(f"  Total processed: {stats.get('total_processed', 0)}")
                click.echo(f"  Throughput: {stats.get('throughput', 0):.2f} tasks/second")
            
            # Display workers if available
            workers = info.get('workers', [])
            if workers:
                click.echo(f"\nWorkers ({len(workers)}):")
                for worker in workers:
                    status = worker.get('status', 'UNKNOWN')
                    click.echo(f"  - {worker.get('id', 'unknown')}: {status}")
            
            # Display tasks if requested
            if show_tasks:
                tasks = await qm.get_queue_tasks(queue_name, limit=limit)
                if tasks:
                    click.echo(f"\nRecent tasks (showing {len(tasks)} of {info.get('size', 0)}):")
                    click.echo(f"{'ID':<15} {'Status':<12} {'Priority':<8} {'Created':<20}")
                    click.echo("-" * 60)
                    for task in tasks:
                        created = task.get('created_at', '')
                        if created:
                            created = datetime.fromisoformat(created).strftime("%Y-%m-%d %H:%M:%S")
                        click.echo(
                            f"{task.get('id', '')[:15]:<15} "
                            f"{task.get('status', ''):<12} "
                            f"{task.get('priority', 0):<8} "
                            f"{created:<20}"
                        )
                else:
                    click.echo("\nNo tasks in queue.")
        
        asyncio.run(run())
    except Exception as e:
        logger.error(f"Error getting queue info: {e}")
        click.echo(f"Error: {e}", err=True)

@queue_group.command(name="create", help="Create a new queue")
@click.argument("queue_name")
@click.option("--type", "-t", type=click.Choice(['fifo', 'lifo', 'priority']), 
              default="fifo", help="Queue type")
@click.option("--capacity", "-c", type=int, default=0, 
              help="Maximum queue capacity (0 for unlimited)")
@click.option("--description", "-d", help="Queue description")
@click.option("--auto-start", "-s", is_flag=True, 
              help="Automatically start the queue")
def create_queue(queue_name: str, type: str, capacity: int, 
                description: Optional[str], auto_start: bool):
    """Create a new task queue."""
    try:
        async def run():
            qm = await _get_queue_manager()
            
            try:
                queue_config = {
                    "type": type,
                    "capacity": capacity if capacity > 0 else None,
                    "description": description,
                    "auto_start": auto_start
                }
                
                await qm.create_queue(queue_name, **queue_config)
                click.echo(f"Queue '{queue_name}' created successfully.")
                
                if auto_start:
                    click.echo("Queue is active and ready for tasks.")
                else:
                    click.echo("Queue is paused. Use 'queue start' to activate it.")
                    
            except QueueError as e:
                click.echo(f"Error creating queue: {e}", err=True)
        
        asyncio.run(run())
    except Exception as e:
        logger.error(f"Error creating queue: {e}")
        click.echo(f"Error: {e}", err=True)

@queue_group.command(name="delete", help="Delete a queue")
@click.argument("queue_name")
@click.option("--force", "-f", is_flag=True, 
              help="Force delete even if queue has pending tasks")
def delete_queue(queue_name: str, force: bool):
    """Delete a task queue."""
    try:
        async def run():
            qm = await _get_queue_manager()
            
            if not force:
                # Check if queue has pending tasks
                try:
                    info = await qm.get_queue_info(queue_name)
                    if info.get('pending', 0) > 0 or info.get('processing', 0) > 0:
                        click.confirm(
                            f"Queue '{queue_name}' has pending or processing tasks. "
                            f"Are you sure you want to delete it?",
                            abort=True
                        )
                except QueueNotFoundError:
                    click.echo(f"Queue '{queue_name}' not found.", err=True)
                    return
            
            try:
                await qm.delete_queue(queue_name)
                click.echo(f"Queue '{queue_name}' deleted successfully.")
            except QueueError as e:
                click.echo(f"Error deleting queue: {e}", err=True)
        
        asyncio.run(run())
    except Exception as e:
        logger.error(f"Error deleting queue: {e}")
        click.echo(f"Error: {e}", err=True)

@queue_group.command(name="start", help="Start a paused queue")
@click.argument("queue_name")
def start_queue(queue_name: str):
    """Start processing tasks in a queue."""
    try:
        async def run():
            qm = await _get_queue_manager()
            
            try:
                await qm.start_queue(queue_name)
                click.echo(f"Queue '{queue_name}' started successfully.")
            except QueueError as e:
                click.echo(f"Error starting queue: {e}", err=True)
        
        asyncio.run(run())
    except Exception as e:
        logger.error(f"Error starting queue: {e}")
        click.echo(f"Error: {e}", err=True)

@queue_group.command(name="pause", help="Pause a queue")
@click.argument("queue_name")
@click.option("--drain", "-d", is_flag=True, 
              help="Drain existing tasks before pausing")
def pause_queue(queue_name: str, drain: bool):
    """Pause task processing in a queue."""
    try:
        async def run():
            qm = await _get_queue_manager()
            
            try:
                if drain:
                    click.echo(f"Draining queue '{queue_name}' before pausing...")
                    # Wait for pending tasks to complete
                    while True:
                        info = await qm.get_queue_info(queue_name)
                        pending = info.get('processing', 0)
                        if pending == 0:
                            break
                        click.echo(f"Waiting for {pending} tasks to complete...")
                        await asyncio.sleep(1)
                
                await qm.pause_queue(queue_name)
                click.echo(f"Queue '{queue_name}' paused successfully.")
            except QueueError as e:
                click.echo(f"Error pausing queue: {e}", err=True)
        
        asyncio.run(run())
    except Exception as e:
        logger.error(f"Error pausing queue: {e}")
        click.echo(f"Error: {e}", err=True)

@queue_group.command(name="clear", help="Clear all tasks from a queue")
@click.argument("queue_name")
@click.option("--status", "-s", multiple=True,
              type=click.Choice(['pending', 'processing', 'completed', 'failed', 'all']),
              default=['pending'], help="Clear tasks with specific status")
@click.option("--force", "-f", is_flag=True, help="Force clear without confirmation")
def clear_queue(queue_name: str, status: list, force: bool):
    """Clear tasks from a queue."""
    try:
        async def run():
            qm = await _get_queue_manager()
            
            # Validate queue exists
            try:
                info = await qm.get_queue_info(queue_name)
            except QueueNotFoundError:
                click.echo(f"Queue '{queue_name}' not found.", err=True)
                return
            
            # Determine which statuses to clear
            if 'all' in status:
                statuses = ['pending', 'processing', 'completed', 'failed']
            else:
                statuses = list(status)
            
            # Count tasks to be cleared
            task_counts = []
            for s in statuses:
                count = info.get(s, 0)
                if count > 0:
                    task_counts.append(f"{count} {s}")
            
            if not task_counts:
                click.echo(f"No tasks to clear in queue '{queue_name}'.")
                return
            
            # Confirm action
            if not force:
                click.confirm(
                    f"Clear {', '.join(task_counts)} tasks from queue '{queue_name}'?",
                    abort=True
                )
            
            try:
                cleared = await qm.clear_queue(queue_name, statuses=statuses)
                click.echo(f"Cleared {cleared} tasks from queue '{queue_name}'.")
            except QueueError as e:
                click.echo(f"Error clearing queue: {e}", err=True)
        
        asyncio.run(run())
    except Exception as e:
        logger.error(f"Error clearing queue: {e}")
        click.echo(f"Error: {e}", err=True)

@queue_group.command(name="stats", help="Show queue statistics")
@click.argument("queue_name")
@click.option("--reset", "-r", is_flag=True, help="Reset statistics after showing")
def queue_stats(queue_name: str, reset: bool):
    """Display detailed statistics for a queue."""
    try:
        async def run():
            qm = await _get_queue_manager()
            
            try:
                stats = await qm.get_queue_stats(queue_name, reset=reset)
                
                click.echo(f"\nStatistics for queue: {queue_name}")
                click.echo("=" * 50)
                
                # Performance metrics
                click.echo("\nPerformance Metrics:")
                click.echo(f"  Total processed: {stats.get('total_processed', 0)}")
                click.echo(f"  Success rate: {stats.get('success_rate', 0):.2%}")
                click.echo(f"  Average processing time: {format_timedelta(stats.get('avg_processing_time', 0))}")
                click.echo(f"  Min processing time: {format_timedelta(stats.get('min_processing_time', 0))}")
                click.echo(f"  Max processing time: {format_timedelta(stats.get('max_processing_time', 0))}")
                click.echo(f"  Throughput: {stats.get('throughput', 0):.2f} tasks/second")
                
                # Task status breakdown
                click.echo("\nTask Status Breakdown:")
                status_counts = stats.get('status_counts', {})
                for status, count in sorted(status_counts.items()):
                    percentage = (count / stats.get('total_tasks', 1)) * 100
                    click.echo(f"  {status.title():<12}: {count:<8} ({percentage:.1f}%)")
                
                # Error statistics
                errors = stats.get('error_counts', {})
                if errors:
                    click.echo("\nError Statistics:")
                    for error_type, count in sorted(errors.items(), key=lambda x: x[1], reverse=True):
                        click.echo(f"  {error_type:<30}: {count}")
                
                # Timeline information
                click.echo("\nTimeline:")
                created = stats.get('created_at')
                if created:
                    click.echo(f"  Created: {datetime.fromisoformat(created).strftime('%Y-%m-%d %H:%M:%S')}")
                last_processed = stats.get('last_processed_at')
                if last_processed:
                    click.echo(f"  Last processed: {datetime.fromisoformat(last_processed).strftime('%Y-%m-%d %H:%M:%S')}")
                
                if reset:
                    click.echo("\nStatistics have been reset.")
                    
            except QueueError as e:
                click.echo(f"Error getting queue stats: {e}", err=True)
        
        asyncio.run(run())
    except Exception as e:
        logger.error(f"Error getting queue stats: {e}")
        click.echo(f"Error: {e}", err=True)

@queue_group.command(name="update", help="Update queue configuration")
@click.argument("queue_name")
@click.option("--capacity", "-c", type=int, help="New queue capacity")
@click.option("--description", "-d", help="New queue description")
@click.option("--max-retries", "-r", type=int, help="Maximum retry attempts for failed tasks")
@click.option("--timeout", "-t", type=int, help="Task timeout in seconds")
def update_queue(queue_name: str, capacity: Optional[int], description: Optional[str],
                max_retries: Optional[int], timeout: Optional[int]):
    """Update queue configuration."""
    try:
        async def run():
            qm = await _get_queue_manager()
            
            # Build update dictionary
            updates = {}
            if capacity is not None:
                updates['capacity'] = capacity if capacity > 0 else None
            if description is not None:
                updates['description'] = description
            if max_retries is not None:
                updates['max_retries'] = max_retries
            if timeout is not None:
                updates['timeout'] = timeout
            
            if not updates:
                click.echo("No updates specified. Use --help to see available options.")
                return
            
            try:
                await qm.update_queue(queue_name, **updates)
                click.echo(f"Queue '{queue_name}' updated successfully.")
                
                # Show updated configuration
                if click.confirm("Show updated queue info?", default=False):
                    info = await qm.get_queue_info(queue_name)
                    for key, value in updates.items():
                        click.echo(f"  {key}: {info.get(key, 'N/A')}")
                        
            except QueueError as e:
                click.echo(f"Error updating queue: {e}", err=True)
        
        asyncio.run(run())
    except Exception as e:
        logger.error(f"Error updating queue: {e}")
        click.echo(f"Error: {e}", err=True)