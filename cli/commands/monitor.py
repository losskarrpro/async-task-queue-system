import asyncio
import json
import sys
from datetime import datetime
from typing import Optional, List

import click

from core.queue_manager import QueueManager
from core.result_store import ResultStore
from utils.logger import get_logger
from workers.worker_pool import WorkerPool
from cli.utils.formatters import format_task_table, format_queue_table, format_worker_table, format_task_details

logger = get_logger(__name__)

def get_queue_manager() -> QueueManager:
    """Get the global queue manager instance."""
    from config.settings import settings
    from core.queue_manager import QueueManager
    return QueueManager.get_instance()

def get_result_store() -> ResultStore:
    """Get the global result store instance."""
    from config.settings import settings
    from core.result_store import ResultStore
    return ResultStore.get_instance()

def get_worker_pool() -> Optional[WorkerPool]:
    """Get the global worker pool instance if available."""
    try:
        from workers.worker_pool import WorkerPool
        return WorkerPool.get_instance()
    except:
        return None

@click.group()
def monitor():
    """Monitor tasks, queues, and workers."""
    pass

@monitor.command()
@click.option('--queue', '-q', help='Filter by queue name.')
@click.option('--status', '-s', help='Filter by task status.')
@click.option('--limit', '-l', default=50, help='Limit number of tasks to display.')
@click.option('--json', 'json_output', is_flag=True, help='Output as JSON.')
def tasks(queue: Optional[str], status: Optional[str], limit: int, json_output: bool):
    """List tasks with optional filtering."""
    try:
        result_store = get_result_store()
        all_tasks = result_store.get_all_tasks()
        
        filtered = []
        for task in all_tasks:
            if queue and task.queue != queue:
                continue
            if status and task.status != status:
                continue
            filtered.append(task)
        
        filtered = filtered[:limit]
        
        if json_output:
            tasks_data = [task.to_dict() for task in filtered]
            click.echo(json.dumps(tasks_data, indent=2, default=str))
        else:
            if not filtered:
                click.echo("No tasks found.")
                return
            
            table = format_task_table(filtered)
            click.echo(table)
            
    except Exception as e:
        logger.error(f"Failed to list tasks: {e}")
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)

@monitor.command()
@click.argument('task_id')
@click.option('--watch', '-w', is_flag=True, help='Watch task in real-time.')
@click.option('--interval', '-i', default=2.0, help='Polling interval in seconds for watch mode.')
def task(task_id: str, watch: bool, interval: float):
    """Show details for a specific task."""
    
    def display_task_details(task):
        details = format_task_details(task)
        click.echo(details)
        if task.result:
            click.echo(f"\nResult: {task.result}")
        if task.error:
            click.echo(f"\nError: {task.error}")
    
    try:
        result_store = get_result_store()
        
        if watch:
            click.echo(f"Watching task {task_id} (Ctrl+C to stop)...")
            last_status = None
            
            try:
                while True:
                    task = result_store.get_task(task_id)
                    if not task:
                        click.echo(f"Task {task_id} not found.")
                        break
                    
                    if task.status != last_status:
                        click.clear()
                        display_task_details(task)
                        last_status = task.status
                    
                    if task.status in ['completed', 'failed', 'cancelled']:
                        click.echo(f"\nTask reached final status: {task.status}")
                        break
                    
                    asyncio.sleep(interval)
            except KeyboardInterrupt:
                click.echo("\nStopped watching.")
        else:
            task = result_store.get_task(task_id)
            if not task:
                click.echo(f"Task {task_id} not found.", err=True)
                sys.exit(1)
            
            display_task_details(task)
            
    except Exception as e:
        logger.error(f"Failed to get task details: {e}")
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)

@monitor.command()
@click.option('--all', '-a', is_flag=True, help='Show all queues including empty ones.')
@click.option('--json', 'json_output', is_flag=True, help='Output as JSON.')
def queues(all: bool, json_output: bool):
    """Display queue status and statistics."""
    try:
        queue_manager = get_queue_manager()
        queues_info = queue_manager.get_queue_stats()
        
        if not all:
            queues_info = {k: v for k, v in queues_info.items() if v['size'] > 0 or v['processing'] > 0}
        
        if json_output:
            click.echo(json.dumps(queues_info, indent=2, default=str))
        else:
            if not queues_info:
                click.echo("No active queues found.")
                return
            
            table = format_queue_table(queues_info)
            click.echo(table)
            
    except Exception as e:
        logger.error(f"Failed to get queue status: {e}")
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)

@monitor.command()
@click.option('--json', 'json_output', is_flag=True, help='Output as JSON.')
def workers(json_output: bool):
    """Display worker pool status."""
    try:
        worker_pool = get_worker_pool()
        if not worker_pool:
            click.echo("Worker pool not available.")
            return
        
        workers_info = worker_pool.get_worker_stats()
        
        if json_output:
            click.echo(json.dumps(workers_info, indent=2, default=str))
        else:
            table = format_worker_table(workers_info)
            click.echo(table)
            
    except Exception as e:
        logger.error(f"Failed to get worker status: {e}")
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)

@monitor.command()
@click.option('--interval', '-i', default=5.0, help='Refresh interval in seconds.')
@click.option('--queues', '-q', is_flag=True, help='Include queue statistics.')
@click.option('--workers', '-w', is_flag=True, help='Include worker statistics.')
def dashboard(interval: float, queues: bool, workers_flag: bool):
    """Display real-time monitoring dashboard."""
    try:
        import time
        from blessed import Terminal
        
        term = Terminal()
        click.echo("Starting monitoring dashboard (Ctrl+C to exit)...")
        
        with term.fullscreen():
            with term.cbreak():
                try:
                    while True:
                        # Clear screen
                        print(term.home + term.clear)
                        
                        # Get data
                        result_store = get_result_store()
                        queue_manager = get_queue_manager()
                        worker_pool = get_worker_pool()
                        
                        # Summary
                        all_tasks = result_store.get_all_tasks()
                        total_tasks = len(all_tasks)
                        completed = sum(1 for t in all_tasks if t.status == 'completed')
                        pending = sum(1 for t in all_tasks if t.status == 'pending')
                        processing = sum(1 for t in all_tasks if t.status == 'processing')
                        failed = sum(1 for t in all_tasks if t.status == 'failed')
                        
                        print(term.bold("ASYNC TASK QUEUE SYSTEM - MONITORING DASHBOARD"))
                        print(f"Last update: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                        print(f"Refresh interval: {interval}s")
                        print("\n" + "=" * 80)
                        
                        # Task summary
                        print(term.bold("\nTASK SUMMARY"))
                        print(f"Total tasks: {total_tasks}")
                        print(f"Completed: {completed} | Pending: {pending} | Processing: {processing} | Failed: {failed}")
                        
                        # Queue statistics
                        if queues:
                            print(term.bold("\nQUEUE STATISTICS"))
                            queue_stats = queue_manager.get_queue_stats()
                            for qname, stats in queue_stats.items():
                                if stats['size'] > 0 or stats['processing'] > 0:
                                    print(f"  {qname}: {stats['size']} pending, {stats['processing']} processing")
                        
                        # Worker statistics
                        if workers_flag and worker_pool:
                            print(term.bold("\nWORKER STATISTICS"))
                            worker_stats = worker_pool.get_worker_stats()
                            print(f"Active workers: {worker_stats['active_workers']}")
                            print(f"Idle workers: {worker_stats['idle_workers']}")
                            print(f"Tasks processed: {worker_stats['total_processed']}")
                        
                        print("\n" + "=" * 80)
                        print(term.bold("Press Ctrl+C to exit"))
                        
                        time.sleep(interval)
                        
                except KeyboardInterrupt:
                    pass
        
    except ImportError:
        click.echo("Dashboard requires 'blessed' package. Install with: pip install blessed")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Dashboard error: {e}")
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)

@monitor.command()
@click.option('--recent', '-r', default=10, help='Number of recent tasks to show.')
@click.option('--failed', '-f', is_flag=True, help='Show only failed tasks.')
def history(recent: int, failed: bool):
    """Show recent task history."""
    try:
        result_store = get_result_store()
        all_tasks = result_store.get_all_tasks()
        
        # Sort by creation time (newest first)
        all_tasks.sort(key=lambda t: t.created_at, reverse=True)
        
        filtered = []
        for task in all_tasks:
            if failed and task.status != 'failed':
                continue
            filtered.append(task)
            if len(filtered) >= recent:
                break
        
        if not filtered:
            click.echo("No tasks found.")
            return
        
        table = format_task_table(filtered)
        click.echo(table)
        
    except Exception as e:
        logger.error(f"Failed to get task history: {e}")
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)

@monitor.command()
@click.option('--queue', '-q', help='Queue name to get metrics for.')
@click.option('--period', '-p', default='hour', type=click.Choice(['hour', 'day', 'week']), help='Time period for metrics.')
def metrics(queue: Optional[str], period: str):
    """Show performance metrics."""
    try:
        result_store = get_result_store()
        metrics_data = result_store.get_metrics(queue, period)
        
        click.echo(f"Metrics for period: {period}")
        if queue:
            click.echo(f"Queue: {queue}")
        
        click.echo(f"\nTotal tasks processed: {metrics_data.get('total_processed', 0)}")
        click.echo(f"Success rate: {metrics_data.get('success_rate', 0):.2%}")
        click.echo(f"Average processing time: {metrics_data.get('avg_processing_time', 0):.2f}s")
        click.echo(f"Tasks per minute: {metrics_data.get('tasks_per_minute', 0):.2f}")
        
        if 'recent_tasks' in metrics_data:
            click.echo("\nRecent tasks:")
            for task in metrics_data['recent_tasks'][:5]:
                click.echo(f"  {task.task_id}: {task.status} ({task.duration:.2f}s)")
        
    except Exception as e:
        logger.error(f"Failed to get metrics: {e}")
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)