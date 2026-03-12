import asyncio
import json
import click
from pathlib import Path
from typing import Dict, Any, Optional

from core.queue_manager import QueueManager
from core.task import Task
from tasks.registry import task_registry
from utils.logger import get_logger
from utils.validators import validate_json
from core.exceptions import TaskNotFoundError, InvalidTaskParametersError

logger = get_logger(__name__)

@click.command()
@click.option('--queue-type', '-t', type=click.Choice(['fifo', 'lifo', 'priority'], case_sensitive=False),
              default='fifo', help='Type of queue to submit task to')
@click.option('--queue-name', '-q', default='default',
              help='Name of the queue (default: "default")')
@click.option('--task-name', '-n', required=True,
              help='Name of the registered task to execute')
@click.option('--params', '-p', type=str,
              help='JSON string of task parameters')
@click.option('--params-file', '-f', type=click.Path(exists=True),
              help='Path to JSON file containing task parameters')
@click.option('--priority', '-P', type=int, default=0,
              help='Priority for priority queue (higher = more important)')
@click.option('--timeout', '-T', type=int, default=None,
              help='Task timeout in seconds')
@click.option('--retry-count', '-r', type=int, default=0,
              help='Number of retry attempts on failure')
@click.option('--tags', '-g', type=str,
              help='Comma-separated tags for task categorization')
def submit(queue_type: str, queue_name: str, task_name: str, params: Optional[str],
           params_file: Optional[str], priority: int, timeout: Optional[int],
           retry_count: int, tags: Optional[str]) -> None:
    """Submit a new task to the task queue."""
    
    try:
        # Validate and load parameters
        task_params = _load_task_parameters(params, params_file)
        
        # Parse tags
        tag_list = [tag.strip() for tag in tags.split(',')] if tags else []
        
        # Submit the task
        task_id = asyncio.run(_submit_task_async(
            queue_type=queue_type,
            queue_name=queue_name,
            task_name=task_name,
            task_params=task_params,
            priority=priority,
            timeout=timeout,
            retry_count=retry_count,
            tags=tag_list
        ))
        
        click.echo(f"✓ Task submitted successfully!")
        click.echo(f"  Task ID: {task_id}")
        click.echo(f"  Queue: {queue_name} ({queue_type})")
        click.echo(f"  Task: {task_name}")
        
    except json.JSONDecodeError:
        click.echo("❌ Error: Invalid JSON in parameters")
        raise click.Abort()
    except TaskNotFoundError as e:
        click.echo(f"❌ Error: {e}")
        click.echo(f"Available tasks: {', '.join(task_registry.list_tasks())}")
        raise click.Abort()
    except InvalidTaskParametersError as e:
        click.echo(f"❌ Error: {e}")
        raise click.Abort()
    except Exception as e:
        logger.exception(f"Failed to submit task: {e}")
        click.echo(f"❌ Error submitting task: {str(e)}")
        raise click.Abort()

def _load_task_parameters(params: Optional[str], params_file: Optional[str]) -> Dict[str, Any]:
    """Load task parameters from either JSON string or file."""
    if params_file:
        with open(params_file, 'r') as f:
            params_content = f.read()
    elif params:
        params_content = params
    else:
        return {}
    
    # Validate JSON
    validate_json(params_content)
    
    # Parse JSON
    return json.loads(params_content)

async def _submit_task_async(
    queue_type: str,
    queue_name: str,
    task_name: str,
    task_params: Dict[str, Any],
    priority: int,
    timeout: Optional[int],
    retry_count: int,
    tags: list
) -> str:
    """Asynchronous task submission."""
    
    # Check if task is registered
    if task_name not in task_registry:
        raise TaskNotFoundError(f"Task '{task_name}' not found in registry")
    
    # Get task function from registry
    task_func = task_registry[task_name]
    
    # Create queue manager
    queue_manager = QueueManager()
    
    # Ensure queue exists
    if not await queue_manager.queue_exists(queue_name, queue_type):
        await queue_manager.create_queue(queue_name, queue_type)
    
    # Create task
    task = Task(
        task_id=None,  # Will be generated
        task_name=task_name,
        task_func=task_func,
        params=task_params,
        queue_name=queue_name,
        queue_type=queue_type,
        priority=priority,
        timeout=timeout,
        max_retries=retry_count,
        tags=tags
    )
    
    # Submit task
    submitted_task = await queue_manager.submit_task(task)
    
    return submitted_task.task_id

@click.command()
@click.argument('task_file', type=click.Path(exists=True))
@click.option('--queue-type', '-t', type=click.Choice(['fifo', 'lifo', 'priority'], case_sensitive=False),
              default='fifo', help='Type of queue to submit task to')
@click.option('--queue-name', '-q', default='default',
              help='Name of the queue (default: "default")')
@click.option('--priority', '-P', type=int, default=0,
              help='Priority for priority queue (higher = more important)')
def submit_file(task_file: str, queue_type: str, queue_name: str, priority: int) -> None:
    """Submit a task defined in a Python file."""
    
    try:
        # Load task from file
        task_module = _load_task_from_file(task_file)
        
        # Submit the task
        task_id = asyncio.run(_submit_file_task_async(
            task_module=task_module,
            queue_type=queue_type,
            queue_name=queue_name,
            priority=priority
        ))
        
        click.echo(f"✓ Task from file submitted successfully!")
        click.echo(f"  Task ID: {task_id}")
        click.echo(f"  File: {task_file}")
        click.echo(f"  Queue: {queue_name} ({queue_type})")
        
    except Exception as e:
        logger.exception(f"Failed to submit task from file: {e}")
        click.echo(f"❌ Error submitting task from file: {str(e)}")
        raise click.Abort()

def _load_task_from_file(task_file: str) -> Any:
    """Dynamically load task from Python file."""
    try:
        import importlib.util
        
        # Load module from file
        spec = importlib.util.spec_from_file_location("task_module", task_file)
        if spec is None or spec.loader is None:
            raise ImportError(f"Could not load module from {task_file}")
        
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        
        # Check if module has a 'task' attribute
        if not hasattr(module, 'task'):
            raise AttributeError("Task file must define a 'task' function or class")
        
        return module.task
        
    except Exception as e:
        raise ImportError(f"Failed to load task from {task_file}: {str(e)}")

async def _submit_file_task_async(
    task_module: Any,
    queue_type: str,
    queue_name: str,
    priority: int
) -> str:
    """Asynchronous file-based task submission."""
    
    # Create queue manager
    queue_manager = QueueManager()
    
    # Ensure queue exists
    if not await queue_manager.queue_exists(queue_name, queue_type):
        await queue_manager.create_queue(queue_name, queue_type)
    
    # Create task
    task = Task(
        task_id=None,  # Will be generated
        task_name=f"file_task_{Path(task_file).stem}",
        task_func=task_module,
        params={},
        queue_name=queue_name,
        queue_type=queue_type,
        priority=priority,
        timeout=None,
        max_retries=0,
        tags=["file_submission"]
    )
    
    # Submit task
    submitted_task = await queue_manager.submit_task(task)
    
    return submitted_task.task_id

@click.command()
@click.option('--file', '-f', type=click.Path(exists=True),
              help='JSON file with multiple task definitions')
@click.option('--queue-type', '-t', type=click.Choice(['fifo', 'lifo', 'priority'], case_sensitive=False),
              default='fifo', help='Type of queue to submit tasks to')
@click.option('--queue-name', '-q', default='default',
              help='Name of the queue (default: "default")')
def submit_batch(file: str, queue_type: str, queue_name: str) -> None:
    """Submit multiple tasks from a batch file."""
    
    try:
        # Load batch file
        with open(file, 'r') as f:
            batch_data = json.load(f)
        
        if not isinstance(batch_data, list):
            raise ValueError("Batch file must contain a JSON array of task definitions")
        
        # Submit all tasks
        task_ids = asyncio.run(_submit_batch_async(
            batch_data=batch_data,
            queue_type=queue_type,
            queue_name=queue_name
        ))
        
        click.echo(f"✓ Successfully submitted {len(task_ids)} tasks!")
        click.echo(f"  Batch file: {file}")
        click.echo(f"  Queue: {queue_name} ({queue_type})")
        click.echo(f"  Task IDs: {', '.join(task_ids)}")
        
    except json.JSONDecodeError:
        click.echo("❌ Error: Invalid JSON in batch file")
        raise click.Abort()
    except Exception as e:
        logger.exception(f"Failed to submit batch: {e}")
        click.echo(f"❌ Error submitting batch: {str(e)}")
        raise click.Abort()

async def _submit_batch_async(
    batch_data: list,
    queue_type: str,
    queue_name: str
) -> list:
    """Asynchronous batch task submission."""
    
    task_ids = []
    queue_manager = QueueManager()
    
    # Ensure queue exists
    if not await queue_manager.queue_exists(queue_name, queue_type):
        await queue_manager.create_queue(queue_name, queue_type)
    
    for task_def in batch_data:
        try:
            # Validate task definition
            if not isinstance(task_def, dict):
                logger.warning(f"Skipping invalid task definition: {task_def}")
                continue
            
            # Check if task is registered
            task_name = task_def.get('task_name')
            if not task_name or task_name not in task_registry:
                logger.warning(f"Skipping unknown task: {task_name}")
                continue
            
            # Get task function
            task_func = task_registry[task_name]
            
            # Create task
            task = Task(
                task_id=None,  # Will be generated
                task_name=task_name,
                task_func=task_func,
                params=task_def.get('params', {}),
                queue_name=queue_name,
                queue_type=queue_type,
                priority=task_def.get('priority', 0),
                timeout=task_def.get('timeout'),
                max_retries=task_def.get('retry_count', 0),
                tags=task_def.get('tags', [])
            )
            
            # Submit task
            submitted_task = await queue_manager.submit_task(task)
            task_ids.append(submitted_task.task_id)
            
        except Exception as e:
            logger.error(f"Failed to submit batch task {task_def}: {e}")
            continue
    
    return task_ids