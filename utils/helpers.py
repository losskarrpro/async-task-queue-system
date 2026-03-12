import asyncio
import json
import hashlib
import time
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union
from functools import wraps
import inspect


def generate_task_id() -> str:
    """Generate a unique task ID."""
    return str(uuid.uuid4())


def get_current_timestamp() -> float:
    """Get current timestamp in seconds with millisecond precision."""
    return time.time()


def format_duration(seconds: float) -> str:
    """Format duration in human-readable format."""
    if seconds < 1:
        return f"{seconds * 1000:.0f}ms"
    elif seconds < 60:
        return f"{seconds:.2f}s"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        secs = seconds % 60
        return f"{minutes}m {secs:.0f}s"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = seconds % 60
        return f"{hours}h {minutes}m {secs:.0f}s"


def safe_json_dumps(data: Any) -> str:
    """Safely serialize data to JSON, handling non-serializable types."""
    def default_serializer(obj):
        if isinstance(obj, (datetime,)):
            return obj.isoformat()
        if hasattr(obj, '__dict__'):
            return obj.__dict__
        if hasattr(obj, 'to_dict'):
            return obj.to_dict()
        return str(obj)
    
    return json.dumps(data, default=default_serializer, indent=2)


def safe_json_loads(json_str: str) -> Any:
    """Safely deserialize JSON string."""
    try:
        return json.loads(json_str)
    except (json.JSONDecodeError, TypeError):
        return {}


def calculate_md5(data: str) -> str:
    """Calculate MD5 hash of data."""
    return hashlib.md5(data.encode('utf-8')).hexdigest()


def retry_with_backoff(
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
    exceptions: tuple = (Exception,)
):
    """Decorator for retrying functions with exponential backoff.
    
    Args:
        max_retries: Maximum number of retry attempts
        base_delay: Base delay in seconds for exponential backoff
        max_delay: Maximum delay in seconds
        exceptions: Tuple of exceptions to catch and retry on
    """
    def decorator(func):
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            retries = 0
            delay = base_delay
            
            while retries <= max_retries:
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    retries += 1
                    if retries > max_retries:
                        raise
                    
                    await asyncio.sleep(min(delay, max_delay))
                    delay *= 2  # Exponential backoff
        
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            retries = 0
            delay = base_delay
            
            while retries <= max_retries:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    retries += 1
                    if retries > max_retries:
                        raise
                    
                    time.sleep(min(delay, max_delay))
                    delay *= 2  # Exponential backoff
        
        if inspect.iscoroutinefunction(func):
            return async_wrapper
        return sync_wrapper
    
    return decorator


def chunk_list(lst: List, chunk_size: int) -> List[List]:
    """Split a list into chunks of specified size."""
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]


def flatten_dict(d: Dict, parent_key: str = '', sep: str = '.') -> Dict:
    """Flatten a nested dictionary."""
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def get_size_in_memory(obj: Any) -> int:
    """Get approximate memory size of an object in bytes."""
    import sys
    return sys.getsizeof(obj)


def validate_interval(interval: Union[int, float], min_value: float = 0.1) -> float:
    """Validate and normalize interval value."""
    try:
        interval = float(interval)
        if interval < min_value:
            return min_value
        return interval
    except (TypeError, ValueError):
        return min_value


def parse_datetime_string(dt_str: str) -> Optional[datetime]:
    """Parse datetime from string in various formats."""
    formats = [
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%dT%H:%M:%S',
        '%Y-%m-%d %H:%M:%S.%f',
        '%Y-%m-%dT%H:%M:%S.%f',
        '%Y-%m-%d',
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(dt_str, fmt)
        except ValueError:
            continue
    
    return None


def format_bytes(size: int) -> str:
    """Format bytes to human-readable string."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size < 1024.0:
            return f"{size:.2f} {unit}"
        size /= 1024.0
    return f"{size:.2f} PB"


def timeout_decorator(timeout_seconds: float):
    """Decorator to add timeout to functions."""
    def decorator(func):
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            try:
                return await asyncio.wait_for(
                    func(*args, **kwargs),
                    timeout=timeout_seconds
                )
            except asyncio.TimeoutError:
                raise TimeoutError(f"Function {func.__name__} timed out after {timeout_seconds} seconds")
        
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            # For synchronous functions, we use threading to implement timeout
            import threading
            result = []
            exception = []
            
            def worker():
                try:
                    result.append(func(*args, **kwargs))
                except Exception as e:
                    exception.append(e)
            
            thread = threading.Thread(target=worker)
            thread.daemon = True
            thread.start()
            thread.join(timeout_seconds)
            
            if thread.is_alive():
                raise TimeoutError(f"Function {func.__name__} timed out after {timeout_seconds} seconds")
            
            if exception:
                raise exception[0]
            
            return result[0] if result else None
        
        if inspect.iscoroutinefunction(func):
            return async_wrapper
        return sync_wrapper
    
    return decorator


def rate_limit(requests_per_second: float):
    """Decorator to rate limit function calls."""
    last_call_time = 0
    
    def decorator(func):
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            nonlocal last_call_time
            current_time = time.time()
            elapsed = current_time - last_call_time
            min_interval = 1.0 / requests_per_second
            
            if elapsed < min_interval:
                await asyncio.sleep(min_interval - elapsed)
            
            last_call_time = time.time()
            return await func(*args, **kwargs)
        
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            nonlocal last_call_time
            current_time = time.time()
            elapsed = current_time - last_call_time
            min_interval = 1.0 / requests_per_second
            
            if elapsed < min_interval:
                time.sleep(min_interval - elapsed)
            
            last_call_time = time.time()
            return func(*args, **kwargs)
        
        if inspect.iscoroutinefunction(func):
            return async_wrapper
        return sync_wrapper
    
    return decorator


def memoize(ttl: Optional[float] = None):
    """Memoization decorator with optional TTL."""
    cache = {}
    
    def decorator(func):
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            key = (args, tuple(sorted(kwargs.items())))
            
            if key in cache:
                value, timestamp = cache[key]
                if ttl is None or (time.time() - timestamp) < ttl:
                    return value
            
            result = await func(*args, **kwargs)
            cache[key] = (result, time.time())
            return result
        
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            key = (args, tuple(sorted(kwargs.items())))
            
            if key in cache:
                value, timestamp = cache[key]
                if ttl is None or (time.time() - timestamp) < ttl:
                    return value
            
            result = func(*args, **kwargs)
            cache[key] = (result, time.time())
            return result
        
        if inspect.iscoroutinefunction(func):
            return async_wrapper
        return sync_wrapper
    
    return decorator


def async_to_sync(func):
    """Convert an async function to sync function."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        return asyncio.run(func(*args, **kwargs))
    return wrapper


def sync_to_async(func):
    """Convert a sync function to async function."""
    @wraps(func)
    async def wrapper(*args, **kwargs):
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, lambda: func(*args, **kwargs))
    return wrapper


class SingletonMeta(type):
    """Metaclass for implementing singleton pattern."""
    _instances = {}
    
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]


def deep_merge_dicts(dict1: Dict, dict2: Dict) -> Dict:
    """Deep merge two dictionaries."""
    result = dict1.copy()
    
    for key, value in dict2.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = deep_merge_dicts(result[key], value)
        else:
            result[key] = value
    
    return result