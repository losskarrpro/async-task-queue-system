import sys
import argparse
import time
from typing import Optional

try:
    import redis
    from redis.exceptions import ConnectionError, TimeoutError
except ImportError:
    print("Redis Python client not installed. Install with: pip install redis")
    sys.exit(1)

try:
    from config.redis_config import REDIS_HOST, REDIS_PORT, REDIS_DB, REDIS_PASSWORD, REDIS_SSL
    from config.redis_config import REDIS_SOCKET_TIMEOUT, REDIS_SOCKET_CONNECT_TIMEOUT
except ImportError:
    REDIS_HOST = "localhost"
    REDIS_PORT = 6379
    REDIS_DB = 0
    REDIS_PASSWORD = None
    REDIS_SSL = False
    REDIS_SOCKET_TIMEOUT = 5
    REDIS_SOCKET_CONNECT_TIMEOUT = 5

def get_redis_connection():
    """Create and return a Redis connection."""
    try:
        conn = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB,
            password=REDIS_PASSWORD,
            ssl=REDIS_SSL,
            socket_timeout=REDIS_SOCKET_TIMEOUT,
            socket_connect_timeout=REDIS_SOCKET_CONNECT_TIMEOUT,
            decode_responses=True
        )
        return conn
    except Exception as e:
        print(f"Error creating Redis connection: {e}")
        return None

def check_connection(retries: int = 3, delay: float = 1.0) -> bool:
    """Check Redis connection with retries."""
    for attempt in range(1, retries + 1):
        print(f"Connection attempt {attempt}/{retries}...")
        conn = get_redis_connection()
        if conn is None:
            return False
        
        try:
            conn.ping()
            print("Redis connection successful.")
            return True
        except (ConnectionError, TimeoutError) as e:
            print(f"Connection failed: {e}")
            if attempt < retries:
                time.sleep(delay)
            continue
        finally:
            if conn:
                conn.close()
    
    return False

def flush_all(confirm: bool = False) -> bool:
    """Flush all Redis data."""
    if not confirm:
        print("WARNING: This will delete ALL data in Redis database.")
        response = input("Type 'yes' to confirm: ")
        if response.lower() != 'yes':
            print("Aborted.")
            return False
    
    conn = get_redis_connection()
    if conn is None:
        return False
    
    try:
        conn.flushdb()
        print("Redis database flushed successfully.")
        return True
    except Exception as e:
        print(f"Error flushing database: {e}")
        return False
    finally:
        if conn:
            conn.close()

def setup_default_keys():
    """Set up default Redis keys and structures."""
    conn = get_redis_connection()
    if conn is None:
        return False
    
    try:
        # Create default queue keys if they don't exist
        default_queues = ["default", "high_priority", "low_priority"]
        for queue in default_queues:
            key = f"queue:{queue}"
            if not conn.exists(key):
                conn.lpush(key, "dummy")  # Add dummy item
                conn.lpop(key)  # Remove it
                print(f"Created queue key: {key}")
        
        # Create results hash key
        results_key = "task_results"
        if not conn.exists(results_key):
            conn.hset(results_key, "initialized", "true")
            print(f"Created results key: {results_key}")
        
        # Create monitoring key
        monitor_key = "queue_monitor"
        if not conn.exists(monitor_key):
            conn.set(monitor_key, "active")
            print(f"Created monitor key: {monitor_key}")
        
        print("Default keys setup completed.")
        return True
    except Exception as e:
        print(f"Error setting up default keys: {e}")
        return False
    finally:
        if conn:
            conn.close()

def show_info():
    """Display Redis server information."""
    conn = get_redis_connection()
    if conn is None:
        return False
    
    try:
        info = conn.info()
        print(f"Redis Server Info:")
        print(f"  Version: {info.get('redis_version', 'N/A')}")
        print(f"  Mode: {info.get('redis_mode', 'N/A')}")
        print(f"  OS: {info.get('os', 'N/A')}")
        print(f"  Connected Clients: {info.get('connected_clients', 'N/A')}")
        print(f"  Used Memory: {info.get('used_memory_human', 'N/A')}")
        print(f"  Database Size: {info.get('db0', 'N/A')}")
        return True
    except Exception as e:
        print(f"Error getting server info: {e}")
        return False
    finally:
        if conn:
            conn.close()

def main():
    parser = argparse.ArgumentParser(description="Redis setup and management for async-task-queue-system")
    parser.add_argument("--check", action="store_true", help="Check Redis connection")
    parser.add_argument("--flush", action="store_true", help="Flush all Redis data (requires confirmation)")
    parser.add_argument("--setup", action="store_true", help="Set up default keys and structures")
    parser.add_argument("--info", action="store_true", help="Display Redis server information")
    parser.add_argument("--force", action="store_true", help="Skip confirmation prompts")
    
    args = parser.parse_args()
    
    if not any(vars(args).values()):
        parser.print_help()
        return
    
    if args.check:
        success = check_connection()
        sys.exit(0 if success else 1)
    
    if args.flush:
        success = flush_all(args.force)
        sys.exit(0 if success else 1)
    
    if args.setup:
        if not check_connection():
            print("Cannot setup keys: Redis connection failed.")
            sys.exit(1)
        success = setup_default_keys()
        sys.exit(0 if success else 1)
    
    if args.info:
        if not check_connection():
            print("Cannot get info: Redis connection failed.")
            sys.exit(1)
        success = show_info()
        sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()