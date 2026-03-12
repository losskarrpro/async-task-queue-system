import os
from enum import Enum


class QueueType(str, Enum):
    FIFO = "fifo"
    LIFO = "lifo"
    PRIORITY = "priority"


class TaskStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMEOUT = "timeout"


# Queue configuration
QUEUE_TYPE = QueueType.FIFO
MAX_QUEUE_SIZE = 1000

# Worker configuration
MAX_WORKERS = 4
WORKER_POLL_INTERVAL = 0.1  # seconds
MAX_RETRIES = 3
WORKER_TIMEOUT = 300  # seconds

# Result store configuration
RESULT_STORE_TYPE = "memory"  # "memory" or "redis"
REDIS_URL = "redis://localhost:6379/0"
RESULT_TTL = 86400  # seconds (24 hours)

# Logging configuration
LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
LOG_FILE = "logs/async_task_queue.log"
LOG_MAX_BYTES = 10485760  # 10MB
LOG_BACKUP_COUNT = 5

# API configuration
API_PORT = 7501
API_HOST = "0.0.0.0"
API_DEBUG = False

# Task configuration
DEFAULT_PRIORITY = 1
MAX_TASK_SIZE = 1024 * 1024  # 1MB
DEFAULT_TIMEOUT = 60  # seconds

# Security
ALLOWED_ORIGINS = ["*"]
API_KEY = os.environ.get("API_KEY", "")

# Monitoring
ENABLE_METRICS = True
METRICS_PORT = 9090
HEALTH_CHECK_INTERVAL = 30  # seconds