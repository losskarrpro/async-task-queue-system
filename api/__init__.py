# API package initializer
# Exposes main API components for easy import

# Do not import app here to avoid circular imports
# from api.app import app
# from api.routes import tasks, queue, monitoring

__all__ = [
    "app",
    "tasks",
    "queue",
    "monitoring"
]

__version__ = "1.0.0"