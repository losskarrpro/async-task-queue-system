import importlib
import pkgutil
from typing import Dict, Type, Optional, List
from tasks.base_task import BaseTask
from core.exceptions import TaskNotFoundError, TaskRegistrationError
from utils.logger import get_logger

logger = get_logger(__name__)

class TaskRegistry:
    """Registry for managing available task classes."""
    
    _instance = None
    _tasks: Dict[str, Type[BaseTask]] = {}
    _initialized = False
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not self._initialized:
            self._tasks = {}
            self._initialized = True
    
    def register(self, task_name: str, task_class: Type[BaseTask]) -> None:
        """Register a task class with the given name."""
        if not issubclass(task_class, BaseTask):
            raise TaskRegistrationError(
                f"Task class {task_class.__name__} must inherit from BaseTask"
            )
        
        if task_name in self._tasks:
            logger.warning(f"Task '{task_name}' is already registered. Overwriting.")
        
        self._tasks[task_name] = task_class
        logger.debug(f"Registered task '{task_name}' -> {task_class.__name__}")
    
    def get(self, task_name: str) -> Type[BaseTask]:
        """Get a task class by name."""
        if task_name not in self._tasks:
            raise TaskNotFoundError(f"Task '{task_name}' not found in registry")
        return self._tasks[task_name]
    
    def unregister(self, task_name: str) -> None:
        """Unregister a task by name."""
        if task_name in self._tasks:
            del self._tasks[task_name]
            logger.debug(f"Unregistered task '{task_name}'")
    
    def get_all(self) -> Dict[str, Type[BaseTask]]:
        """Get all registered tasks."""
        return self._tasks.copy()
    
    def list_names(self) -> List[str]:
        """List all registered task names."""
        return list(self._tasks.keys())
    
    def clear(self) -> None:
        """Clear all registered tasks."""
        self._tasks.clear()
        logger.debug("Cleared all tasks from registry")
    
    def register_from_module(self, module_name: str) -> None:
        """Register all tasks from a module."""
        try:
            module = importlib.import_module(module_name)
            for attr_name in dir(module):
                attr = getattr(module, attr_name)
                if (isinstance(attr, type) and 
                    issubclass(attr, BaseTask) and 
                    attr is not BaseTask):
                    # Use the class name as default task name
                    task_name = getattr(attr, 'task_name', attr_name)
                    self.register(task_name, attr)
        except ImportError as e:
            logger.error(f"Failed to import module {module_name}: {e}")
            raise TaskRegistrationError(f"Cannot import module {module_name}")
    
    def auto_discover(self, package_name: str = "tasks") -> None:
        """Auto-discover and register tasks from a package."""
        try:
            package = importlib.import_module(package_name)
            for _, module_name, is_pkg in pkgutil.iter_modules(package.__path__):
                full_module_name = f"{package_name}.{module_name}"
                if not is_pkg and module_name not in ["base_task", "registry"]:
                    try:
                        self.register_from_module(full_module_name)
                    except TaskRegistrationError:
                        continue
        except ImportError as e:
            logger.error(f"Failed to auto-discover tasks from {package_name}: {e}")
            raise TaskRegistrationError(f"Cannot auto-discover tasks from {package_name}")


# Create global registry instance
registry = TaskRegistry()


def register_task(name: Optional[str] = None):
    """Decorator to register a task class."""
    def decorator(task_class: Type[BaseTask]) -> Type[BaseTask]:
        task_name = name or getattr(task_class, 'task_name', task_class.__name__)
        registry.register(task_name, task_class)
        return task_class
    return decorator


def get_task_class(task_name: str) -> Type[BaseTask]:
    """Helper function to get a task class from the global registry."""
    return registry.get(task_name)


def list_available_tasks() -> List[str]:
    """Helper function to list all available task names."""
    return registry.list_names()