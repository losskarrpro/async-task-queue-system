from pydantic import BaseModel, Field
from typing import Any, Dict, Optional, List
from datetime import datetime
from enum import Enum


class TaskStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class QueueType(str, Enum):
    FIFO = "fifo"
    LIFO = "lifo"
    PRIORITY = "priority"


class TaskCreate(BaseModel):
    task_type: str = Field(..., description="Type of task to execute")
    task_data: Dict[str, Any] = Field(default_factory=dict, description="Task data payload")
    priority: int = Field(default=0, ge=0, le=10, description="Task priority (0-10)")
    queue_type: QueueType = Field(default=QueueType.FIFO, description="Type of queue to use")


class TaskUpdate(BaseModel):
    status: Optional[TaskStatus] = Field(None, description="Updated task status")
    result: Optional[Dict[str, Any]] = Field(None, description="Task result data")
    error: Optional[str] = Field(None, description="Error message if task failed")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Additional task metadata")


class TaskResponse(BaseModel):
    task_id: str = Field(..., description="Unique task identifier")
    message: str = Field(..., description="Response message")
    status: str = Field(..., description="Current task status")


class TaskStatusResponse(BaseModel):
    task_id: str = Field(..., description="Unique task identifier")
    status: str = Field(..., description="Current task status")
    queue_position: Optional[int] = Field(None, description="Position in queue if pending")
    estimated_wait_time: Optional[float] = Field(None, description="Estimated wait time in seconds")
    created_at: datetime = Field(..., description="When the task was created")
    updated_at: datetime = Field(..., description="When the task was last updated")


class TaskResultResponse(BaseModel):
    task_id: str = Field(..., description="Unique task identifier")
    status: str = Field(..., description="Task status")
    result: Optional[Dict[str, Any]] = Field(None, description="Task result data")
    error: Optional[str] = Field(None, description="Error message if task failed")
    execution_time: Optional[float] = Field(None, description="Execution time in seconds")
    created_at: datetime = Field(..., description="When the task was created")
    completed_at: Optional[datetime] = Field(None, description="When the task was completed")


class TaskListResponse(BaseModel):
    tasks: List[Dict[str, Any]] = Field(..., description="List of tasks")
    total: int = Field(..., description="Total number of tasks matching filter")
    limit: int = Field(..., description="Maximum number of tasks returned")
    offset: int = Field(..., description="Offset for pagination")
