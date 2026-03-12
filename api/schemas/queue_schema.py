from enum import Enum
from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field

from config.settings import QueueType


class QueueTypeEnum(str, Enum):
    fifo = "fifo"
    lifo = "lifo"
    priority = "priority"


class QueueConfig(BaseModel):
    """Configuration for queue creation"""
    max_retries: Optional[int] = Field(default=3, ge=0)
    default_priority: Optional[int] = Field(default=5, ge=1, le=10)
    timeout: Optional[float] = Field(default=None, gt=0)


class QueueCreate(BaseModel):
    """Schema for creating a new queue"""
    name: str = Field(..., min_length=1, max_length=100, pattern=r'^[a-zA-Z0-9_-]+$')
    queue_type: QueueTypeEnum
    max_size: Optional[int] = Field(default=None, ge=1)
    config: Optional[QueueConfig] = None


class QueueUpdate(BaseModel):
    """Schema for updating a queue"""
    max_size: Optional[int] = Field(default=None, ge=1)
    config: Optional[QueueConfig] = None


class QueueStats(BaseModel):
    """Statistics for a queue"""
    total_tasks: int = Field(default=0, ge=0)
    completed_tasks: int = Field(default=0, ge=0)
    failed_tasks: int = Field(default=0, ge=0)
    pending_tasks: int = Field(default=0, ge=0)
    average_processing_time: Optional[float] = Field(default=None, ge=0)


class QueueInfo(BaseModel):
    """Information about a queue"""
    name: str
    queue_type: str
    size: int = Field(ge=0)
    total_tasks: Optional[int] = Field(default=0, ge=0)
    completed_tasks: Optional[int] = Field(default=0, ge=0)
    failed_tasks: Optional[int] = Field(default=0, ge=0)
    pending_tasks: Optional[int] = Field(default=0, ge=0)
    average_processing_time: Optional[float] = Field(default=None, ge=0)

    class Config:
        extra = "allow"  # Allow extra fields from stats dict


class QueueList(BaseModel):
    """List of queues"""
    queues: List[QueueInfo]
    total: int = Field(ge=0)