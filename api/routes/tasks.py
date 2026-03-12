from fastapi import APIRouter, HTTPException, Depends, status
from typing import List, Optional
from uuid import UUID

from core.queue_manager import QueueManager
from core.result_store import ResultStore
from core.exceptions import TaskNotFoundError, QueueError
from api.schemas.task_schema import (
    TaskCreate,
    TaskResponse,
    TaskStatusResponse,
    TaskResultResponse,
    TaskListResponse
)
from utils.logger import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/tasks", tags=["tasks"])

def get_queue_manager():
    """Dependency for QueueManager"""
    # This would be injected from the main app
    from main import queue_manager
    return queue_manager

def get_result_store():
    """Dependency for ResultStore"""
    # This would be injected from the main app
    from main import result_store
    return result_store

@router.post("/", response_model=TaskResponse, status_code=status.HTTP_201_CREATED)
async def submit_task(
    task_data: TaskCreate,
    queue_manager: QueueManager = Depends(get_queue_manager),
    result_store: ResultStore = Depends(get_result_store)
):
    """Submit a new task to the queue"""
    try:
        task_id = await queue_manager.enqueue(
            task_type=task_data.task_type,
            task_data=task_data.task_data,
            priority=task_data.priority,
            queue_type=task_data.queue_type
        )
        
        # Create initial task record in result store
        await result_store.store_task(
            task_id=task_id,
            task_type=task_data.task_type,
            task_data=task_data.task_data,
            status="pending"
        )
        
        logger.info(f"Task submitted: {task_id}")
        return TaskResponse(
            task_id=task_id,
            message="Task submitted successfully",
            status="pending"
        )
    except Exception as e:
        logger.error(f"Error submitting task: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to submit task: {str(e)}"
        )

@router.get("/", response_model=TaskListResponse)
async def list_tasks(
    status_filter: Optional[str] = None,
    task_type: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
    result_store: ResultStore = Depends(get_result_store)
):
    """List tasks with optional filtering"""
    try:
        tasks = await result_store.get_tasks(
            status_filter=status_filter,
            task_type=task_type,
            limit=limit,
            offset=offset
        )
        
        total = await result_store.count_tasks(
            status_filter=status_filter,
            task_type=task_type
        )
        
        return TaskListResponse(
            tasks=tasks,
            total=total,
            limit=limit,
            offset=offset
        )
    except Exception as e:
        logger.error(f"Error listing tasks: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list tasks: {str(e)}"
        )

@router.get("/{task_id}/status", response_model=TaskStatusResponse)
async def get_task_status(
    task_id: str,
    queue_manager: QueueManager = Depends(get_queue_manager),
    result_store: ResultStore = Depends(get_result_store)
):
    """Get the status of a specific task"""
    try:
        task_info = await result_store.get_task(task_id)
        if not task_info:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Task {task_id} not found"
            )
        
        # Get queue position if task is pending
        queue_position = None
        estimated_wait_time = None
        if task_info.get("status") == "pending":
            queue_position = await queue_manager.get_queue_position(task_id)
            # Simple estimation: position * average processing time
            estimated_wait_time = queue_position * 0.5 if queue_position else 0
        
        return TaskStatusResponse(
            task_id=task_id,
            status=task_info.get("status", "unknown"),
            queue_position=queue_position,
            estimated_wait_time=estimated_wait_time,
            created_at=task_info.get("created_at"),
            updated_at=task_info.get("updated_at")
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting task status: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get task status: {str(e)}"
        )

@router.get("/{task_id}/result", response_model=TaskResultResponse)
async def get_task_result(
    task_id: str,
    result_store: ResultStore = Depends(get_result_store)
):
    """Get the result of a completed task"""
    try:
        task_info = await result_store.get_task(task_id)
        if not task_info:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Task {task_id} not found"
            )
        
        return TaskResultResponse(
            task_id=task_id,
            status=task_info.get("status", "unknown"),
            result=task_info.get("result"),
            error=task_info.get("error"),
            execution_time=task_info.get("execution_time"),
            created_at=task_info.get("created_at"),
            completed_at=task_info.get("completed_at")
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting task result: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get task result: {str(e)}"
        )

@router.delete("/{task_id}", status_code=status.HTTP_204_NO_CONTENT)
async def cancel_task(
    task_id: str,
    queue_manager: QueueManager = Depends(get_queue_manager),
    result_store: ResultStore = Depends(get_result_store)
):
    """Cancel a pending or processing task"""
    try:
        # Remove from queue
        removed = await queue_manager.remove_task(task_id)
        
        # Update status in result store
        if removed:
            await result_store.update_task(
                task_id=task_id,
                status="cancelled",
                error="Task cancelled by user"
            )
            logger.info(f"Task cancelled: {task_id}")
        else:
            # Check if task exists in result store
            task_info = await result_store.get_task(task_id)
            if not task_info:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Task {task_id} not found"
                )
            
            # Task might already be completed/failed
            current_status = task_info.get("status")
            if current_status in ["completed", "failed", "cancelled"]:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Cannot cancel task with status: {current_status}"
                )
            
            # For processing tasks, mark as cancelled
            await result_store.update_task(
                task_id=task_id,
                status="cancelled",
                error="Task cancelled by user"
            )
            logger.info(f"Task marked as cancelled: {task_id}")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error cancelling task: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to cancel task: {str(e)}"
        )
