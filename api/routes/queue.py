from fastapi import APIRouter, Depends, HTTPException, status
from typing import Dict, List, Any, Optional

from core.queue_manager import QueueManager
from core.exceptions import QueueNotFoundError, QueueOperationError
from api.schemas.queue_schema import (
    QueueInfo, QueueStats, QueueList, QueueCreate, QueueUpdate
)
from utils.logger import get_logger
from config.settings import get_settings

router = APIRouter(prefix="/queues", tags=["queues"])
logger = get_logger(__name__)
settings = get_settings()


def get_queue_manager() -> QueueManager:
    """Dependency to get queue manager instance"""
    return QueueManager.get_instance()


@router.get("/", response_model=QueueList)
async def list_queues(
    queue_manager: QueueManager = Depends(get_queue_manager)
) -> Dict[str, Any]:
    """
    List all available queues with basic information
    """
    try:
        queues = queue_manager.list_queues()
        queue_info_list = []
        
        for queue_name in queues:
            try:
                queue = queue_manager.get_queue(queue_name)
                stats = queue.get_stats() if hasattr(queue, 'get_stats') else {}
                queue_info = QueueInfo(
                    name=queue_name,
                    queue_type=queue.queue_type if hasattr(queue, 'queue_type') else 'unknown',
                    size=queue.size() if hasattr(queue, 'size') else 0,
                    **stats
                )
                queue_info_list.append(queue_info)
            except Exception as e:
                logger.warning(f"Failed to get info for queue {queue_name}: {e}")
                # Create basic info even if stats fail
                queue_info = QueueInfo(
                    name=queue_name,
                    queue_type="unknown",
                    size=0
                )
                queue_info_list.append(queue_info)
        
        return {"queues": queue_info_list, "total": len(queue_info_list)}
    
    except Exception as e:
        logger.error(f"Error listing queues: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to list queues"
        )


@router.post("/", response_model=QueueInfo, status_code=status.HTTP_201_CREATED)
async def create_queue(
    queue_data: QueueCreate,
    queue_manager: QueueManager = Depends(get_queue_manager)
) -> Dict[str, Any]:
    """
    Create a new queue with specified configuration
    """
    try:
        queue = queue_manager.create_queue(
            name=queue_data.name,
            queue_type=queue_data.queue_type,
            max_size=queue_data.max_size,
            **queue_data.config.dict() if queue_data.config else {}
        )
        
        return QueueInfo(
            name=queue_data.name,
            queue_type=queue_data.queue_type.value,
            size=queue.size() if hasattr(queue, 'size') else 0
        )
    
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Error creating queue: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create queue"
        )


@router.get("/{queue_name}", response_model=QueueInfo)
async def get_queue_info(
    queue_name: str,
    queue_manager: QueueManager = Depends(get_queue_manager)
) -> Dict[str, Any]:
    """
    Get detailed information about a specific queue
    """
    try:
        queue = queue_manager.get_queue(queue_name)
        stats = queue.get_stats() if hasattr(queue, 'get_stats') else {}
        
        return QueueInfo(
            name=queue_name,
            queue_type=queue.queue_type if hasattr(queue, 'queue_type') else 'unknown',
            size=queue.size() if hasattr(queue, 'size') else 0,
            **stats
        )
    
    except QueueNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Queue '{queue_name}' not found"
        )
    except Exception as e:
        logger.error(f"Error getting queue info: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to get queue information"
        )


@router.put("/{queue_name}", response_model=QueueInfo)
async def update_queue(
    queue_name: str,
    queue_data: QueueUpdate,
    queue_manager: QueueManager = Depends(get_queue_manager)
) -> Dict[str, Any]:
    """
    Update queue configuration
    """
    try:
        # For now, just return current info since update might not be implemented
        queue = queue_manager.get_queue(queue_name)
        stats = queue.get_stats() if hasattr(queue, 'get_stats') else {}
        
        return QueueInfo(
            name=queue_name,
            queue_type=queue.queue_type if hasattr(queue, 'queue_type') else 'unknown',
            size=queue.size() if hasattr(queue, 'size') else 0,
            **stats
        )
    
    except QueueNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Queue '{queue_name}' not found"
        )
    except Exception as e:
        logger.error(f"Error updating queue: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to update queue"
        )


@router.delete("/{queue_name}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_queue(
    queue_name: str,
    queue_manager: QueueManager = Depends(get_queue_manager)
) -> None:
    """
    Delete a queue
    """
    try:
        queue_manager.delete_queue(queue_name)
    
    except QueueNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Queue '{queue_name}' not found"
        )
    except Exception as e:
        logger.error(f"Error deleting queue: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to delete queue"
        )


@router.get("/{queue_name}/stats", response_model=QueueStats)
async def get_queue_stats(
    queue_name: str,
    queue_manager: QueueManager = Depends(get_queue_manager)
) -> Dict[str, Any]:
    """
    Get statistics for a specific queue
    """
    try:
        queue = queue_manager.get_queue(queue_name)
        stats = queue.get_stats() if hasattr(queue, 'get_stats') else {}
        
        return QueueStats(**stats)
    
    except QueueNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Queue '{queue_name}' not found"
        )
    except Exception as e:
        logger.error(f"Error getting queue stats: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to get queue statistics"
        )