from fastapi import APIRouter, Depends, HTTPException
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
import asyncio
import psutil

from core.queue_manager import QueueManager
from core.result_store import ResultStore
from workers.worker_pool import WorkerPool
from utils.logger import get_logger
from api.schemas.task_schema import TaskStatusResponse, TaskStatsResponse
from api.schemas.queue_schema import QueueStatsResponse, WorkerStatsResponse

router = APIRouter(prefix="/monitoring", tags=["monitoring"])
logger = get_logger(__name__)

async def get_queue_manager() -> QueueManager:
    """Dependency to get queue manager instance"""
    from main import queue_manager
    return queue_manager

async def get_result_store() -> ResultStore:
    """Dependency to get result store instance"""
    from main import result_store
    return result_store

async def get_worker_pool() -> WorkerPool:
    """Dependency to get worker pool instance"""
    from main import worker_pool
    return worker_pool

@router.get("/health", response_model=Dict[str, Any])
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "service": "async-task-queue-system"
    }

@router.get("/queues/stats", response_model=List[QueueStatsResponse])
async def get_queue_stats(
    queue_manager: QueueManager = Depends(get_queue_manager)
):
    """Get statistics for all queues"""
    try:
        stats = []
        for queue_name, queue in queue_manager.get_all_queues().items():
            queue_stats = {
                "queue_name": queue_name,
                "queue_type": queue.queue_type,
                "size": queue.size(),
                "pending_tasks": queue.size(),
                "max_size": queue.max_size,
                "created_at": queue.created_at.isoformat() if hasattr(queue, 'created_at') else None,
                "last_processed": queue.last_processed.isoformat() if hasattr(queue, 'last_processed') and queue.last_processed else None
            }
            stats.append(queue_stats)
        return stats
    except Exception as e:
        logger.error(f"Error getting queue stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/workers/stats", response_model=List[WorkerStatsResponse])
async def get_worker_stats(
    worker_pool: WorkerPool = Depends(get_worker_pool)
):
    """Get statistics for all workers"""
    try:
        stats = []
        workers = worker_pool.get_all_workers()
        
        for worker_id, worker in workers.items():
            worker_stats = {
                "worker_id": worker_id,
                "status": worker.status.value,
                "current_task": worker.current_task_id,
                "tasks_processed": worker.tasks_processed,
                "tasks_failed": worker.tasks_failed,
                "started_at": worker.started_at.isoformat() if worker.started_at else None,
                "last_activity": worker.last_activity.isoformat() if worker.last_activity else None,
                "is_active": worker.is_active
            }
            stats.append(worker_stats)
        return stats
    except Exception as e:
        logger.error(f"Error getting worker stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/tasks/stats", response_model=TaskStatsResponse)
async def get_task_stats(
    result_store: ResultStore = Depends(get_result_store),
    queue_manager: QueueManager = Depends(get_queue_manager)
):
    """Get overall task statistics"""
    try:
        # Get completed tasks from result store
        completed_tasks = result_store.get_all_results()
        
        # Count tasks by status
        status_counts = {
            "pending": 0,
            "running": 0,
            "completed": 0,
            "failed": 0,
            "cancelled": 0
        }
        
        for task_result in completed_tasks.values():
            if task_result.status in status_counts:
                status_counts[task_result.status] += 1
        
        # Get pending tasks from queues
        total_pending = 0
        for queue_name, queue in queue_manager.get_all_queues().items():
            total_pending += queue.size()
        
        status_counts["pending"] = total_pending
        
        # Calculate totals
        total_tasks = sum(status_counts.values())
        
        # Calculate success rate
        completed = status_counts["completed"]
        failed = status_counts["failed"]
        total_processed = completed + failed
        success_rate = (completed / total_processed * 100) if total_processed > 0 else 0
        
        return {
            "total_tasks": total_tasks,
            "pending_tasks": status_counts["pending"],
            "running_tasks": status_counts["running"],
            "completed_tasks": status_counts["completed"],
            "failed_tasks": status_counts["failed"],
            "cancelled_tasks": status_counts["cancelled"],
            "success_rate": round(success_rate, 2),
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Error getting task stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/system/stats", response_model=Dict[str, Any])
async def get_system_stats():
    """Get system statistics (CPU, memory, etc.)"""
    try:
        # Get system metrics
        cpu_percent = psutil.cpu_percent(interval=0.1)
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        
        # Get process metrics
        process = psutil.Process()
        process_memory = process.memory_info().rss / 1024 / 1024  # MB
        
        # Get asyncio event loop stats
        loop = asyncio.get_event_loop()
        
        return {
            "system": {
                "cpu_percent": cpu_percent,
                "memory_percent": memory.percent,
                "memory_total_gb": round(memory.total / 1024 / 1024 / 1024, 2),
                "memory_available_gb": round(memory.available / 1024 / 1024 / 1024, 2),
                "disk_percent": disk.percent,
                "disk_free_gb": round(disk.free / 1024 / 1024 / 1024, 2)
            },
            "process": {
                "memory_mb": round(process_memory, 2),
                "threads": process.num_threads(),
                "cpu_times": process.cpu_times()._asdict()
            },
            "asyncio": {
                "loop_running": loop.is_running(),
                "loop_closed": loop.is_closed(),
                "number_of_tasks": len(asyncio.all_tasks(loop))
            },
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Error getting system stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/tasks/recent", response_model=List[TaskStatusResponse])
async def get_recent_tasks(
    limit: int = 50,
    result_store: ResultStore = Depends(get_result_store)
):
    """Get recent tasks with their status"""
    try:
        all_results = result_store.get_all_results()
        
        # Sort by creation time (newest first)
        sorted_tasks = sorted(
            all_results.values(),
            key=lambda x: x.created_at if x.created_at else datetime.min,
            reverse=True
        )
        
        # Limit results
        recent_tasks = sorted_tasks[:limit]
        
        # Convert to response format
        response = []
        for task in recent_tasks:
            response.append({
                "task_id": task.task_id,
                "status": task.status,
                "queue_name": task.queue_name,
                "task_type": task.task_type,
                "created_at": task.created_at.isoformat() if task.created_at else None,
                "started_at": task.started_at.isoformat() if task.started_at else None,
                "completed_at": task.completed_at.isoformat() if task.completed_at else None,
                "result": task.result,
                "error": task.error
            })
        
        return response
    except Exception as e:
        logger.error(f"Error getting recent tasks: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/performance/metrics", response_model=Dict[str, Any])
async def get_performance_metrics(
    hours: int = 24,
    result_store: ResultStore = Depends(get_result_store)
):
    """Get performance metrics for the specified time period"""
    try:
        all_results = result_store.get_all_results()
        cutoff_time = datetime.now() - timedelta(hours=hours)
        
        # Filter tasks within time period
        recent_tasks = [
            task for task in all_results.values()
            if task.completed_at and task.completed_at >= cutoff_time
        ]
        
        if not recent_tasks:
            return {
                "period_hours": hours,
                "total_tasks": 0,
                "metrics": {}
            }
        
        # Calculate processing times
        processing_times = []
        for task in recent_tasks:
            if task.started_at and task.completed_at:
                processing_time = (task.completed_at - task.started_at).total_seconds()
                processing_times.append(processing_time)
        
        # Calculate statistics
        if processing_times:
            avg_processing_time = sum(processing_times) / len(processing_times)
            max_processing_time = max(processing_times)
            min_processing_time = min(processing_times)
        else:
            avg_processing_time = max_processing_time = min_processing_time = 0
        
        # Count by status
        status_counts = {}
        for task in recent_tasks:
            status_counts[task.status] = status_counts.get(task.status, 0) + 1
        
        # Count by task type
        type_counts = {}
        for task in recent_tasks:
            type_counts[task.task_type] = type_counts.get(task.task_type, 0) + 1
        
        return {
            "period_hours": hours,
            "total_tasks": len(recent_tasks),
            "metrics": {
                "processing_time_seconds": {
                    "average": round(avg_processing_time, 2),
                    "max": round(max_processing_time, 2),
                    "min": round(min_processing_time, 2)
                },
                "tasks_by_status": status_counts,
                "tasks_by_type": type_counts,
                "success_rate": round(
                    (status_counts.get("completed", 0) / len(recent_tasks) * 100), 2
                )
            }
        }
    except Exception as e:
        logger.error(f"Error getting performance metrics: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/alerts/active", response_model=List[Dict[str, Any]])
async def get_active_alerts():
    """Get active system alerts"""
    try:
        alerts = []
        
        # Check system resources
        cpu_percent = psutil.cpu_percent(interval=0.1)
        if cpu_percent > 90:
            alerts.append({
                "level": "high",
                "type": "cpu_usage",
                "message": f"CPU usage is high: {cpu_percent}%",
                "timestamp": datetime.now().isoformat()
            })
        
        memory = psutil.virtual_memory()
        if memory.percent > 90:
            alerts.append({
                "level": "high",
                "type": "memory_usage",
                "message": f"Memory usage is high: {memory.percent}%",
                "timestamp": datetime.now().isoformat()
            })
        
        # Check event loop
        loop = asyncio.get_event_loop()
        if len(asyncio.all_tasks(loop)) > 1000:
            alerts.append({
                "level": "medium",
                "type": "too_many_tasks",
                "message": f"Too many asyncio tasks: {len(asyncio.all_tasks(loop))}",
                "timestamp": datetime.now().isoformat()
            })
        
        return alerts
    except Exception as e:
        logger.error(f"Error getting active alerts: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/status/summary", response_model=Dict[str, Any])
async def get_status_summary(
    queue_manager: QueueManager = Depends(get_queue_manager),
    result_store: ResultStore = Depends(get_result_store),
    worker_pool: WorkerPool = Depends(get_worker_pool)
):
    """Get comprehensive status summary"""
    try:
        # Get all stats
        queue_stats = await get_queue_stats(queue_manager)
        worker_stats = await get_worker_stats(worker_pool)
        task_stats = await get_task_stats(result_store, queue_manager)
        system_stats = await get_system_stats()
        alerts = await get_active_alerts()
        
        # Calculate overall status
        if alerts and any(alert["level"] == "high" for alert in alerts):
            overall_status = "degraded"
        elif not worker_stats or all(not worker["is_active"] for worker in worker_stats):
            overall_status = "unhealthy"
        else:
            overall_status = "healthy"
        
        return {
            "overall_status": overall_status,
            "timestamp": datetime.now().isoformat(),
            "queues": {
                "total": len(queue_stats),
                "stats": queue_stats
            },
            "workers": {
                "total": len(worker_stats),
                "active": sum(1 for w in worker_stats if w["is_active"]),
                "stats": worker_stats
            },
            "tasks": task_stats,
            "system": system_stats,
            "alerts": {
                "total": len(alerts),
                "active": alerts
            }
        }
    except Exception as e:
        logger.error(f"Error getting status summary: {e}")
        raise HTTPException(status_code=500, detail=str(e))