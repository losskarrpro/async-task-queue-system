#!/usr/bin/env python3
"""
Health check script for async-task-queue-system
Checks the health of various system components and services
"""

import sys
import asyncio
import logging
from typing import Dict, Any, List, Tuple
import argparse
import json
import time
import socket
from enum import Enum

try:
    import aiohttp
    from aiohttp import ClientSession, ClientTimeout
except ImportError:
    aiohttp = None

try:
    import redis
except ImportError:
    redis = None

from utils.logger import setup_logger
from config.settings import (
    REDIS_ENABLED,
    REDIS_HOST,
    REDIS_PORT,
    API_HOST,
    API_PORT,
    WORKER_COUNT,
    QUEUE_TYPE,
    HEALTH_CHECK_TIMEOUT,
    LOG_LEVEL
)

# Configure logger
logger = setup_logger("health_check", LOG_LEVEL)

class HealthStatus(Enum):
    """Health status enumeration"""
    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"
    DEGRADED = "degraded"

class ComponentHealth:
    """Health status for a single component"""
    
    def __init__(self, name: str, status: HealthStatus, 
                 details: Dict[str, Any] = None, response_time: float = 0.0):
        self.name = name
        self.status = status
        self.details = details or {}
        self.response_time = response_time
        self.timestamp = time.time()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return {
            "name": self.name,
            "status": self.status.value,
            "details": self.details,
            "response_time": self.response_time,
            "timestamp": self.timestamp
        }
    
    @property
    def is_healthy(self) -> bool:
        """Check if component is healthy"""
        return self.status == HealthStatus.HEALTHY

class HealthChecker:
    """Main health checker class"""
    
    def __init__(self, timeout: float = HEALTH_CHECK_TIMEOUT):
        self.timeout = timeout
        self.results: List[ComponentHealth] = []
        self.overall_status = HealthStatus.HEALTHY
    
    async def check_redis(self) -> ComponentHealth:
        """Check Redis connection"""
        start_time = time.time()
        
        if not REDIS_ENABLED:
            return ComponentHealth(
                "redis",
                HealthStatus.HEALTHY,
                {"enabled": False, "message": "Redis is not enabled"}
            )
        
        if redis is None:
            return ComponentHealth(
                "redis",
                HealthStatus.UNHEALTHY,
                {"error": "redis-py package not installed"}
            )
        
        try:
            # Try to connect to Redis
            r = redis.Redis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                socket_connect_timeout=2,
                socket_timeout=2,
                decode_responses=True
            )
            
            # Test connection
            ping_result = r.ping()
            info = r.info()
            
            response_time = time.time() - start_time
            
            return ComponentHealth(
                "redis",
                HealthStatus.HEALTHY if ping_result else HealthStatus.UNHEALTHY,
                {
                    "host": REDIS_HOST,
                    "port": REDIS_PORT,
                    "ping": ping_result,
                    "version": info.get("redis_version", "unknown"),
                    "used_memory": info.get("used_memory_human", "unknown"),
                    "connected_clients": info.get("connected_clients", 0)
                },
                response_time
            )
            
        except Exception as e:
            response_time = time.time() - start_time
            return ComponentHealth(
                "redis",
                HealthStatus.UNHEALTHY,
                {
                    "host": REDIS_HOST,
                    "port": REDIS_PORT,
                    "error": str(e),
                    "error_type": type(e).__name__
                },
                response_time
            )
    
    async def check_api(self) -> ComponentHealth:
        """Check API health endpoint"""
        start_time = time.time()
        
        if aiohttp is None:
            return ComponentHealth(
                "api",
                HealthStatus.UNHEALTHY,
                {"error": "aiohttp package not installed"}
            )
        
        api_url = f"http://{API_HOST}:{API_PORT}/health"
        
        try:
            timeout = ClientTimeout(total=self.timeout)
            async with ClientSession(timeout=timeout) as session:
                async with session.get(api_url) as response:
                    response_time = time.time() - start_time
                    
                    if response.status == 200:
                        try:
                            data = await response.json()
                            return ComponentHealth(
                                "api",
                                HealthStatus.HEALTHY if data.get("status") == "healthy" 
                                else HealthStatus.DEGRADED,
                                {
                                    "url": api_url,
                                    "status_code": response.status,
                                    "response": data,
                                    "host": API_HOST,
                                    "port": API_PORT
                                },
                                response_time
                            )
                        except:
                            # Non-JSON response
                            return ComponentHealth(
                                "api",
                                HealthStatus.HEALTHY,
                                {
                                    "url": api_url,
                                    "status_code": response.status,
                                    "host": API_HOST,
                                    "port": API_PORT
                                },
                                response_time
                            )
                    else:
                        return ComponentHealth(
                            "api",
                            HealthStatus.UNHEALTHY,
                            {
                                "url": api_url,
                                "status_code": response.status,
                                "host": API_HOST,
                                "port": API_PORT
                            },
                            response_time
                        )
        
        except Exception as e:
            response_time = time.time() - start_time
            return ComponentHealth(
                "api",
                HealthStatus.UNHEALTHY,
                {
                    "url": api_url,
                    "error": str(e),
                    "error_type": type(e).__name__,
                    "host": API_HOST,
                    "port": API_PORT
                },
                response_time
            )
    
    async def check_queue_system(self) -> ComponentHealth:
        """Check internal queue system"""
        start_time = time.time()
        
        try:
            # Try to import queue manager
            from core.queue_manager import QueueManager
            from core.result_store import ResultStore
            from core.exceptions import QueueError
            
            # Initialize components
            queue_manager = QueueManager()
            result_store = ResultStore()
            
            # Test basic functionality
            test_result = queue_manager.get_queue_stats()
            
            response_time = time.time() - start_time
            
            return ComponentHealth(
                "queue_system",
                HealthStatus.HEALTHY,
                {
                    "queue_type": QUEUE_TYPE,
                    "queue_stats": test_result,
                    "worker_count": WORKER_COUNT,
                    "result_store_type": result_store.store_type
                },
                response_time
            )
            
        except Exception as e:
            response_time = time.time() - start_time
            return ComponentHealth(
                "queue_system",
                HealthStatus.UNHEALTHY,
                {
                    "queue_type": QUEUE_TYPE,
                    "error": str(e),
                    "error_type": type(e).__name__
                },
                response_time
            )
    
    async def check_network(self) -> ComponentHealth:
        """Check network connectivity"""
        start_time = time.time()
        
        checks = []
        
        # Check localhost
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2)
            result = sock.connect_ex(('127.0.0.1', 80))
            sock.close()
            checks.append(("localhost", result == 0))
        except Exception as e:
            checks.append(("localhost", False))
        
        # Check external connectivity (Google DNS)
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2)
            result = sock.connect_ex(('8.8.8.8', 53))
            sock.close()
            checks.append(("external_dns", result == 0))
        except Exception as e:
            checks.append(("external_dns", False))
        
        response_time = time.time() - start_time
        
        healthy_checks = sum(1 for _, status in checks if status)
        total_checks = len(checks)
        
        status = HealthStatus.HEALTHY
        if healthy_checks == 0:
            status = HealthStatus.UNHEALTHY
        elif healthy_checks < total_checks:
            status = HealthStatus.DEGRADED
        
        return ComponentHealth(
            "network",
            status,
            {
                "checks": [{"target": target, "reachable": status} 
                          for target, status in checks],
                "healthy_checks": healthy_checks,
                "total_checks": total_checks
            },
            response_time
        )
    
    async def check_disk_space(self) -> ComponentHealth:
        """Check disk space availability"""
        start_time = time.time()
        
        try:
            import shutil
            
            # Check current directory disk space
            disk_usage = shutil.disk_usage(".")
            total_gb = disk_usage.total / (1024**3)
            used_gb = disk_usage.used / (1024**3)
            free_gb = disk_usage.free / (1024**3)
            free_percent = (disk_usage.free / disk_usage.total) * 100
            
            response_time = time.time() - start_time
            
            status = HealthStatus.HEALTHY
            if free_percent < 10:
                status = HealthStatus.UNHEALTHY
            elif free_percent < 20:
                status = HealthStatus.DEGRADED
            
            return ComponentHealth(
                "disk",
                status,
                {
                    "total_gb": round(total_gb, 2),
                    "used_gb": round(used_gb, 2),
                    "free_gb": round(free_gb, 2),
                    "free_percent": round(free_percent, 2),
                    "path": "."
                },
                response_time
            )
            
        except Exception as e:
            response_time = time.time() - start_time
            return ComponentHealth(
                "disk",
                HealthStatus.DEGRADED,
                {
                    "error": str(e),
                    "error_type": type(e).__name__
                },
                response_time
            )
    
    async def check_system_resources(self) -> ComponentHealth:
        """Check system resources (CPU, memory)"""
        start_time = time.time()
        
        try:
            import psutil
            
            checks = []
            
            # Check CPU usage
            cpu_percent = psutil.cpu_percent(interval=0.5)
            checks.append(("cpu_usage", cpu_percent))
            
            # Check memory
            memory = psutil.virtual_memory()
            memory_percent = memory.percent
            checks.append(("memory_usage", memory_percent))
            
            # Check swap if exists
            swap = psutil.swap_memory()
            if swap.total > 0:
                swap_percent = swap.percent
                checks.append(("swap_usage", swap_percent))
            
            response_time = time.time() - start_time
            
            # Determine overall status
            status = HealthStatus.HEALTHY
            if cpu_percent > 90 or memory_percent > 90:
                status = HealthStatus.UNHEALTHY
            elif cpu_percent > 80 or memory_percent > 80:
                status = HealthStatus.DEGRADED
            
            details = {
                "cpu_percent": round(cpu_percent, 2),
                "memory_percent": round(memory_percent, 2),
                "memory_total_gb": round(memory.total / (1024**3), 2),
                "memory_available_gb": round(memory.available / (1024**3), 2)
            }
            
            if swap.total > 0:
                details["swap_percent"] = round(swap.percent, 2)
                details["swap_total_gb"] = round(swap.total / (1024**3), 2)
            
            return ComponentHealth(
                "system_resources",
                status,
                details,
                response_time
            )
            
        except ImportError:
            response_time = time.time() - start_time
            return ComponentHealth(
                "system_resources",
                HealthStatus.DEGRADED,
                {"error": "psutil package not installed"},
                response_time
            )
        except Exception as e:
            response_time = time.time() - start_time
            return ComponentHealth(
                "system_resources",
                HealthStatus.DEGRADED,
                {
                    "error": str(e),
                    "error_type": type(e).__name__
                },
                response_time
            )
    
    async def run_all_checks(self, components: List[str] = None) -> Dict[str, Any]:
        """
        Run all health checks or specific components
        
        Args:
            components: List of component names to check, or None for all
            
        Returns:
            Dictionary with health check results
        """
        self.results = []
        
        # Define available checks
        available_checks = {
            "redis": self.check_redis,
            "api": self.check_api,
            "queue_system": self.check_queue_system,
            "network": self.check_network,
            "disk": self.check_disk_space,
            "system_resources": self.check_system_resources
        }
        
        # Determine which checks to run
        checks_to_run = available_checks
        if components:
            checks_to_run = {k: v for k, v in available_checks.items() 
                            if k in components}
        
        # Run checks concurrently
        tasks = []
        for name, check_func in checks_to_run.items():
            tasks.append(check_func())
        
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for i, (name, check_func) in enumerate(checks_to_run.items()):
                result = results[i]
                
                if isinstance(result, Exception):
                    self.results.append(ComponentHealth(
                        name,
                        HealthStatus.UNHEALTHY,
                        {"error": str(result), "error_type": type(result).__name__}
                    ))
                else:
                    self.results.append(result)
        
        # Calculate overall status
        healthy_count = sum(1 for r in self.results if r.is_healthy)
        total_count = len(self.results)
        
        if healthy_count == total_count:
            self.overall_status = HealthStatus.HEALTHY
        elif healthy_count == 0:
            self.overall_status = HealthStatus.UNHEALTHY
        else:
            self.overall_status = HealthStatus.DEGRADED
        
        return self.generate_report()
    
    def generate_report(self) -> Dict[str, Any]:
        """Generate health check report"""
        report = {
            "status": self.overall_status.value,
            "timestamp": time.time(),
            "components": [r.to_dict() for r in self.results],
            "summary": {
                "total_checks": len(self.results),
                "healthy": sum(1 for r in self.results if r.is_healthy),
                "unhealthy": sum(1 for r in self.results 
                               if r.status == HealthStatus.UNHEALTHY),
                "degraded": sum(1 for r in self.results 
                              if r.status == HealthStatus.DEGRADED)
            }
        }
        
        # Calculate average response time
        if self.results:
            avg_time = sum(r.response_time for r in self.results) / len(self.results)
            report["summary"]["avg_response_time"] = round(avg_time, 3)
        
        return report
    
    def print_report(self, report: Dict[str, Any], output_format: str = "human"):
        """Print health check report in specified format"""
        
        if output_format == "json":
            print(json.dumps(report, indent=2, default=str))
            return
        
        # Human readable format
        print("\n" + "="*60)
        print("ASYNC TASK QUEUE SYSTEM - HEALTH CHECK REPORT")
        print("="*60)
        
        status_symbols = {
            "healthy": "✅",
            "unhealthy": "❌",
            "degraded": "⚠️"
        }
        
        overall_status = report["status"]
        print(f"\nOverall Status: {status_symbols.get(overall_status, '❓')} {overall_status.upper()}")
        
        summary = report["summary"]
        print(f"\nSummary: {summary['healthy']} healthy, "
              f"{summary['degraded']} degraded, "
              f"{summary['unhealthy']} unhealthy")
        
        if "avg_response_time" in summary:
            print(f"Average Response Time: {summary['avg_response_time']}s")
        
        print(f"\nComponent Details:")
        print("-"*60)
        
        for component in report["components"]:
            symbol = status_symbols.get(component["status"], "❓")
            print(f"\n{symbol} {component['name'].upper()}")
            print(f"  Status: {component['status']}")
            print(f"  Response Time: {component['response_time']:.3f}s")
            
            if component["details"]:
                print(f"  Details:")
                for key, value in component["details"].items():
                    if isinstance(value, dict):
                        print(f"    {key}:")
                        for subkey, subvalue in value.items():
                            print(f"      {subkey}: {subvalue}")
                    else:
                        print(f"    {key}: {value}")
        
        print("\n" + "="*60)
        
        # Exit with appropriate code
        if overall_status == "unhealthy":
            print("❌ Health check FAILED")
            return 1
        elif overall_status == "degraded":
            print("⚠️  Health check DEGRADED")
            return 0
        else:
            print("✅ Health check PASSED")
            return 0

async def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Health check for async-task-queue-system"
    )
    
    parser.add_argument(
        "--components",
        nargs="+",
        choices=["redis", "api", "queue_system", "network", "disk", "system_resources", "all"],
        default=["all"],
        help="Components to check (default: all)"
    )
    
    parser.add_argument(
        "--format",
        choices=["human", "json"],
        default="human",
        help="Output format (default: human)"
    )
    
    parser.add_argument(
        "--timeout",
        type=float,
        default=HEALTH_CHECK_TIMEOUT,
        help=f"Timeout for each check in seconds (default: {HEALTH_CHECK_TIMEOUT})"
    )
    
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging"
    )
    
    args = parser.parse_args()
    
    # Configure logging
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    
    # Handle "all" components
    components_to_check = args.components
    if "all" in components_to_check:
        components_to_check = ["redis", "api", "queue_system", "network", "disk", "system_resources"]
    
    logger.info(f"Starting health check for components: {', '.join(components_to_check)}")
    
    # Run health checks
    checker = HealthChecker(timeout=args.timeout)
    report = await checker.run_all_checks(components_to_check)
    
    # Print report
    exit_code = checker.print_report(report, args.format)
    
    return exit_code

if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        logger.info("Health check interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Health check failed with error: {e}")
        sys.exit(1)