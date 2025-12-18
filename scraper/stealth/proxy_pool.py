"""
Proxy Pool Manager Module

Manages a pool of proxies with health-checking and rotation strategies.
Supports both datacenter and residential proxies.
"""

import asyncio
import random
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Optional

import httpx

from scraper.config import config


class ProxyType(Enum):
    """Type of proxy."""
    DATACENTER = "datacenter"
    RESIDENTIAL = "residential"
    UNKNOWN = "unknown"


@dataclass
class Proxy:
    """A single proxy entry."""
    
    url: str  # Format: protocol://user:pass@host:port or protocol://host:port
    proxy_type: ProxyType = ProxyType.UNKNOWN
    
    # Health tracking
    is_healthy: bool = True
    failure_count: int = 0
    last_check: float = 0.0
    last_used: float = 0.0
    
    # Performance metrics
    avg_response_time: float = 0.0
    total_requests: int = 0
    successful_requests: int = 0
    
    def mark_failure(self) -> None:
        """Mark a failed request."""
        self.failure_count += 1
        if self.failure_count >= config.proxy.max_failures:
            self.is_healthy = False
    
    def mark_success(self, response_time: float) -> None:
        """Mark a successful request."""
        self.failure_count = 0
        self.is_healthy = True
        self.total_requests += 1
        self.successful_requests += 1
        
        # Update rolling average response time
        if self.avg_response_time == 0:
            self.avg_response_time = response_time
        else:
            self.avg_response_time = (self.avg_response_time * 0.8) + (response_time * 0.2)
    
    def reset_health(self) -> None:
        """Reset health status for retry."""
        self.is_healthy = True
        self.failure_count = 0
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate percentage."""
        if self.total_requests == 0:
            return 100.0
        return (self.successful_requests / self.total_requests) * 100


class ProxyPool:
    """
    Manages a pool of proxies with rotation and health-checking.
    
    Features:
    - Round-robin or random rotation strategies
    - Auto-disable unhealthy proxies
    - Periodic health checks
    - Load from file or programmatic addition
    
    Example:
        pool = ProxyPool()
        pool.load_from_file("proxies.txt")
        
        proxy = await pool.get_proxy()
        if proxy:
            # Use proxy for request
            ...
    """
    
    def __init__(
        self,
        rotation_strategy: str | None = None,
        health_check_interval: int | None = None,
    ):
        """
        Initialize the proxy pool.
        
        Args:
            rotation_strategy: "random" or "round_robin" (default from config)
            health_check_interval: Seconds between health checks (default from config)
        """
        self._proxies: list[Proxy] = []
        self._current_index = 0
        self._lock = asyncio.Lock()
        
        self._strategy = rotation_strategy or config.proxy.rotation_strategy
        self._health_check_interval = health_check_interval or config.proxy.health_check_interval
        self._health_check_task: Optional[asyncio.Task] = None
    
    def add_proxy(
        self,
        url: str,
        proxy_type: ProxyType = ProxyType.UNKNOWN,
    ) -> None:
        """
        Add a proxy to the pool.
        
        Args:
            url: Proxy URL (protocol://host:port or with auth)
            proxy_type: Type of proxy (datacenter/residential)
        """
        proxy = Proxy(url=url.strip(), proxy_type=proxy_type)
        self._proxies.append(proxy)
    
    def load_from_file(self, file_path: str | Path) -> int:
        """
        Load proxies from a file (one per line).
        
        Args:
            file_path: Path to proxy list file
            
        Returns:
            Number of proxies loaded
        """
        path = Path(file_path)
        if not path.exists():
            return 0
        
        count = 0
        with open(path, "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    self.add_proxy(line)
                    count += 1
        
        return count
    
    async def get_proxy(self) -> Optional[Proxy]:
        """
        Get the next available proxy.
        
        Returns:
            A healthy Proxy, or None if no proxies available
        """
        if not config.proxy.enabled or not self._proxies:
            return None
        
        async with self._lock:
            healthy_proxies = [p for p in self._proxies if p.is_healthy]
            
            if not healthy_proxies:
                return None
            
            if self._strategy == "random":
                proxy = random.choice(healthy_proxies)
            else:  # round_robin
                proxy = healthy_proxies[self._current_index % len(healthy_proxies)]
                self._current_index += 1
            
            proxy.last_used = time.time()
            return proxy
    
    def get_proxy_dict(self, proxy: Proxy) -> dict:
        """
        Convert proxy to httpx-compatible format.
        
        Args:
            proxy: The Proxy object
            
        Returns:
            Dict for httpx client
        """
        return {
            "http://": proxy.url,
            "https://": proxy.url,
        }
    
    async def report_success(self, proxy: Proxy, response_time: float) -> None:
        """Report a successful request."""
        proxy.mark_success(response_time)
    
    async def report_failure(self, proxy: Proxy) -> None:
        """Report a failed request."""
        proxy.mark_failure()
    
    async def health_check(self, proxy: Proxy) -> bool:
        """
        Perform a health check on a single proxy.
        
        Args:
            proxy: The proxy to check
            
        Returns:
            True if healthy, False otherwise
        """
        test_url = "https://httpbin.org/ip"
        
        try:
            async with httpx.AsyncClient(
                proxies=self.get_proxy_dict(proxy),
                timeout=10.0,
            ) as client:
                start = time.time()
                response = await client.get(test_url)
                response_time = time.time() - start
                
                if response.status_code == 200:
                    proxy.mark_success(response_time)
                    proxy.last_check = time.time()
                    return True
                    
        except Exception:
            proxy.mark_failure()
        
        proxy.last_check = time.time()
        return proxy.is_healthy
    
    async def check_all_proxies(self) -> dict:
        """
        Health check all proxies.
        
        Returns:
            Dict with health check results
        """
        results = {"healthy": 0, "unhealthy": 0, "total": len(self._proxies)}
        
        tasks = [self.health_check(proxy) for proxy in self._proxies]
        checks = await asyncio.gather(*tasks)
        
        results["healthy"] = sum(checks)
        results["unhealthy"] = len(checks) - results["healthy"]
        
        return results
    
    async def _health_check_loop(self) -> None:
        """Background task for periodic health checks."""
        while True:
            await asyncio.sleep(self._health_check_interval)
            await self.check_all_proxies()
    
    def start_health_checks(self) -> None:
        """Start background health check task."""
        if self._health_check_task is None:
            self._health_check_task = asyncio.create_task(self._health_check_loop())
    
    def stop_health_checks(self) -> None:
        """Stop background health check task."""
        if self._health_check_task:
            self._health_check_task.cancel()
            self._health_check_task = None
    
    def reset_all(self) -> None:
        """Reset all proxies to healthy status."""
        for proxy in self._proxies:
            proxy.reset_health()
    
    def get_stats(self) -> dict:
        """Get pool statistics."""
        healthy = sum(1 for p in self._proxies if p.is_healthy)
        return {
            "total": len(self._proxies),
            "healthy": healthy,
            "unhealthy": len(self._proxies) - healthy,
            "strategy": self._strategy,
        }
    
    @property
    def size(self) -> int:
        """Total number of proxies in pool."""
        return len(self._proxies)
    
    @property
    def healthy_count(self) -> int:
        """Number of healthy proxies."""
        return sum(1 for p in self._proxies if p.is_healthy)
