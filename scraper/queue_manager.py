"""
URL Queue Manager Module

Manages the URL queue with priority, deduplication, and robots.txt filtering.
"""

import asyncio
import hashlib
import heapq
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Optional, Set

from scraper.safety.robots_parser import RobotsParser


class Priority(IntEnum):
    """URL priority levels (lower = higher priority)."""
    CRITICAL = 0
    HIGH = 1
    NORMAL = 2
    LOW = 3


@dataclass(order=True)
class QueueItem:
    """An item in the URL queue."""
    
    priority: int
    url: str = field(compare=False)
    depth: int = field(default=0, compare=False)
    parent_url: Optional[str] = field(default=None, compare=False)
    metadata: dict = field(default_factory=dict, compare=False)


class URLQueue:
    """
    Priority queue for URLs with deduplication and filtering.
    
    Features:
    - Priority-based ordering
    - URL deduplication via hash set
    - Robots.txt filtering (optional)
    - Depth tracking for crawlers
    - Thread-safe async operations
    
    Example:
        queue = URLQueue()
        await queue.add("https://example.com")
        url_item = await queue.get()
    """
    
    def __init__(
        self,
        robots_parser: RobotsParser | None = None,
        max_size: int = 10000,
        filter_robots: bool = True,
    ):
        """
        Initialize the queue.
        
        Args:
            robots_parser: RobotsParser for filtering (created if None and filter_robots=True)
            max_size: Maximum queue size
            filter_robots: Enable robots.txt filtering
        """
        self._queue: list[QueueItem] = []
        self._seen: Set[str] = set()
        self._lock = asyncio.Lock()
        
        self._max_size = max_size
        self._filter_robots = filter_robots
        self._robots = robots_parser if filter_robots else None
        
        if self._filter_robots and self._robots is None:
            self._robots = RobotsParser()
        
        # Stats
        self._added = 0
        self._processed = 0
        self._filtered = 0
        self._duplicates = 0
    
    def _url_hash(self, url: str) -> str:
        """Generate hash for deduplication."""
        # Normalize URL for better deduplication
        normalized = url.rstrip("/").lower()
        return hashlib.md5(normalized.encode()).hexdigest()
    
    async def add(
        self,
        url: str,
        priority: Priority = Priority.NORMAL,
        depth: int = 0,
        parent_url: str | None = None,
        metadata: dict | None = None,
    ) -> bool:
        """
        Add a URL to the queue.
        
        Args:
            url: URL to add
            priority: Queue priority
            depth: Crawl depth
            parent_url: URL this was discovered from
            metadata: Additional metadata
            
        Returns:
            True if added, False if filtered/duplicate
        """
        url_hash = self._url_hash(url)
        
        async with self._lock:
            # Check for duplicate
            if url_hash in self._seen:
                self._duplicates += 1
                return False
            
            # Check queue size
            if len(self._queue) >= self._max_size:
                return False
            
            # Check robots.txt
            if self._filter_robots and self._robots:
                if not await self._robots.can_fetch(url):
                    self._filtered += 1
                    return False
            
            # Add to queue
            item = QueueItem(
                priority=priority,
                url=url,
                depth=depth,
                parent_url=parent_url,
                metadata=metadata or {},
            )
            
            heapq.heappush(self._queue, item)
            self._seen.add(url_hash)
            self._added += 1
            
            return True
    
    async def add_many(
        self,
        urls: list[str],
        priority: Priority = Priority.NORMAL,
        **kwargs,
    ) -> int:
        """
        Add multiple URLs to the queue.
        
        Args:
            urls: List of URLs to add
            priority: Queue priority for all
            **kwargs: Additional arguments for add()
            
        Returns:
            Number of URLs successfully added
        """
        count = 0
        for url in urls:
            if await self.add(url, priority, **kwargs):
                count += 1
        return count
    
    async def get(self, timeout: float | None = None) -> Optional[QueueItem]:
        """
        Get the next URL from the queue.
        
        Args:
            timeout: Max seconds to wait (None = no wait)
            
        Returns:
            QueueItem or None if empty
        """
        start_time = asyncio.get_event_loop().time()
        
        while True:
            async with self._lock:
                if self._queue:
                    item = heapq.heappop(self._queue)
                    self._processed += 1
                    return item
            
            if timeout is None:
                return None
            
            elapsed = asyncio.get_event_loop().time() - start_time
            if elapsed >= timeout:
                return None
            
            await asyncio.sleep(0.1)
    
    async def peek(self) -> Optional[QueueItem]:
        """Peek at the next item without removing it."""
        async with self._lock:
            if self._queue:
                return self._queue[0]
            return None
    
    async def clear(self) -> int:
        """
        Clear the queue.
        
        Returns:
            Number of items cleared
        """
        async with self._lock:
            count = len(self._queue)
            self._queue = []
            return count
    
    async def reset_seen(self) -> None:
        """Reset the seen URLs set (allow re-scraping)."""
        async with self._lock:
            self._seen.clear()
    
    @property
    def size(self) -> int:
        """Current queue size."""
        return len(self._queue)
    
    @property
    def is_empty(self) -> bool:
        """Check if queue is empty."""
        return len(self._queue) == 0
    
    def get_stats(self) -> dict:
        """Get queue statistics."""
        return {
            "current_size": len(self._queue),
            "max_size": self._max_size,
            "total_added": self._added,
            "total_processed": self._processed,
            "filtered_robots": self._filtered,
            "duplicates_skipped": self._duplicates,
            "seen_count": len(self._seen),
        }
