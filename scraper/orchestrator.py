"""
Main Orchestrator Module

The central coordinator that connects all scraping components.
Implements the complete "Safe System" workflow.
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn

from scraper.config import config, ScraperConfig
from scraper.queue_manager import URLQueue, Priority
from scraper.safety.robots_parser import RobotsParser
from scraper.safety.rate_limiter import TokenBucketRateLimiter
from scraper.stealth.user_agents import UserAgentRotator
from scraper.stealth.proxy_pool import ProxyPool
from scraper.fetchers.http_fetcher import HTTPFetcher, FetchResult
from scraper.fetchers.browser_fetcher import BrowserFetcher
from scraper.fetchers.site_detector import SiteDetector
from scraper.pipeline.raw_storage import RawStorage
from scraper.pipeline.validator import DataValidator
from scraper.pipeline.cleaner import DataCleaner
from scraper.pipeline.exporters import JSONExporter, create_exporter


console = Console()
logger = logging.getLogger(__name__)


class ScraperStatus(Enum):
    """Status of the scraper."""
    IDLE = "idle"
    RUNNING = "running"
    PAUSED = "paused"
    STOPPED = "stopped"
    ERROR = "error"


@dataclass
class ScrapeResult:
    """Result of a single scrape operation."""
    
    url: str
    success: bool
    status_code: int = 0
    content: str = ""
    data: Dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None
    response_time: float = 0.0
    used_browser: bool = False


@dataclass
class ScraperStats:
    """Statistics for a scraping session."""
    
    started_at: float = 0.0
    finished_at: float = 0.0
    urls_processed: int = 0
    urls_successful: int = 0
    urls_failed: int = 0
    bytes_downloaded: int = 0
    browser_fetches: int = 0
    http_fetches: int = 0
    
    @property
    def duration(self) -> float:
        """Duration in seconds."""
        if self.finished_at:
            return self.finished_at - self.started_at
        return time.time() - self.started_at
    
    @property
    def success_rate(self) -> float:
        """Success rate percentage."""
        if self.urls_processed == 0:
            return 0.0
        return (self.urls_successful / self.urls_processed) * 100
    
    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "duration_seconds": round(self.duration, 2),
            "urls_processed": self.urls_processed,
            "urls_successful": self.urls_successful,
            "urls_failed": self.urls_failed,
            "success_rate": round(self.success_rate, 2),
            "bytes_downloaded": self.bytes_downloaded,
            "browser_fetches": self.browser_fetches,
            "http_fetches": self.http_fetches,
        }


class Orchestrator:
    """
    Main scraper orchestrator connecting all components.
    
    Implements the complete workflow:
    1. URL enters queue
    2. Robots.txt check
    3. Rate limiter wait
    4. Stealth headers applied
    5. Fetch (HTTP or Browser)
    6. Save raw data
    7. Clean and export
    
    Example:
        orchestrator = Orchestrator()
        await orchestrator.run(["https://example.com"])
    """
    
    def __init__(
        self,
        config: ScraperConfig | None = None,
        parser: Callable[[str, str], Dict[str, Any]] | None = None,
    ):
        """
        Initialize the orchestrator.
        
        Args:
            config: Custom configuration (uses global if None)
            parser: Custom parser function (url, html) -> data
        """
        self._config = config or globals()["config"]
        self._parser = parser
        
        # Initialize components
        self._robots = RobotsParser()
        self._rate_limiter = TokenBucketRateLimiter()
        self._ua_rotator = UserAgentRotator()
        self._proxy_pool = ProxyPool()
        self._queue = URLQueue(robots_parser=self._robots)
        
        self._http_fetcher = HTTPFetcher(
            user_agent_rotator=self._ua_rotator,
            proxy_pool=self._proxy_pool,
        )
        self._browser_fetcher: Optional[BrowserFetcher] = None
        self._site_detector = SiteDetector()
        
        self._storage = RawStorage()
        self._cleaner = DataCleaner()
        
        # State
        self._status = ScraperStatus.IDLE
        self._stats = ScraperStats()
        self._results: List[ScrapeResult] = []
        self._stop_event = asyncio.Event()
    
    async def _init_browser(self) -> None:
        """Initialize browser fetcher on demand."""
        if self._browser_fetcher is None:
            self._browser_fetcher = BrowserFetcher()
            await self._browser_fetcher.start()
    
    async def _close_browser(self) -> None:
        """Close browser fetcher."""
        if self._browser_fetcher:
            await self._browser_fetcher.close()
            self._browser_fetcher = None
    
    async def _fetch_url(self, url: str) -> FetchResult:
        """
        Fetch a URL using appropriate method.
        
        Tries HTTP first, falls back to browser if needed.
        """
        # Try HTTP first
        result = await self._http_fetcher.fetch(url)
        
        if result.success:
            # Check if content looks JS-rendered
            if self._site_detector.quick_check(result):
                logger.info(f"Switching to browser for {url}")
                await self._init_browser()
                browser_result = await self._browser_fetcher.fetch(url)
                self._stats.browser_fetches += 1
                return browser_result
            
            self._stats.http_fetches += 1
            return result
        
        # If HTTP failed with block, try browser
        if result.status_code in (403, 429):
            await self._rate_limiter.halt_domain(
                url,
                reason=str(result.status_code),
            )
            
            await self._init_browser()
            browser_result = await self._browser_fetcher.fetch(url)
            self._stats.browser_fetches += 1
            return browser_result
        
        self._stats.http_fetches += 1
        return result
    
    async def _process_url(self, url: str) -> ScrapeResult:
        """Process a single URL through the complete pipeline."""
        try:
            # Wait for rate limiter
            await self._rate_limiter.acquire(url)
            
            # Check if already stored
            if await self._storage.exists(url):
                logger.info(f"Loading from cache: {url}")
                content = await self._storage.load(url)
                
                # Parse cached content if parser provided
                data = {}
                if self._parser and content:
                    try:
                        data = self._parser(url, content)
                        data, _ = self._cleaner.clean_dict(data)
                    except Exception as e:
                        logger.error(f"Parser error for cached {url}: {e}")
                
                return ScrapeResult(
                    url=url,
                    success=True,
                    content=content or "",
                    data=data,
                )
            
            # Fetch content
            result = await self._fetch_url(url)
            
            if not result.success:
                return ScrapeResult(
                    url=url,
                    success=False,
                    status_code=result.status_code,
                    error=result.error,
                    response_time=result.response_time,
                )
            
            # Save raw content
            await self._storage.save(
                url=url,
                content=result.content,
                status_code=result.status_code,
            )
            
            self._stats.bytes_downloaded += len(result.content)
            
            # Parse if parser provided
            data = {}
            if self._parser:
                try:
                    data = self._parser(url, result.content)
                    # Clean the parsed data
                    data, _ = self._cleaner.clean_dict(data)
                except Exception as e:
                    logger.error(f"Parser error for {url}: {e}")
            
            # Report success to rate limiter
            await self._rate_limiter.report_success(url)
            
            return ScrapeResult(
                url=url,
                success=True,
                status_code=result.status_code,
                content=result.content,
                data=data,
                response_time=result.response_time,
                used_browser=hasattr(result, "final_url"),
            )
            
        except Exception as e:
            logger.error(f"Error processing {url}: {e}")
            return ScrapeResult(
                url=url,
                success=False,
                error=str(e),
            )
    
    async def _worker(self, worker_id: int) -> None:
        """Worker coroutine that processes URLs from queue."""
        while not self._stop_event.is_set():
            # Get next URL
            item = await self._queue.get(timeout=1.0)
            
            if item is None:
                if self._queue.is_empty:
                    break
                continue
            
            logger.debug(f"Worker {worker_id} processing: {item.url}")
            
            # Process URL
            result = await self._process_url(item.url)
            self._results.append(result)
            
            # Update stats
            self._stats.urls_processed += 1
            if result.success:
                self._stats.urls_successful += 1
            else:
                self._stats.urls_failed += 1
    
    async def run(
        self,
        urls: List[str],
        workers: int = 3,
        priority: Priority = Priority.NORMAL,
    ) -> List[ScrapeResult]:
        """
        Run the scraper on a list of URLs.
        
        Args:
            urls: List of URLs to scrape
            workers: Number of concurrent workers
            priority: Default priority for URLs
            
        Returns:
            List of ScrapeResult objects
        """
        self._status = ScraperStatus.RUNNING
        self._stats = ScraperStats(started_at=time.time())
        self._results = []
        self._stop_event.clear()
        
        # Ensure storage directories exist
        self._config.ensure_directories()
        
        # Add URLs to queue
        added = await self._queue.add_many(urls, priority=priority)
        console.print(f"[green]Added {added}/{len(urls)} URLs to queue[/green]")
        
        if added == 0:
            console.print("[yellow]No URLs to process (all filtered or duplicates)[/yellow]")
            return []
        
        # Start workers
        console.print(f"[blue]Starting {workers} workers...[/blue]")
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            task = progress.add_task("Scraping...", total=None)
            
            worker_tasks = [
                asyncio.create_task(self._worker(i))
                for i in range(workers)
            ]
            
            # Wait for completion
            await asyncio.gather(*worker_tasks)
            
            progress.update(task, description="Complete!")
        
        # Cleanup
        await self._close_browser()
        
        self._stats.finished_at = time.time()
        self._status = ScraperStatus.IDLE
        
        # Print summary
        console.print("\n[bold]Scraping Complete![/bold]")
        console.print(f"  Processed: {self._stats.urls_processed}")
        console.print(f"  Success: {self._stats.urls_successful}")
        console.print(f"  Failed: {self._stats.urls_failed}")
        console.print(f"  Duration: {self._stats.duration:.2f}s")
        
        return self._results
    
    async def run_single(self, url: str) -> ScrapeResult:
        """
        Scrape a single URL.
        
        Args:
            url: URL to scrape
            
        Returns:
            ScrapeResult
        """
        self._config.ensure_directories()
        
        # Check robots.txt
        if not await self._robots.can_fetch(url):
            return ScrapeResult(
                url=url,
                success=False,
                error="Blocked by robots.txt",
            )
        
        result = await self._process_url(url)
        await self._close_browser()
        
        return result
    
    def stop(self) -> None:
        """Stop the scraper gracefully."""
        self._stop_event.set()
        self._status = ScraperStatus.STOPPED
    
    def get_stats(self) -> dict:
        """Get current statistics."""
        return {
            "status": self._status.value,
            "scraper": self._stats.to_dict(),
            "queue": self._queue.get_stats(),
        }
    
    async def export_results(
        self,
        format: str = "json",
        filename: str | None = None,
    ) -> str:
        """
        Export results to file.
        
        Args:
            format: Export format (json, csv, sqlite)
            filename: Output filename
            
        Returns:
            Path to exported file
        """
        exporter = create_exporter(format)
        
        data = [
            {
                "url": r.url,
                "success": r.success,
                "status_code": r.status_code,
                "error": r.error,
                "response_time": r.response_time,
                **r.data,
            }
            for r in self._results
        ]
        
        return await exporter.export(data, filename)
