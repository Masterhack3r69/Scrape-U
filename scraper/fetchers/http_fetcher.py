"""
HTTP Fetcher Module

Lightweight async HTTP client for static HTML sites.
Uses httpx with User-Agent rotation and proxy support.
"""

import time
from dataclasses import dataclass, field
from typing import Optional

import httpx

from scraper.config import config
from scraper.stealth.user_agents import UserAgentRotator
from scraper.stealth.proxy_pool import ProxyPool, Proxy


@dataclass
class FetchResult:
    """Result of a fetch operation."""
    
    url: str
    status_code: int
    content: str = ""
    headers: dict = field(default_factory=dict)
    response_time: float = 0.0
    error: Optional[str] = None
    used_proxy: Optional[str] = None
    
    @property
    def success(self) -> bool:
        """Check if fetch was successful."""
        return 200 <= self.status_code < 300 and self.error is None
    
    @property
    def is_blocked(self) -> bool:
        """Check if request was blocked (403/429)."""
        return self.status_code in (403, 429)
    
    @property
    def needs_browser(self) -> bool:
        """Check if this result suggests browser rendering is needed."""
        # Empty content or very short content might indicate JS-rendered page
        return self.success and len(self.content.strip()) < 500


class HTTPFetcher:
    """
    Async HTTP fetcher for static HTML content.
    
    Features:
    - User-Agent rotation per request
    - Optional proxy support
    - Automatic header management
    - Response time tracking
    
    Example:
        fetcher = HTTPFetcher()
        result = await fetcher.fetch("https://example.com")
        if result.success:
            print(result.content)
    """
    
    def __init__(
        self,
        user_agent_rotator: UserAgentRotator | None = None,
        proxy_pool: ProxyPool | None = None,
        timeout: float = 30.0,
    ):
        """
        Initialize the HTTP fetcher.
        
        Args:
            user_agent_rotator: UA rotator instance (creates default if None)
            proxy_pool: Proxy pool instance (optional)
            timeout: Request timeout in seconds
        """
        self._ua_rotator = user_agent_rotator or UserAgentRotator()
        self._proxy_pool = proxy_pool
        self._timeout = timeout
    
    async def fetch(
        self,
        url: str,
        headers: dict | None = None,
        follow_redirects: bool = True,
    ) -> FetchResult:
        """
        Fetch a URL and return the content.
        
        Args:
            url: The URL to fetch
            headers: Optional additional headers
            follow_redirects: Whether to follow redirects
            
        Returns:
            FetchResult with content and metadata
        """
        # Prepare headers
        request_headers = self._ua_rotator.get_headers()
        if headers:
            request_headers.update(headers)
        
        # Get proxy if available
        proxy: Optional[Proxy] = None
        proxy_dict = None
        
        if self._proxy_pool and config.proxy.enabled:
            proxy = await self._proxy_pool.get_proxy()
            if proxy:
                proxy_dict = self._proxy_pool.get_proxy_dict(proxy)
        
        start_time = time.time()
        
        try:
            async with httpx.AsyncClient(
                timeout=self._timeout,
                follow_redirects=follow_redirects,
                proxies=proxy_dict,
                http2=True,
            ) as client:
                response = await client.get(url, headers=request_headers)
                response_time = time.time() - start_time
                
                result = FetchResult(
                    url=url,
                    status_code=response.status_code,
                    content=response.text,
                    headers=dict(response.headers),
                    response_time=response_time,
                    used_proxy=proxy.url if proxy else None,
                )
                
                # Report to proxy pool
                if proxy and self._proxy_pool:
                    if result.success:
                        await self._proxy_pool.report_success(proxy, response_time)
                    elif result.is_blocked:
                        await self._proxy_pool.report_failure(proxy)
                
                return result
                
        except httpx.TimeoutException:
            return FetchResult(
                url=url,
                status_code=0,
                error="Request timed out",
                response_time=time.time() - start_time,
                used_proxy=proxy.url if proxy else None,
            )
        except httpx.RequestError as e:
            if proxy and self._proxy_pool:
                await self._proxy_pool.report_failure(proxy)
            
            return FetchResult(
                url=url,
                status_code=0,
                error=str(e),
                response_time=time.time() - start_time,
                used_proxy=proxy.url if proxy else None,
            )
    
    async def fetch_multiple(
        self,
        urls: list[str],
        concurrency: int = 5,
    ) -> list[FetchResult]:
        """
        Fetch multiple URLs with controlled concurrency.
        
        Args:
            urls: List of URLs to fetch
            concurrency: Maximum concurrent requests
            
        Returns:
            List of FetchResult objects
        """
        import asyncio
        
        semaphore = asyncio.Semaphore(concurrency)
        
        async def fetch_with_semaphore(url: str) -> FetchResult:
            async with semaphore:
                return await self.fetch(url)
        
        tasks = [fetch_with_semaphore(url) for url in urls]
        return await asyncio.gather(*tasks)
