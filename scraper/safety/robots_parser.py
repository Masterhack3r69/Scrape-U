"""
Robots.txt Parser Module

Fetches, caches, and interprets robots.txt rules for ethical scraping compliance.
This is the #1 rule of ethical scraping - always respect robots.txt.
"""

import asyncio
import hashlib
import time
from dataclasses import dataclass, field
from typing import Dict, Optional
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

import httpx
from diskcache import Cache

from scraper.config import config


@dataclass
class RobotsRule:
    """Parsed robots.txt rules for a domain."""
    
    domain: str
    allowed_paths: list[str] = field(default_factory=list)
    disallowed_paths: list[str] = field(default_factory=list)
    crawl_delay: Optional[float] = None
    sitemaps: list[str] = field(default_factory=list)
    fetched_at: float = field(default_factory=time.time)
    raw_content: str = ""


class RobotsParser:
    """
    Fetches and caches robots.txt for domains.
    Filters URLs before they enter the scraping queue.
    
    Example:
        parser = RobotsParser()
        if await parser.can_fetch("https://example.com/products"):
            # Safe to scrape
            ...
    """
    
    # Default user agent to check against
    USER_AGENT = "*"
    
    def __init__(
        self,
        cache_dir: str = ".cache/robots",
        cache_ttl: int | None = None,
        user_agent: str | None = None,
    ):
        """
        Initialize the robots.txt parser.
        
        Args:
            cache_dir: Directory to cache robots.txt files
            cache_ttl: Cache TTL in seconds (default from config)
            user_agent: User agent string to check rules against
        """
        self._cache = Cache(cache_dir)
        self._cache_ttl = cache_ttl or config.robots_cache_ttl
        self._user_agent = user_agent or self.USER_AGENT
        self._parsers: Dict[str, RobotFileParser] = {}
        self._lock = asyncio.Lock()
    
    def _get_robots_url(self, url: str) -> str:
        """Extract the robots.txt URL from any URL."""
        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.netloc}/robots.txt"
    
    def _get_domain(self, url: str) -> str:
        """Extract domain from URL for caching."""
        parsed = urlparse(url)
        return parsed.netloc
    
    def _cache_key(self, domain: str) -> str:
        """Generate a cache key for a domain."""
        return f"robots_{hashlib.md5(domain.encode()).hexdigest()}"
    
    async def _fetch_robots_txt(self, robots_url: str) -> str:
        """Fetch robots.txt content from a URL."""
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(robots_url)
                
                if response.status_code == 200:
                    return response.text
                elif response.status_code in (404, 403):
                    # No robots.txt or forbidden - assume everything allowed
                    return ""
                else:
                    # Other errors - be conservative, assume blocked
                    return "User-agent: *\nDisallow: /"
                    
        except httpx.RequestError:
            # Network error - be conservative
            return "User-agent: *\nDisallow: /"
    
    async def _get_parser(self, url: str) -> RobotFileParser:
        """Get or create a RobotFileParser for a URL's domain."""
        domain = self._get_domain(url)
        cache_key = self._cache_key(domain)
        
        async with self._lock:
            # Check memory cache first
            if domain in self._parsers:
                return self._parsers[domain]
            
            # Check disk cache
            cached = self._cache.get(cache_key)
            if cached is not None:
                parser = RobotFileParser()
                parser.parse(cached.split("\n"))
                self._parsers[domain] = parser
                return parser
            
            # Fetch from web
            robots_url = self._get_robots_url(url)
            content = await self._fetch_robots_txt(robots_url)
            
            # Cache it
            self._cache.set(cache_key, content, expire=self._cache_ttl)
            
            # Parse and store
            parser = RobotFileParser()
            parser.parse(content.split("\n"))
            self._parsers[domain] = parser
            
            return parser
    
    async def can_fetch(self, url: str) -> bool:
        """
        Check if a URL is allowed to be fetched according to robots.txt.
        
        Args:
            url: The URL to check
            
        Returns:
            True if allowed, False if disallowed
        """
        if not config.respect_robots_txt:
            return True
            
        parser = await self._get_parser(url)
        return parser.can_fetch(self._user_agent, url)
    
    async def get_crawl_delay(self, url: str) -> Optional[float]:
        """
        Get the crawl delay specified in robots.txt for a domain.
        
        Args:
            url: Any URL from the domain
            
        Returns:
            Crawl delay in seconds, or None if not specified
        """
        parser = await self._get_parser(url)
        return parser.crawl_delay(self._user_agent)
    
    async def filter_urls(self, urls: list[str]) -> list[str]:
        """
        Filter a list of URLs, removing those disallowed by robots.txt.
        
        Args:
            urls: List of URLs to filter
            
        Returns:
            List of allowed URLs
        """
        allowed = []
        for url in urls:
            if await self.can_fetch(url):
                allowed.append(url)
        return allowed
    
    def clear_cache(self) -> None:
        """Clear all cached robots.txt data."""
        self._cache.clear()
        self._parsers.clear()
    
    def close(self) -> None:
        """Close the cache connection."""
        self._cache.close()
