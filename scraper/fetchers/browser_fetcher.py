"""
Browser Fetcher Module

Headless browser fetcher using Playwright for JavaScript-rendered sites.
Includes stealth plugins and resource blocking for efficiency.
"""

import time
from dataclasses import dataclass, field
from typing import Optional

from scraper.config import config
from scraper.fetchers.http_fetcher import FetchResult


@dataclass
class BrowserFetchResult(FetchResult):
    """Extended result for browser fetches."""
    
    screenshot: Optional[bytes] = None
    final_url: str = ""
    js_errors: list[str] = field(default_factory=list)


class BrowserFetcher:
    """
    Headless browser fetcher for JavaScript-rendered content.
    
    Features:
    - Playwright-based headless Chromium
    - Stealth plugin to avoid detection
    - Resource blocking (images, fonts, analytics)
    - wait_for_selector support
    - Screenshot capture option
    
    Example:
        async with BrowserFetcher() as fetcher:
            result = await fetcher.fetch(
                "https://example.com/spa",
                wait_for="div.content",
            )
    """
    
    # Resource types to block
    BLOCKED_RESOURCE_TYPES = {"image", "media", "font", "stylesheet"}
    
    def __init__(
        self,
        headless: bool | None = None,
        timeout: int | None = None,
    ):
        """
        Initialize the browser fetcher.
        
        Args:
            headless: Run in headless mode (default from config)
            timeout: Page load timeout in ms (default from config)
        """
        self._headless = headless if headless is not None else config.browser.headless
        self._timeout = timeout or config.browser.timeout
        self._browser = None
        self._playwright = None
    
    async def __aenter__(self):
        """Start the browser."""
        await self.start()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Close the browser."""
        await self.close()
    
    async def start(self) -> None:
        """Start the browser instance."""
        try:
            from playwright.async_api import async_playwright
            from playwright_stealth import stealth_async
        except ImportError:
            raise ImportError(
                "playwright and playwright-stealth are required. "
                "Install with: pip install playwright playwright-stealth && playwright install chromium"
            )
        
        self._stealth_async = stealth_async
        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(
            headless=self._headless,
        )
    
    async def close(self) -> None:
        """Close the browser instance."""
        if self._browser:
            await self._browser.close()
            self._browser = None
        if self._playwright:
            await self._playwright.stop()
            self._playwright = None
    
    def _should_block_request(self, route) -> bool:
        """Check if a request should be blocked."""
        request = route.request
        
        # Block by resource type
        if config.browser.block_images and request.resource_type == "image":
            return True
        if config.browser.block_fonts and request.resource_type == "font":
            return True
        if config.browser.block_media and request.resource_type == "media":
            return True
        
        # Block analytics/ads by domain
        if config.browser.block_analytics:
            url = request.url.lower()
            for blocked in config.browser.blocked_domains:
                if blocked in url:
                    return True
        
        return False
    
    async def fetch(
        self,
        url: str,
        wait_for: str | None = None,
        wait_timeout: int = 10000,
        take_screenshot: bool = False,
        scroll_to_bottom: bool = False,
    ) -> BrowserFetchResult:
        """
        Fetch a URL using headless browser.
        
        Args:
            url: The URL to fetch
            wait_for: CSS selector to wait for before extraction
            wait_timeout: Timeout for wait_for in ms
            take_screenshot: Whether to capture a screenshot
            scroll_to_bottom: Scroll to trigger lazy loading
            
        Returns:
            BrowserFetchResult with rendered content
        """
        if not self._browser:
            await self.start()
        
        start_time = time.time()
        js_errors: list[str] = []
        
        try:
            # Create new page
            page = await self._browser.new_page()
            
            # Apply stealth
            await self._stealth_async(page)
            
            # Set up request interception for blocking
            async def handle_route(route):
                if self._should_block_request(route):
                    await route.abort()
                else:
                    await route.continue_()
            
            await page.route("**/*", handle_route)
            
            # Capture JS errors
            page.on("pageerror", lambda error: js_errors.append(str(error)))
            
            # Navigate
            response = await page.goto(
                url,
                timeout=self._timeout,
                wait_until="networkidle",
            )
            
            status_code = response.status if response else 0
            
            # Wait for specific element if requested
            if wait_for:
                try:
                    await page.wait_for_selector(wait_for, timeout=wait_timeout)
                except Exception:
                    pass  # Continue even if selector not found
            
            # Scroll to bottom for lazy loading
            if scroll_to_bottom:
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await page.wait_for_timeout(1000)  # Wait for lazy content
            
            # Get content
            content = await page.content()
            final_url = page.url
            
            # Screenshot if requested
            screenshot = None
            if take_screenshot:
                screenshot = await page.screenshot(full_page=True)
            
            await page.close()
            
            return BrowserFetchResult(
                url=url,
                status_code=status_code,
                content=content,
                final_url=final_url,
                response_time=time.time() - start_time,
                screenshot=screenshot,
                js_errors=js_errors,
            )
            
        except Exception as e:
            return BrowserFetchResult(
                url=url,
                status_code=0,
                error=str(e),
                response_time=time.time() - start_time,
                js_errors=js_errors,
            )
    
    async def fetch_with_interaction(
        self,
        url: str,
        actions: list[dict],
        wait_for: str | None = None,
    ) -> BrowserFetchResult:
        """
        Fetch with custom interactions (click, type, etc.).
        
        Args:
            url: The URL to fetch
            actions: List of actions [{"type": "click", "selector": "..."}]
            wait_for: Selector to wait for after actions
            
        Returns:
            BrowserFetchResult with rendered content
        """
        if not self._browser:
            await self.start()
        
        start_time = time.time()
        js_errors: list[str] = []
        
        try:
            page = await self._browser.new_page()
            await self._stealth_async(page)
            
            # Navigate
            response = await page.goto(url, timeout=self._timeout)
            status_code = response.status if response else 0
            
            # Execute actions
            for action in actions:
                action_type = action.get("type")
                selector = action.get("selector")
                
                if action_type == "click":
                    await page.click(selector)
                elif action_type == "type":
                    await page.fill(selector, action.get("text", ""))
                elif action_type == "wait":
                    await page.wait_for_timeout(action.get("ms", 1000))
                elif action_type == "scroll":
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            
            # Wait for final state
            if wait_for:
                await page.wait_for_selector(wait_for, timeout=10000)
            
            content = await page.content()
            final_url = page.url
            await page.close()
            
            return BrowserFetchResult(
                url=url,
                status_code=status_code,
                content=content,
                final_url=final_url,
                response_time=time.time() - start_time,
                js_errors=js_errors,
            )
            
        except Exception as e:
            return BrowserFetchResult(
                url=url,
                status_code=0,
                error=str(e),
                response_time=time.time() - start_time,
            )
