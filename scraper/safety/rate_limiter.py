"""
Token Bucket Rate Limiter Module

Implements the "Politeness" rate limiter to prevent overwhelming target servers.
Uses a Token Bucket algorithm with configurable refill rates.
"""

import asyncio
import random
import time
from dataclasses import dataclass, field
from typing import Dict, Optional

from scraper.config import config


@dataclass
class TokenBucket:
    """A single token bucket for rate limiting."""
    
    max_tokens: int
    refill_rate: float  # tokens per second
    tokens: float = field(init=False)
    last_refill: float = field(default_factory=time.time)
    
    def __post_init__(self):
        self.tokens = float(self.max_tokens)
    
    def refill(self) -> None:
        """Refill tokens based on elapsed time."""
        now = time.time()
        elapsed = now - self.last_refill
        self.tokens = min(self.max_tokens, self.tokens + elapsed * self.refill_rate)
        self.last_refill = now
    
    def consume(self, tokens: int = 1) -> bool:
        """
        Try to consume tokens from the bucket.
        
        Args:
            tokens: Number of tokens to consume
            
        Returns:
            True if tokens were consumed, False if not enough tokens
        """
        self.refill()
        if self.tokens >= tokens:
            self.tokens -= tokens
            return True
        return False
    
    def time_until_available(self, tokens: int = 1) -> float:
        """
        Calculate time until enough tokens are available.
        
        Args:
            tokens: Number of tokens needed
            
        Returns:
            Seconds until tokens available (0 if already available)
        """
        self.refill()
        if self.tokens >= tokens:
            return 0.0
        needed = tokens - self.tokens
        return needed / self.refill_rate


@dataclass
class DomainState:
    """Track state for a specific domain."""
    
    bucket: TokenBucket
    last_request: float = 0.0
    halted_until: float = 0.0
    consecutive_errors: int = 0
    strict_mode: bool = False


class TokenBucketRateLimiter:
    """
    Per-domain rate limiter using Token Bucket algorithm.
    
    Features:
    - Token bucket with configurable capacity and refill rate
    - Random jitter between requests
    - Domain-specific tracking
    - "Red Light Law" halt support for 403/429/captcha
    - Strict mode for sensitive sites
    
    Example:
        limiter = TokenBucketRateLimiter()
        await limiter.acquire("https://example.com/page1")
        # Now safe to make request
    """
    
    def __init__(
        self,
        max_tokens: int | None = None,
        refill_rate: float | None = None,
        min_delay: float | None = None,
        max_delay: float | None = None,
    ):
        """
        Initialize the rate limiter.
        
        Args:
            max_tokens: Maximum tokens per bucket (default from config)
            refill_rate: Token refill rate per second (default from config)
            min_delay: Minimum delay between requests (default from config)
            max_delay: Maximum delay between requests (default from config)
        """
        self._max_tokens = max_tokens or config.rate_limit.max_tokens
        self._refill_rate = refill_rate or config.rate_limit.refill_rate
        self._min_delay = min_delay or config.rate_limit.min_delay
        self._max_delay = max_delay or config.rate_limit.max_delay
        
        self._domains: Dict[str, DomainState] = {}
        self._lock = asyncio.Lock()
    
    def _get_domain(self, url: str) -> str:
        """Extract domain from URL."""
        from urllib.parse import urlparse
        return urlparse(url).netloc
    
    async def _get_domain_state(self, domain: str) -> DomainState:
        """Get or create domain state."""
        async with self._lock:
            if domain not in self._domains:
                bucket = TokenBucket(
                    max_tokens=self._max_tokens,
                    refill_rate=self._refill_rate,
                )
                self._domains[domain] = DomainState(bucket=bucket)
            return self._domains[domain]
    
    def _get_delay(self, state: DomainState) -> float:
        """Calculate the delay with jitter."""
        if state.strict_mode:
            return random.uniform(
                config.rate_limit.strict_min_delay,
                config.rate_limit.strict_max_delay,
            )
        return random.uniform(self._min_delay, self._max_delay)
    
    async def acquire(self, url: str) -> None:
        """
        Acquire permission to make a request to the given URL.
        Blocks until it's safe to proceed.
        
        Args:
            url: The URL to request
        """
        domain = self._get_domain(url)
        state = await self._get_domain_state(domain)
        
        # Check if domain is halted (Red Light Law)
        now = time.time()
        if state.halted_until > now:
            wait_time = state.halted_until - now
            await asyncio.sleep(wait_time)
        
        # Wait for token availability
        while not state.bucket.consume():
            wait_time = state.bucket.time_until_available()
            await asyncio.sleep(wait_time)
        
        # Apply jitter delay
        delay = self._get_delay(state)
        time_since_last = now - state.last_request
        
        if time_since_last < delay:
            await asyncio.sleep(delay - time_since_last)
        
        # Update last request time
        state.last_request = time.time()
    
    async def halt_domain(
        self,
        url: str,
        duration: float | None = None,
        reason: str = "unknown",
    ) -> None:
        """
        Halt all requests to a domain (Red Light Law).
        
        Args:
            url: Any URL from the domain to halt
            duration: Halt duration in seconds (default based on reason)
            reason: Reason for halt ("403", "429", "captcha")
        """
        domain = self._get_domain(url)
        state = await self._get_domain_state(domain)
        
        if duration is None:
            duration_map = {
                "403": config.halt_on_403,
                "429": config.halt_on_429,
                "captcha": config.halt_on_captcha,
            }
            duration = duration_map.get(reason, 60)
        
        state.halted_until = time.time() + duration
        state.consecutive_errors += 1
    
    async def set_strict_mode(self, url: str, strict: bool = True) -> None:
        """
        Enable/disable strict mode for a domain.
        
        Args:
            url: Any URL from the domain
            strict: Whether to enable strict mode
        """
        domain = self._get_domain(url)
        state = await self._get_domain_state(domain)
        state.strict_mode = strict
    
    async def report_success(self, url: str) -> None:
        """
        Report a successful request (resets error counter).
        
        Args:
            url: The URL that succeeded
        """
        domain = self._get_domain(url)
        state = await self._get_domain_state(domain)
        state.consecutive_errors = 0
    
    async def get_stats(self, url: str) -> dict:
        """
        Get statistics for a domain.
        
        Args:
            url: Any URL from the domain
            
        Returns:
            Dict with domain stats
        """
        domain = self._get_domain(url)
        state = await self._get_domain_state(domain)
        
        return {
            "domain": domain,
            "tokens": state.bucket.tokens,
            "max_tokens": state.bucket.max_tokens,
            "consecutive_errors": state.consecutive_errors,
            "strict_mode": state.strict_mode,
            "halted_until": state.halted_until,
            "is_halted": state.halted_until > time.time(),
        }
