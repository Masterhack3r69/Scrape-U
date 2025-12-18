"""
Tests for the rate limiter module.
"""

import pytest
import asyncio
import time

from scraper.safety.rate_limiter import TokenBucket, TokenBucketRateLimiter


class TestTokenBucket:
    """Tests for TokenBucket class."""
    
    def test_initial_tokens(self):
        """Test that bucket starts with max tokens."""
        bucket = TokenBucket(max_tokens=5, refill_rate=1.0)
        assert bucket.tokens == 5.0
    
    def test_consume_success(self):
        """Test consuming tokens when available."""
        bucket = TokenBucket(max_tokens=5, refill_rate=1.0)
        
        assert bucket.consume(1) is True
        # After consuming 1 token from 5, we should have ~4 (may gain tiny amount from refill)
        assert 3.5 <= bucket.tokens <= 5.0
        
        assert bucket.consume(2) is True
        # After consuming 2 more, should have ~2 left
        assert 1.5 <= bucket.tokens <= 3.0
    
    def test_consume_failure(self):
        """Test consuming fails when not enough tokens."""
        bucket = TokenBucket(max_tokens=2, refill_rate=0.1)
        
        bucket.consume(2)  # Use all tokens
        assert bucket.consume(1) is False
    
    def test_refill(self):
        """Test token refill over time."""
        bucket = TokenBucket(max_tokens=5, refill_rate=10.0)  # Fast refill for test
        
        bucket.consume(5)  # Empty bucket
        assert bucket.tokens < 1
        
        # Wait a bit
        time.sleep(0.2)
        bucket.refill()
        
        # Should have some tokens back
        assert bucket.tokens >= 1
    
    def test_time_until_available(self):
        """Test calculation of time until tokens available."""
        bucket = TokenBucket(max_tokens=5, refill_rate=1.0)
        
        # Full bucket - should be available immediately
        assert bucket.time_until_available(1) == 0.0
        
        # Empty bucket
        bucket.consume(5)
        wait_time = bucket.time_until_available(2)
        
        # Should be approximately 2 seconds (2 tokens / 1 token per second)
        assert 1.0 < wait_time < 3.0


class TestTokenBucketRateLimiter:
    """Tests for TokenBucketRateLimiter class."""
    
    def test_get_domain(self):
        """Test domain extraction."""
        limiter = TokenBucketRateLimiter()
        
        cases = [
            ("https://example.com/page", "example.com"),
            ("https://api.example.com/v1", "api.example.com"),
            ("http://localhost:8000/test", "localhost:8000"),
        ]
        
        for url, expected in cases:
            assert limiter._get_domain(url) == expected


@pytest.mark.asyncio
async def test_rate_limiter_acquire():
    """Test acquiring permission from rate limiter."""
    limiter = TokenBucketRateLimiter(
        max_tokens=5,
        refill_rate=10.0,  # Fast refill
        min_delay=0.01,    # Minimal delay for testing
        max_delay=0.02,
    )
    
    start = time.time()
    await limiter.acquire("https://example.com/page1")
    duration = time.time() - start
    
    # Should complete quickly
    assert duration < 0.5


@pytest.mark.asyncio
async def test_rate_limiter_halt_domain():
    """Test domain halting (Red Light Law)."""
    limiter = TokenBucketRateLimiter()
    
    url = "https://example.com/page"
    await limiter.halt_domain(url, duration=0.5, reason="429")
    
    stats = await limiter.get_stats(url)
    assert stats["is_halted"] is True


@pytest.mark.asyncio
async def test_rate_limiter_strict_mode():
    """Test strict mode activation."""
    limiter = TokenBucketRateLimiter()
    
    url = "https://sensitive-site.com/page"
    await limiter.set_strict_mode(url, strict=True)
    
    stats = await limiter.get_stats(url)
    assert stats["strict_mode"] is True
