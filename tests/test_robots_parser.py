"""
Tests for the robots.txt parser module.
"""

import pytest
from scraper.safety.robots_parser import RobotsParser


class TestRobotsParser:
    """Tests for RobotsParser class."""
    
    def test_url_hash_consistency(self):
        """Test that the same URL always produces the same hash."""
        parser = RobotsParser()
        url = "https://example.com/page"
        
        hash1 = parser._cache_key(parser._get_domain(url))
        hash2 = parser._cache_key(parser._get_domain(url))
        
        assert hash1 == hash2
    
    def test_get_robots_url(self):
        """Test robots.txt URL extraction."""
        parser = RobotsParser()
        
        cases = [
            ("https://example.com/page/subpage", "https://example.com/robots.txt"),
            ("https://api.example.com/v1/users", "https://api.example.com/robots.txt"),
            ("http://localhost:8000/test", "http://localhost:8000/robots.txt"),
        ]
        
        for input_url, expected in cases:
            assert parser._get_robots_url(input_url) == expected
    
    def test_get_domain(self):
        """Test domain extraction from URLs."""
        parser = RobotsParser()
        
        cases = [
            ("https://example.com/page", "example.com"),
            ("https://sub.example.com/page", "sub.example.com"),
            ("http://localhost:8000/test", "localhost:8000"),
        ]
        
        for input_url, expected in cases:
            assert parser._get_domain(input_url) == expected


@pytest.mark.asyncio
async def test_robots_parser_caching():
    """Test that robots.txt content is cached."""
    parser = RobotsParser(cache_dir=".cache/test_robots")
    
    # First call should fetch
    # (This would normally make a network request, but we're testing the caching logic)
    # For a full test, we'd mock the HTTP client
    
    parser.close()


@pytest.mark.asyncio 
async def test_robots_parser_filter_urls():
    """Test URL filtering functionality."""
    parser = RobotsParser(cache_dir=".cache/test_robots")
    
    # Test with empty list
    result = await parser.filter_urls([])
    assert result == []
    
    parser.close()
