"""
Configuration module for the Advanced Web Scraper.

Uses Pydantic Settings for type-safe configuration with environment variable support.
"""

from pathlib import Path
from typing import Literal
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class RateLimitConfig(BaseSettings):
    """Rate limiting configuration using Token Bucket algorithm."""
    
    model_config = SettingsConfigDict(env_prefix="SCRAPER_RATE_")
    
    # Token bucket settings
    max_tokens: int = Field(default=5, description="Maximum tokens in bucket")
    refill_rate: float = Field(default=0.5, description="Tokens per second refill rate")
    
    # Delay settings
    min_delay: float = Field(default=2.0, description="Minimum delay between requests (seconds)")
    max_delay: float = Field(default=5.0, description="Maximum delay between requests (seconds)")
    
    # Strict mode for sensitive sites
    strict_min_delay: float = Field(default=10.0, description="Strict mode minimum delay")
    strict_max_delay: float = Field(default=30.0, description="Strict mode maximum delay")


class ProxyConfig(BaseSettings):
    """Proxy pool configuration."""
    
    model_config = SettingsConfigDict(env_prefix="SCRAPER_PROXY_")
    
    enabled: bool = Field(default=False, description="Enable proxy rotation")
    proxy_file: Path | None = Field(default=None, description="Path to proxy list file")
    rotation_strategy: Literal["round_robin", "random"] = Field(
        default="random", 
        description="Proxy rotation strategy"
    )
    health_check_interval: int = Field(default=300, description="Seconds between health checks")
    max_failures: int = Field(default=3, description="Max failures before disabling proxy")


class BrowserConfig(BaseSettings):
    """Headless browser configuration."""
    
    model_config = SettingsConfigDict(env_prefix="SCRAPER_BROWSER_")
    
    headless: bool = Field(default=True, description="Run browser in headless mode")
    timeout: int = Field(default=30000, description="Page load timeout in milliseconds")
    
    # Resource blocking
    block_images: bool = Field(default=True, description="Block image requests")
    block_fonts: bool = Field(default=True, description="Block font requests")
    block_media: bool = Field(default=True, description="Block media requests")
    block_analytics: bool = Field(default=True, description="Block analytics/tracking")
    
    # Blocked domains for analytics/ads
    blocked_domains: list[str] = Field(
        default=[
            "google-analytics.com",
            "googletagmanager.com",
            "facebook.com",
            "doubleclick.net",
            "analytics.",
            "tracker.",
            "ads.",
        ],
        description="Domains to block"
    )


class StorageConfig(BaseSettings):
    """Data storage configuration."""
    
    model_config = SettingsConfigDict(env_prefix="SCRAPER_STORAGE_")
    
    base_path: Path = Field(default=Path("storage"), description="Base storage directory")
    raw_subdir: str = Field(default="raw", description="Raw HTML storage subdirectory")
    export_subdir: str = Field(default="exports", description="Export files subdirectory")
    
    # Export formats
    export_format: Literal["json", "csv", "sqlite"] = Field(
        default="json",
        description="Default export format"
    )
    sqlite_db_name: str = Field(default="scraped_data.db", description="SQLite database filename")
    
    @property
    def raw_path(self) -> Path:
        """Full path to raw storage directory."""
        return self.base_path / self.raw_subdir
    
    @property
    def export_path(self) -> Path:
        """Full path to export directory."""
        return self.base_path / self.export_subdir


class ScraperConfig(BaseSettings):
    """Main scraper configuration aggregating all sub-configs."""
    
    model_config = SettingsConfigDict(
        env_prefix="SCRAPER_",
        env_nested_delimiter="__",
    )
    
    # Sub-configurations
    rate_limit: RateLimitConfig = Field(default_factory=RateLimitConfig)
    proxy: ProxyConfig = Field(default_factory=ProxyConfig)
    browser: BrowserConfig = Field(default_factory=BrowserConfig)
    storage: StorageConfig = Field(default_factory=StorageConfig)
    
    # General settings
    user_agent_rotation: bool = Field(default=True, description="Enable User-Agent rotation")
    respect_robots_txt: bool = Field(default=True, description="Respect robots.txt rules")
    robots_cache_ttl: int = Field(default=3600, description="Robots.txt cache TTL in seconds")
    
    # Retry settings
    max_retries: int = Field(default=3, description="Maximum retry attempts")
    retry_backoff: float = Field(default=2.0, description="Exponential backoff multiplier")
    
    # Red Light Law - halt durations
    halt_on_403: int = Field(default=60, description="Seconds to halt after 403")
    halt_on_429: int = Field(default=60, description="Seconds to halt after 429")
    halt_on_captcha: int = Field(default=120, description="Seconds to halt after captcha")
    
    # Logging
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = Field(
        default="INFO",
        description="Logging level"
    )
    
    def ensure_directories(self) -> None:
        """Create necessary storage directories if they don't exist."""
        self.storage.raw_path.mkdir(parents=True, exist_ok=True)
        self.storage.export_path.mkdir(parents=True, exist_ok=True)


# Global config instance (can be overridden)
config = ScraperConfig()
