"""
Raw Storage Module

Saves raw HTML/JSON to disk for later reprocessing.
Prevents re-scraping via file existence check.
"""

import hashlib
import json
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Optional

import aiofiles

from scraper.config import config


@dataclass
class StoredContent:
    """Metadata about stored content."""
    
    url: str
    url_hash: str
    filename: str
    content_type: str  # "html" or "json"
    stored_at: float
    size_bytes: int
    status_code: int
    
    def to_dict(self) -> dict:
        return asdict(self)


class RawStorage:
    """
    Raw content storage for scraped data.
    
    Features:
    - URL-based file naming (hash)
    - Prevents duplicate scraping
    - Metadata tracking
    - Async file operations
    
    Example:
        storage = RawStorage()
        
        # Check before scraping
        if not await storage.exists(url):
            content = await fetch(url)
            await storage.save(url, content)
        
        # Later, retrieve for processing
        content = await storage.load(url)
    """
    
    def __init__(self, base_path: Path | str | None = None):
        """
        Initialize raw storage.
        
        Args:
            base_path: Base directory for storage (default from config)
        """
        self._base_path = Path(base_path) if base_path else config.storage.raw_path
        self._metadata_file = self._base_path / "metadata.json"
        self._metadata: dict[str, dict] = {}
        self._initialized = False
    
    async def _ensure_initialized(self) -> None:
        """Ensure storage directory exists and metadata is loaded."""
        if self._initialized:
            return
        
        self._base_path.mkdir(parents=True, exist_ok=True)
        
        if self._metadata_file.exists():
            async with aiofiles.open(self._metadata_file, "r", encoding="utf-8") as f:
                content = await f.read()
                self._metadata = json.loads(content) if content else {}
        
        self._initialized = True
    
    async def _save_metadata(self) -> None:
        """Save metadata to disk."""
        async with aiofiles.open(self._metadata_file, "w", encoding="utf-8") as f:
            await f.write(json.dumps(self._metadata, indent=2))
    
    def _url_hash(self, url: str) -> str:
        """Generate a hash from URL for filename."""
        return hashlib.sha256(url.encode()).hexdigest()[:16]
    
    def _get_filepath(self, url: str, content_type: str = "html") -> Path:
        """Get the file path for a URL."""
        url_hash = self._url_hash(url)
        extension = "json" if content_type == "json" else "html"
        return self._base_path / f"{url_hash}.{extension}"
    
    async def exists(self, url: str) -> bool:
        """
        Check if content for a URL already exists.
        
        Args:
            url: The URL to check
            
        Returns:
            True if content exists
        """
        await self._ensure_initialized()
        url_hash = self._url_hash(url)
        return url_hash in self._metadata
    
    async def save(
        self,
        url: str,
        content: str,
        content_type: str = "html",
        status_code: int = 200,
    ) -> StoredContent:
        """
        Save content to disk.
        
        Args:
            url: The source URL
            content: The content to save
            content_type: "html" or "json"
            status_code: HTTP status code
            
        Returns:
            StoredContent metadata
        """
        await self._ensure_initialized()
        
        filepath = self._get_filepath(url, content_type)
        url_hash = self._url_hash(url)
        
        # Save content
        async with aiofiles.open(filepath, "w", encoding="utf-8") as f:
            await f.write(content)
        
        # Create metadata
        stored = StoredContent(
            url=url,
            url_hash=url_hash,
            filename=filepath.name,
            content_type=content_type,
            stored_at=time.time(),
            size_bytes=len(content.encode()),
            status_code=status_code,
        )
        
        # Update metadata
        self._metadata[url_hash] = stored.to_dict()
        await self._save_metadata()
        
        return stored
    
    async def load(self, url: str) -> Optional[str]:
        """
        Load content from disk.
        
        Args:
            url: The URL to load content for
            
        Returns:
            Content string or None if not found
        """
        await self._ensure_initialized()
        
        url_hash = self._url_hash(url)
        if url_hash not in self._metadata:
            return None
        
        meta = self._metadata[url_hash]
        filepath = self._base_path / meta["filename"]
        
        if not filepath.exists():
            return None
        
        async with aiofiles.open(filepath, "r", encoding="utf-8") as f:
            return await f.read()
    
    async def get_metadata(self, url: str) -> Optional[StoredContent]:
        """
        Get metadata for a stored URL.
        
        Args:
            url: The URL to get metadata for
            
        Returns:
            StoredContent or None
        """
        await self._ensure_initialized()
        
        url_hash = self._url_hash(url)
        if url_hash not in self._metadata:
            return None
        
        return StoredContent(**self._metadata[url_hash])
    
    async def delete(self, url: str) -> bool:
        """
        Delete stored content for a URL.
        
        Args:
            url: The URL to delete
            
        Returns:
            True if deleted, False if not found
        """
        await self._ensure_initialized()
        
        url_hash = self._url_hash(url)
        if url_hash not in self._metadata:
            return False
        
        meta = self._metadata[url_hash]
        filepath = self._base_path / meta["filename"]
        
        if filepath.exists():
            filepath.unlink()
        
        del self._metadata[url_hash]
        await self._save_metadata()
        
        return True
    
    async def list_all(self) -> list[StoredContent]:
        """List all stored content metadata."""
        await self._ensure_initialized()
        return [StoredContent(**m) for m in self._metadata.values()]
    
    async def get_stats(self) -> dict:
        """Get storage statistics."""
        await self._ensure_initialized()
        
        total_size = sum(m.get("size_bytes", 0) for m in self._metadata.values())
        
        return {
            "total_files": len(self._metadata),
            "total_size_bytes": total_size,
            "total_size_mb": round(total_size / (1024 * 1024), 2),
            "storage_path": str(self._base_path),
        }
    
    async def clear(self) -> int:
        """
        Clear all stored content.
        
        Returns:
            Number of files deleted
        """
        await self._ensure_initialized()
        
        count = 0
        for meta in self._metadata.values():
            filepath = self._base_path / meta["filename"]
            if filepath.exists():
                filepath.unlink()
                count += 1
        
        self._metadata = {}
        await self._save_metadata()
        
        return count
