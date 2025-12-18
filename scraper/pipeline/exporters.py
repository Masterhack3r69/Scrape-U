"""
Data Exporters Module

Export cleaned data to various formats: JSON, CSV, SQLite.
"""

import csv
import json
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import aiosqlite
import aiofiles

from scraper.config import config


class BaseExporter(ABC):
    """Abstract base class for data exporters."""
    
    @abstractmethod
    async def export(
        self,
        data: List[Dict[str, Any]],
        filename: str | None = None,
    ) -> str:
        """
        Export data to the target format.
        
        Args:
            data: List of dictionaries to export
            filename: Optional filename (auto-generated if None)
            
        Returns:
            Path to the exported file
        """
        pass
    
    def _ensure_export_dir(self) -> Path:
        """Ensure export directory exists."""
        export_path = config.storage.export_path
        export_path.mkdir(parents=True, exist_ok=True)
        return export_path
    
    def _generate_filename(self, extension: str) -> str:
        """Generate a timestamped filename."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"export_{timestamp}.{extension}"


class JSONExporter(BaseExporter):
    """
    Export data to JSON format.
    
    Features:
    - Pretty-printed output
    - JSON Lines option for large datasets
    - Streaming support
    
    Example:
        exporter = JSONExporter()
        filepath = await exporter.export(data)
    """
    
    def __init__(
        self,
        pretty: bool = True,
        jsonl: bool = False,
    ):
        """
        Initialize JSON exporter.
        
        Args:
            pretty: Pretty-print JSON (ignored if jsonl=True)
            jsonl: Export as JSON Lines (one object per line)
        """
        self._pretty = pretty
        self._jsonl = jsonl
    
    async def export(
        self,
        data: List[Dict[str, Any]],
        filename: str | None = None,
    ) -> str:
        """Export data to JSON file."""
        export_dir = self._ensure_export_dir()
        
        ext = "jsonl" if self._jsonl else "json"
        filename = filename or self._generate_filename(ext)
        filepath = export_dir / filename
        
        async with aiofiles.open(filepath, "w", encoding="utf-8") as f:
            if self._jsonl:
                # JSON Lines format
                for item in data:
                    await f.write(json.dumps(item, ensure_ascii=False) + "\n")
            else:
                # Standard JSON
                indent = 2 if self._pretty else None
                await f.write(json.dumps(
                    data,
                    indent=indent,
                    ensure_ascii=False,
                    default=str,
                ))
        
        return str(filepath)


class CSVExporter(BaseExporter):
    """
    Export data to CSV format.
    
    Features:
    - Auto-detect headers from data
    - Configurable delimiter
    - Handle nested data (flatten)
    
    Example:
        exporter = CSVExporter()
        filepath = await exporter.export(data)
    """
    
    def __init__(
        self,
        delimiter: str = ",",
        include_headers: bool = True,
        flatten_nested: bool = True,
    ):
        """
        Initialize CSV exporter.
        
        Args:
            delimiter: Field delimiter
            include_headers: Include header row
            flatten_nested: Flatten nested dicts/lists
        """
        self._delimiter = delimiter
        self._include_headers = include_headers
        self._flatten_nested = flatten_nested
    
    def _flatten_dict(
        self,
        data: Dict[str, Any],
        parent_key: str = "",
        separator: str = "_",
    ) -> Dict[str, Any]:
        """Flatten nested dictionary."""
        items: List[tuple] = []
        
        for key, value in data.items():
            new_key = f"{parent_key}{separator}{key}" if parent_key else key
            
            if isinstance(value, dict) and self._flatten_nested:
                items.extend(self._flatten_dict(value, new_key, separator).items())
            elif isinstance(value, list):
                # Convert list to string
                items.append((new_key, json.dumps(value)))
            else:
                items.append((new_key, value))
        
        return dict(items)
    
    async def export(
        self,
        data: List[Dict[str, Any]],
        filename: str | None = None,
    ) -> str:
        """Export data to CSV file."""
        if not data:
            raise ValueError("No data to export")
        
        export_dir = self._ensure_export_dir()
        filename = filename or self._generate_filename("csv")
        filepath = export_dir / filename
        
        # Flatten data if needed
        flat_data = [self._flatten_dict(item) for item in data]
        
        # Collect all headers
        all_headers = set()
        for item in flat_data:
            all_headers.update(item.keys())
        headers = sorted(all_headers)
        
        # Write CSV
        async with aiofiles.open(filepath, "w", encoding="utf-8", newline="") as f:
            # Use sync csv writer with string buffer
            import io
            buffer = io.StringIO()
            writer = csv.DictWriter(
                buffer,
                fieldnames=headers,
                delimiter=self._delimiter,
                extrasaction="ignore",
            )
            
            if self._include_headers:
                writer.writeheader()
            
            for item in flat_data:
                writer.writerow(item)
            
            await f.write(buffer.getvalue())
        
        return str(filepath)


class SQLiteExporter(BaseExporter):
    """
    Export data to SQLite database.
    
    Features:
    - Auto-create table from data
    - Append or replace modes
    - Type inference
    
    Example:
        exporter = SQLiteExporter(table_name="products")
        filepath = await exporter.export(data)
    """
    
    def __init__(
        self,
        table_name: str = "scraped_data",
        db_name: str | None = None,
        replace: bool = False,
    ):
        """
        Initialize SQLite exporter.
        
        Args:
            table_name: Name of the table to create/use
            db_name: Database filename (default from config)
            replace: Replace existing table (vs append)
        """
        self._table_name = table_name
        self._db_name = db_name or config.storage.sqlite_db_name
        self._replace = replace
    
    def _infer_type(self, value: Any) -> str:
        """Infer SQLite type from Python value."""
        if isinstance(value, bool):
            return "INTEGER"
        elif isinstance(value, int):
            return "INTEGER"
        elif isinstance(value, float):
            return "REAL"
        elif isinstance(value, (dict, list)):
            return "TEXT"  # Store as JSON
        else:
            return "TEXT"
    
    def _prepare_value(self, value: Any) -> Any:
        """Prepare value for SQLite insertion."""
        if isinstance(value, (dict, list)):
            return json.dumps(value)
        return value
    
    async def export(
        self,
        data: List[Dict[str, Any]],
        filename: str | None = None,
    ) -> str:
        """Export data to SQLite database."""
        if not data:
            raise ValueError("No data to export")
        
        export_dir = self._ensure_export_dir()
        db_path = export_dir / (filename or self._db_name)
        
        # Collect all columns and infer types from first row
        sample = data[0]
        columns = list(sample.keys())
        column_types = {col: self._infer_type(sample.get(col)) for col in columns}
        
        async with aiosqlite.connect(db_path) as db:
            # Drop table if replacing
            if self._replace:
                await db.execute(f"DROP TABLE IF EXISTS {self._table_name}")
            
            # Create table
            columns_sql = ", ".join(
                f'"{col}" {column_types[col]}'
                for col in columns
            )
            create_sql = f"""
                CREATE TABLE IF NOT EXISTS {self._table_name} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    {columns_sql},
                    _scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
            await db.execute(create_sql)
            
            # Insert data
            placeholders = ", ".join("?" for _ in columns)
            columns_str = ", ".join(f'"{col}"' for col in columns)
            insert_sql = f"""
                INSERT INTO {self._table_name} ({columns_str})
                VALUES ({placeholders})
            """
            
            for item in data:
                values = [self._prepare_value(item.get(col)) for col in columns]
                await db.execute(insert_sql, values)
            
            await db.commit()
        
        return str(db_path)


# Factory function
def create_exporter(
    format: str = "json",
    **kwargs,
) -> BaseExporter:
    """
    Create an exporter for the specified format.
    
    Args:
        format: "json", "jsonl", "csv", or "sqlite"
        **kwargs: Additional arguments for the specific exporter
        
    Returns:
        Configured exporter instance
    """
    exporters = {
        "json": lambda: JSONExporter(jsonl=False, **kwargs),
        "jsonl": lambda: JSONExporter(jsonl=True, **kwargs),
        "csv": lambda: CSVExporter(**kwargs),
        "sqlite": lambda: SQLiteExporter(**kwargs),
    }
    
    if format not in exporters:
        raise ValueError(f"Unknown format: {format}. Use: {list(exporters.keys())}")
    
    return exporters[format]()
