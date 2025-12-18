"""
Advanced Web Scraper - CLI Entry Point

A production-grade, ethical web scraping system.
"""

import asyncio
import argparse
import logging
import sys
from pathlib import Path
from typing import Dict, Any, Optional

from bs4 import BeautifulSoup
from rich.console import Console
from rich.logging import RichHandler

from scraper.config import config
from scraper.orchestrator import Orchestrator


console = Console()


def setup_logging(level: str = "INFO") -> None:
    """Configure logging with rich handler."""
    logging.basicConfig(
        level=getattr(logging, level),
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(console=console, rich_tracebacks=True)],
    )


def default_parser(url: str, html: str) -> Dict[str, Any]:
    """
    Default HTML parser that extracts basic page info.
    
    Override this with your own parser for specific sites.
    """
    soup = BeautifulSoup(html, "lxml")
    
    # Extract title
    title = ""
    title_tag = soup.find("title")
    if title_tag:
        title = title_tag.get_text(strip=True)
    
    # Extract meta description
    description = ""
    meta_desc = soup.find("meta", attrs={"name": "description"})
    if meta_desc:
        description = meta_desc.get("content", "")
    
    # Extract h1
    h1 = ""
    h1_tag = soup.find("h1")
    if h1_tag:
        h1 = h1_tag.get_text(strip=True)
    
    # Extract all links
    links = []
    for a in soup.find_all("a", href=True):
        href = a.get("href", "")
        text = a.get_text(strip=True)
        if href.startswith("http"):
            links.append({"url": href, "text": text[:100]})
    
    # Extract main text content
    main_text = ""
    main = soup.find("main") or soup.find("article") or soup.find("body")
    if main:
        # Remove script and style
        for tag in main.find_all(["script", "style", "nav", "footer", "header"]):
            tag.decompose()
        main_text = main.get_text(separator=" ", strip=True)[:5000]
    
    return {
        "title": title,
        "description": description,
        "h1": h1,
        "links_count": len(links),
        "text_length": len(main_text),
        "text_preview": main_text[:500] if main_text else "",
    }


def load_urls_from_file(filepath: str) -> list[str]:
    """Load URLs from a text file (one per line)."""
    path = Path(filepath)
    if not path.exists():
        console.print(f"[red]File not found: {filepath}[/red]")
        sys.exit(1)
    
    urls = []
    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                urls.append(line)
    
    return urls


async def main_async(args: argparse.Namespace) -> None:
    """Async main function."""
    setup_logging(args.log_level)
    
    # Collect URLs
    urls = []
    
    if args.url:
        urls.append(args.url)
    
    if args.file:
        urls.extend(load_urls_from_file(args.file))
    
    if not urls:
        console.print("[red]No URLs provided. Use --url or --file[/red]")
        sys.exit(1)
    
    console.print(f"\n[bold blue]Advanced Web Scraper[/bold blue]")
    console.print(f"URLs to process: {len(urls)}")
    console.print(f"Workers: {args.workers}")
    console.print(f"Export format: {args.format}")
    console.print()
    
    # Create orchestrator
    orchestrator = Orchestrator(parser=default_parser)
    
    # Load proxies if provided
    if args.proxies:
        from scraper.stealth.proxy_pool import ProxyPool
        proxy_path = Path(args.proxies)
        if proxy_path.exists():
            count = orchestrator._proxy_pool.load_from_file(proxy_path)
            console.print(f"[green]Loaded {count} proxies[/green]")
            config.proxy.enabled = True
    
    # Run scraper
    try:
        results = await orchestrator.run(
            urls=urls,
            workers=args.workers,
        )
        
        # Export results
        if results:
            export_path = await orchestrator.export_results(
                format=args.format,
                filename=args.output,
            )
            console.print(f"\n[green]Results exported to: {export_path}[/green]")
        
        # Print stats
        stats = orchestrator.get_stats()
        console.print("\n[bold]Final Statistics:[/bold]")
        for key, value in stats["scraper"].items():
            console.print(f"  {key}: {value}")
        
    except KeyboardInterrupt:
        console.print("\n[yellow]Interrupted by user[/yellow]")
        orchestrator.stop()
    except Exception as e:
        console.print(f"\n[red]Error: {e}[/red]")
        raise


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Advanced Web Scraper - Production-grade ethical scraping",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --url https://example.com
  %(prog)s --file urls.txt --workers 5
  %(prog)s --url https://example.com --format csv --output results.csv
        """,
    )
    
    # URL sources
    parser.add_argument(
        "--url", "-u",
        help="Single URL to scrape",
    )
    parser.add_argument(
        "--file", "-f",
        help="File containing URLs (one per line)",
    )
    
    # Execution options
    parser.add_argument(
        "--workers", "-w",
        type=int,
        default=3,
        help="Number of concurrent workers (default: 3)",
    )
    
    # Output options
    parser.add_argument(
        "--format",
        choices=["json", "jsonl", "csv", "sqlite"],
        default="json",
        help="Export format (default: json)",
    )
    parser.add_argument(
        "--output", "-o",
        help="Output filename (auto-generated if not specified)",
    )
    
    # Proxy options
    parser.add_argument(
        "--proxies", "-p",
        help="File containing proxy URLs (one per line)",
    )
    
    # Logging
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level (default: INFO)",
    )
    
    args = parser.parse_args()
    
    # Run async main
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
