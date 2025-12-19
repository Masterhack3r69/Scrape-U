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
    Default HTML parser that extracts basic page info and product listings.
    
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
    
    # Extract product listings (e-commerce pattern)
    products = extract_products(soup)
    
    # Extract quotes (quotes.toscrape.com pattern)
    quotes = extract_quotes(soup)
    
    result = {
        "title": title,
        "description": description,
        "h1": h1,
        "links_count": len(links),
        "text_length": len(main_text),
        "text_preview": main_text[:500] if main_text else "",
    }
    
    # Add products if found
    if products:
        result["products"] = products
        result["products_count"] = len(products)
    
    # Add quotes if found
    if quotes:
        result["quotes"] = quotes
        result["quotes_count"] = len(quotes)
    
    return result


def extract_quotes(soup: BeautifulSoup) -> list:
    """
    Extract quotes from quote listing pages (like quotes.toscrape.com).
    
    Looks for div.quote pattern with:
    - span.text for quote text
    - small.author for author name
    - a.tag for tags
    """
    quotes = []
    
    # Find all quote containers
    quote_divs = soup.find_all("div", class_="quote")
    
    for quote_div in quote_divs:
        quote = {}
        
        # Extract quote text
        text_tag = quote_div.find("span", class_="text")
        if text_tag:
            # Remove curly quotes and clean up
            quote["text"] = text_tag.get_text(strip=True)
        
        # Extract author
        author_tag = quote_div.find("small", class_="author")
        if author_tag:
            quote["author"] = author_tag.get_text(strip=True)
        
        # Extract author URL
        author_link = quote_div.find("a", href=lambda x: x and "/author/" in x)
        if author_link:
            quote["author_url"] = author_link.get("href", "")
        
        # Extract tags
        tags = []
        tags_div = quote_div.find("div", class_="tags")
        if tags_div:
            for tag_link in tags_div.find_all("a", class_="tag"):
                tags.append(tag_link.get_text(strip=True))
        if tags:
            quote["tags"] = tags
        
        if quote.get("text"):
            quotes.append(quote)
    
    return quotes


def extract_products(soup: BeautifulSoup) -> list:
    """
    Extract product listings from common e-commerce HTML patterns.
    
    Supports multiple common patterns:
    - article.product_pod (books.toscrape.com)
    - .product-item, .product-card
    - Generic product containers
    """
    products = []
    
    # Pattern 1: books.toscrape.com style (article.product_pod)
    product_pods = soup.find_all("article", class_="product_pod")
    if product_pods:
        for pod in product_pods:
            product = {}
            
            # Extract product name from h3 > a title attribute or text
            h3 = pod.find("h3")
            if h3:
                a_tag = h3.find("a")
                if a_tag:
                    product["name"] = a_tag.get("title", "") or a_tag.get_text(strip=True)
                    product["url"] = a_tag.get("href", "")
            
            # Extract price
            price_tag = pod.find("p", class_="price_color")
            if price_tag:
                product["price"] = price_tag.get_text(strip=True)
            
            # Extract rating from star-rating class
            rating_tag = pod.find("p", class_=lambda x: x and "star-rating" in x)
            if rating_tag:
                rating_classes = rating_tag.get("class", [])
                for cls in rating_classes:
                    if cls != "star-rating":
                        # Convert word to number
                        rating_map = {"One": 1, "Two": 2, "Three": 3, "Four": 4, "Five": 5}
                        product["rating"] = rating_map.get(cls, cls)
                        break
            
            # Extract availability
            avail_tag = pod.find("p", class_="instock")
            if avail_tag:
                product["availability"] = avail_tag.get_text(strip=True)
            
            # Extract image
            img_tag = pod.find("img")
            if img_tag:
                product["image"] = img_tag.get("src", "")
            
            if product:
                products.append(product)
        
        return products
    
    # Pattern 2: Generic product containers
    product_selectors = [
        ("div", "product-item"),
        ("div", "product-card"),
        ("div", "product"),
        ("li", "product"),
        ("div", "item"),
    ]
    
    for tag, class_name in product_selectors:
        items = soup.find_all(tag, class_=lambda x: x and class_name in str(x).lower())
        if items:
            for item in items:
                product = {}
                
                # Try to find product name
                name_tag = item.find(["h2", "h3", "h4", "a"])
                if name_tag:
                    product["name"] = name_tag.get_text(strip=True)[:200]
                
                # Try to find price (look for currency symbols or "price" class)
                price_tag = item.find(class_=lambda x: x and "price" in str(x).lower())
                if price_tag:
                    product["price"] = price_tag.get_text(strip=True)
                
                # Try to find rating
                rating_tag = item.find(class_=lambda x: x and "rating" in str(x).lower())
                if rating_tag:
                    product["rating"] = rating_tag.get_text(strip=True)
                
                if product.get("name"):
                    products.append(product)
            
            if products:
                return products
    
    return products


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
