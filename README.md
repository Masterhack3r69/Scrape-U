# I Scrape U

An ethical web scraping system built with Python. This system follows the **"Stealth over Speed"** and **"Politeness First"** principles to ensure sustainable, long-term scraping without getting blocked or overwhelming target servers.

---

## Overview

This scraper is designed as a **modular data extraction pipeline** that separates the "Brain" (management and orchestration) from the "Muscle" (execution and fetching). Instead of a fragile single script, it provides a clean architecture that handles the complexities of real-world web scraping.

---

## Key Features

### 🛡️ Safety First Protocol (The Brain)

The system performs ethics and feasibility checks before any scraping begins:

- **Robots.txt Compliance** — Automatically fetches and respects `robots.txt` rules for every domain. Disallowed paths are dropped from the queue before any request is made.
- **Token Bucket Rate Limiter** — Implements a rate limiting algorithm that prevents overwhelming target servers. Configurable for standard (2-5 seconds) or strict mode (10-30 seconds).
- **Red Light Law** — Automatically halts operations for 60+ seconds when receiving 403 Forbidden, 429 Too Many Requests, or CAPTCHA challenges.

### 🎭 Stealth Layer (The Camouflage)

To scrape safely, the system masks itself as a legitimate browser:

- **User-Agent Rotation** — Includes 10 modern browser fingerprints (Chrome, Firefox, Safari, Edge) with properly matching Client Hints headers (Sec-Ch-Ua) to avoid detection through header inconsistencies.
- **Proxy Pool Management** — Supports both datacenter and residential proxies with automatic health-checking. Unhealthy proxies are disabled after repeated failures.
- **Fingerprint Protection** — When using the headless browser, applies stealth patches to avoid bot detection through canvas fingerprinting and other techniques.

### 💪 Dynamic Execution (The Muscle)

The system intelligently chooses the right tool for each website:

- **Static Site Detection** — Analyzes responses for JavaScript framework signatures (React, Vue, Angular, Next.js) to determine the optimal fetching strategy.
- **Lightweight HTTP Fetcher** — Uses async HTTP/2 for fast, efficient scraping of traditional HTML websites with minimal resource usage.
- **Headless Browser Fetcher** — Deploys Playwright with Chromium for JavaScript-rendered single-page applications, with automatic resource blocking for images, fonts, and analytics scripts to improve speed by 3x.

### 📊 Data Pipeline (ETL)

Never lose scraped data due to parsing failures:

- **Raw Storage** — Immediately saves raw HTML to disk using URL-based hashing. If parsing fails, you can re-process without re-scraping.
- **Data Validation** — Uses Pydantic schemas to validate extracted data, flagging incomplete or malformed records for manual review.
- **Data Cleaning** — Automatically removes emojis, normalizes whitespace, decodes HTML entities, and cleans currency symbols from prices.
- **Multi-Format Export** — Export to JSON, JSON Lines, CSV, or SQLite database with a single command.

---

## Architecture

The scraper follows a modular, layered architecture:

| Layer             | Purpose                | Modules                                            |
| ----------------- | ---------------------- | -------------------------------------------------- |
| **Safety**        | Ethics & rate limiting | `robots_parser`, `rate_limiter`                    |
| **Stealth**       | Detection avoidance    | `user_agents`, `proxy_pool`                        |
| **Fetchers**      | Content retrieval      | `http_fetcher`, `browser_fetcher`, `site_detector` |
| **Pipeline**      | Data processing        | `raw_storage`, `validator`, `cleaner`, `exporters` |
| **Orchestration** | Coordination           | `queue_manager`, `orchestrator`                    |

---

## How It Works

1. **Input** — URLs are fed to the system via command line or Python API
2. **Check** — Each URL is validated against the domain's `robots.txt` rules
3. **Queue** — Approved URLs enter a priority queue with deduplication
4. **Delay** — The rate limiter enforces politeness delays between requests
5. **Mask** — Random User-Agent and proxy are selected for the request
6. **Fetch** — Either HTTP client or headless browser retrieves the content
7. **Store** — Raw HTML is saved to disk immediately
8. **Process** — Content is parsed, cleaned, validated, and exported

---

## Ethical Scraping Principles

This scraper is built with ethics at its core:

| Principle                    | Implementation                                |
| ---------------------------- | --------------------------------------------- |
| **Respect robots.txt**       | Never scrape disallowed paths                 |
| **Rate limit all requests**  | Enforce delays between requests               |
| **Don't overload servers**   | Back off when receiving 429 errors            |
| **Use real User-Agents**     | Never impersonate Googlebot or other crawlers |
| **Cache aggressively**       | Don't re-scrape unchanged content             |
| **Minimize data collection** | Only extract what you need                    |

---

## Requirements

- Python 3.10+
- Playwright (for JavaScript-rendered sites)
- See `requirements.txt` for full dependency list

---

## Project Structure

```
Scrape_U/
├── main.py                 # CLI entry point
├── requirements.txt        # Dependencies
├── scraper/
│   ├── config.py           # Configuration settings
│   ├── orchestrator.py     # Main coordinator
│   ├── queue_manager.py    # URL queue
│   ├── safety/             # Robots.txt & rate limiting
│   ├── stealth/            # User-Agents & proxies
│   ├── fetchers/           # HTTP & browser fetching
│   └── pipeline/           # Storage, validation, export
├── storage/
│   ├── raw/                # Cached HTML files
│   └── exports/            # Exported data
└── tests/                  # Unit tests
```

---

## Configuration

The scraper can be configured through environment variables or programmatically:

| Setting            | Description               | Default     |
| ------------------ | ------------------------- | ----------- |
| Rate limit delay   | Seconds between requests  | 2-5 seconds |
| Max tokens         | Burst capacity            | 5 tokens    |
| Respect robots.txt | Enable/disable compliance | Enabled     |
| Proxy enabled      | Use proxy rotation        | Disabled    |
| Browser headless   | Run browser without UI    | Enabled     |
| Block images       | Skip downloading images   | Enabled     |

---

## Use Cases

- **Price Monitoring** — Track product prices across e-commerce sites
- **Lead Generation** — Extract contact information from business directories
- **Content Aggregation** — Collect articles, reviews, or listings
- **Market Research** — Gather competitive intelligence data
- **Academic Research** — Collect datasets for analysis

---

## Limitations

- Cannot bypass authentication without provided credentials
- CAPTCHA challenges require manual intervention or third-party services
- Some anti-bot systems may still detect and block requests
- JavaScript-heavy sites require the browser fetcher (slower)

---

## Walkthrough

This section provides a step-by-step guide to get you up and running with the Advanced Web Scraper.

### Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/Scrape_U.git
cd Scrape_U

# Create virtual environment (recommended)
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows

# Install dependencies
pip install -r requirements.txt

# Install browser for JavaScript-rendered sites
playwright install chromium
```

### Basic Usage

#### Single URL Scraping

```bash
# Scrape a single URL
python main.py --url https://example.com

# Scrape with specific export format
python main.py --url https://quotes.toscrape.com --format csv
```

#### Multiple URLs

```bash
# Create a file with URLs (one per line)
echo "https://example.com" > urls.txt
echo "https://quotes.toscrape.com" >> urls.txt

# Scrape from file
python main.py --file urls.txt

# Scrape with multiple workers for parallel processing
python main.py --file urls.txt --workers 5
```

### CLI Options

| Option       | Description                          | Default   |
| ------------ | ------------------------------------ | --------- |
| `--url`      | Single URL to scrape                 | -         |
| `--file`     | File containing URLs (one per line)  | -         |
| `--format`   | Export format (json, csv, sqlite)    | json      |
| `--workers`  | Number of concurrent workers         | 3         |
| `--output`   | Output directory for exports         | ./storage |
| `--strict`   | Enable strict rate limiting (10-30s) | False     |
| `--no-cache` | Disable HTML caching                 | False     |

### Export Examples

```bash
# Export to JSON (default)
python main.py --url https://example.com --format json

# Export to CSV (one product per row)
python main.py --url https://quotes.toscrape.com --format csv

# Export to SQLite database
python main.py --url https://example.com --format sqlite
```

### Data Flow Example

1. **Input URL**: `https://quotes.toscrape.com`
2. **Robots Check**: Fetches `/robots.txt`, validates path is allowed
3. **Rate Limit**: Waits 2-5 seconds (politeness delay)
4. **Stealth**: Applies random User-Agent and optional proxy
5. **Fetch**: Detects static site → uses HTTP fetcher
6. **Store**: Saves raw HTML to `storage/raw/`
7. **Extract**: Parses quotes, authors, and tags
8. **Export**: Outputs to `storage/exports/quotes.csv`

### Tips for Effective Scraping

| Tip                        | Why                                     |
| -------------------------- | --------------------------------------- |
| Start with `--strict` mode | Reduces ban risk on sensitive sites     |
| Use `--workers 1` first    | Test before scaling up                  |
| Check `storage/raw/`       | Raw HTML is cached for re-processing    |
| Monitor console output     | Watch for 403/429 warnings              |
| Use proxies for scale      | Configure in environment or `config.py` |

### Running Tests

```bash
# Run all tests
pytest

# Run with verbose output
pytest -v

# Run specific test module
pytest tests/test_safety.py
```

---

![alt text](https://media.giphy.com/media/v1.Y2lkPWVjZjA1ZTQ3eWN2YnZrNjNzN29ybmU3dDloNmdrNDBjZ3U0eml0Y3g0M2JzaGQ1MiZlcD12MV9naWZzX3NlYXJjaCZjdD1n/gQJyPqc6E4xoc/giphy.gif)
