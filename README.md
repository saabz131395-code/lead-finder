# Lead Finder — Multi-Source Business Lead Scraper

A desktop app (Python + PySide6) that finds local business leads from several sources at once, merges duplicates, enriches each lead from the business's own website, and exports clean results to CSV.

> **Disclaimer:** This project is for educational and portfolio purposes. Scraping some websites may go against their Terms of Service. Use it responsibly, respect each site's rules and `robots.txt`, and prefer official APIs for commercial use.

## Features

- **Multi-source search:** Google Maps, Yellow Pages, Hotfrog and LinkedIn company pages, run in parallel
- **Smart deduplication:** merges the same business found on different sources (name, phone, domain and fuzzy matching)
- **Website enrichment:** visits each lead's website to collect emails, phone numbers and social links
- **Live results:** leads appear in the table while the search is still running
- **Pause / resume / stop** controls for long searches
- **Crash-safe checkpoints:** an interrupted search can be resumed after the app restarts
- **Seen-lead cache (SQLite):** skips leads already found in earlier searches
- **Search history:** reopen any previous search
- **CSV export**
- **Threaded design:** the GUI stays responsive during scraping
- **Filters:** 30 preset niches or a custom keyword; country, city, state and ZIP
- **Packaging:** can be built into a single Windows `.exe` with PyInstaller

## Tech Stack

| Area | Tools |
|---|---|
| GUI | PySide6 (Qt) |
| HTTP & parsing | httpx, BeautifulSoup4, Scrapling |
| Directory search | DDGS (DuckDuckGo search) |
| Storage | SQLite |
| Concurrency | `ThreadPoolExecutor`, Qt threads |
| Packaging | PyInstaller |

## Project Structure

```
lead-finder/
├── main.py                 # PySide6 GUI: search form, live table, history, export
├── sources.py              # SourceManager: registers all source adapters
├── google_maps_source.py   # Runs the external Google Maps scraper as a subprocess
├── adapters/
│   ├── google_maps.py      # Google Maps adapter
│   └── ddgs_directory.py   # Yellow Pages / Hotfrog / LinkedIn via site search
├── engine/
│   ├── hybrid_engine.py    # Runs sources in parallel, merges and ranks leads
│   ├── deduplicator.py     # Duplicate detection and merging
│   └── cache.py            # SQLite seen-lead cache
├── providers/
│   ├── http_provider.py    # HTTP fetching (Scrapling, with httpx fallback)
│   └── website_enricher.py # Extracts emails, phones and socials from websites
└── tools/                  # Developer scripts (benchmarks, quick tests)
```

## Installation

Requires **Python 3.10+**.

```bash
git clone https://github.com/saabz131395-code/lead-finder.git
cd lead-finder
python -m venv .venv
.venv\Scripts\activate        # Windows
# source .venv/bin/activate   # macOS / Linux
python -m pip install -r requirements.txt
python main.py
```

### Google Maps source (optional)

The Google Maps source uses my companion project, **Lead-Scraper-Google-Maps** (a Playwright-based Google Maps scraper), which runs as a subprocess. Set it up with its own `.venv`, then point Lead Finder at it:

```powershell
$env:GMAPS_SCRAPER_PATH="C:\path\to\Lead-Scraper-Google-Maps"
python main.py
```

If it is not set, the app looks in `Desktop\Lead-Scraper-Google-Maps`. The other sources work without it.

## Build a Windows EXE

```powershell
python -m PyInstaller --noconfirm --clean --onefile --windowed --name LeadFinder main.py
```

The file is created at `dist\LeadFinder.exe`.

## Where Data Is Stored

Search history, cache and checkpoints are stored outside the project folder:

- Windows: `%LOCALAPPDATA%\USA Lead Finder\`
- macOS / Linux: `~/.local/share/USA Lead Finder/`

## Author

**Saba Nawaz** — QA Engineer · Lahore, Pakistan
GitHub: [@saabz131395-code](https://github.com/saabz131395-code)

## License

MIT — see [LICENSE](LICENSE).
