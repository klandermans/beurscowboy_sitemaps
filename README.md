# Sitemap Aggregator - News Scraper

High-performance news sitemap aggregator with page metadata enrichment and caching.

## Features

- **Recursive sitemap discovery** - Automatically finds and processes nested sitemaps
- **Parquet caching** - Fast incremental updates with Apache Parquet cache
- **Page metadata enrichment** - Extracts title, description, images, author, tags from pages
- **200 concurrent workers** - Nuclear-speed scraping
- **Historical archive** - Build and maintain historical datasets

## Installation

```bash
pip install -r requirements.txt
```

Required packages:
- `requests` - HTTP requests
- `pandas`, `pyarrow` - Parquet caching
- `tqdm` - Progress bars

## Usage

### Normal Run (Cached, Last 24 Hours)
```bash
python sitemap_aggregator.py
```

### Force Refresh (Ignore Cache)
```bash
python sitemap_aggregator.py --refresh
```

### Extended Historical Collection
```bash
# Get last 72 hours
python sitemap_aggregator.py --extend-hours 72

# Get last week (168 hours)
python sitemap_aggregator.py --extend-hours 168
```

### Refresh + Extended Collection
```bash
# Full week, fresh data
python sitemap_aggregator.py --refresh --extend-hours 168
```

### Build Historical Archive
```bash
python build_historical.py
```

## Output Files

| File | Description |
|------|-------------|
| `combined_sitemap.xml` | Latest news articles with enriched metadata |
| `historical_archive.parquet` | Complete historical dataset (Parquet) |
| `historical_archive.json` | Complete historical dataset (JSON) |
| `historical_archive.json.gz` | Compressed JSON archive |
| `cache_parquet/*.parquet` | Sitemap cache (Parquet) |
| `cache_metadata.json` | Sitemap metadata cache |
| `page_metadata_cache.json` | Page metadata cache |

## Page Metadata Fields

The following fields are extracted from pages without headlines:

| Field | Source |
|-------|--------|
| `title` | og:title → twitter:title → `<title>` → h1 |
| `description` | og:description → twitter:description → meta description |
| `image` | og:image → twitter:image |
| `author` | meta author |
| `published_time` | article:published_time |
| `modified_time` | article:modified_time |
| `section` | article:section |
| `tags` | article:tag (multiple) |
| `site_name` | og:site_name |
| `canonical_url` | rel=canonical |

## Cache Strategy

| Cache Type | TTL | Purpose |
|------------|-----|---------|
| Parquet (sitemaps) | 1 hour | Fast re-processing of known sitemaps |
| Sitemap metadata | 1 day | HTTP If-Modified-Since optimization |
| Page metadata | 24 hours | Avoid re-fetching page content |

## GitHub Actions

This scraper is designed to run on GitHub Actions (free tier):

```yaml
- name: Run scraper
  run: python sitemap_aggregator.py --extend-hours 24

- name: Build historical archive
  run: python build_historical.py

- name: Commit and push
  run: |
    git add *.xml *.parquet *.json
    git commit -m "Update news archive"
    git push
```

## Performance

Typical run times:
- **Cached run**: ~2 minutes for 700+ sitemaps, 20k+ articles
- **Page metadata enrichment**: ~1 second per page (only for items without title)
- **Historical archive build**: ~30 seconds for 85 parquet files

## Statistics

Example output:
```
Scan statistics: {'cached': 571, 'index': 29, 'items': 108, 'errors': 211}
Valid sitemaps: 708
Fetching page metadata for 25 items without title...
Enriched 22/25 items with page metadata
DONE! Found 20403 items with headlines.
```

## License

MIT
