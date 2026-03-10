import random
import hashlib
import requests
import re
import os
import gzip
import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse, urljoin
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
    HAS_PARQUET = True
except ImportError:
    HAS_PARQUET = False
    print("Warning: pandas/pyarrow not installed. Parquet caching disabled.")

# Increase file descriptor limit
try:
    import resource
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (min(4096, hard), hard))
except:
    pass

# Robust TQDM
try:
    from tqdm import tqdm
except ImportError:
    def tqdm(iterable, *args, **kwargs): return iterable

# De lijst van domeinen
DOMAINS_FILE = "domains.json"
if os.path.exists(DOMAINS_FILE):
    with open(DOMAINS_FILE, "r") as f:
        raw_data = json.load(f)
else:
    raw_data = ["bloomberg.com", "reuters.com", "wsj.com"]
raw_data = list(set(raw_data))

# SETTINGS
NOW = datetime.now(timezone.utc)
CACHE_SITEMAPS = "sitemaps_list.txt"
CACHE_METADATA = "cache_metadata.json"
CACHE_PAGE_METADATA = "page_metadata_cache.json"
CACHE_DIR = "cache_parquet"
PARQUET_CACHE_MAX_AGE_HOURS = 1  # Skip request if parquet cache is newer than 1 hour
NEWS_MAX_AGE_HOURS = 24  # Only include news articles from last 24 hours
SITEMAP_CACHE_MAX_AGE_DAYS = 1  # Sitemap metadata cache max age in days
PAGE_METADATA_CACHE_MAX_AGE_HOURS = 24  # Page metadata cache max age in hours
NEWS_ONLY = True  # Only process news sitemaps (skip regular sitemaps)

# Command line arguments
REFRESH_CACHE = '--refresh' in sys.argv
HISTORICAL_MODE = "--historical" in sys.argv
EXTEND_HOURS = None
for i, arg in enumerate(sys.argv):
    if arg == '--extend-hours' and i + 1 < len(sys.argv):
        try:
            EXTEND_HOURS = int(sys.argv[i + 1])
        except ValueError:
            pass
    if arg == '--help' or arg == '-h':
        print("""
Sitemap Aggregator - News Scraper with Page Metadata Enrichment

Usage: python sitemap_aggregator.py [OPTIONS]

Options:
  --refresh           Ignore cache age, re-fetch all sitemaps
  --historical        Ignore date filters, collect ALL items from sitemaps
  --extend-hours N    Collect articles from last N hours (default: 24)
  --help, -h          Show this help message

Examples:
  python sitemap_aggregator.py                    # Normal run (24h, cached)
  python sitemap_aggregator.py --refresh          # Force refresh all caches
  python sitemap_aggregator.py --extend-hours 72  # Get last 72 hours
  python sitemap_aggregator.py --refresh --extend-hours 168  # Full week, fresh

After running, build historical archive:
  python build_historical.py
""")
        sys.exit(0)

# Create cache directory
os.makedirs(CACHE_DIR, exist_ok=True)

# Optimized Nuclear Session
session = requests.Session()
adapter = HTTPAdapter(pool_connections=200, pool_maxsize=200, max_retries=Retry(total=5, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=['GET', 'HEAD']))
session.mount("http://", adapter)
session.mount("https://", adapter)
session.verify = False
requests.packages.urllib3.disable_warnings()
headers_base = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}

def is_news_sitemap(url, content=None):
    """Check if a sitemap is a news sitemap based on URL pattern or content."""
    url_lower = url.lower()
    
    # URL patterns that indicate news sitemap
    news_patterns = [
        'news', 'sitemap-news', 'google-news', 'news-sitemap',
        'articles', 'press', 'presse', 'aktuelles', 'noticias', 'archive', 'archief'
    ]
    
    # Check URL first
    if any(pattern in url_lower for pattern in news_patterns):
        return True
    
    # Check content for news namespace
    if content:
        if b'xmlns:news=' in content or b'sitemap-news' in content:
            return True
    
    return False

def get_parquet_path(url):
    """Generate parquet file path for a sitemap URL."""
    parsed = urlparse(url)
    safe_name = re.sub(r'[^\w\-_]', '_', f"{parsed.netloc}{parsed.path}")
    return os.path.join(CACHE_DIR, f"{safe_name}.parquet")

def load_parquet_cache(url, cutoff):
    """Load items from parquet cache if it's fresh enough."""
    if REFRESH_CACHE:
        return None  # Skip cache in refresh mode
    
    if not HAS_PARQUET:
        return None

    pq_path = get_parquet_path(url)
    if not os.path.exists(pq_path):
        return None

    # Check file age
    file_mtime = datetime.fromtimestamp(os.path.getmtime(pq_path), tz=timezone.utc)
    max_age = NOW - timedelta(hours=PARQUET_CACHE_MAX_AGE_HOURS)
    if file_mtime < max_age:
        return None  # Cache too old

    try:
        df = pd.read_parquet(pq_path)
        # Filter by cutoff
        if 'lastmod' in df.columns:
            df['lastmod_dt'] = pd.to_datetime(df['lastmod'], utc=True, errors='coerce')
            df = df[df['lastmod_dt'] >= cutoff]
            df = df.drop(columns=['lastmod_dt'])
        return df.to_dict('records')
    except:
        return None

def save_parquet_cache(url, items):
    """Save items to parquet cache."""
    if not HAS_PARQUET or not items:
        return
    
    try:
        pq_path = get_parquet_path(url)
        df = pd.DataFrame(items)
        df.to_parquet(pq_path, index=False, compression='snappy')
    except Exception as e:
        print(f"  Warning: Could not save parquet cache for {url}: {e}")

def cleanup_old_parquet_files():
    """Remove parquet files older than SITEMAP_CACHE_MAX_AGE_DAYS."""
    if not HAS_PARQUET:
        return
    
    if REFRESH_CACHE or EXTEND_HOURS:
        return  # Don't cleanup in refresh or extend mode

    cutoff = NOW - timedelta(days=SITEMAP_CACHE_MAX_AGE_DAYS)
    removed = 0
    for filename in os.listdir(CACHE_DIR):
        if filename.endswith('.parquet'):
            filepath = os.path.join(CACHE_DIR, filename)
            file_mtime = datetime.fromtimestamp(os.path.getmtime(filepath), tz=timezone.utc)
            if file_mtime < cutoff:
                os.remove(filepath)
                removed += 1
    if removed > 0:
        print(f"Cleaned up {removed} old parquet cache files")

def parse_date(date_str):
    if not date_str: return None
    try:
        dt = datetime.fromisoformat(date_str.strip().replace('Z', '+00:00'))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except:
        try:
            return datetime.strptime(date_str.strip()[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except:
            return None

def clean_xml_text(b_text):
    if not b_text: return ""
    return b_text.decode('utf-8', 'ignore').strip().replace('<![CDATA[', '').replace(']]>', '').replace('&', '&amp;')

# Regex
RE_SITEMAPINDEX = re.compile(b'<sitemapindex', re.I)
RE_LOC = re.compile(b'<loc>(.*?)</loc>', re.I | re.DOTALL)
RE_LASTMOD = re.compile(b'<lastmod>(.*?)</lastmod>', re.I | re.DOTALL)
RE_SITEMAP_BLOCK = re.compile(b'<sitemap>(.*?)</sitemap>', re.I | re.DOTALL)
RE_URL_BLOCK = re.compile(b'<url>(.*?)</url>', re.I | re.DOTALL)
RE_STOCK_TICKERS = re.compile(b'<news:stock_tickers>(.*?)</news:stock_tickers>', re.I | re.DOTALL)

# Page metadata regex patterns
RE_TITLE_TAG = re.compile(b'<title[^>]*>(.*?)</title>', re.I | re.DOTALL)
RE_META_DESC = re.compile(b'<meta[^>]+name=["\']description["\'][^>]+content=["\']([^"\']*)["\']', re.I | re.DOTALL)
RE_META_DESC_ALT = re.compile(b'<meta[^>]+content=["\']([^"\']*)["\'][^>]+name=["\']description["\']', re.I | re.DOTALL)
RE_OG_TITLE = re.compile(b'<meta[^>]+property=["\']og:title["\'][^>]+content=["\']([^"\']*)["\']', re.I | re.DOTALL)
RE_OG_DESC = re.compile(b'<meta[^>]+property=["\']og:description["\'][^>]+content=["\']([^"\']*)["\']', re.I | re.DOTALL)
RE_OG_IMAGE = re.compile(b'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']*)["\']', re.I | re.DOTALL)
RE_OG_URL = re.compile(b'<meta[^>]+property=["\']og:url["\'][^>]+content=["\']([^"\']*)["\']', re.I | re.DOTALL)
RE_OG_SITE_NAME = re.compile(b'<meta[^>]+property=["\']og:site_name["\'][^>]+content=["\']([^"\']*)["\']', re.I | re.DOTALL)
RE_OG_TYPE = re.compile(b'<meta[^>]+property=["\']og:type["\'][^>]+content=["\']([^"\']*)["\']', re.I | re.DOTALL)
RE_TWITTER_CARD = re.compile(b'<meta[^>]+name=["\']twitter:card["\'][^>]+content=["\']([^"\']*)["\']', re.I | re.DOTALL)
RE_TWITTER_TITLE = re.compile(b'<meta[^>]+name=["\']twitter:title["\'][^>]+content=["\']([^"\']*)["\']', re.I | re.DOTALL)
RE_TWITTER_DESC = re.compile(b'<meta[^>]+name=["\']twitter:description["\'][^>]+content=["\']([^"\']*)["\']', re.I | re.DOTALL)
RE_TWITTER_IMAGE = re.compile(b'<meta[^>]+name=["\']twitter:image["\'][^>]+content=["\']([^"\']*)["\']', re.I | re.DOTALL)
RE_CANONICAL = re.compile(b'<link[^>]+rel=["\']canonical["\'][^>]+href=["\']([^"\']*)["\']', re.I | re.DOTALL)
RE_CANONICAL_ALT = re.compile(b'<link[^>]+href=["\']([^"\']*)["\'][^>]+rel=["\']canonical["\']', re.I | re.DOTALL)
RE_AUTHOR = re.compile(b'<meta[^>]+name=["\']author["\'][^>]+content=["\']([^"\']*)["\']', re.I | re.DOTALL)
RE_PUBLISHED_TIME = re.compile(b'<meta[^>]+property=["\']article:published_time["\'][^>]+content=["\']([^"\']*)["\']', re.I | re.DOTALL)
RE_MODIFIED_TIME = re.compile(b'<meta[^>]+property=["\']article:modified_time["\'][^>]+content=["\']([^"\']*)["\']', re.I | re.DOTALL)
RE_SECTION = re.compile(b'<meta[^>]+property=["\']article:section["\'][^>]+content=["\']([^"\']*)["\']', re.I | re.DOTALL)
RE_TAG = re.compile(b'<meta[^>]+property=["\']article:tag["\'][^>]+content=["\']([^"\']*)["\']', re.I | re.DOTALL)
RE_H1_TAG = re.compile(b'<h1[^>]*>(.*?)</h1>', re.I | re.DOTALL)

def fetch_page_metadata(url, page_metadata_cache):
    """Fetch page metadata (title, description, image, etc.) from HTML page."""
    # Check cache first
    cutoff_cache = NOW - timedelta(hours=PAGE_METADATA_CACHE_MAX_AGE_HOURS)
    if url in page_metadata_cache:
        cached = page_metadata_cache[url]
        cached_time = parse_date(cached.get('fetched_at'))
        if cached_time and cached_time >= cutoff_cache:
            return cached
    
    try:
        headers = headers_base.copy()
        resp = session.get(url, timeout=10, headers=headers)
        if resp.status_code != 200:
            return {}
        
        content = resp.content
        metadata = {}
        
        # Extract title (prefer og:title, then twitter:title, fallback to <title>)
        title = None
        og_title_m = RE_OG_TITLE.search(content)
        if og_title_m:
            title = clean_xml_text(og_title_m.group(1))
        else:
            twitter_title_m = RE_TWITTER_TITLE.search(content)
            if twitter_title_m:
                title = clean_xml_text(twitter_title_m.group(1))
            else:
                title_m = RE_TITLE_TAG.search(content)
                if title_m:
                    title = clean_xml_text(title_m.group(1))
        if title:
            metadata['title'] = title
        
        # Extract description (prefer og:description, then twitter:description, fallback to meta description)
        description = None
        og_desc_m = RE_OG_DESC.search(content)
        if og_desc_m:
            description = clean_xml_text(og_desc_m.group(1))
        else:
            twitter_desc_m = RE_TWITTER_DESC.search(content)
            if twitter_desc_m:
                description = clean_xml_text(twitter_desc_m.group(1))
            else:
                desc_m = RE_META_DESC.search(content)
                if not desc_m:
                    desc_m = RE_META_DESC_ALT.search(content)
                if desc_m:
                    description = clean_xml_text(desc_m.group(1))
        if description:
            metadata['description'] = description
        
        # Extract image (prefer og:image, then twitter:image)
        image = None
        og_image_m = RE_OG_IMAGE.search(content)
        if og_image_m:
            image = clean_xml_text(og_image_m.group(1))
        else:
            twitter_image_m = RE_TWITTER_IMAGE.search(content)
            if twitter_image_m:
                image = clean_xml_text(twitter_image_m.group(1))
        if image:
            # Resolve relative URLs
            if image.startswith('/'):
                parsed = urlparse(url)
                image = f"{parsed.scheme}://{parsed.netloc}{image}"
            elif image.startswith('//'):
                parsed = urlparse(url)
                image = f"{parsed.scheme}:{image}"
            metadata['image'] = image
        
        # Extract canonical URL
        canonical = None
        canonical_m = RE_CANONICAL.search(content)
        if canonical_m:
            canonical = clean_xml_text(canonical_m.group(1))
        else:
            canonical_alt_m = RE_CANONICAL_ALT.search(content)
            if canonical_alt_m:
                canonical = clean_xml_text(canonical_alt_m.group(1))
        if canonical:
            metadata['canonical_url'] = canonical
        
        # Extract author
        author_m = RE_AUTHOR.search(content)
        if author_m:
            metadata['author'] = clean_xml_text(author_m.group(1))
        
        # Extract article published time
        pub_time_m = RE_PUBLISHED_TIME.search(content)
        if pub_time_m:
            metadata['published_time'] = clean_xml_text(pub_time_m.group(1))
        
        # Extract article modified time
        mod_time_m = RE_MODIFIED_TIME.search(content)
        if mod_time_m:
            metadata['modified_time'] = clean_xml_text(mod_time_m.group(1))
        
        # Extract article section
        section_m = RE_SECTION.search(content)
        if section_m:
            metadata['section'] = clean_xml_text(section_m.group(1))
        
        # Extract article tags (can be multiple)
        tags = []
        for tag_m in RE_TAG.finditer(content):
            tag = clean_xml_text(tag_m.group(1))
            if tag:
                tags.append(tag)
        if tags:
            metadata['tags'] = tags
        
        # Extract Open Graph site name
        site_name_m = RE_OG_SITE_NAME.search(content)
        if site_name_m:
            metadata['site_name'] = clean_xml_text(site_name_m.group(1))
        
        # Extract Open Graph type
        og_type_m = RE_OG_TYPE.search(content)
        if og_type_m:
            metadata['og_type'] = clean_xml_text(og_type_m.group(1))
        
        # Extract Twitter card type
        twitter_card_m = RE_TWITTER_CARD.search(content)
        if twitter_card_m:
            metadata['twitter_card'] = clean_xml_text(twitter_card_m.group(1))
        
        # Extract og:url
        og_url_m = RE_OG_URL.search(content)
        if og_url_m:
            metadata['og_url'] = clean_xml_text(og_url_m.group(1))
        
        # Fallback to h1 if no title found
        if not title:
            h1_m = RE_H1_TAG.search(content)
            if h1_m:
                metadata['title'] = clean_xml_text(h1_m.group(1))
        
        # Save to cache
        if metadata:
            metadata['fetched_at'] = datetime.now(timezone.utc).isoformat()
            page_metadata_cache[url] = metadata
        
        return metadata
    except Exception as e:
        print(f"  ERROR fetching page metadata for {url}: {e}")
        return {}

def fetch_sitemap_content(url, cutoff, metadata_cache):
    global HISTORICAL_MODE
    # Skip relative URLs without scheme
    if not url.startswith(('http://', 'https://')):
        return ("error", (None, None))
    
    # Skip non-news sitemaps if NEWS_ONLY is enabled (quick check before cache lookup)
    if NEWS_ONLY and not is_news_sitemap(url):
        return ("skipped", (None, None))
    
    # Try parquet cache first
    cached_items = load_parquet_cache(url, cutoff)
    if cached_items is not None and len(cached_items) > 0:
        return ("cached", (cached_items, None))
    
    # Extract base URL for resolving relative paths
    parsed = urlparse(url)
    base_url = f"{parsed.scheme}://{parsed.netloc}"
    
    headers = headers_base.copy()
    if url in metadata_cache: headers['If-Modified-Since'] = metadata_cache[url]
    try:
        resp = session.get(url, timeout=15, headers=headers)
        if resp.status_code == 304: return ("cached", (None, None))
        if resp.status_code != 200: return ("error", (None, None))

        new_lmod = resp.headers.get('Last-Modified')
        content = resp.content
        
        # Handle gzipped content
        if url.endswith('.gz') or content.startswith(b'\x1f\x8b'):
            try:
                content = gzip.decompress(content)
            except gzip.BadGzipFile:
                # Not actually gzipped, use raw content
                pass
        
        if RE_SITEMAPINDEX.search(content):
            subs = []
            for loc_b in RE_LOC.findall(content):
                loc = loc_b.decode('utf-8', 'ignore').strip()
                if any(w in loc.lower() for w in ['image', 'video', 'tag', 'category', 'author']): continue
                # Resolve relative URLs
                if not loc.startswith(('http://', 'https://')):
                    loc = urljoin(base_url, loc)
                
                # Skip non-news sitemaps if NEWS_ONLY is enabled
                if NEWS_ONLY and not is_news_sitemap(loc):
                    continue
                
                subs.append(loc)
            return ("index", (subs, new_lmod))

        items = []
        for block in RE_URL_BLOCK.finditer(content):
            data = block.group(1)
            lm_match = RE_LASTMOD.search(data)
            lm_str = lm_match.group(1).decode('utf-8', 'ignore').strip() if lm_match else ""
            dt = parse_date(lm_str)
            if HISTORICAL_MODE or not dt or (cutoff <= dt <= (NOW + timedelta(minutes=10))):
                title_m = re.search(b'<news:title>(.*?)</news:title>', data, re.I | re.S)
                if not title_m: title_m = re.search(b'<title>(.*?)</title>', data, re.I | re.S)
                if title_m:
                    loc_m = RE_LOC.search(data)
                    if loc_m:
                        loc = loc_m.group(1).decode('utf-8', 'ignore').strip()
                        # Resolve relative URLs
                        if not loc.startswith(('http://', 'https://')):
                            loc = urljoin(base_url, loc)
                        meta = {"id": hashlib.md5(loc.encode()).hexdigest(), "loc": loc, "lastmod": lm_str, "title": clean_xml_text(title_m.group(1))}
                        kw_m = re.search(b'<news:keywords>(.*?)</news:keywords>', data, re.I | re.S)
                        if kw_m: meta["keywords"] = clean_xml_text(kw_m.group(1))
                        pub_m = re.search(b'<news:name>(.*?)</news:name>', data, re.I | re.S)
                        if pub_m: meta["source"] = clean_xml_text(pub_m.group(1))
                        # Parse stock tickers
                        ticker_m = RE_STOCK_TICKERS.search(data)
                        if ticker_m: meta["stock_tickers"] = clean_xml_text(ticker_m.group(1))
                        items.append(meta)
        
        # Save to parquet cache
        if items:
            save_parquet_cache(url, items)
        
        return ("items", (items, new_lmod))
    except (requests.exceptions.Timeout, requests.exceptions.ConnectionError, requests.exceptions.RequestException):
        return ("error", (None, None))
    except gzip.BadGzipFile:
        return ("error", (None, None))
    except Exception as e:
        print(f"  ERROR fetching {url}: {e}")
        return ("error", (None, None))

def main():
    cutoff = NOW - timedelta(hours=NEWS_MAX_AGE_HOURS)  # 24 uur voor nieuws
    
    # Override cutoff if --extend-hours is used
    if EXTEND_HOURS:
        cutoff = NOW - timedelta(hours=EXTEND_HOURS)
        print(f"EXTENDED MODE: Collecting articles from last {EXTEND_HOURS} hours")
    
    print(f"NUCLEAR RECURSIVE Scan | Start: {NOW.strftime('%H:%M:%S')} UTC | Workers: 200")
    print(f"Looking for articles from the last {NEWS_MAX_AGE_HOURS} hours")
    print(f"Parquet cache enabled: {HAS_PARQUET}, max age: {PARQUET_CACHE_MAX_AGE_HOURS}h")
    print(f"NEWS_ONLY mode: {NEWS_ONLY}")
    if REFRESH_CACHE:
        print("REFRESH MODE: Ignoring cache age, re-fetching all sitemaps")
    if EXTEND_HOURS:
        print(f"EXTEND MODE: Historical collection for {EXTEND_HOURS} hours")

    # Cleanup old parquet files (older than SITEMAP_CACHE_MAX_AGE_DAYS)
    if HAS_PARQUET:
        cleanup_old_parquet_files()

    metadata_cache = {}
    if os.path.exists(CACHE_METADATA):
        try:
            with open(CACHE_METADATA, 'r') as f:
                metadata_cache = json.load(f)
            # Remove old metadata entries
            cutoff_cache = NOW - timedelta(days=SITEMAP_CACHE_MAX_AGE_DAYS)
            metadata_cache = {k: v for k, v in metadata_cache.items()
                           if parse_date(v) is None or parse_date(v) >= cutoff_cache}
            print(f"Loaded {len(metadata_cache)} cached sitemap metadata entries")
        except: pass

    # Load page metadata cache
    page_metadata_cache = {}
    if os.path.exists(CACHE_PAGE_METADATA) and not REFRESH_CACHE:
        try:
            with open(CACHE_PAGE_METADATA, 'r') as f:
                page_metadata_cache = json.load(f)
            # Remove old entries
            cutoff_cache = NOW - timedelta(hours=PAGE_METADATA_CACHE_MAX_AGE_HOURS)
            page_metadata_cache = {k: v for k, v in page_metadata_cache.items()
                                 if parse_date(v.get('fetched_at')) is None or parse_date(v.get('fetched_at')) >= cutoff_cache}
            print(f"Loaded {len(page_metadata_cache)} cached page metadata entries")
        except: pass

    discovery_queue = set()
    if os.path.exists(CACHE_SITEMAPS):
        with open(CACHE_SITEMAPS, "r") as f:
            for line in f: discovery_queue.add(line.strip())
        print(f"Loaded {len(discovery_queue)} sitemaps from cache")

    if not discovery_queue:
        print("Scanning robots.txt for sitemaps...")
        sys.stdout.flush()
        def scan_robots(d):
            try:
                r = session.get(f"https://{d}/robots.txt", timeout=5, headers=headers_base)
                return re.findall(r'^Sitemap:\s*(.*)', r.text, re.I | re.M)
            except: return []
        
        # Scan met futures voor betere progress
        with ThreadPoolExecutor(max_workers=50) as ex:
            futures = {ex.submit(scan_robots, d): d for d in raw_data}
            for i, f in enumerate(tqdm(as_completed(futures), total=len(futures), desc="Scanning", file=sys.stdout, mininterval=0.5)):
                try:
                    res = f.result()
                    for s in res: discovery_queue.add(s.strip())
                except: pass
                if (i + 1) % 50 == 0:
                    print(f"\n  Scanned {i + 1}/{len(raw_data)} domains, found {len(discovery_queue)} sitemaps so far...")
                    sys.stdout.flush()
        print(f"\nFound {len(discovery_queue)} sitemaps from robots.txt")
        sys.stdout.flush()

    final_items = {}
    processed = set()
    to_process = list(discovery_queue)
    new_metadata = metadata_cache.copy()
    all_valid_sitemaps = set()
    stats = {"cached": 0, "index": 0, "items": 0, "errors": 0, "skipped": 0}

    print(f"Starting to process {len(to_process)} sitemaps...")
    print(f"NEWS_ONLY mode: {NEWS_ONLY} (skipping non-news sitemaps)")
    level = 0
    while to_process and level < 5:
        print(f"\nLevel {level}: processing {len(to_process)} sitemaps...")
        sys.stdout.flush()
        next_batch = []
        with ThreadPoolExecutor(max_workers=200) as executor:
            futures = {executor.submit(fetch_sitemap_content, url, cutoff, metadata_cache): url for url in to_process if url not in processed}
            processed.update(to_process)

            for f in tqdm(as_completed(futures), total=len(futures), desc=f"L{level}", file=sys.stdout, mininterval=1):
                url = futures[f]
                try:
                    res_type, (data, lmod) = f.result()
                    if lmod: new_metadata[url] = lmod
                    if res_type == "cached":
                        stats["cached"] += 1
                        all_valid_sitemaps.add(url)
                    elif res_type == "index":
                        stats["index"] += 1
                        next_batch.extend([u for u in data if u not in processed])
                        all_valid_sitemaps.add(url)
                    elif res_type == "items":
                        if data:
                            stats["items"] += 1
                            all_valid_sitemaps.add(url)
                            for it in data:
                                loc = it["loc"]
                                it_id = it.get("id") or hashlib.md5(loc.encode()).hexdigest()
                                it["id"] = it_id
                                if it_id not in final_items or it["lastmod"] > final_items[it_id].get("lastmod", ""):
                                    final_items[it_id] = it
                    elif res_type == "skipped":
                        stats["skipped"] += 1
                    elif res_type == "error":
                        stats["errors"] += 1
                except Exception as e:
                    stats["errors"] += 1
        to_process = next_batch
        print(f"Level {level} done - stats: {stats}, next batch: {len(to_process)} sitemaps")
        sys.stdout.flush()
        level += 1

    print(f"\nScan statistics: {stats}")
    print(f"Valid sitemaps: {len(all_valid_sitemaps)}")

    # Enrich items without title using page metadata
    
    # Pre-save latest 24h as Parquet and JSON (before enrichment)
    latest_items_pre = sorted(final_items.values(), key=lambda x: x.get('lastmod', ''), reverse=True)
    if HAS_PARQUET and latest_items_pre:
        try:
            pd.DataFrame(latest_items_pre).to_parquet('latest_24h.parquet', index=False, compression='snappy')
            print(f'Pre-saved latest 24h Parquet: latest_24h.parquet ({len(latest_items_pre)} items)')
        except Exception as e:
            pass

    try:
        with open('latest_24h.json', 'w') as f:
            json.dump(latest_items_pre, f, indent=2, default=str)
        print(f'Pre-saved latest 24h JSON: latest_24h.json ({len(latest_items_pre)} items)')
    except Exception as e:
        pass

        # Enrich items without title using page metadata (PARALLEL)
    items_to_enrich = [item["loc"] for item_id, item in final_items.items() if not item.get('title') or not item.get('description') or not item.get('image')]
    if items_to_enrich:
        random.shuffle(items_to_enrich)
        print(f"Fetching page metadata for {len(items_to_enrich)} items with missing metadata (Parallel)...")
        sys.stdout.flush()
        
        def enrich_item(loc):
            return loc, fetch_page_metadata(loc, page_metadata_cache)

        with ThreadPoolExecutor(max_workers=50) as executor:
            futures = {executor.submit(enrich_item, loc): loc for loc in items_to_enrich}
            for f in tqdm(as_completed(futures), total=len(futures), desc="Enriching", file=sys.stdout, mininterval=1):
                loc, metadata = f.result()
                if metadata:
                    it_id = hashlib.md5(loc.encode()).hexdigest()
                    if it_id in final_items:
                        item = final_items[it_id]
                        if metadata.get('title') and not item.get('title'):
                            item['title'] = metadata['title']
                        if metadata.get('description') and not item.get('description'):
                            item['description'] = metadata['description']
                        if metadata.get('image') and not item.get('image'):
                            item['image'] = metadata['image']
                        if metadata.get('author') and not item.get('author'):
                            item['author'] = metadata['author']
                        if metadata.get('tags') and not item.get('tags'):
                            existing_keywords = item.get('keywords', '')
                            new_tags = ', '.join(metadata['tags'])
                            item['keywords'] = f"{existing_keywords}, {new_tags}" if existing_keywords else new_tags
                        if metadata.get('published_time') and not item.get('published_time'):
                            item['published_time'] = metadata['published_time']
                        if metadata.get('section') and not item.get('section'):
                            item['section'] = metadata['section']
                        if metadata.get('site_name') and not item.get('site_name'):
                            item['site_name'] = metadata['site_name']
        print(f"Enrichment completed for {len(items_to_enrich)} items.")


    # Save state
    with open(CACHE_SITEMAPS, "w") as f:
        for s in sorted(all_valid_sitemaps): f.write(s + "\n")
    with open(CACHE_METADATA, 'w') as f: json.dump(new_metadata, f, indent=2)
    with open(CACHE_PAGE_METADATA, 'w') as f: json.dump(page_metadata_cache, f, indent=2)

    # XML creation removed as per request

    
    # Save latest 24h as Parquet and JSON
    latest_items = sorted(final_items.values(), key=lambda x: x.get('lastmod', ''), reverse=True)
    if HAS_PARQUET and latest_items:
        try:
            df_latest = pd.DataFrame(latest_items)
            df_latest.to_parquet('latest_24h.parquet', index=False, compression='snappy')
            print(f'Saved latest 24h Parquet: latest_24h.parquet ({len(latest_items)} items)')
        except Exception as e:
            print(f'Warning: Could not save latest_24h.parquet: {e}')

    try:
        with open('latest_24h.json', 'w') as f:
            json.dump(latest_items, f, indent=2, default=str)
        print(f'Saved latest 24h JSON: latest_24h.json ({len(latest_items)} items)')
    except Exception as e:
        print(f'Warning: Could not save latest_24h.json: {e}')

    print(f"DONE! Found {len(final_items)} items with headlines.")

if __name__ == "__main__":
    main()
