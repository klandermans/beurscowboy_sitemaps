#!/usr/bin/env python3
"""
Enrich historical archive with page metadata.
Fetches missing metadata for all items without description/image.
"""

import os
import json
import gzip
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.parse import urlparse
import re

try:
    import pandas as pd
    import pyarrow as pq
    HAS_PARQUET = True
except ImportError:
    HAS_PARQUET = False
    print("Error: pandas/pyarrow not installed")
    exit(1)

try:
    from tqdm import tqdm
except ImportError:
    def tqdm(iterable, *args, **kwargs):
        return iterable

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Settings
HISTORICAL_ARCHIVE = "historical_archive.parquet"
HISTORICAL_JSON = "historical_archive.json"
PAGE_METADATA_CACHE = "page_metadata_cache.json"
CACHE_MAX_AGE_HOURS = 24

# Regex patterns for metadata extraction
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

# HTTP Session
session = requests.Session()
adapter = HTTPAdapter(pool_connections=50, pool_maxsize=50, max_retries=Retry(total=2, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504]))
session.mount("http://", adapter)
session.mount("https://", adapter)
headers_base = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}

def clean_xml_text(b_text):
    if not b_text:
        return ""
    return b_text.decode('utf-8', 'ignore').strip().replace('<![CDATA[', '').replace(']]>', '').replace('&', '&amp;')

def fetch_page_metadata(url, page_metadata_cache, force_refresh=False):
    """Fetch page metadata from HTML page."""
    # Check cache first
    cutoff_cache = datetime.now(timezone.utc) - timedelta(hours=CACHE_MAX_AGE_HOURS)
    if not force_refresh and url in page_metadata_cache:
        cached = page_metadata_cache[url]
        cached_time = None
        if cached.get('fetched_at'):
            try:
                cached_time = datetime.fromisoformat(cached['fetched_at'].replace('Z', '+00:00'))
            except:
                pass
        if cached_time and cached_time >= cutoff_cache:
            return cached
    
    try:
        headers = headers_base.copy()
        resp = session.get(url, timeout=10, headers=headers)
        if resp.status_code != 200:
            return {}
        
        content = resp.content
        metadata = {}
        
        # Title
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
        
        # Description
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
        
        # Image
        image = None
        og_image_m = RE_OG_IMAGE.search(content)
        if og_image_m:
            image = clean_xml_text(og_image_m.group(1))
        else:
            twitter_image_m = RE_TWITTER_IMAGE.search(content)
            if twitter_image_m:
                image = clean_xml_text(twitter_image_m.group(1))
        if image:
            if image.startswith('/'):
                parsed = urlparse(url)
                image = f"{parsed.scheme}://{parsed.netloc}{image}"
            elif image.startswith('//'):
                parsed = urlparse(url)
                image = f"{parsed.scheme}:{image}"
            metadata['image'] = image
        
        # Canonical URL
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
        
        # Author
        author_m = RE_AUTHOR.search(content)
        if author_m:
            metadata['author'] = clean_xml_text(author_m.group(1))
        
        # Published time
        pub_time_m = RE_PUBLISHED_TIME.search(content)
        if pub_time_m:
            metadata['published_time'] = clean_xml_text(pub_time_m.group(1))
        
        # Modified time
        mod_time_m = RE_MODIFIED_TIME.search(content)
        if mod_time_m:
            metadata['modified_time'] = clean_xml_text(mod_time_m.group(1))
        
        # Section
        section_m = RE_SECTION.search(content)
        if section_m:
            metadata['section'] = clean_xml_text(section_m.group(1))
        
        # Tags
        tags = []
        for tag_m in RE_TAG.finditer(content):
            tag = clean_xml_text(tag_m.group(1))
            if tag:
                tags.append(tag)
        if tags:
            metadata['tags'] = tags
        
        # Site name
        site_name_m = RE_OG_SITE_NAME.search(content)
        if site_name_m:
            metadata['site_name'] = clean_xml_text(site_name_m.group(1))
        
        # OG type
        og_type_m = RE_OG_TYPE.search(content)
        if og_type_m:
            metadata['og_type'] = clean_xml_text(og_type_m.group(1))
        
        # Twitter card
        twitter_card_m = RE_TWITTER_CARD.search(content)
        if twitter_card_m:
            metadata['twitter_card'] = clean_xml_text(twitter_card_m.group(1))
        
        # OG URL
        og_url_m = RE_OG_URL.search(content)
        if og_url_m:
            metadata['og_url'] = clean_xml_text(og_url_m.group(1))
        
        # H1 fallback
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
        return {}

def needs_enrichment(item):
    """Check if an item needs metadata enrichment."""
    # Needs enrichment if missing key fields
    has_description = bool(item.get('description'))
    has_image = bool(item.get('image'))
    has_author = bool(item.get('author'))
    has_published_time = bool(item.get('published_time'))
    
    return not (has_description and has_image)

def main():
    print("="*70)
    print("ENRICH HISTORICAL ARCHIVE WITH PAGE METADATA")
    print("="*70)
    
    # Load page metadata cache
    page_metadata_cache = {}
    if os.path.exists(PAGE_METADATA_CACHE):
        try:
            with open(PAGE_METADATA_CACHE, 'r') as f:
                page_metadata_cache = json.load(f)
            print(f"Loaded {len(page_metadata_cache):,} page metadata entries from cache")
        except:
            pass
    
    # Load historical archive
    print(f"\nLoading historical archive: {HISTORICAL_ARCHIVE}")
    if not os.path.exists(HISTORICAL_ARCHIVE):
        print(f"Error: {HISTORICAL_ARCHIVE} not found")
        return
    
    df = pd.read_parquet(HISTORICAL_ARCHIVE)
    items = df.to_dict('records')
    print(f"Loaded {len(items):,} items from archive")
    
    # Find items needing enrichment
    items_to_enrich = []
    for i, item in enumerate(items):
        loc = item.get('loc', '')
        if loc and needs_enrichment(item):
            # Check if we have recent cache
            if loc in page_metadata_cache:
                cached = page_metadata_cache[loc]
                cached_time = None
                if cached.get('fetched_at'):
                    try:
                        cached_time = datetime.fromisoformat(cached['fetched_at'].replace('Z', '+00:00'))
                    except:
                        pass
                if cached_time and cached_time >= datetime.now(timezone.utc) - timedelta(hours=CACHE_MAX_AGE_HOURS):
                    # Apply cached metadata
                    for key, value in cached.items():
                        if key != 'fetched_at' and not item.get(key):
                            item[key] = value
                    continue
            items_to_enrich.append((i, loc, item))
    
    print(f"\nItems needing enrichment: {len(items_to_enrich):,} ({len(items_to_enrich)/len(items)*100:.1f}%)")
    
    if not items_to_enrich:
        print("All items already enriched!")
        return
    
    # Fetch metadata in batches
    print(f"\nFetching page metadata for {len(items_to_enrich):,} items...")
    print(f"Rate: ~50 requests/sec, ETA: {len(items_to_enrich)/50/60:.1f} minutes")
    sys.stdout.flush()
    
    enriched_count = 0
    error_count = 0
    cache_hits = 0
    
    for idx, (i, loc, item) in enumerate(tqdm(items_to_enrich, desc="Enriching", file=sys.stdout, mininterval=1)):
        metadata = fetch_page_metadata(loc, page_metadata_cache)
        
        if metadata:
            # Apply metadata to item
            for key, value in metadata.items():
                if key != 'fetched_at' and not item.get(key):
                    item[key] = value
                    enriched_count += 1
            
            # Check if we got description/image
            if metadata.get('description') or metadata.get('image'):
                cache_hits += 1
        else:
            error_count += 1
        
        # Progress update every 1000 items
        if (idx + 1) % 1000 == 0:
            print(f"\n  Progress: {idx + 1:,}/{len(items_to_enrich):,} - Enriched: {enriched_count:,}, Cache hits: {cache_hits:,}, Errors: {error_count:,}")
            sys.stdout.flush()
    
    print(f"\nEnrichment complete!")
    print(f"  Items processed: {len(items_to_enrich):,}")
    print(f"  Metadata fields added: {enriched_count:,}")
    print(f"  Items with description/image: {cache_hits:,}")
    print(f"  Errors/timeouts: {error_count:,}")
    
    # Save updated page metadata cache
    print(f"\nSaving page metadata cache ({len(page_metadata_cache):,} entries)...")
    with open(PAGE_METADATA_CACHE, 'w') as f:
        json.dump(page_metadata_cache, f, indent=2, default=str)
    
    # Save updated archive
    print(f"\nSaving updated historical archive...")
    df_updated = pd.DataFrame(items)
    df_updated.to_parquet(HISTORICAL_ARCHIVE, index=False, compression='snappy')
    print(f"  Saved: {HISTORICAL_ARCHIVE}")
    
    # Also save JSON if not too large
    if len(items) < 100000:
        with open(HISTORICAL_JSON, 'w') as f:
            json.dump(items, f, indent=2, default=str)
        print(f"  Saved: {HISTORICAL_JSON}")
        
        with open(HISTORICAL_JSON + '.gz', 'wb') as f:
            f.write(gzip.compress(json.dumps(items, indent=2, default=str).encode()))
        print(f"  Saved: {HISTORICAL_JSON}.gz")
    
    # Print statistics
    print("\n" + "="*70)
    print("UPDATED ARCHIVE STATISTICS")
    print("="*70)
    
    field_counts = {}
    for item in items:
        for key in item.keys():
            field_counts[key] = field_counts.get(key, 0) + 1
    
    print("\nField coverage:")
    for field, count in sorted(field_counts.items(), key=lambda x: -x[1]):
        pct = (count / len(items)) * 100
        bar = "█" * int(pct / 5)
        print(f"  {field:20s}: {count:6,} ({pct:5.1f}%) {bar}")
    
    print("\n" + "="*70)
    print("DONE! Historical archive enriched successfully.")
    print("="*70)

if __name__ == "__main__":
    main()
