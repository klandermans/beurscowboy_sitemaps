import hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed
#!/usr/bin/env python3
"""
Build historical dataset from cached parquet files and page metadata.
Merges all cached data into a single historical archive incrementally.
"""

import os
import json
import gzip
import sys
from datetime import datetime, timezone
from pathlib import Path

try:
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
    HAS_PARQUET = True
except ImportError:
    HAS_PARQUET = False
    print("Error: pandas/pyarrow not installed")
    exit(1)

CACHE_DIR = "cache_parquet"
CACHE_PAGE_METADATA = "page_metadata_cache.json"
CACHE_METADATA = "cache_metadata.json"
HISTORICAL_OUTPUT = "historical_archive.parquet"
HISTORICAL_JSON = "historical_archive.json"
DOMAINS_FILE = "domains.json"

def load_domains():
    """Load domains from shared domains.json."""
    if os.path.exists(DOMAINS_FILE):
        try:
            with open(DOMAINS_FILE, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"Warning: Could not read {DOMAINS_FILE}: {e}")
    return []

def load_existing_archive():
    """Load existing historical archive if it exists."""
    if os.path.exists(HISTORICAL_OUTPUT):
        try:
            df = pd.read_parquet(HISTORICAL_OUTPUT)
            if "id" not in df.columns:
                df["id"] = df["loc"].apply(lambda x: hashlib.md5(x.encode()).hexdigest())
            return df.to_dict('records')
        except Exception as e:
            print(f"Warning: Could not read existing archive {HISTORICAL_OUTPUT}: {e}")
    return []

def load_parquet_files():
    """Load all parquet files from cache directory (PARALLEL)."""
    all_items = []
    parquet_files = list(Path(CACHE_DIR).glob("*.parquet"))
    
    print(f"Found {len(parquet_files)} parquet cache files (Parallel loading)")
    
    def load_file(pq_path):
        try:
            df = pd.read_parquet(pq_path)
            return df.to_dict('records')
        except:
            return []

    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(load_file, path): path for path in parquet_files}
        for f in as_completed(futures):
            all_items.extend(f.result())
    
    return all_items

def load_page_metadata():
    """Load page metadata cache."""
    if os.path.exists(CACHE_PAGE_METADATA):
        with open(CACHE_PAGE_METADATA, 'r') as f:
            return json.load(f)
    return {}

def merge_data(new_items, existing_items, page_metadata, domains=None):
    """
    Merge new items with existing archive and page metadata.
    Avoids re-processing items already in existing_items.
    """
    merged = {}
    
    # Start with existing items (already "done")
    for item in existing_items:
        loc = item.get('loc', '')
        item_id = item.get('id') or hashlib.md5(loc.encode()).hexdigest()
        if loc:
            merged[item_id] = item
    
    existing_count = len(merged)
    new_added = 0
    enriched_count = 0
    
    # Domain set for filtering if provided
    domain_set = set(domains) if domains else None

    # Add new items
    for item in new_items:
        loc = item.get('loc', '')
        item_id = item.get('id') or hashlib.md5(loc.encode()).hexdigest()
        if not loc:
            continue
            
        # Optional: filter by domain if domains list is provided
        if domain_set:
            try:
                domain = item.get('loc', '').split('//')[-1].split('/')[0]
                # Basic domain check (could be improved)
                if not any(d in domain for d in domain_set):
                    continue
            except:
                pass

        # If already in archive, we only update if this item is "newer" (based on lastmod)
        # However, for historical building, we usually assume the archive is the source of truth
        # unless we want to allow updates to existing entries.
        if item_id in merged:
            # Maybe update metadata if it was missing?
            merged_item = merged[item_id]
            if loc in page_metadata:
                meta = page_metadata[loc]
                for key, value in meta.items():
                    if key != 'fetched_at' and not merged_item.get(key):
                        merged_item[key] = value
                        enriched_count += 1
            continue
        
        # New item!
        merged_item = item.copy()
        new_added += 1
        
        # Enrich with page metadata if available
        if loc in page_metadata:
            meta = page_metadata[loc]
            for key, value in meta.items():
                if key != 'fetched_at' and not merged_item.get(key):
                    merged_item[key] = value
                    enriched_count += 1
        
        merged[item_id] = merged_item
    
    return list(merged.values()), new_added, enriched_count

def save_historical_archive(items):
    """Save historical archive as parquet and JSON."""
    if not items:
        print("No items to save")
        return
    
    print(f"\nSaving {len(items):,} items to historical archive...")
    
    # Save as Parquet
    try:
        df = pd.DataFrame(items)
        
        # Add archive timestamp for the whole collection
        df['archived_at'] = datetime.now(timezone.utc).isoformat()
        
        # Sort by lastmod descending
        if 'lastmod' in df.columns:
            # Convert to datetime for sorting
            df['lastmod_dt'] = pd.to_datetime(df['lastmod'], utc=True, errors='coerce')
            df = df.sort_values('lastmod_dt', ascending=False)
            df = df.drop(columns=['lastmod_dt'])
        
        df.to_parquet(HISTORICAL_OUTPUT, index=False, compression='snappy')
        print(f"  Saved Parquet: {HISTORICAL_OUTPUT}")
    except Exception as e:
        print(f"  Error saving Parquet: {e}")
    
    # Save as JSON (for easy inspection) - Optional: only save small subset or skip if too large
    if len(items) < 50000:
        try:
            with open(HISTORICAL_JSON, 'w') as f:
                json.dump(items, f, indent=2, default=str)
            print(f"  Saved JSON: {HISTORICAL_JSON}")
            
            # Also create a gzipped version
            with open(HISTORICAL_JSON + '.gz', 'wb') as f:
                f.write(gzip.compress(json.dumps(items, indent=2, default=str).encode()))
            print(f"  Saved Gzipped JSON: {HISTORICAL_JSON}.gz")
        except Exception as e:
            print(f"  Error saving JSON: {e}")
    else:
        print(f"  Note: JSON output skipped because archive is large ({len(items)} items)")

def print_statistics(items, page_metadata):
    """Print statistics about the historical archive."""
    print("\n" + "="*60)
    print("HISTORICAL ARCHIVE STATISTICS")
    print("="*60)
    
    # Basic counts
    print(f"Total items: {len(items):,}")
    
    if not items:
        return

    # Count fields
    field_counts = {}
    for item in items:
        for key in item.keys():
            field_counts[key] = field_counts.get(key, 0) + 1
    
    print("\nField coverage:")
    for field, count in sorted(field_counts.items(), key=lambda x: -x[1]):
        pct = (count / len(items)) * 100
        print(f"  {field}: {count:,} ({pct:.1f}%)")
    
    # Date range
    lastmods = []
    for item in items:
        if item.get('lastmod'):
            try:
                dt = datetime.fromisoformat(item['lastmod'].replace('Z', '+00:00'))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                lastmods.append(dt)
            except:
                pass
    
    if lastmods:
        print(f"\nDate range:")
        print(f"  Oldest: {min(lastmods).isoformat()}")
        print(f"  Newest: {max(lastmods).isoformat()}")
    
    # Page metadata usage
    print(f"\nPage metadata cache: {len(page_metadata):,} entries")
    
    # Sources
    sources = {}
    for item in items:
        source = item.get('source', 'Unknown')
        sources[source] = sources.get(source, 0) + 1
    
    print("\nTop 10 sources:")
    for source, count in sorted(sources.items(), key=lambda x: -x[1])[:10]:
        print(f"  {source}: {count:,}")
    
    print("="*60)

def main():
    print("BUILDING HISTORICAL ARCHIVE (Incremental)")
    print("="*60)
    print(f"Domains list: {DOMAINS_FILE}")
    print(f"Parquet cache dir: {CACHE_DIR}")
    print(f"Page metadata cache: {CACHE_PAGE_METADATA}")
    print(f"Output: {HISTORICAL_OUTPUT}")
    print("="*60)
    
    # Load domains
    domains = load_domains()
    print(f"Loaded {len(domains)} domains to track")
    
    # Load existing archive
    print("\nLoading existing archive...")
    existing_items = load_existing_archive()
    print(f"Loaded {len(existing_items):,} existing items")
    
    # Load all cached data
    print("\nLoading new cached sitemap data...")
    new_sitemap_items = load_parquet_files()
    print(f"Loaded {len(new_sitemap_items):,} items from parquet cache")
    
    print("\nLoading page metadata cache...")
    page_metadata = load_page_metadata()
    print(f"Loaded {len(page_metadata):,} page metadata entries")
    
    # Merge data
    print("\nMerging data...")
    merged_items, new_added, enrichments = merge_data(new_sitemap_items, existing_items, page_metadata, domains)
    print(f"Total archive: {len(merged_items):,} items")
    print(f"  New items added: {new_added:,}")
    print(f"  Items enriched from metadata: {enrichments:,}")
    
    # Save archive
    save_historical_archive(merged_items)
    
    # Print statistics
    print_statistics(merged_items, page_metadata)
    
    print("\nDONE! Historical archive updated successfully.")

if __name__ == "__main__":
    main()
