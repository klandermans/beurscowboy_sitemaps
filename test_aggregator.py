import requests
import re
import os
import gzip
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# TEST DOMAINS ONLY
raw_data = ["cointelegraph.com", "reuters.com", "wsj.com", "cnbc.com", "ft.com"]

NOW = datetime.now(timezone.utc)
session = requests.Session()
adapter = HTTPAdapter(pool_connections=50, pool_maxsize=50, max_retries=Retry(total=0))
session.mount("http://", adapter)
session.mount("https://", adapter)
session.verify = False
requests.packages.urllib3.disable_warnings()
headers_base = {'User-Agent': 'Mozilla/5.0'}

def parse_date(date_str):
    if not date_str: return None
    try: return datetime.fromisoformat(date_str.strip().replace('Z', '+00:00'))
    except:
        try: return datetime.strptime(date_str.strip()[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except: return None

def clean_xml_text(b_text):
    if not b_text: return ""
    return b_text.decode('utf-8', 'ignore').strip().replace('<![CDATA[', '').replace(']]>', '').replace('&', '&amp;')

RE_SITEMAPINDEX = re.compile(b'<sitemapindex', re.I)
RE_LOC = re.compile(b'<loc>(.*?)</loc>', re.I | re.DOTALL)
RE_LASTMOD = re.compile(b'<lastmod>(.*?)</lastmod>', re.I | re.DOTALL)
RE_SITEMAP_BLOCK = re.compile(b'<sitemap>(.*?)</sitemap>', re.I | re.DOTALL)
RE_URL_BLOCK = re.compile(b'<url>(.*?)</url>', re.I | re.DOTALL)

def fetch_sitemap_content(url, cutoff):
    try:
        resp = session.get(url, timeout=10, headers=headers_base)
        if resp.status_code != 200: return ("error", None)
        content = resp.content
        if url.endswith('.gz') or content.startswith(b'\x1f\x8b'): content = gzip.decompress(content)
        if RE_SITEMAPINDEX.search(content):
            subs = []
            for block in RE_SITEMAP_BLOCK.finditer(content):
                data = block.group(1)
                loc_match = RE_LOC.search(data)
                if loc_match:
                    loc = loc_match.group(1).decode('utf-8', 'ignore').strip()
                    lm_match = RE_LASTMOD.search(data)
                    if lm_match:
                        lm = parse_date(lm_match.group(1).decode('utf-8', 'ignore'))
                        if lm and lm < cutoff: continue
                    subs.append(loc)
            if not subs:
                for loc_b in RE_LOC.findall(content):
                    subs.append(loc_b.decode('utf-8', 'ignore').strip())
            return ("index", subs)
        items = []
        for block in RE_URL_BLOCK.finditer(content):
            data = block.group(1)
            lm_match = RE_LASTMOD.search(data)
            lm_str = lm_match.group(1).decode('utf-8', 'ignore').strip() if lm_match else ""
            dt = parse_date(lm_str)
            if not dt or (cutoff <= dt <= (NOW + timedelta(minutes=10))):
                title_m = re.search(b'<news:title>(.*?)</news:title>', data, re.I | re.S)
                if not title_m: title_m = re.search(b'<title>(.*?)</title>', data, re.I | re.S)
                if title_m:
                    loc_m = RE_LOC.search(data)
                    if loc_m:
                        loc = loc_m.group(1).decode('utf-8', 'ignore').strip()
                        items.append({"loc": loc, "lastmod": lm_str, "title": clean_xml_text(title_m.group(1))})
        return ("items", items)
    except: return ("error", None)

def main():
    cutoff = NOW - timedelta(hours=24)
    discovery_queue = set()
    for d in raw_data:
        try:
            r = session.get(f"https://{d}/robots.txt", timeout=3, headers=headers_base)
            for s in re.findall(r'^Sitemap:\s*(.*)', r.text, re.I | re.M): discovery_queue.add(s.strip())
        except: pass
    
    final_items = {}
    processed = set()
    to_process = list(discovery_queue)
    level = 0
    while to_process and level < 5:
        next_batch = []
        with ThreadPoolExecutor(max_workers=50) as executor:
            futures = {executor.submit(fetch_sitemap_content, url, cutoff): url for url in to_process if url not in processed}
            processed.update(to_process)
            for f in as_completed(futures):
                res_type, data = f.result()
                if res_type == "index": next_batch.extend([u for u in data if u not in processed])
                elif res_type == "items" and data:
                    for it in data:
                        if it["loc"] not in final_items: final_items[it["loc"]] = it
        to_process = next_batch
        level += 1
    
    with open("combined_sitemap.xml", "w") as f:
        f.write('<?xml version="1.0" encoding="UTF-8"?>\n<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9" xmlns:news="http://www.google.com/schemas/sitemap-news/0.9">\n')
        for item in sorted(final_items.values(), key=lambda x: x["lastmod"], reverse=True):
            f.write(f'  <url>\n    <loc>{item["loc"]}</loc>\n    <lastmod>{item["lastmod"]}</lastmod>\n    <news:news>\n      <news:title>{item["title"]}</news:title>\n    </news:news>\n  </url>\n')
        f.write('</urlset>\n')
    print(f"SUCCESS! Found {len(final_items)} items.")

if __name__ == "__main__":
    main()
