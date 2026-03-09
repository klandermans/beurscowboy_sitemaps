import requests
import re
import os
import gzip
import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
import urllib.parse
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

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
raw_data = [
    "bloomberg.com", "reuters.com", "wsj.com", "cnbc.com", "ft.com", "finance.yahoo.com", "investing.com", "marketwatch.com", "barrons.com", "economist.com", "forbes.com", "businessinsider.com", "fortune.com", "nasdaq.com", "investopedia.com", "thestreet.com", "morningstar.com", "seekingalpha.com", "benzinga.com", "zerohedge.com", "nikkei.com", "lesechos.fr", "tradingview.com", "koyfin.com", "ycharts.com", "finviz.com", "tipranks.com", "fool.com", "kiplinger.com", "marketbeat.com", "briefing.com", "newsquawk.com", "fxstreet.com", "forexlive.com", "marketaux.com", "stocktwits.com", "etf.com", "etftrends.com", "barchart.com", "gurufocus.com", "simplywall.st", "wallstreetzen.com", "theblock.co", "coindesk.com", "cointelegraph.com", "economictimes.indiatimes.com", "caixinglobal.com", "scmp.com", "theglobeandmail.com", "afr.com", "moneycontrol.com", "dgap.de", "borsen.dk", "fd.nl", "tijd.be", "beursduivel.be", "privateequitywire.co.uk", "venturebeat.com", "techcrunch.com", "pymnts.com", "finextra.com", "fintechmagazine.com", "thepharmaletter.com", "platts.com", "mining.com", "shippingwatch.com", "reit.com", "realvision.com", "pionline.com", "goldmansachs.com", "jpmorgan.com", "morganstanley.com", "blackrock.com", "imf.org", "ecb.europa.eu", "federalreserve.gov", "bis.org", "stlouisfed.org", "project-syndicate.org", "voxeu.org", "morningbrew.com", "thehustle.co", "axios.com", "puck.news", "substack.com", "reddit.com/r/investing", "reddit.com/r/stocks", "quiverquant.com", "unusualwhales.com", "openinsider.com", "whalewisdom.com", "glassnode.com", "messari.io", "defillama.com", "moodys.com", "spglobal.com", "fitchratings.com", "msci.com", "vanguard.com", "bridgewater.com", "citadel.com", "a16z.com", "sequoiacap.com", "ycombinator.com", "dealbook.nytimes.com", "qz.com", "fastcompany.com", "inc.com", "entrepreneur.com", "hbr.org", "mckinsey.com", "bcg.com", "bain.com", "deloitte.com", "pwc.com", "ey.com", "kpmg.com", "worldbank.org", "oecd.org", "wto.org", "iif.com", "isda.org", "sifma.org", "finra.org", "sec.gov", "fca.org.uk", "esma.europa.eu", "tmx.com", "lseg.com", "euronext.com", "deutsche-boerse.com", "hkex.com.hk", "sgx.com", "tse.or.jp", "jse.co.za", "chicagobusiness.com", "crainsnewyork.com", "bizjournals.com", "americanbanker.com", "insurancejournal.com", "institutionalinvestor.com", "citywire.com", "wealthmanagement.com", "financial-planning.com", "investmentnews.com", "thinkadvisor.com", "advisorhub.com", "fa-mag.com", "etfstream.com", "mutualfundobserver.com", "hedgeweek.com", "pehub.com", "pitchbook.com", "crunchbase.com", "cbinsights.com", "tracxn.com", "preqin.com", "tradingeconomics.com", "indexmundi.com", "quandl.com", "intrinio.com", "iexcloud.io", "polygon.io", "alpha-vantage.co", "tiingo.com", "yardeni.com", "calculatedriskblog.com", "marginalrevolution.com", "econbrowser.com", "wolfstreet.com", "bondvigilantes.com", "capitalspectator.com", "valueline.com", "investors.com", "marketwatch.com", "schaeffersresearch.com", "cboe.com", "cmegroup.com", "morningstar.co.uk", "advfn.com", "moneyweek.com", "sharesmagazine.co.uk", "investorschronicle.co.uk", "iii.co.uk", "hl.co.uk", "ig.com", "saxobank.com", "etoro.com", "degiro.com", "interactivebrokers.com", "robinhood.com", "schwab.com", "fidelity.com", "vanguard.com", "blackrock.com/us", "pimco.com", "allianz.com", "axa.com", "generali.com", "prudential.com", "metlife.com", "manulife.com", "sunlife.com", "hsbc.com", "bnpparibas.com", "societegenerale.com", "santander.com", "bbva.com", "unicreditgroup.eu", "intesasanpaolo.com", "ubs.com", "deutschebank.com", "commerzbank.com", "ing.com", "rabobank.com", "abnamro.com", "kbc.com", "belfius.be", "societe.com", "boursorama.com", "lesechos.fr", "latribune.fr", "lefigaro.fr/economie", "lemonde.fr/economie", "capital.fr", "zonebourse.com", "ilsole24ore.com", "milanofinanza.it", "corriere.it/economia", "repubblica.it/economia", "expansion.com", "cincodias.elpais.com", "eleconomista.es", "jornaldenegocios.pt", "dinheirovivo.pt", "faz.net", "welt.de/wirtschaft", "wiwo.de", "boerse-online.de", "godmode-trader.de", "finanzen.net", "onvista.de", "wallstreet-online.de", "nzz.ch/wirtschaft", "fuw.ch", "cash.ch", "handelszeitung.ch", "standard.at", "diepresse.com/economie", "di.se", "e24.no", "kauppalehti.fi", "pge.pl", "money.pl", "bankier.pl", "portfolio.hu", "ekonomika.cz", "rbc.ru", "kommersant.ru", "vedomosti.ru", "finanz.ru", "money.163.com", "finance.sina.com.cn", "eastmoney.com", "hexun.com", "asahi.com/business", "mainichi.jp/biz", "yomiuri.co.jp/economy", "chosun.com/business", "donga.com/economy", "hankyung.com", "mk.co.kr", "livemint.com", "business-standard.com", "financialexpress.com", "moneycontrol.com", "dawn.com/business", "aljazeera.com/business", "arabnews.com/business-economy", "gulfnews.com/business", "thenationalnews.com/business", "economist.co.il", "globes.co.il", "themarker.com", "businessday.ng", "vanguardngr.com/business", "standardmedia.co.ke/business", "mg.co.za", "fin24.com", "valor.globo.com", "exame.com", "cronista.com", "lanacion.com.ar/economia", "df.cl", "portafolio.co", "eleconomista.com.mx",
    "stcn.com", "cnstock.com", "jrj.com.cn", "hket.com", "aastocks.com", 
    "yicaiglobal.com", "kr-asia.com", "japantimes.co.jp", "nikkan.co.jp", 
    "kabutan.jp", "minkabu.jp", "fisco.co.jp", "stockhead.com.au", 
    "marketindex.com.au", "interest.co.nz", "theedgesingapore.com", 
    "businesstimes.com.sg", "bworldonline.com", "vietnamnews.vn", 
    "etfdb.com", "stockcharts.com", "bamsec.com", "insiderviz.com",
    "globenewswire.com", "businesswire.com", "infomoney.com.br"
]
raw_data = list(set(raw_data))

# SETTINGS
NOW = datetime.now(timezone.utc)
CACHE_METADATA = "cache_metadata.json"

# Optimized Nuclear Session
session = requests.Session()
adapter = HTTPAdapter(pool_connections=200, pool_maxsize=200, max_retries=Retry(total=0))
session.mount("http://", adapter)
session.mount("https://", adapter)
session.verify = False
requests.packages.urllib3.disable_warnings()
headers_base = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

def parse_date(date_str):
    if not date_str: return None
    try: return datetime.fromisoformat(date_str.strip().replace('Z', '+00:00'))
    except:
        try: return datetime.strptime(date_str.strip()[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except: return None

def clean_xml_text(b_text):
    if not b_text: return ""
    return b_text.decode('utf-8', 'ignore').strip().replace('<![CDATA[', '').replace(']]>', '').replace('&', '&amp;')

# Regex
RE_SITEMAPINDEX = re.compile(b'<sitemapindex', re.I)
RE_LOC = re.compile(b'<loc>(.*?)</loc>', re.I | re.DOTALL)
RE_LASTMOD = re.compile(b'<lastmod>(.*?)</lastmod>', re.I | re.DOTALL)
RE_SITEMAP_BLOCK = re.compile(b'<sitemap>(.*?)</sitemap>', re.I | re.DOTALL)
RE_URL_BLOCK = re.compile(b'<url>(.*?)</url>', re.I | re.DOTALL)

def fetch_sitemap_content(url, cutoff, metadata_cache):
    headers = headers_base.copy()
    if url in metadata_cache: headers['If-Modified-Since'] = metadata_cache[url]
    try:
        resp = session.get(url, timeout=10, headers=headers)
        if resp.status_code == 304: return ("cached", (None, None))
        if resp.status_code != 200: return ("error", (None, None))
        
        new_lmod = resp.headers.get('Last-Modified')
        content = resp.content
        if url.endswith('.gz') or content.startswith(b'\x1f\x8b'): content = gzip.decompress(content)
        
        if RE_SITEMAPINDEX.search(content):
            subs = []
            # BE AGGRESSIVE: Follow almost all sitemaps found
            for loc_b in RE_LOC.findall(content):
                loc = loc_b.decode('utf-8', 'ignore').strip()
                # Skip common garbage to save time
                if any(w in loc.lower() for w in ['image', 'video', 'tag', 'category', 'author']): continue
                subs.append(loc)
            return ("index", (subs, new_lmod))
        
        items = []
        for block in RE_URL_BLOCK.finditer(content):
            data = block.group(1)
            lm_match = RE_LASTMOD.search(data)
            lm_str = lm_match.group(1).decode('utf-8', 'ignore').strip() if lm_match else ""
            dt = parse_date(lm_str)
            # If date is within last 24h OR missing (we check headline anyway)
            if not dt or (cutoff <= dt <= (NOW + timedelta(minutes=10))):
                title_m = re.search(b'<news:title>(.*?)</news:title>', data, re.I | re.S)
                if not title_m: title_m = re.search(b'<title>(.*?)</title>', data, re.I | re.S)
                if title_m:
                    loc_m = RE_LOC.search(data)
                    if loc_m:
                        loc = loc_m.group(1).decode('utf-8', 'ignore').strip()
                        meta = {"loc": loc, "lastmod": lm_str, "title": clean_xml_text(title_m.group(1))}
                        # Optional fields
                        kw_m = re.search(b'<news:keywords>(.*?)</news:keywords>', data, re.I | re.S)
                        if kw_m: meta["keywords"] = clean_xml_text(kw_m.group(1))
                        pub_m = re.search(b'<news:name>(.*?)</news:name>', data, re.I | re.S)
                        if pub_m: meta["source"] = clean_xml_text(pub_m.group(1))
                        items.append(meta)
        return ("items", (items, new_last_mod))
    except: return ("error", (None, None))

def main():
    cutoff = NOW - timedelta(hours=24)
    print(f"NUCLEAR AGGRESSIVE Scan | Start: {NOW.strftime('%H:%M:%S')} UTC")
    
    metadata_cache = {}
    if os.path.exists(CACHE_METADATA):
        try:
            with open(CACHE_METADATA, 'r') as f: metadata_cache = json.load(f)
        except: pass

    # ALWAYS SCAN robots.txt
    print("Broad discovery from robots.txt...")
    discovery_queue = set()
    def scan_robots(d):
        try:
            r = session.get(f"https://{d}/robots.txt", timeout=3, headers=headers_base)
            return re.findall(r'^Sitemap:\s*(.*)', r.text, re.I | re.M)
        except: return []
    with ThreadPoolExecutor(max_workers=100) as ex:
        for res in ex.map(scan_robots, raw_data):
            for s in res: discovery_queue.add(s.strip())

    final_items = {}
    processed = set()
    to_process = list(discovery_queue)
    new_metadata = metadata_cache.copy()

    # Fully Recursive Loop
    level = 0
    while to_process and level < 10:
        print(f"Level {level}: processing {len(to_process)} sitemaps...")
        next_batch = []
        with ThreadPoolExecutor(max_workers=200) as executor:
            futures = {executor.submit(fetch_sitemap_content, url, cutoff, metadata_cache): url for url in to_process if url not in processed}
            processed.update(to_process)
            
            for f in tqdm(as_completed(futures), total=len(futures), desc=f"L{level}"):
                url = futures[f]
                try:
                    res_type, (data, lmod) = f.result()
                    if lmod: new_metadata[url] = lmod
                    
                    if res_type == "index":
                        next_batch.extend([u for u in data if u not in processed])
                    elif res_type == "items":
                        if data:
                            for it in data:
                                loc = it["loc"]
                                if loc not in final_items or it["lastmod"] > final_items[loc].get("lastmod", ""):
                                    final_items[loc] = it
                except: pass
        to_process = next_batch
        level += 1

    # Save state
    with open(CACHE_METADATA, 'w') as f: json.dump(new_metadata, f, indent=2)

    # Write combined_sitemap.xml
    with open("combined_sitemap.xml", "w") as f:
        f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        f.write('<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9" xmlns:news="http://www.google.com/schemas/sitemap-news/0.9">\n')
        for item in sorted(final_items.values(), key=lambda x: x.get("lastmod", ""), reverse=True):
            f.write('  <url>\n')
            f.write(f'    <loc>{item["loc"]}</loc>\n')
            if item.get("lastmod"): f.write(f'    <lastmod>{item["lastmod"]}</lastmod>\n')
            f.write('    <news:news>\n')
            if "source" in item: f.write(f'      <news:publication><news:name>{item["source"]}</news:name><news:language>en</news:language></news:publication>\n')
            if item.get("lastmod"): f.write(f'      <news:publication_date>{item["lastmod"]}</news:publication_date>\n')
            f.write(f'      <news:title>{item["title"]}</news:title>\n')
            if "keywords" in item: f.write(f'      <news:keywords>{item["keywords"]}</news:keywords>\n')
            f.write('    </news:news>\n')
            f.write('  </url>\n')
        f.write('</urlset>\n')
    
    print(f"DONE! Found {len(final_items)} items with headlines.")

if __name__ == "__main__":
    main()
