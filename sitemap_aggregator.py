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
raw_data = [
    "bloomberg.com", "reuters.com", "wsj.com", "cnbc.com", "ft.com", "finance.yahoo.com", "investing.com", "marketwatch.com", "barrons.com", "economist.com", "forbes.com", "businessinsider.com", "fortune.com", "nasdaq.com", "investopedia.com", "thestreet.com", "morningstar.com", "seekingalpha.com", "benzinga.com", "zerohedge.com", "nikkei.com", "lesechos.fr", "tradingview.com", "koyfin.com", "ycharts.com", "finviz.com", "tipranks.com", "fool.com", "kiplinger.com", "marketbeat.com", "briefing.com", "newsquawk.com", "fxstreet.com", "forexlive.com", "marketaux.com", "stocktwits.com", "etf.com", "etftrends.com", "barchart.com", "gurufocus.com", "simplywall.st", "wallstreetzen.com", "theblock.co", "coindesk.com", "cointelegraph.com", "economictimes.indiatimes.com", "caixinglobal.com", "scmp.com", "theglobeandmail.com", "afr.com", "moneycontrol.com", "dgap.de", "borsen.dk", "fd.nl", "tijd.be", "beursduivel.be", "privateequitywire.co.uk", "venturebeat.com", "techcrunch.com", "pymnts.com", "finextra.com", "fintechmagazine.com", "thepharmaletter.com", "platts.com", "mining.com", "shippingwatch.com", "reit.com", "realvision.com", "pionline.com", "goldmansachs.com", "jpmorgan.com", "morganstanley.com", "blackrock.com", "imf.org", "ecb.europa.eu", "federalreserve.gov", "bis.org", "stlouisfed.org", "project-syndicate.org", "voxeu.org", "morningbrew.com", "thehustle.co", "axios.com", "puck.news", "substack.com", "reddit.com/r/investing", "reddit.com/r/stocks", "quiverquant.com", "unusualwhales.com", "openinsider.com", "whalewisdom.com", "glassnode.com", "messari.io", "defillama.com", "moodys.com", "spglobal.com", "fitchratings.com", "msci.com", "vanguard.com", "bridgewater.com", "citadel.com", "a16z.com", "sequoiacap.com", "ycombinator.com", "dealbook.nytimes.com", "qz.com", "fastcompany.com", "inc.com", "entrepreneur.com", "hbr.org", "mckinsey.com", "bcg.com", "bain.com", "deloitte.com", "pwc.com", "ey.com", "kpmg.com", "worldbank.org", "oecd.org", "wto.org", "iif.com", "isda.org", "sifma.org", "finra.org", "sec.gov", "fca.org.uk", "esma.europa.eu", "tmx.com", "lseg.com", "euronext.com", "deutsche-boerse.com", "hkex.com.hk", "sgx.com", "tse.or.jp", "jse.co.za", "chicagobusiness.com", "crainsnewyork.com", "bizjournals.com", "americanbanker.com", "insurancejournal.com", "institutionalinvestor.com", "citywire.com", "wealthmanagement.com", "financial-planning.com", "investmentnews.com", "thinkadvisor.com", "advisorhub.com", "fa-mag.com", "etfstream.com", "mutualfundobserver.com", "hedgeweek.com", "pehub.com", "pitchbook.com", "crunchbase.com", "cbinsights.com", "tracxn.com", "preqin.com", "tradingeconomics.com", "indexmundi.com", "quandl.com", "intrinio.com", "iexcloud.io", "polygon.io", "alpha-vantage.co", "tiingo.com", "yardeni.com", "calculatedriskblog.com", "marginalrevolution.com", "econbrowser.com", "wolfstreet.com", "bondvigilantes.com", "capitalspectator.com", "valueline.com", "investors.com", "marketwatch.com", "schaeffersresearch.com", "cboe.com", "cmegroup.com", "morningstar.co.uk", "advfn.com", "moneyweek.com", "sharesmagazine.co.uk", "investorschronicle.co.uk", "iii.co.uk", "hl.co.uk", "ig.com", "saxobank.com", "etoro.com", "degiro.com", "interactivebrokers.com", "robinhood.com", "schwab.com", "fidelity.com", "vanguard.com", "blackrock.com/us", "pimco.com", "allianz.com", "axa.com", "generali.com", "prudential.com", "metlife.com", "manulife.com", "sunlife.com", "hsbc.com", "bnpparibas.com", "societegenerale.com", "santander.com", "bbva.com", "unicreditgroup.eu", "intesasanpaolo.com", "ubs.com", "deutschebank.com", "commerzbank.com", "ing.com", "rabobank.com", "abnamro.com", "kbc.com", "belfius.be", "societe.com", "boursorama.com", "lesechos.fr", "latribune.fr", "lefigaro.fr/economie", "lemonde.fr/economie", "capital.fr", "zonebourse.com", "ilsole24ore.com", "milanofinanza.it", "corriere.it/economia", "repubblica.it/economia", "expansion.com", "cincodias.elpais.com", "eleconomista.es", "jornaldenegocios.pt", "dinheirovivo.pt", "faz.net", "welt.de/wirtschaft", "wiwo.de", "boerse-online.de", "godmode-trader.de", "finanzen.net", "onvista.de", "wallstreet-online.de", "nzz.ch/wirtschaft", "fuw.ch", "cash.ch", "handelszeitung.ch", "standard.at", "diepresse.com/economie", "di.se", "e24.no", "kauppalehti.fi", "pge.pl", "money.pl", "bankier.pl", "portfolio.hu", "ekonomika.cz", "rbc.ru", "kommersant.ru", "vedomosti.ru", "finanz.ru", "money.163.com", "finance.sina.com.cn", "eastmoney.com", "hexun.com", "asahi.com/business", "mainichi.jp/biz", "yomiuri.co.jp/economy", "chosun.com/business", "donga.com/economy", "hankyung.com", "mk.co.kr", "livemint.com", "business-standard.com", "financialexpress.com", "moneycontrol.com", "dawn.com/business", "aljazeera.com/business", "arabnews.com/business-economy", "gulfnews.com/business", "thenationalnews.com/business", "economist.co.il", "globes.co.il", "themarker.com", "businessday.ng", "vanguardngr.com/business", "standardmedia.co.ke/business", "mg.co.za", "fin24.com", "valor.globo.com", "exame.com", "cronista.com", "lanacion.com.ar/economia", "df.cl", "portafolio.co", "eleconomista.com.mx",
    "stcn.com", "cnstock.com", "jrj.com.cn", "hket.com", "aastocks.com",
    "yicaiglobal.com", "kr-asia.com", "japantimes.co.jp", "nikkan.co.jp",
    "kabutan.jp", "minkabu.jp", "fisco.co.jp", "stockhead.com.au",
    "marketindex.com.au", "interest.co.nz", "theedgesingapore.com",
    "businesstimes.com.sg", "bworldonline.com", "vietnamnews.vn",
    "etfdb.com", "stockcharts.com", "bamsec.com", "insiderviz.com",
    "globenewswire.com", "businesswire.com", "infomoney.com.br",
    "a16z.com", "aastocks.com", "abnamro.com", "advfn.com", "advisorhub.com",
    "afr.com", "aljazeera.com/business", "allianz.com", "alpha-vantage.co",
    "americanbanker.com", "arabnews.com/business-economy", "axa.com",
    "axios.com", "bain.com", "bamsec.com", "bankier.pl", "barchart.com",
    "barrons.com", "bbva.com", "bcg.com", "belfius.be", "benzinga.com",
    "beursduivel.be", "bis.org", "bizjournals.com", "blackrock.com",
    "blackrock.com/us", "bloomberg.com", "bnpparibas.com", "boerse-online.de",
    "bondvigilantes.com", "borsen.dk", "boursorama.com", "bridgewater.com",
    "briefing.com", "business-standard.com", "businessday.ng",
    "businessinsider.com", "businesstimes.com.sg", "bworldonline.com",
    "caixinglobal.com", "calculatedriskblog.com", "capital.fr",
    "capitalspectator.com", "cash.ch", "cbinsights.com", "cboe.com",
    "chicagobusiness.com", "chosun.com/business", "cincodias.elpais.com",
    "citadel.com", "citywire.com", "cmegroup.com", "cnbc.com", "cnstock.com",
    "coindesk.com", "cointelegraph.com", "commerzbank.com",
    "corriere.it/economia", "crainsnewyork.com", "credit-suisse.com",
    "cronista.com", "crunchbase.com", "dawn.com/business", "defillama.com",
    "degiro.com", "deloitte.com", "deutsche-boerse.com", "deutschebank.com",
    "df.cl", "dgap.de", "di.se", "diepresse.com/economie", "dinheirovivo.pt",
    "donga.com/economy", "e24.no", "eastmoney.com", "ecb.europa.eu",
    "econbrowser.com", "economictimes.indiatimes.com", "economist.co.il",
    "economist.com", "ekonomika.cz", "eleconomista.com.mx", "eleconomista.es",
    "entrepreneur.com", "esma.europa.eu", "etf.com", "etfdb.com",
    "etfstream.com", "etftrends.com", "etoro.com", "euronext.com", "exame.com",
    "expansion.com", "ey.com", "fa-mag.com", "fastcompany.com", "faz.net",
    "fca.org.uk", "fd.nl", "federalreserve.gov", "fidelity.com",
    "finance.sina.com.cn", "finance.yahoo.com", "financial-planning.com",
    "financialexpress.com", "finanz.ru", "finanzen.net", "finextra.com",
    "finra.org", "fintechmagazine.com", "finviz.com", "fisco.co.jp",
    "fitchratings.com", "fool.com", "forbes.com", "forexlive.com", "ft.com",
    "fuw.ch", "fxstreet.com", "generali.com", "glassnode.com",
    "globenewswire.com", "globes.co.il", "godmode-trader.de",
    "goldmansachs.com", "gulfnews.com/business", "handelszeitung.ch",
    "hankyung.com", "hbr.org", "hedgeweek.com", "hexun.com", "hket.com",
    "hkex.com.hk", "hl.co.uk", "hsbc.com", "iexcloud.io", "ig.com", "iif.com",
    "iii.co.uk", "ilsole24ore.com", "imf.org", "inc.com", "indexmundi.com",
    "infomoney.com.br", "ing.com", "insiderviz.com", "institutionalinvestor.com",
    "insurancejournal.com", "interactivebrokers.com", "interest.co.nz",
    "intesasanpaolo.com", "intrinio.com", "investing.com", "investmentnews.com",
    "investopedia.com", "investors.com", "investorschronicle.co.uk", "isda.org",
    "japantimes.co.jp", "jornaldenegocios.pt", "jpmorgan.com", "jrj.com.cn",
    "jse.co.za", "kabutan.jp", "kauppalehti.fi", "kbc.com", "kiplinger.com",
    "kommersant.ru", "koyfin.com", "kpmg.com", "kr-asia.com",
    "lanacion.com.ar/economia", "latribune.fr", "lefigaro.fr/economie",
    "lemonde.fr/economie", "lesechos.fr", "livemint.com", "lseg.com",
    "mainichi.jp/biz", "manulife.com", "marginalrevolution.com", "marketaux.com",
    "marketbeat.com", "marketindex.com.au", "marketwatch.com", "mckinsey.com",
    "messari.io", "metlife.com", "mg.co.za", "milanofinanza.it", "mining.com",
    "minkabu.jp", "mk.no.kr", "money.163.com", "money.pl", "moneycontrol.com",
    "moneyweek.com", "moodys.com", "morganstanley.com", "morningbrew.com",
    "morningstar.co.uk", "morningstar.com", "msci.com",
    "mutualfundobserver.com", "nasdaq.com", "newsquawk.com", "nikkan.co.jp",
    "nikkei.com", "oecd.org", "onvista.de", "openinsider.com", "pehub.com",
    "pge.pl", "pimco.com", "pionline.com", "pitchbook.com", "platts.com",
    "polygon.io", "portafolio.co", "portfolio.hu", "preqin.com",
    "privateequitywire.co.uk", "project-syndicate.org", "prudential.com",
    "puck.news", "pwc.com", "pymnts.com", "quandl.com", "quiverquant.com",
    "qz.com", "rabobank.com", "rbc.ru", "realvision.com",
    "reddit.com/r/investing", "reddit.com/r/stocks", "reit.com",
    "repubblica.it/economia", "reuters.com", "robinhood.com", "santander.com",
    "saxobank.com", "schaeffersresearch.com", "schwab.com", "scmp.com",
    "sec.gov", "seekingalpha.com", "sequoiacap.com", "sgx.com",
    "sharesmagazine.co.uk", "shippingwatch.com", "sifma.org", "simplywall.st",
    "societe.com", "societegenerale.com", "spglobal.com", "standard.at",
    "standardmedia.co.ke/business", "stcn.com", "stlouisfed.org",
    "stockcharts.com", "stockhead.com.au", "stocktwits.com", "substack.com",
    "sunlife.com", "techcrunch.com", "theblock.co", "theedgesingapore.com",
    "theglobeandmail.com", "thehustle.co", "themarker.com",
    "thenationalnews.com/business", "thepharmaletter.com", "thestreet.com",
    "thinkadvisor.com", "tiingo.com", "tijd.be", "tipranks.com", "tmx.com",
    "tracxn.com", "tradingeconomics.com", "tradingview.com", "tse.or.jp",
    "ubs.com", "unicreditgroup.eu", "unusualwhales.com", "valor.globo.com",
    "valueline.com", "vanguard.com", "vanguardngr.com/business", "vedomosti.ru",
    "venturebeat.com", "vietnamnews.vn", "voxeu.org", "wallstreet-online.de",
    "wallstreetzen.com", "wealthmanagement.com", "welt.de/wirtschaft",
    "whalewisdom.com", "wiwo.de", "wolfstreet.com", "worldbank.org", "wsj.com",
    "wto.org", "yardeni.com", "ycharts.com", "ycombinator.com",
    "yicaiglobal.com", "yomiuri.co.jp/economy", "zerohedge.com", "zonebourse.com"
]
raw_data = list(set(raw_data))

# SETTINGS
NOW = datetime.now(timezone.utc)
CACHE_SITEMAPS = "sitemaps_list.txt"
CACHE_METADATA = "cache_metadata.json"
CACHE_DIR = "cache_parquet"
PARQUET_CACHE_MAX_AGE_HOURS = 1  # Skip request if parquet cache is newer than 1 hour
NEWS_MAX_AGE_HOURS = 24  # Only include news articles from last 24 hours
SITEMAP_CACHE_MAX_AGE_DAYS = 1  # Sitemap metadata cache max age in days
NEWS_ONLY = True  # Only process news sitemaps (skip regular sitemaps)

# Create cache directory
os.makedirs(CACHE_DIR, exist_ok=True)

# Optimized Nuclear Session
session = requests.Session()
adapter = HTTPAdapter(pool_connections=200, pool_maxsize=200, max_retries=Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=['GET', 'HEAD']))
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
        'articles', 'press', 'presse', 'aktuelles', 'noticias'
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

def fetch_sitemap_content(url, cutoff, metadata_cache):
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
            if not dt or (cutoff <= dt <= (NOW + timedelta(minutes=10))):
                title_m = re.search(b'<news:title>(.*?)</news:title>', data, re.I | re.S)
                if not title_m: title_m = re.search(b'<title>(.*?)</title>', data, re.I | re.S)
                if title_m:
                    loc_m = RE_LOC.search(data)
                    if loc_m:
                        loc = loc_m.group(1).decode('utf-8', 'ignore').strip()
                        # Resolve relative URLs
                        if not loc.startswith(('http://', 'https://')):
                            loc = urljoin(base_url, loc)
                        meta = {"loc": loc, "lastmod": lm_str, "title": clean_xml_text(title_m.group(1))}
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
    print(f"NUCLEAR RECURSIVE Scan | Start: {NOW.strftime('%H:%M:%S')} UTC | Workers: 200")
    print(f"Looking for articles from the last {NEWS_MAX_AGE_HOURS} hours")
    print(f"Parquet cache enabled: {HAS_PARQUET}, max age: {PARQUET_CACHE_MAX_AGE_HOURS}h")
    print(f"NEWS_ONLY mode: {NEWS_ONLY}")

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
                                if loc not in final_items or it["lastmod"] > final_items[loc].get("lastmod", ""):
                                    final_items[loc] = it
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

    # Save state
    with open(CACHE_SITEMAPS, "w") as f:
        for s in sorted(all_valid_sitemaps): f.write(s + "\n")
    with open(CACHE_METADATA, 'w') as f: json.dump(new_metadata, f, indent=2)

    # Write output with stock_tickers
    with open("combined_sitemap.xml", "w") as f:
        f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        f.write('<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9" xmlns:news="http://www.google.com/schemas/sitemap-news/0.9">\n')
        for item in sorted(final_items.values(), key=lambda x: x.get("lastmod", ""), reverse=True):
            f.write('  <url>\n')
            f.write(f'    <loc>{item["loc"]}</loc>\n')
            if item.get("lastmod"): f.write(f'    <lastmod>{item["lastmod"]}</lastmod>\n')
            f.write('    <news:news>\n')
            if "source" in item: f.write(f'      <news:publication><news:name>{item["source"]}</news:name><news:language>en</news:language></news:publication>\n')
            if item.get("lastmod"):
                f.write(f'      <news:publication_date>{item["lastmod"]}</news:publication_date>\n')
            f.write(f'      <news:title>{item["title"]}</news:title>\n')
            if "keywords" in item: f.write(f'      <news:keywords>{item["keywords"]}</news:keywords>\n')
            if "stock_tickers" in item: f.write(f'      <news:stock_tickers>{item["stock_tickers"]}</news:stock_tickers>\n')
            f.write('    </news:news>\n')
            f.write('  </url>\n')
        f.write('</urlset>\n')

    print(f"DONE! Found {len(final_items)} items with headlines.")

if __name__ == "__main__":
    main()
