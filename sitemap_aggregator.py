import requests
import re
import os
import gzip
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
import urllib.parse
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# De lijst van domeinen
raw_data = [
    "bloomberg.com", "reuters.com", "wsj.com", "cnbc.com", "ft.com", "finance.yahoo.com", "investing.com", "marketwatch.com", "barrons.com", "economist.com", "forbes.com", "businessinsider.com", "fortune.com", "nasdaq.com", "investopedia.com", "thestreet.com", "morningstar.com", "seekingalpha.com", "benzinga.com", "zerohedge.com", "nikkei.com", "lesechos.fr", "tradingview.com", "koyfin.com", "ycharts.com", "finviz.com", "tipranks.com", "fool.com", "kiplinger.com", "marketbeat.com", "briefing.com", "newsquawk.com", "fxstreet.com", "forexlive.com", "marketaux.com", "stocktwits.com", "etf.com", "etftrends.com", "barchart.com", "gurufocus.com", "simplywall.st", "wallstreetzen.com", "theblock.co", "coindesk.com", "cointelegraph.com", "economictimes.indiatimes.com", "caixinglobal.com", "scmp.com", "theglobeandmail.com", "afr.com", "moneycontrol.com", "dgap.de", "borsen.dk", "fd.nl", "tijd.be", "beursduivel.be", "privateequitywire.co.uk", "venturebeat.com", "techcrunch.com", "pymnts.com", "finextra.com", "fintechmagazine.com", "thepharmaletter.com", "platts.com", "mining.com", "shippingwatch.com", "reit.com", "realvision.com", "pionline.com", "goldmansachs.com", "jpmorgan.com", "morganstanley.com", "blackrock.com", "imf.org", "ecb.europa.eu", "federalreserve.gov", "bis.org", "stlouisfed.org", "project-syndicate.org", "voxeu.org", "morningbrew.com", "thehustle.co", "axios.com", "puck.news", "substack.com", "reddit.com/r/investing", "reddit.com/r/stocks", "quiverquant.com", "unusualwhales.com", "openinsider.com", "whalewisdom.com", "glassnode.com", "messari.io", "defillama.com", "moodys.com", "spglobal.com", "fitchratings.com", "msci.com", "vanguard.com", "bridgewater.com", "citadel.com", "a16z.com", "sequoiacap.com", "ycombinator.com", "dealbook.nytimes.com", "qz.com", "fastcompany.com", "inc.com", "entrepreneur.com", "hbr.org", "mckinsey.com", "bcg.com", "bain.com", "deloitte.com", "pwc.com", "ey.com", "kpmg.com", "worldbank.org", "oecd.org", "wto.org", "iif.com", "isda.org", "sifma.org", "finra.org", "sec.gov", "fca.org.uk", "esma.europa.eu", "tmx.com", "lseg.com", "euronext.com", "deutsche-boerse.com", "hkex.com.hk", "sgx.com", "tse.or.jp", "jse.co.za", "chicagobusiness.com", "crainsnewyork.com", "bizjournals.com", "americanbanker.com", "insurancejournal.com", "institutionalinvestor.com", "citywire.com", "wealthmanagement.com", "financial-planning.com", "investmentnews.com", "thinkadvisor.com", "advisorhub.com", "fa-mag.com", "etfstream.com", "mutualfundobserver.com", "hedgeweek.com", "pehub.com", "pitchbook.com", "crunchbase.com", "cbinsights.com", "tracxn.com", "preqin.com", "tradingeconomics.com", "indexmundi.com", "quandl.com", "intrinio.com", "iexcloud.io", "polygon.io", "alpha-vantage.co", "tiingo.com", "yardeni.com", "calculatedriskblog.com", "marginalrevolution.com", "econbrowser.com", "wolfstreet.com", "bondvigilantes.com", "capitalspectator.com", "valueline.com", "investors.com", "marketwatch.com", "schaeffersresearch.com", "cboe.com", "cmegroup.com", "morningstar.co.uk", "advfn.com", "moneyweek.com", "sharesmagazine.co.uk", "investorschronicle.co.uk", "iii.co.uk", "hl.co.uk", "ig.com", "saxobank.com", "etoro.com", "degiro.com", "interactivebrokers.com", "robinhood.com", "schwab.com", "fidelity.com", "vanguard.com", "blackrock.com/us", "pimco.com", "allianz.com", "axa.com", "generali.com", "prudential.com", "metlife.com", "manulife.com", "sunlife.com", "hsbc.com", "bnpparibas.com", "societegenerale.com", "santander.com", "bbva.com", "unicreditgroup.eu", "intesasanpaolo.com", "ubs.com", "deutschebank.com", "commerzbank.com", "ing.com", "rabobank.com", "abnamro.com", "kbc.com", "belfius.be", "societe.com", "boursorama.com", "lesechos.fr", "latribune.fr", "lefigaro.fr/economie", "lemonde.fr/economie", "capital.fr", "zonebourse.com", "ilsole24ore.com", "milanofinanza.it", "corriere.it/economia", "repubblica.it/economia", "expansion.com", "cincodias.elpais.com", "eleconomista.es", "jornaldenegocios.pt", "dinheirovivo.pt", "faz.net", "welt.de/wirtschaft", "wiwo.de", "boerse-online.de", "godmode-trader.de", "finanzen.net", "onvista.de", "wallstreet-online.de", "nzz.ch/wirtschaft", "fuw.ch", "cash.ch", "handelszeitung.ch", "standard.at", "diepresse.com/economie", "di.se", "e24.no", "kauppalehti.fi", "pge.pl", "money.pl", "bankier.pl", "portfolio.hu", "ekonomika.cz", "rbc.ru", "kommersant.ru", "vedomosti.ru", "finanz.ru", "money.163.com", "finance.sina.com.cn", "eastmoney.com", "hexun.com", "asahi.com/business", "mainichi.jp/biz", "yomiuri.co.jp/economy", "chosun.com/business", "donga.com/economy", "hankyung.com", "mk.co.kr", "livemint.com", "business-standard.com", "financialexpress.com", "moneycontrol.com", "dawn.com/business", "aljazeera.com/business", "arabnews.com/business-economy", "gulfnews.com/business", "thenationalnews.com/business", "economist.co.il", "globes.co.il", "themarker.com", "businessday.ng", "vanguardngr.com/business", "standardmedia.co.ke/business", "mg.co.za", "fin24.com", "valor.globo.com", "exame.com", "cronista.com", "lanacion.com.ar/economia", "df.cl", "portafolio.co", "eleconomista.com.mx",
"stcn.com", "cnstock.com", "jrj.com.cn", "hket.com", "aastocks.com", 
    "yicaiglobal.com", "kr-asia.com", "japantimes.co.jp", "nikkan.co.jp", 
    "kabutan.jp", "minkabu.jp", "fisco.co.jp", "stockhead.com.au", 
    "marketindex.com.au", "interest.co.nz", "theedgesingapore.com", 
    "businesstimes.com.sg", "bworldonline.com", "vietnamnews.vn", 
    "handelsblatt.com", "dpa-afx.de", "finanznachrichten.de", 
    "nebenwerte-magazin.com", "hegnar.no", "affarsvarlden.se", 
    "cityam.com", "moneyweb.co.za", "nairametrics.com", "zawya.com", 
    "mubasher.info", "oilprice.com", "kitco.com", "biopharmcatalyst.com",
    "stcn.com", "cnstock.com", "jrj.com.cn", "hket.com", "aastocks.com",
    "yicaiglobal.com", "kr-asia.com", "japantimes.co.jp", "nikkan.co.jp",
    "kabutan.jp", "minkabu.jp", "fisco.co.jp", "stockhead.com.au",
    "marketindex.com.au", "interest.co.nz", "theedgesingapore.com",
    "businesstimes.com.sg", "bworldonline.com", "vietnamnews.vn",
    "handelsblatt.com", "dpa-afx.de", "finanznachrichten.de",
    "nebenwerte-magazin.com", "hegnar.no", "affarsvarlden.se",
    "cityam.com", "moneyweb.co.za", "nairametrics.com", "zawya.com",
    "mubasher.info", "oilprice.com", "kitco.com", "biopharmcatalyst.com",
    "etfdb.com", "stockcharts.com", "bamsec.com", "insiderviz.com",
    "globenewswire.com", "businesswire.com", "infomoney.com.br"
    

]
# unique 
raw_data = list(set(raw_data))
# NUCLEAR SETTINGS
NOW = datetime.now(timezone.utc)
FINANCIAL_KEYWORDS = ['finance', 'market', 'stock', 'invest', 'crypto', 'economy', 'business', 'bank', 'trade', 'beurs', 'aandeel', 'rente', 'dividend', 'earnings', 'ipo']
NEWS_INDICATORS = ['news', 'nieuws', 'breaking', 'recent', 'latest', 'today', 'daily', 'article', NOW.strftime("%Y-%m"), (NOW - timedelta(days=28)).strftime("%Y-%m")]

CACHE_SITEMAPS = "sitemaps_list.txt"
CACHE_METADATA = "cache_metadata.json"

# Optimized Nuclear Session
session = requests.Session()
adapter = HTTPAdapter(pool_connections=500, pool_maxsize=500, max_retries=Retry(total=0))
session.mount("http://", adapter)
session.mount("https://", adapter)
session.verify = False
requests.packages.urllib3.disable_warnings()
headers_base = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}

def is_fresh_and_financial(url):
    url_l = url.lower()
    if any(yr in url_l for yr in ['201', '2020', '2021', '2022', '2023', '2024']):
        if not any(m in url_l for m in NEWS_INDICATORS[-2:]): return False
    return any(ind in url_l for ind in NEWS_INDICATORS + FINANCIAL_KEYWORDS) or ('sitemap' in url_l and 'news' in url_l)

def clean_xml_text(b_text):
    if not b_text: return ""
    return b_text.decode('utf-8', 'ignore').strip().replace('<![CDATA[', '').replace(']]>', '')

def fetch_items(url, cutoff, metadata_cache):
    results = []
    headers = headers_base.copy()
    if url in metadata_cache: headers['If-Modified-Since'] = metadata_cache[url]
    try:
        resp = session.get(url, timeout=5, headers=headers)
        if resp.status_code == 304: return ("cached", None)
        if resp.status_code != 200: return ("error", None)
        new_last_mod = resp.headers.get('Last-Modified')
        content = resp.content
        if url.endswith('.gz') or content.startswith(b'\x1f\x8b'): content = gzip.decompress(content)
        
        if b'<sitemapindex' in content[:500].lower():
            sub_sitemaps = []
            for loc_b in re.findall(b'<loc>(.*?)</loc>', content, re.I | re.S):
                loc = loc_b.decode('utf-8', 'ignore').strip()
                if is_fresh_and_financial(loc): sub_sitemaps.append(loc)
            return ("index", (sub_sitemaps, new_last_mod))

        for block in re.findall(b'<url>(.*?)</url>', content, re.I | re.S):
            lastmod_match = re.search(b'<lastmod>(.*?)</lastmod>', block, re.I)
            lm_str = lastmod_match.group(1).decode('utf-8', 'ignore').strip() if lastmod_match else ""
            
            try:
                dt = datetime.fromisoformat(lm_str.replace('Z', '+00:00')) if lm_str else None
                if not dt or (cutoff <= dt <= (NOW + timedelta(minutes=10))):
                    loc_match = re.search(b'<loc>(.*?)</loc>', block, re.I)
                    if loc_match:
                        loc = loc_match.group(1).decode('utf-8', 'ignore').strip()
                        if any(kw in loc.lower() for kw in FINANCIAL_KEYWORDS + ['news', 'nieuws', 'article']):
                            # EXTRACT METADATA
                            meta = {"loc": loc, "lastmod": lm_str}
                            
                            # Google News specific
                            title_m = re.search(b'<news:title>(.*?)</news:title>', block, re.I | re.S)
                            if title_m: meta["title"] = clean_xml_text(title_m.group(1))
                            
                            keywords_m = re.search(b'<news:keywords>(.*?)</news:keywords>', block, re.I | re.S)
                            if keywords_m: meta["keywords"] = clean_xml_text(keywords_m.group(1))
                            
                            pub_name_m = re.search(b'<news:name>(.*?)</news:name>', block, re.I | re.S)
                            if pub_name_m: meta["source"] = clean_xml_text(pub_name_m.group(1))
                            
                            # Generic/Image/Description (often in <description> or <caption>)
                            desc_m = re.search(b'<description>(.*?)</description>', block, re.I | re.S)
                            if desc_m: meta["description"] = clean_xml_text(desc_m.group(1))
                            
                            results.append(meta)
            except: pass
        return ("items", (results, new_last_mod))
    except: return ("error", None)

def main():
    cutoff = NOW - timedelta(hours=24)
    print(f"NUCLEAR Context Scan | Start: {NOW.strftime('%H:%M:%S')} UTC")
    
    metadata_cache = {}
    if os.path.exists(CACHE_METADATA):
        with open(CACHE_METADATA, 'r') as f: metadata_cache = json.load(f)
    
    discovered_sitemaps = set()
    if os.path.exists(CACHE_SITEMAPS):
        with open(CACHE_SITEMAPS, "r") as f:
            for line in f: discovered_sitemaps.add(line.strip())
    
    if not discovered_sitemaps:
        print("Rapid discovery phase...")
        def get_robots(d):
            try:
                r = session.get(f"https://{d}/robots.txt", timeout=2, headers=headers_base)
                return [s.strip() for s in re.findall(r'^Sitemap:\s*(.*)', r.text, re.I | re.M) if is_fresh_and_financial(s.strip())]
            except: return []
        with ThreadPoolExecutor(max_workers=200) as ex:
            for res in ex.map(get_robots, raw_data): discovered_sitemaps.update(res)

    final_items = {}
    to_process = list(discovered_sitemaps)
    new_metadata = metadata_cache.copy()
    valid_sitemaps = set()

    for depth in range(2):
        if not to_process: break
        next_batch = []
        with ThreadPoolExecutor(max_workers=500) as executor:
            futures = {executor.submit(fetch_items, url, cutoff, metadata_cache): url for url in to_process}
            for f in as_completed(futures):
                url = futures[f]
                try:
                    res_type, res_data = f.result()
                    if res_type == "cached":
                        valid_sitemaps.add(url)
                    elif res_type == "index":
                        subs, lmod = res_data
                        if lmod: new_metadata[url] = lmod
                        next_batch.extend(subs)
                        valid_sitemaps.add(url)
                    elif res_type == "items":
                        items, lmod = res_data
                        if lmod: new_metadata[url] = lmod
                        if items:
                            valid_sitemaps.add(url)
                            for item in items:
                                loc = item["loc"]
                                if loc not in final_items or item["lastmod"] > final_items[loc].get("lastmod", ""):
                                    final_items[loc] = item
                except: pass
        to_process = [u for u in next_batch if u not in discovered_sitemaps]

    # Save state
    with open(CACHE_SITEMAPS, "w") as f:
        for s in sorted(valid_sitemaps): f.write(s + "\n")
    with open(CACHE_METADATA, 'w') as f: json.dump(new_metadata, f, indent=2)

    if final_items:
        sorted_items = sorted(final_items.values(), key=lambda x: x.get("lastmod", ""), reverse=True)
        with open("combined_sitemap.xml", "w") as f:
            f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
            f.write('<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9" xmlns:news="http://www.google.com/schemas/sitemap-news/0.9">\n')
            for item in sorted_items:
                f.write('  <url>\n')
                f.write(f'    <loc>{item["loc"]}</loc>\n')
                if item.get("lastmod"): f.write(f'    <lastmod>{item["lastmod"]}</lastmod>\n')
                if "title" in item or "source" in item:
                    f.write('    <news:news>\n')
                    if "source" in item:
                        f.write(f'      <news:publication><news:name>{item["source"]}</news:name><news:language>en</news:language></news:publication>\n')
                    if item.get("lastmod"):
                        f.write(f'      <news:publication_date>{item["lastmod"]}</news:publication_date>\n')
                    if "title" in item:
                        f.write(f'      <news:title>{item["title"]}</news:title>\n')
                    if "keywords" in item:
                        f.write(f'      <news:keywords>{item["keywords"]}</news:keywords>\n')
                    f.write('    </news:news>\n')
                if "description" in item:
                    f.write(f'    <description>{item["description"]}</description>\n')
                f.write('  </url>\n')
            f.write('</urlset>\n')
        print(f"SUCCESS! Found {len(final_items)} items with metadata.")
    else:
        print("Everything up to date.")

if __name__ == "__main__":
    main()
