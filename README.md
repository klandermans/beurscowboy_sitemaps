# News Sitemap Scraper & Historical Database

Een krachtige toolset om nieuwsartikelen te verzamelen uit sitemaps van honderden domeinen, verrijkt met paginametadata (titel, beschrijving, afbeelding) en opgebouwd in een historisch archief.

## Belangrijkste Functies

- **Gedeelde Domeinenlijst:** Zowel de crawler als de archief-builder werken met `domains.json`.
- **Recursieve Sitemaps:** Verwerkt sitemaps tot 5 niveaus diep, inclusief speciale `archive` en `archief` patronen.
- **Slimme Scraper:** Scrapt automatisch de HTML van de pagina als de titel, beschrijving of afbeelding ontbreekt in de sitemap.
- **Laatste 24 Uur Database:** Direct overzicht van de nieuwste artikelen in `latest_24h.parquet` en `latest_24h.json`.
- **Incrementeel Historisch Archief:** `build_historical.py` voegt nieuwe data toe aan het bestaande archief zonder dubbel werk te doen of reeds gescrapte metadata opnieuw op te halen.
- **Parquet Caching:** Maakt gebruik van Parquet-bestanden voor snelle opslag en lage verwerkingstijd.

## Workflow

### 1. Domeinen Beheren
Pas `domains.json` aan om domeinen toe te voegen of te verwijderen. Beiden scripts gebruiken dit bestand.

### 2. Sitemaps Crawlen
Haal de laatste artikelen op (standaard laatste 24 uur):
```bash
python3 sitemap_aggregator.py
```
Voor een volledige scan van alle sitemaps (inclusief oude archieven):
```bash
python3 sitemap_aggregator.py --historical
```
*Opties:*
- `--refresh`: Negeer cache en haal alles opnieuw op.
- `--extend-hours N`: Haal artikelen op van de laatste N uur.

### 3. Historisch Archief Bouwen
Nadat de crawler klaar is, voeg je de nieuwe data toe aan het historische archief:
```bash
python3 build_historical.py
```
Dit script laadt `historical_archive.parquet`, voegt nieuwe unieke pagina's uit de `cache_parquet/` map toe en slaat het resultaat weer op.

## Bestandsstructuur

- `sitemap_aggregator.py`: De crawler die sitemaps afgaat en metadata verzamelt.
- `build_historical.py`: De script om het incrementele historische archief op te bouwen.
- `domains.json`: De centrale lijst met te scrapen domeinen.
- `cache_parquet/`: Lokale cache van individuele sitemaps.
- `latest_24h.json / .parquet`: De database van de laatste 24 uur.
- `historical_archive.json / .parquet`: De volledige historische database.
- `combined_sitemap.xml`: Geaggregeerde sitemap in Google News formaat.

## Installatie
Zorg dat de benodigde Python-packages zijn geïnstalleerd:
```bash
pip install -r requirements.txt
```
*Vereist o.a. `pandas`, `pyarrow` en `requests`.*
