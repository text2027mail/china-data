#!/usr/bin/env python3
"""
China Box Office Tracker

Fetches daily box office data from english.entgroup.cn, parses the HTML table,
and stores the data in a local JSON database. Supports incremental updates,
manual corrections (slug/title/merge), and generates daywise and yearly indices.

Source URL: http://english.entgroup.cn/boxoffice/cn/daily/?date=MM/DD/YYYY
Data fields used: Rank, Title (with ID), Gross(M), Cume Gross(M), Showings, Admissions, Days
"""

import asyncio
import aiohttp
import aiofiles
import json
import re
import hashlib
from datetime import datetime, timedelta
from pathlib import Path
from collections import defaultdict
from bs4 import BeautifulSoup
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ---------- Configuration ----------
BASE_URL = "http://english.entgroup.cn/boxoffice/cn/daily/?date={}"
START_DATE = datetime(2013, 1, 1)          # first available date

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
DATABASE_DIR = BASE_DIR / "database"
DAYWISE_DIR = DATA_DIR / "daywise"
YEARLY_DIR = DATA_DIR                      # year folders inside DATA_DIR
STATE_FILE = BASE_DIR / "state.json"
MOVIE_SLUG_MAP_FILE = DATA_DIR / "movieslug.json"
CORRECTIONS_FILE = BASE_DIR / "correctedslug.json"

SEMAPHORE = asyncio.Semaphore(200)          # concurrency limit

# ---------- Utilities ----------
def format_date_ymd(dt):
    """YYYY-MM-DD (for URL)"""
    return dt.strftime("%Y-%m-%d")

def format_date_dmy(dt):
    """DD-MM-YYYY (for filenames and display)"""
    return dt.strftime("%d-%m-%Y")

def format_date_mdy(dt):
    """MM/DD/YYYY (for the URL parameter)"""
    return dt.strftime("%m/%d/%Y")

def parse_date_dmy(date_str):
    return datetime.strptime(date_str, "%d-%m-%Y")

def ordinal(n):
    if 10 <= n % 100 <= 20:
        return "th"
    return {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")

def week_day_string(release_date, current_date):
    if release_date is None:
        return "Unknown"
    diff = (current_date - release_date).days
    week_num = diff // 7 + 1
    weekday = current_date.strftime("%a")
    return f"{week_num}{ordinal(week_num)} {weekday}"

def generate_slug(title):
    """Generate a URL-friendly slug from a title."""
    slug = re.sub(r'[^a-z0-9]+', '-', title.lower().strip())
    slug = slug.strip('-')
    return slug[:50] if slug else "movie"

def parse_money(value):
    """Parse a string like '$8.68' or '$123.45' into a float (in millions)."""
    if not value:
        return 0.0
    cleaned = re.sub(r'[^\d.]', '', value.strip())
    try:
        return float(cleaned)
    except ValueError:
        return 0.0

def parse_int(value):
    """Parse an integer string (with commas possible)."""
    if not value:
        return 0
    cleaned = re.sub(r'[^\d]', '', value.strip())
    try:
        return int(cleaned)
    except ValueError:
        return 0

# ---------- Movie Slug Mapper ----------
class MovieSlugMapper:
    """
    Maps movie ID (int) → {slug, title, manual_title, manual_slug, redirect}
    Redirects are used for merging movies.
    """
    def __init__(self):
        self.map = {}
        self.dirty = False
        self.lock = asyncio.Lock()

    async def load(self):
        if not MOVIE_SLUG_MAP_FILE.exists():
            return
        try:
            async with aiofiles.open(MOVIE_SLUG_MAP_FILE, 'r', encoding='utf-8') as f:
                data = json.loads(await f.read())
            self.map = {int(k): v for k, v in data.items()}  # IDs are integers
        except Exception as e:
            logger.warning(f"Failed to load movie slug map: {e}")

    async def save(self):
        if not self.dirty:
            return
        async with self.lock:
            try:
                MOVIE_SLUG_MAP_FILE.parent.mkdir(parents=True, exist_ok=True)
                # Convert int keys to str for JSON
                data = {str(k): v for k, v in self.map.items()}
                async with aiofiles.open(MOVIE_SLUG_MAP_FILE, 'w', encoding='utf-8') as f:
                    await f.write(json.dumps(data, indent=2, ensure_ascii=False))
                self.dirty = False
            except Exception as e:
                logger.error(f"Failed to write movie slug map: {e}")

    def resolve(self, movie_id):
        """Follow redirect chain to get the primary ID."""
        current = movie_id
        seen = set()
        while current in self.map and "redirect" in self.map[current]:
            if current in seen:
                break
            seen.add(current)
            current = self.map[current]["redirect"]
        return current

    def get_slug(self, movie_id):
        primary = self.resolve(movie_id)
        entry = self.map.get(primary)
        return entry["slug"] if entry else None

    def get_title(self, movie_id):
        primary = self.resolve(movie_id)
        entry = self.map.get(primary)
        return entry["title"] if entry else None

    def ensure_movie(self, movie_id, title):
        """
        Ensure the movie exists in the map.
        Returns (slug, is_new).
        If the movie exists, title is updated only if not manually overridden and currently empty.
        """
        primary = self.resolve(movie_id)
        if primary in self.map:
            entry = self.map[primary]
            slug = entry["slug"]
            stored_title = entry.get("title", "")
            if title and not stored_title and not entry.get("manual_title", False):
                entry["title"] = title
                self.dirty = True
            return slug, False
        else:
            # New movie: generate slug from title
            base_slug = generate_slug(title)
            existing_slugs = {v["slug"] for v in self.map.values() if "slug" in v}
            slug = base_slug
            if slug in existing_slugs:
                h = hashlib.md5(str(primary).encode('utf-8')).hexdigest()[:6]
                suffix = f"-{h}"
                max_base_len = 50 - len(suffix)
                slug = base_slug[:max_base_len] + suffix
                if slug in existing_slugs:
                    for i in range(1, 100):
                        alt = f"{base_slug[:48]}-{i}"[:50]
                        if alt not in existing_slugs:
                            slug = alt
                            break
            self.map[primary] = {
                "slug": slug,
                "title": title,
                "manual_title": False,
                "manual_slug": False
            }
            self.dirty = True
            return slug, True

# ---------- Movie Store ----------
class MovieStore:
    """In-memory store for per-movie data, backed by JSON files in database/."""
    def __init__(self, mapper):
        self.mapper = mapper
        self.movies = {}          # slug -> movie dict
        self.dirty = set()
        self.lock = asyncio.Lock()

    async def load_all(self):
        """Load all movie JSON files from database/ and consolidate by primary ID."""
        DATABASE_DIR.mkdir(parents=True, exist_ok=True)
        groups = defaultdict(list)   # primary_id -> list of (slug, data)

        for filepath in DATABASE_DIR.glob("*.json"):
            try:
                async with aiofiles.open(filepath, 'r', encoding='utf-8') as f:
                    data = json.loads(await f.read())
                slug = filepath.stem
                movie_id = data.get("movie_id")
                if not movie_id:
                    continue
                primary = self.mapper.resolve(movie_id)
                groups[primary].append((slug, data))
            except Exception as e:
                logger.warning(f"Failed to load {filepath}: {e}")

        for primary_id, items in groups.items():
            canonical_slug = self.mapper.get_slug(primary_id)
            if not canonical_slug:
                first_title = items[0][1].get("title", "Unknown")
                canonical_slug, _ = self.mapper.ensure_movie(primary_id, first_title)
                await self.mapper.save()

            # Merge entries (deduplicate by date)
            merged_entries = []
            seen = set()
            for _, data in items:
                for entry in data.get("entries", []):
                    key = entry["date"]
                    if key not in seen:
                        merged_entries.append(entry)
                        seen.add(key)
            merged_entries.sort(key=lambda e: parse_date_dmy(e["date"]))

            # Compute release date from earliest entry's "days" or earliest date
            release_date = None
            if merged_entries:
                # Try to derive from days if available; otherwise use earliest entry date
                for e in merged_entries:
                    if "days" in e and e["days"] is not None and e["days"] > 0:
                        try:
                            entry_date = parse_date_dmy(e["date"])
                            if e["days"] < 10000:  # sanity check
                                release_date = entry_date - timedelta(days=e["days"])
                                break
                        except:
                            pass
                if not release_date:
                    # fallback: earliest date in entries
                    earliest = min(parse_date_dmy(e["date"]) for e in merged_entries)
                    release_date = earliest

            movie_data = {
                "movie_id": primary_id,
                "title": self.mapper.get_title(primary_id) or items[0][1].get("title", "Unknown"),
                "releaseDate": format_date_dmy(release_date) if release_date else "Unknown",
                "entries": merged_entries
            }

            filepath = DATABASE_DIR / f"{canonical_slug}.json"
            try:
                async with aiofiles.open(filepath, 'w', encoding='utf-8') as f:
                    await f.write(json.dumps(movie_data, indent=2, ensure_ascii=False))
                logger.info(f"Consolidated ID {primary_id} -> {canonical_slug}.json ({len(merged_entries)} entries)")
            except Exception as e:
                logger.error(f"Failed to write consolidated file {filepath}: {e}")
                continue

            # Remove old duplicate files
            for slug, _ in items:
                if slug != canonical_slug:
                    old_file = DATABASE_DIR / f"{slug}.json"
                    try:
                        old_file.unlink()
                        logger.info(f"Removed duplicate {old_file.name}")
                    except Exception as e:
                        logger.warning(f"Could not delete {old_file}: {e}")

            self.movies[canonical_slug] = movie_data

        # Load any remaining orphan files (not in groups)
        for filepath in DATABASE_DIR.glob("*.json"):
            slug = filepath.stem
            if slug not in self.movies:
                try:
                    async with aiofiles.open(filepath, 'r', encoding='utf-8') as f:
                        data = json.loads(await f.read())
                    self.movies[slug] = data
                    movie_id = data.get("movie_id")
                    if movie_id and not self.mapper.get_slug(movie_id):
                        self.mapper.map[movie_id] = {
                            "slug": slug,
                            "title": data.get("title", "Unknown"),
                            "manual_title": False,
                            "manual_slug": False
                        }
                        self.mapper.dirty = True
                except Exception as e:
                    logger.warning(f"Failed to load {filepath}: {e}")

        await self.mapper.save()

    async def get_or_create(self, movie_id, title, date_obj):
        primary_id = self.mapper.resolve(movie_id)
        slug, is_new = self.mapper.ensure_movie(primary_id, title)
        if is_new:
            movie_data = {
                "movie_id": primary_id,
                "title": title,
                "releaseDate": format_date_dmy(date_obj),
                "entries": []
            }
            self.movies[slug] = movie_data
            self.dirty.add(slug)
        else:
            movie_data = self.movies.get(slug)
            if not movie_data:
                movie_data = {
                    "movie_id": primary_id,
                    "title": title,
                    "releaseDate": format_date_dmy(date_obj),
                    "entries": []
                }
                self.movies[slug] = movie_data
                self.dirty.add(slug)
            else:
                # Update release date if earlier
                existing_release = parse_date_dmy(movie_data["releaseDate"]) if movie_data["releaseDate"] != "Unknown" else None
                if existing_release and date_obj < existing_release:
                    movie_data["releaseDate"] = format_date_dmy(date_obj)
                    self.dirty.add(slug)
        return slug, self.movies[slug]

    async def add_entry(self, slug, entry):
        async with self.lock:
            movie = self.movies[slug]
            # Replace if same date exists (should not happen, but guard)
            for i, e in enumerate(movie["entries"]):
                if e["date"] == entry["date"]:
                    movie["entries"][i] = entry
                    break
            else:
                movie["entries"].append(entry)
            self.dirty.add(slug)

    async def flush(self):
        async with self.lock:
            for slug in list(self.dirty):
                filepath = DATABASE_DIR / f"{slug}.json"
                try:
                    async with aiofiles.open(filepath, 'w', encoding='utf-8') as f:
                        await f.write(json.dumps(self.movies[slug], indent=2, ensure_ascii=False))
                except Exception as e:
                    logger.error(f"Failed to write {filepath}: {e}")
                else:
                    self.dirty.remove(slug)
        await self.mapper.save()

    def get_release_date(self, slug):
        movie = self.movies.get(slug)
        if movie and movie["releaseDate"] != "Unknown":
            return parse_date_dmy(movie["releaseDate"])
        return None

    def get_all_movies(self):
        return list(self.movies.values())

# ---------- Global instances ----------
movie_slug_mapper = MovieSlugMapper()
movie_store = MovieStore(movie_slug_mapper)

# ---------- Fetch & Parse ----------
async def fetch_date(session, date_str_mdy):
    """Fetch HTML for a given date (MM/DD/YYYY)."""
    url = BASE_URL.format(date_str_mdy)
    try:
        async with SEMAPHORE:
            async with session.get(url, timeout=30) as resp:
                if resp.status != 200:
                    logger.warning(f"Date {date_str_mdy} returned {resp.status}")
                    return None
                return await resp.text()
    except Exception as e:
        logger.error(f"Error fetching {date_str_mdy}: {e}")
        return None

def parse_html(html, date_obj):
    soup = BeautifulSoup(html, 'html.parser')
    table = soup.find('table', class_='person')
    if not table:
        logger.warning("No table with class 'person' found.")
        return []

    rows = table.find_all('tr')
    entries = []
    for row in rows:
        # Skip header and separator rows
        if row.find('td', colspan='9') or row.find('b', string='Rank'):
            continue
        # Get ONLY the direct td children (no nested table cells)
        cells = row.find_all('td', recursive=False)
        if len(cells) < 9:
            continue

        # Rank
        rank_str = cells[0].get_text(strip=True)
        rank = parse_int(rank_str)

        # Title column (index 1) – contains the link
        title_cell = cells[1]
        link = title_cell.find('a')
        if not link:
            continue
        href = link.get('href', '')
        movie_id = None
        m = re.search(r'/(\d+)/', href)
        if m:
            movie_id = int(m.group(1))
        title = link.get_text(strip=True)

        # Gross(M) – index 2
        gross_str = cells[2].get_text(strip=True)
        gross = parse_money(gross_str)

        # Cume Gross(M) – index 3
        cume_str = cells[3].get_text(strip=True)
        cume = parse_money(cume_str)

        # Per Screen Avg. (index 4) – skip
        # Per Ticket Avg. (index 5) – skip

        # Showings – index 6
        showings_str = cells[6].get_text(strip=True)
        showings = parse_int(showings_str)

        # Admissions – index 7
        admissions_str = cells[7].get_text(strip=True)
        admissions = parse_int(admissions_str)

        # Days – index 8
        days_str = cells[8].get_text(strip=True)
        days = parse_int(days_str) if days_str and days_str.strip() != '-' else None

        # Compute release date if days is valid and reasonable
        release_date = None
        if days is not None and days > 0 and days < 10000:
            try:
                release_date = date_obj - timedelta(days=days)
            except OverflowError:
                logger.warning(f"Overflow when subtracting {days} days from {date_obj}; skipping release date.")
                release_date = None

        entries.append({
            "movie_id": movie_id,
            "title": title,
            "rank": rank,
            "gross": gross,
            "cume": cume,
            "showings": showings,
            "admissions": admissions,
            "days": days,
            "release_date": release_date
        })
    return entries

# ---------- Daywise Aggregation ----------
def ensure_daywise_date(daywise_acc, date_obj):
    date_str = format_date_dmy(date_obj)
    if date_str not in daywise_acc:
        daywise_acc[date_str] = {
            "date": date_str,
            "data": []   # list of entries for this date
        }
    return date_str

async def process_fetched_date(date_str_mdy, html, daywise_acc):
    """Parse HTML and update movie store and daywise accumulator."""
    date_obj = datetime.strptime(date_str_mdy, "%m/%d/%Y")
    entries = parse_html(html, date_obj)
    if not entries:
        return

    for entry in entries:
        movie_id = entry["movie_id"]
        if movie_id is None:
            continue
        title = entry["title"]
        slug, movie_data = await movie_store.get_or_create(movie_id, title, date_obj)

        # Get release date (from store or from entry)
        release_date = movie_store.get_release_date(slug)
        if release_date is None and entry["release_date"] is not None:
            release_date = entry["release_date"]

        # Build entry for database
        db_entry = {
            "date": format_date_dmy(date_obj),
            "rank": entry["rank"],
            "gross": entry["gross"],
            "cume": entry["cume"],
            "showings": entry["showings"],
            "admissions": entry["admissions"],
            "days": entry["days"],
            "day": week_day_string(release_date, date_obj) if release_date else "Unknown"
        }
        await movie_store.add_entry(slug, db_entry)

        # Build daywise entry
        daywise_entry = {
            "movie_id": movie_id,
            "title": title,
            "gross": entry["gross"],
            "cume": entry["cume"],
            "showings": entry["showings"],
            "admissions": entry["admissions"],
            "rank": entry["rank"],
            "days": entry["days"],
            "day": db_entry["day"]
        }
        date_str = format_date_dmy(date_obj)
        ensure_daywise_date(daywise_acc, date_obj)
        daywise_acc[date_str]["data"].append(daywise_entry)

async def write_daywise(daywise_acc):
    for date_str, obj in daywise_acc.items():
        filepath = DAYWISE_DIR / f"{date_str}.json"
        filepath.parent.mkdir(parents=True, exist_ok=True)
        try:
            async with aiofiles.open(filepath, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(obj, indent=2, ensure_ascii=False))
        except Exception as e:
            logger.error(f"Failed to write daywise {filepath}: {e}")

# ---------- Rebuild daywise from database ----------
async def rebuild_daywise_from_database():
    """Rebuild all daywise JSON files from the consolidated movie database."""
    logger.info("Rebuilding all daywise files from database...")
    movies = movie_store.get_all_movies()
    daywise_data = defaultdict(lambda: {"date": None, "data": []})

    for movie in movies:
        for entry in movie["entries"]:
            date_str = entry["date"]
            if daywise_data[date_str]["date"] is None:
                daywise_data[date_str]["date"] = date_str
            day_entry = {
                "movie_id": movie["movie_id"],
                "title": movie["title"],
                "gross": entry["gross"],
                "cume": entry["cume"],
                "showings": entry["showings"],
                "admissions": entry["admissions"],
                "rank": entry["rank"],
                "days": entry["days"],
                "day": entry["day"]
            }
            daywise_data[date_str]["data"].append(day_entry)

    for date_str, obj in daywise_data.items():
        filepath = DAYWISE_DIR / f"{date_str}.json"
        filepath.parent.mkdir(parents=True, exist_ok=True)
        try:
            async with aiofiles.open(filepath, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(obj, indent=2, ensure_ascii=False))
        except Exception as e:
            logger.error(f"Failed to write daywise {filepath}: {e}")

    logger.info(f"Rebuilt {len(daywise_data)} daywise files.")

# ---------- Yearly Index ----------
async def build_yearly_index():
    all_movies = movie_store.get_all_movies()
    yearly = defaultdict(list)

    for movie in all_movies:
        release_date_str = movie["releaseDate"]
        if release_date_str == "Unknown":
            continue
        release_date = parse_date_dmy(release_date_str)
        year = release_date.year

        total_gross = sum(e["gross"] for e in movie["entries"])
        total_admissions = sum(e["admissions"] for e in movie["entries"])
        total_showings = sum(e["showings"] for e in movie["entries"])

        yearly[year].append({
            "movie_id": movie["movie_id"],
            "title": movie["title"],
            "releaseDate": release_date_str,
            "total_gross": total_gross,
            "total_admissions": total_admissions,
            "total_showings": total_showings
        })

    for year, movies in yearly.items():
        year_dir = DATA_DIR / str(year)
        year_dir.mkdir(parents=True, exist_ok=True)
        filepath = year_dir / "index.json"
        try:
            async with aiofiles.open(filepath, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(movies, indent=2, ensure_ascii=False))
        except Exception as e:
            logger.error(f"Failed to write {filepath}: {e}")

    logger.info("Yearly indices rebuilt.")

# ---------- Apply Corrections ----------
async def apply_corrections():
    """
    Apply manual overrides from correctedslug.json.
    Returns True if any changes were made.
    """
    if not CORRECTIONS_FILE.exists():
        logger.info("No corrections file found, skipping.")
        return False

    try:
        async with aiofiles.open(CORRECTIONS_FILE, 'r', encoding='utf-8') as f:
            corrections = json.loads(await f.read())
        logger.info(f"Loaded corrections file with {len(corrections)} entries.")
    except Exception as e:
        logger.error(f"Failed to read corrections file: {e}")
        return False

    await movie_slug_mapper.load()
    changed = False

    for primary_id_str, data in corrections.items():
        try:
            primary_id = int(primary_id_str)
        except ValueError:
            logger.warning(f"Correction key '{primary_id_str}' is not an integer, skipping.")
            continue

        primary = movie_slug_mapper.resolve(primary_id)
        if primary != primary_id:
            logger.warning(f"Correction key '{primary_id}' resolves to '{primary}'; using primary.")

        if primary not in movie_slug_mapper.map:
            movie_slug_mapper.map[primary] = {
                "slug": generate_slug(data.get("new_title", "Unknown")),
                "title": data.get("new_title", "Unknown"),
                "manual_title": False,
                "manual_slug": False
            }
            changed = True

        entry = movie_slug_mapper.map[primary]

        if "new_slug" in data:
            new_slug = data["new_slug"]
            if entry.get("slug") != new_slug:
                logger.info(f"Updating slug for ID {primary}: '{entry.get('slug')}' -> '{new_slug}'")
                entry["slug"] = new_slug
                entry["manual_slug"] = True
                changed = True

        if "new_title" in data:
            new_title = data["new_title"]
            if entry.get("title") != new_title:
                logger.info(f"Updating title for ID {primary}: '{entry.get('title')}' -> '{new_title}'")
                entry["title"] = new_title
                entry["manual_title"] = True
                changed = True

        if "merge" in data:
            for merged_id_str in data["merge"]:
                try:
                    merged_id = int(merged_id_str)
                except ValueError:
                    continue
                if merged_id == primary:
                    continue
                if movie_slug_mapper.map.get(merged_id, {}).get("redirect") != primary:
                    logger.info(f"Redirecting ID {merged_id} -> {primary}")
                    movie_slug_mapper.map[merged_id] = {"redirect": primary}
                    changed = True

    if changed:
        await movie_slug_mapper.save()
        # Re‑consolidate the database files
        await reconsolidate_movies()
        # Rebuild daywise from the newly consolidated database
        await rebuild_daywise_from_database()
        logger.info("Corrections applied, database and daywise rebuilt.")
    else:
        logger.info("No corrections needed (already up‑to‑date).")

    return changed

async def reconsolidate_movies():
    """Re‑write all database files from current mapper (merge, rename, delete orphans)."""
    DATABASE_DIR.mkdir(parents=True, exist_ok=True)

    groups = defaultdict(list)
    for filepath in DATABASE_DIR.glob("*.json"):
        try:
            async with aiofiles.open(filepath, 'r', encoding='utf-8') as f:
                data = json.loads(await f.read())
            slug = filepath.stem
            movie_id = data.get("movie_id")
            if not movie_id:
                continue
            primary = movie_slug_mapper.resolve(movie_id)
            groups[primary].append((slug, data))
        except Exception as e:
            logger.warning(f"Failed to read {filepath}: {e}")

    for primary_id, items in groups.items():
        canonical_slug = movie_slug_mapper.get_slug(primary_id)
        if not canonical_slug:
            title = movie_slug_mapper.get_title(primary_id) or "Unknown"
            canonical_slug, _ = movie_slug_mapper.ensure_movie(primary_id, title)
            await movie_slug_mapper.save()

        # Merge entries
        merged_entries = []
        seen = set()
        for _, data in items:
            for entry in data.get("entries", []):
                key = entry["date"]
                if key not in seen:
                    merged_entries.append(entry)
                    seen.add(key)
        merged_entries.sort(key=lambda e: parse_date_dmy(e["date"]))

        release_date = None
        if merged_entries:
            for e in merged_entries:
                if "days" in e and e["days"] is not None and e["days"] > 0 and e["days"] < 10000:
                    try:
                        entry_date = parse_date_dmy(e["date"])
                        release_date = entry_date - timedelta(days=e["days"])
                        break
                    except:
                        pass
            if not release_date:
                earliest = min(parse_date_dmy(e["date"]) for e in merged_entries)
                release_date = earliest

        movie_data = {
            "movie_id": primary_id,
            "title": movie_slug_mapper.get_title(primary_id) or "Unknown",
            "releaseDate": format_date_dmy(release_date) if release_date else "Unknown",
            "entries": merged_entries
        }

        new_file = DATABASE_DIR / f"{canonical_slug}.json"
        try:
            async with aiofiles.open(new_file, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(movie_data, indent=2, ensure_ascii=False))
        except Exception as e:
            logger.error(f"Failed to write {new_file}: {e}")
            continue

        for slug, _ in items:
            if slug != canonical_slug:
                old_file = DATABASE_DIR / f"{slug}.json"
                try:
                    old_file.unlink()
                    logger.info(f"Removed old file {old_file.name}")
                except Exception as e:
                    logger.warning(f"Could not delete {old_file}: {e}")

        movie_store.movies[canonical_slug] = movie_data

# ---------- Main ----------
async def main(full_fetch=False):
    # 1. Load mapper
    await movie_slug_mapper.load()

    # 2. Load all movie data (consolidates existing files)
    await movie_store.load_all()

    # 3. Apply corrections (rebuilds daywise if needed)
    await apply_corrections()

    # 4. Determine date range to fetch
    today = datetime.today().date()
    if full_fetch:
        start_date = START_DATE.date()
        end_date = today
        logger.info("Performing full fetch from %s to %s", start_date, end_date)
    else:
        # Incremental: from last_run+1 to today
        last_run = None
        if STATE_FILE.exists():
            try:
                async with aiofiles.open(STATE_FILE, 'r') as f:
                    state = json.loads(await f.read())
                last_run = datetime.fromisoformat(state.get("last_run", "")).date()
            except:
                pass
        if last_run:
            start_date = last_run + timedelta(days=1)
        else:
            start_date = START_DATE.date()
        if start_date < START_DATE.date():
            start_date = START_DATE.date()
        end_date = today
        if start_date > end_date:
            logger.info("No new dates to fetch.")
            # Still build yearly indices (in case corrections changed totals)
            await build_yearly_index()
            return

    # Build list of dates in MM/DD/YYYY format
    date_list_mdy = []
    current = start_date
    while current <= end_date:
        date_list_mdy.append(current.strftime("%m/%d/%Y"))
        current += timedelta(days=1)

    daywise_acc = {}

    async with aiohttp.ClientSession() as session:
        tasks = [fetch_date(session, d) for d in date_list_mdy]
        responses = await asyncio.gather(*tasks)

        for date_str_mdy, html in zip(date_list_mdy, responses):
            if html:
                await process_fetched_date(date_str_mdy, html, daywise_acc)
                logger.info("Processed %s", date_str_mdy)

    if daywise_acc:
        await write_daywise(daywise_acc)

    # Flush all movie data to disk
    await movie_store.flush()

    # Rebuild yearly indices
    await build_yearly_index()

    # Save state
    state = {"last_run": today.isoformat()}
    try:
        async with aiofiles.open(STATE_FILE, 'w') as f:
            await f.write(json.dumps(state))
    except Exception as e:
        logger.error(f"Failed to write state file: {e}")

    logger.info("All done!")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="China Box Office Tracker")
    parser.add_argument("--full", action="store_true",
                        help="Perform full fetch from 2013-01-01 (ignores state)")
    args = parser.parse_args()
    asyncio.run(main(full_fetch=args.full))
