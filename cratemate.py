#!/usr/bin/env python3
"""
CrateMate — Smart music file organizer.

Parses artist/title/mix info from filenames, fetches album art from Spotify,
writes clean tags, and copies files to your library with normalized names.

No manual intervention. Your originals are never touched.

Usage:
    python3 cratemate.py /path/to/downloads
    python3 cratemate.py /path/to/downloads --dry-run
    python3 cratemate.py  (opens folder picker)
"""

import argparse
import base64
import dataclasses
import json
import math
import os
import re
import shutil
import subprocess
import sys
import threading
import time
import unicodedata
from io import BytesIO
from pathlib import Path

import mediafile
import requests
from dotenv import load_dotenv
from PIL import Image

_SCRIPT_DIR = Path(__file__).resolve().parent
CONFIG_DIR = Path.home() / ".config" / "cratemate"
# Load API keys: prefer ~/.config/cratemate/.env (works after pip install),
# fall back to script directory (convenient for local dev).
load_dotenv(CONFIG_DIR / ".env")
load_dotenv(_SCRIPT_DIR / ".env", override=False)

# ── CONFIG ──────────────────────────────────────────────────────────────────

DEFAULT_LIBRARY_DIR = Path.home() / "Music" / "DJ_Library"
CONFIG_FILE = CONFIG_DIR / "config.json"
_OLD_CONFIG_FILE = Path.home() / ".cratemate_config.json"  # pre-v1.2 location, migrated on first load
UNSORTED_GENRE = "Unsorted"


def load_config() -> dict:
    """Load persistent config from ~/.config/cratemate/config.json.
    Auto-migrates from the old ~/.cratemate_config.json location on first load."""
    if not CONFIG_FILE.exists() and _OLD_CONFIG_FILE.exists():
        try:
            CONFIG_DIR.mkdir(parents=True, exist_ok=True)
            shutil.copy2(_OLD_CONFIG_FILE, CONFIG_FILE)
        except Exception:
            pass
    try:
        return json.loads(CONFIG_FILE.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save_config(config: dict) -> None:
    """Save persistent config to ~/.config/cratemate/config.json."""
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    CONFIG_FILE.write_text(json.dumps(config, indent=2) + "\n")


def get_library_dir() -> Path:
    """Get the library directory from config, falling back to default."""
    config = load_config()
    return Path(config.get("library_dir", str(DEFAULT_LIBRARY_DIR)))


def set_library_dir(path: str | Path) -> Path:
    """Save a new library directory to persistent config. Returns the resolved Path."""
    path = Path(os.path.expanduser(str(path))).resolve()
    config = load_config()
    config["library_dir"] = str(path)
    save_config(config)
    return path


# Resolve library dir at import time (used by CLI --library default)
LIBRARY_DIR = get_library_dir()

SPOTIFY_CLIENT_ID = os.environ.get("SPOTIFY_CLIENT_ID", "")
SPOTIFY_CLIENT_SECRET = os.environ.get("SPOTIFY_CLIENT_SECRET", "")
DISCOGS_USER_TOKEN = os.environ.get("DISCOGS_USER_TOKEN", "")

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
GEMINI_MODEL = "gemini-3.1-flash-lite-preview"
GEMINI_BATCH_SIZE = 30  # filenames per API call

AUDIO_EXTENSIONS = {".mp3", ".flac", ".aiff", ".aif", ".wav", ".m4a", ".ogg", ".opus"}

ART_MAX_SIZE = 800        # Max pixel dimension for embedded art
ART_JPEG_QUALITY = 92     # JPEG compression quality for embedded art
ART_JUNK_THRESHOLD = 5000 # Bytes — art smaller than this is likely junk/placeholder

# Format quality ranking — lower = better (used by remove_duplicates)
FORMAT_RANK = {
    ".flac": 0,
    ".aiff": 1, ".aif": 1, ".wav": 1,
    ".m4a": 2,
    ".mp3": 3,
    ".ogg": 4, ".opus": 4,
}

# ── RATE-LIMITED REQUESTS ───────────────────────────────────────────────────

_last_spotify_call = 0.0
_spotify_call_count = 0
_spotify_window_start = 0.0
SPOTIFY_MIN_DELAY = 0.15   # 150ms between calls (~6/sec, well under Spotify's limit)
SPOTIFY_BURST_LIMIT = 50   # max calls per 30-second window
SPOTIFY_BURST_WINDOW = 30  # seconds


def api_get(url, **kwargs):
    """GET with per-call throttling and retry on 429.
    Raises RateLimitError if wait time is too long so caller can skip to Discogs."""
    global _last_spotify_call, _spotify_call_count, _spotify_window_start

    # Throttle Spotify calls to avoid hitting rate limits
    if "spotify.com" in url:
        now = time.time()

        # Reset burst window if expired
        if now - _spotify_window_start > SPOTIFY_BURST_WINDOW:
            _spotify_call_count = 0
            _spotify_window_start = now

        # If we've hit the burst limit, wait for the window to reset
        if _spotify_call_count >= SPOTIFY_BURST_LIMIT:
            wait_for = SPOTIFY_BURST_WINDOW - (now - _spotify_window_start) + 1
            if wait_for > 0:
                print(f"    Spotify throttle — pausing {wait_for:.0f}s to avoid rate limit...")
                time.sleep(wait_for)
            _spotify_call_count = 0
            _spotify_window_start = time.time()

        # Per-call delay
        elapsed = time.time() - _last_spotify_call
        if elapsed < SPOTIFY_MIN_DELAY:
            time.sleep(SPOTIFY_MIN_DELAY - elapsed)
        _last_spotify_call = time.time()
        _spotify_call_count += 1

    for attempt in range(3):
        resp = requests.get(url, **kwargs)
        if resp.status_code == 429:
            wait = int(resp.headers.get("Retry-After", 2))
            if wait > 30:
                raise RateLimitError(f"Rate limited for {wait}s")
            print(f"    Rate limited — waiting {wait}s...")
            time.sleep(wait + 1)
            continue
        if resp.status_code == 401 and "spotify.com" in url:
            # Token expired — refresh and retry
            global _spotify_token
            _spotify_token = None
            new_token = spotify_token()
            if not new_token:
                break
            kwargs_copy = dict(kwargs)
            kwargs_copy["headers"] = {**kwargs.get("headers", {}),
                                       "Authorization": f"Bearer {new_token}"}
            kwargs = kwargs_copy
            continue
        resp.raise_for_status()
        return resp
    resp.raise_for_status()
    return resp


class RateLimitError(Exception):
    pass


# ── SPOTIFY AUTH ────────────────────────────────────────────────────────────

_spotify_token = None


def spotify_token() -> str | None:
    """Get a Spotify API token using client credentials. Returns None if keys are not set."""
    global _spotify_token
    if _spotify_token:
        return _spotify_token
    if not SPOTIFY_CLIENT_ID or not SPOTIFY_CLIENT_SECRET:
        return None
    try:
        resp = requests.post(
            "https://accounts.spotify.com/api/token",
            data={"grant_type": "client_credentials"},
            headers={
                "Authorization": "Basic "
                + base64.b64encode(
                    f"{SPOTIFY_CLIENT_ID}:{SPOTIFY_CLIENT_SECRET}".encode()
                ).decode()
            },
            timeout=10,
        )
        resp.raise_for_status()
        _spotify_token = resp.json()["access_token"]
        return _spotify_token
    except Exception as e:
        print(f"    Spotify auth failed: {e}")
        return None


def spotify_search(artist: str, title: str) -> dict | None:
    """Search Spotify for a track. Returns dict with album art URL and genre, or None."""
    token = spotify_token()
    if not token:
        return None
    headers = {"Authorization": f"Bearer {token}"}

    # Build search strategies — quotes around multi-word field values are critical,
    # otherwise Spotify parses "artist:Sam Alfred" as "artist:Sam" + loose "Alfred"
    # Ordered from most specific (fewest results, best match) to loosest
    if artist:
        search_queries = [
            f'track:"{title}" artist:"{artist}"',       # strict quoted field search
            f'"{artist}" "{title}"',                    # quoted exact phrases
            f"{artist} {title}",                        # fully loose
        ]
    else:
        search_queries = [
            f'track:"{title}"',
            f'"{title}"',
        ]

    # Try each strategy; stop as soon as we find results with an artist match
    all_tracks = []
    for query in search_queries:
        try:
            resp = api_get(
                "https://api.spotify.com/v1/search",
                params={"q": query, "type": "track", "limit": 10},
                headers=headers,
                timeout=10,
            )
            items = resp.json().get("tracks", {}).get("items", [])
        except RateLimitError as e:
            print(f"    Spotify rate limited — skipping ({e})")
            break
        except Exception as e:
            print(f"    Spotify search failed: {e}")
            continue
        if not items:
            continue
        # Check if any result actually matches our artist (if we have one)
        if artist:
            has_artist_match = any(
                artist.lower() in [a["name"].lower() for a in t["artists"]]
                or any(artist.lower() in a["name"].lower() or a["name"].lower() in artist.lower()
                       for a in t["artists"])
                for t in items
            )
            if has_artist_match:
                all_tracks = items
                break
            # No artist match — keep as fallback but try looser queries
            if not all_tracks:
                all_tracks = items
        else:
            all_tracks = items
            break

    tracks = all_tracks

    if not tracks:
        return None

    # Score each result — prefer artist match + title match
    def match_score(t):
        score = 0
        artists_lower = [a["name"].lower() for a in t["artists"]]
        t_name = t.get("name", "").lower()

        if artist:
            # Exact artist match is most important
            if artist.lower() in artists_lower:
                score += 100
            # Partial artist match (e.g. "Megra" in "MEGRA")
            elif any(artist.lower() in a or a in artist.lower() for a in artists_lower):
                score += 50
            # No artist match at all — penalise heavily
            else:
                score -= 50

        # Title match
        if title.lower() == t_name:
            score += 40
        elif title.lower() in t_name or t_name in title.lower():
            score += 20
        return score

    best = max(tracks, key=match_score)

    # Require a minimum score — must match at least the title
    if match_score(best) < 20:
        return None

    images = best.get("album", {}).get("images", [])
    art_url = images[0]["url"] if images else None

    # Get artist genres
    artist_id = best["artists"][0]["id"] if best.get("artists") else None
    genre = None
    if artist_id:
        try:
            resp = api_get(
                f"https://api.spotify.com/v1/artists/{artist_id}",
                headers={"Authorization": f"Bearer {token}"},
                timeout=10,
            )
            genres = resp.json().get("genres", [])
            if genres:
                # Pick the most specific genre, title-cased
                genre = genres[0].title()
        except Exception:
            pass

    album = best.get("album", {})
    return {
        "art_url": art_url,
        "genre": genre,
        "spotify_title": best.get("name"),
        "spotify_artist": best["artists"][0]["name"] if best.get("artists") else None,
        "album_name": album.get("name"),
        "album_artist": ", ".join(a["name"] for a in album.get("artists", [])) or None,
        "year": (album.get("release_date") or "")[:4] or None,
    }


def fetch_art(url: str, max_size: int = ART_MAX_SIZE) -> bytes | None:
    """Download album art, resize if needed, return as JPEG bytes."""
    try:
        resp = requests.get(url, timeout=15, headers={"User-Agent": "CrateMate/1.0"})
        resp.raise_for_status()
        img = Image.open(BytesIO(resp.content))
        if img.mode != "RGB":
            img = img.convert("RGB")
        if max(img.size) > max_size:
            img.thumbnail((max_size, max_size), Image.LANCZOS)
        buf = BytesIO()
        img.save(buf, format="JPEG", quality=ART_JPEG_QUALITY)
        return buf.getvalue()
    except Exception as e:
        print(f"    Failed to fetch art: {e}")
        return None


# ── DISCOGS FALLBACK ────────────────────────────────────────────────────────

def discogs_search_art(artist, title):
    """Search Discogs for album art. Returns image URL or None."""
    if not DISCOGS_USER_TOKEN:
        return None
    query = f"{artist} {title}" if artist else title
    try:
        resp = api_get(
            "https://api.discogs.com/database/search",
            params={"q": query, "type": "release", "per_page": 5},
            headers={
                "Authorization": f"Discogs token={DISCOGS_USER_TOKEN}",
                "User-Agent": "CrateMate/1.0",
            },
            timeout=10,
        )
        resp.raise_for_status()
        results = resp.json().get("results", [])
    except Exception as e:
        print(f"    Discogs search failed: {e}")
        return None

    for r in results:
        img = r.get("cover_image", "")
        # Skip the default "spacer" placeholder image
        if img and "spacer.gif" not in img:
            return img
    return None


def discogs_search_genre(artist: str, title: str) -> str:
    """Search Discogs for genre/style info. Returns a comma-separated hint string or empty."""
    if not DISCOGS_USER_TOKEN:
        return ""
    query = f"{artist} {title}" if artist else title
    try:
        resp = api_get(
            "https://api.discogs.com/database/search",
            params={"q": query, "type": "release", "per_page": 3},
            headers={
                "Authorization": f"Discogs token={DISCOGS_USER_TOKEN}",
                "User-Agent": "CrateMate/1.0",
            },
            timeout=10,
        )
        resp.raise_for_status()
        results = resp.json().get("results", [])
    except Exception:
        return ""

    # Prefer style (more specific) over genre (broad like "Electronic")
    for r in results:
        styles = r.get("style", [])
        genres = r.get("genre", [])
        # Combine styles first (more specific), then genres
        combined = styles + [g for g in genres if g != "Electronic"]
        if combined:
            return ", ".join(combined)
    return ""


# ── GEMINI AI NAME FIXING ──────────────────────────────────────────────────

GEMINI_PROMPT = """You are a music filename parser for a DJ library organizer.
Extract artist, title, and mix/remix info from each raw filename.

CLEANING — remove all of these:
- Track numbers (01, A1, Track 03)
- Website watermarks (www.anything.com)
- Scene hashes (-e55b3645, -zzzz)
- BPM/key info (128, 6A 144)
- Junk tags: [FREE DOWNLOAD], (OUT NOW), [PREVIEW], [CLIP], [MASTER]
- Year tags: (2024), (2025)
- Underscores (replace with spaces)

ARTIST:
- Use proper title case (capitalize words, lowercase: a, an, the, and, or, but, in, on, at, to, for, of)
- Always uppercase: DJ, MC, VIP, UK, US
- Multiple artists: normalize "x", "X", "vs" separators to "&" (e.g. "DJ Heartstring X Southstar" → "DJ Heartstring & Southstar")
- Keep "ft.", "feat." for featured artists
- If no artist can be determined from the filename, use empty string — do NOT guess

TITLE:
- Use proper title case (same rules as artist)
- The title must NOT include mix/remix info — that goes in the "mix" field
- Do NOT rename or alter the title beyond cleaning and casing

MIX:
- Extract to separate field: Extended Mix, Original Mix, Radio Mix, Club Mix, Dub, VIP, etc.
- Remixes: "Remixer Name Remix" (e.g. "Skrillex Remix")
- If no mix info is present, use empty string
- Title case the mix info

CRITICAL:
- Only extract what is in the filename — do NOT use external knowledge to guess, correct, or add information
- Do NOT invent artist names, rename songs, or associate tracks with artists not in the filename

Return ONLY a JSON array in input order. Each object: {"artist": "", "title": "", "mix": ""}

Examples:
Input: ["01-mall_grab-you_thought-e55b3645.flac", "Peggy Gou - 1+1=11 (Spray Remix) [FREE DOWNLOAD].mp3", "green wide open.flac"]
Output: [{"artist": "Mall Grab", "title": "You Thought", "mix": ""}, {"artist": "Peggy Gou", "title": "1+1=11", "mix": "Spray Remix"}, {"artist": "", "title": "Green Wide Open", "mix": ""}]

Parse these filenames:
"""


def gemini_fix_names(filenames):
    """Send a batch of filenames to Gemini for AI-powered name parsing.
    Returns a dict mapping filename -> (artist, title, mix) for successful parses."""
    if not GEMINI_API_KEY:
        print("  Gemini API key not set — skipping AI name fixing")
        return {}

    results = {}
    # Process in batches
    for i in range(0, len(filenames), GEMINI_BATCH_SIZE):
        batch = filenames[i:i + GEMINI_BATCH_SIZE]
        batch_stems = [Path(f).stem for f in batch]

        print(f"  Gemini: processing batch {i // GEMINI_BATCH_SIZE + 1} "
              f"({len(batch)} files)...")

        try:
            resp = requests.post(
                f"https://generativelanguage.googleapis.com/v1beta/models/"
                f"{GEMINI_MODEL}:generateContent",
                params={"key": GEMINI_API_KEY},
                headers={"Content-Type": "application/json"},
                json={
                    "contents": [{"parts": [{"text": GEMINI_PROMPT + json.dumps(batch_stems)}]}],
                    "generationConfig": {
                        "responseMimeType": "application/json",
                        "temperature": 0.1,
                    },
                },
                timeout=30,
            )
            resp.raise_for_status()

            # Extract the text response
            resp_json = resp.json()
            text = resp_json["candidates"][0]["content"]["parts"][0]["text"]
            parsed = json.loads(text)

            if not isinstance(parsed, list) or len(parsed) != len(batch):
                print(f"    Gemini returned {len(parsed) if isinstance(parsed, list) else 'invalid'} "
                      f"results for {len(batch)} files — skipping batch")
                continue

            for filepath, entry in zip(batch, parsed):
                if isinstance(entry, dict) and "artist" in entry and "title" in entry:
                    artist = (entry.get("artist") or "").strip()
                    title = (entry.get("title") or "").strip()
                    mix = (entry.get("mix") or "").strip()
                    if title:  # Must have at least a title
                        results[str(filepath)] = (artist, title, mix)

        except requests.exceptions.HTTPError as e:
            print(f"    Gemini API error: {e}")
            if hasattr(e, 'response') and e.response is not None:
                try:
                    print(f"    {e.response.json().get('error', {}).get('message', '')}")
                except Exception:
                    pass
        except (json.JSONDecodeError, KeyError, IndexError) as e:
            print(f"    Gemini response parsing error: {e}")
        except Exception as e:
            print(f"    Gemini request failed: {e}")

    return results


# ── FILENAME PARSING ────────────────────────────────────────────────────────

def parse_filename(filepath: str | Path) -> tuple[str, str, str]:
    """Extract artist, title, and mix info from a filename.

    Handles common patterns:
        Artist - Title (Extended Mix)
        01 - Artist - Title
        01. Artist - Title (DJ X Remix)
        Artist - Title [Original Mix]
        Artist_-_Title_(Extended)
    """
    stem = Path(filepath).stem

    # Normalize underscores and multiple spaces
    stem = stem.replace("_", " ")
    stem = re.sub(r"\s+", " ", stem).strip()

    # Scene format: "05-artist-title-hash" — if all dashes (no spaces around them),
    # replace dashes with " - " for proper splitting later
    if " - " not in stem and " – " not in stem and "-" in stem:
        # Split on dashes, skip leading track number and trailing hash
        dash_parts = stem.split("-")
        # Strip leading number
        if dash_parts and re.match(r"^\d{1,3}$", dash_parts[0].strip()):
            dash_parts = dash_parts[1:]
        # Strip trailing short hash-like segment
        if dash_parts and re.match(r"^[a-f0-9]{4,10}$", dash_parts[-1].strip(), re.IGNORECASE):
            dash_parts = dash_parts[:-1]
        if len(dash_parts) >= 2:
            stem = " - ".join(p.strip() for p in dash_parts)
        elif dash_parts:
            stem = dash_parts[0].strip()

    # Remove year in parens before mix extraction so "(2026)" isn't grabbed
    stem = re.sub(r"\((?:19|20)\d{2}\)", "", stem).strip()

    # Extract mix info from parentheses or brackets
    mix = ""
    mix_patterns = [
        r"\(([^)]*(?:Mix|Remix|Edit|Dub|Rework|Bootleg|VIP|Extended|Original|Radio|Club|Instrumental|Vocal|Acapella|Acoustic)[^)]*)\)",
        r"\[([^\]]*(?:Mix|Remix|Edit|Dub|Rework|Bootleg|VIP|Extended|Original|Radio|Club|Instrumental|Vocal|Acapella|Acoustic)[^\]]*)\]",
    ]
    for pat in mix_patterns:
        m = re.search(pat, stem, re.IGNORECASE)
        if m:
            mix = m.group(1).strip()
            stem = stem[:m.start()].strip() + " " + stem[m.end():].strip()
            stem = stem.strip()
            break

    # If no parens, check for "- Extended Mix" / "- Remix" at the end
    if not mix:
        m = re.search(
            r"[-–]\s*((?:Extended|Original|Radio|Club|Instrumental|Vocal)\s*(?:Mix)?|[^-]*?Remix|[^-]*?Edit|[^-]*?Dub|[^-]*?Rework|[^-]*?Bootleg|[^-]*?VIP)\s*$",
            stem, re.IGNORECASE,
        )
        if m:
            mix = m.group(1).strip()
            stem = stem[:m.start()].strip()

    # Remove leading track numbers: "01 - ", "01. ", "01 Title", "A1 ", "10A - "
    stem = re.sub(r"^(?:\d{1,3}[A-Za-z]?|[A-Z]\d{1,2})\s*[.\-–)\]]\s*", "", stem).strip()
    # "01 Title" with no separator (only if followed by a capital letter)
    stem = re.sub(r"^(\d{1,3})\s+(?=[A-Z])", "", stem).strip()
    # "Track 01 - "
    stem = re.sub(r"^Track\s*\d+\s*[-–.]\s*", "", stem, flags=re.IGNORECASE).strip()

    # Remove scene release suffixes: -zzzz, -idc, -e55b3645, etc.
    stem = re.sub(r"-[a-z0-9]{2,8}$", "", stem, flags=re.IGNORECASE).strip()

    # Remove website watermarks
    stem = re.sub(r"\s*[-–]?\s*www\.[a-z0-9.-]+\.[a-z]{2,4}", "", stem, flags=re.IGNORECASE).strip()

    # Remove trailing BPM/key info: "(Clean) 6A 144", "3A 143", "135" at end
    stem = re.sub(r"\s*\(Clean\)", "", stem, flags=re.IGNORECASE).strip()
    stem = re.sub(r"\s+\d{1,2}[A-B]\s+\d{2,3}\s*$", "", stem).strip()
    stem = re.sub(r"\s+\d{2,3}\s*$", "", stem).strip()  # trailing BPM only

    # Remove duplicate parenthetical sections (e.g. "(Extended) (Extended)")
    stem = re.sub(r"(\([^)]+\))\s*\1", r"\1", stem).strip()

    # Remove common junk tags
    junk_patterns = [
        r"\[FREE\s*(?:DOWNLOAD|DL)\]", r"\(FREE\s*(?:DOWNLOAD|DL)\)",
        r"\[OUT NOW\]", r"\(OUT NOW\)",
        r"\[PREVIEW\]", r"\(PREVIEW\)",
        r"\[CLIP\]", r"\(CLIP\)",
        r"\[MASTER\]", r"\(MASTER\)",
        r"\[AVENU\]", r"\[SNKRS\d*\]",  # label tags in brackets
    ]
    for pat in junk_patterns:
        stem = re.sub(pat, "", stem, flags=re.IGNORECASE)
    stem = re.sub(r"\s+", " ", stem).strip()

    # Split into artist and title
    # Try " - " first (most common), then " – " (en-dash)
    parts = re.split(r"\s+[-–]\s+", stem)
    if len(parts) >= 3:
        # e.g. "01 - Artist - Title" — skip leading numeric parts
        non_numeric = [(i, p.strip()) for i, p in enumerate(parts)
                       if not re.match(r"^\d{1,3}[A-Za-z]?$", p.strip())]
        if len(non_numeric) >= 2:
            artist, title = non_numeric[-2][1], non_numeric[-1][1]
        elif len(non_numeric) == 1:
            artist, title = "", non_numeric[0][1]
        else:
            artist, title = parts[-2].strip(), parts[-1].strip()
    elif len(parts) == 2:
        a, t = parts[0].strip(), parts[1].strip()
        # If "artist" is just a number, it's a track number not an artist
        if re.match(r"^\d{1,3}[A-Za-z]?$", a):
            artist, title = "", t
        else:
            artist, title = a, t
    else:
        # Can't split — use whole thing as title, artist unknown
        artist, title = "", stem.strip()

    # Clean up extra whitespace and dashes
    artist = re.sub(r"\s+", " ", artist).strip()
    title = re.sub(r"\s+", " ", title).strip()

    # Title-case if everything is lowercase or uppercase
    if artist == artist.lower() or artist == artist.upper():
        artist = smart_title(artist)
    if title == title.lower() or title == title.upper():
        title = smart_title(title)

    # Normalize mix casing
    if mix and (mix == mix.lower() or mix == mix.upper()):
        mix = smart_title(mix)

    return artist, title, mix


def smart_title(s: str) -> str:
    """Title-case but preserve common patterns like 'DJ', 'MC', etc."""
    words = s.split()
    result = []
    always_upper = {"dj", "mc", "vip", "uk", "us", "ii", "iii", "iv"}
    always_lower = {"a", "an", "the", "and", "or", "but", "in", "on", "at",
                    "to", "for", "of", "vs", "vs.", "ft", "ft.", "feat", "feat."}
    for i, w in enumerate(words):
        wl = w.lower()
        if wl in always_upper:
            result.append(w.upper())
        elif i > 0 and wl in always_lower:
            result.append(wl)
        else:
            result.append(w.capitalize())
    return " ".join(result)


# ── SAFE FILENAME ───────────────────────────────────────────────────────────

def safe_filename(s: str) -> str:
    """Make a string safe for use as a filename."""
    # Normalize unicode
    s = unicodedata.normalize("NFC", s)
    # Replace slash with comma (common for collabs: "Artist1/Artist2")
    s = s.replace("/", ", ")
    # Remove other unsafe chars
    s = re.sub(r'[<>:"|?*\\]', "", s)
    s = re.sub(r"\s+", " ", s).strip()
    s = s.strip(". ")
    return s


# ── SHARED HELPERS ─────────────────────────────────────────────────────────

def find_audio_files(directory: str | Path) -> list[Path]:
    """Find all audio files recursively, sorted by path."""
    return sorted(
        f for f in Path(directory).rglob("*")
        if f.is_file() and f.suffix.lower() in AUDIO_EXTENSIONS
    )


def parse_library_filename(filepath: str | Path) -> tuple[str, str, str]:
    """Parse artist, title, mix from a clean library filename like 'Artist - Title (Mix).ext'.

    Returns (artist, title, mix). Mix may be empty string.
    """
    stem = Path(filepath).stem
    if " - " in stem:
        artist, rest = stem.split(" - ", 1)
        m = re.search(r"\(([^)]+)\)\s*$", rest.strip())
        if m:
            title = rest[:m.start()].strip()
            mix = m.group(1)
        else:
            title = rest.strip()
            mix = ""
    else:
        artist = ""
        title = stem
        mix = ""
    return artist, title, mix


def search_cover_art(artist: str, title: str) -> tuple[bytes | None, str | None]:
    """Search Spotify then Discogs for cover art.

    Returns (art_bytes, source_name) or (None, None).
    """
    search_artist = artist if artist else title
    spotify = spotify_search(search_artist, title)
    if spotify and spotify.get("art_url"):
        art_data = fetch_art(spotify["art_url"])
        if art_data:
            return art_data, "Spotify"

    discogs_url = discogs_search_art(search_artist, title)
    if discogs_url:
        art_data = fetch_art(discogs_url)
        if art_data:
            return art_data, "Discogs"

    return None, None


# ── IMPORT STATISTICS ──────────────────────────────────────────────────────

@dataclasses.dataclass
class ImportStats:
    """Tracks statistics during an import operation."""
    total_files: int = 0
    imported: int = 0
    skipped_existing: int = 0
    skipped_parse_fail: int = 0
    skipped_other: int = 0
    cover_spotify: int = 0
    cover_discogs: int = 0
    cover_none: int = 0
    genres: dict = dataclasses.field(default_factory=dict)
    errors: int = 0
    total_bytes_copied: int = 0
    start_time: float = 0.0


def print_import_summary(stats: ImportStats) -> None:
    """Print a formatted summary of the import operation."""
    elapsed = time.time() - stats.start_time
    mins, secs = divmod(int(elapsed), 60)

    print("\n── Import Summary ─────────────────────────────────")
    print(f"  Files scanned:      {stats.total_files}")
    print(f"  Imported:           {stats.imported}")
    if stats.skipped_existing:
        print(f"  Skipped (exists):   {stats.skipped_existing}")
    if stats.skipped_parse_fail:
        print(f"  Skipped (no parse): {stats.skipped_parse_fail}")
    if stats.skipped_other:
        print(f"  Skipped (other):    {stats.skipped_other}")
    if stats.errors:
        print(f"  Errors:             {stats.errors}")

    print()
    print(f"  Cover art:  Spotify {stats.cover_spotify}  |  "
          f"Discogs {stats.cover_discogs}  |  None {stats.cover_none}")

    if stats.genres:
        top = sorted(stats.genres.items(), key=lambda x: -x[1])[:5]
        genre_str = ", ".join(f"{g} ({c})" for g, c in top)
        print(f"  Top genres: {genre_str}")

    if stats.total_bytes_copied > 0:
        size_mb = stats.total_bytes_copied / (1024 * 1024)
        if size_mb >= 1024:
            print(f"  Total copied:       {size_mb / 1024:.1f} GB")
        else:
            print(f"  Total copied:       {size_mb:.0f} MB")

    print(f"  Duration:           {mins}m {secs:02d}s")
    print("───────────────────────────────────────────────────")


# ── MAIN PROCESSING ────────────────────────────────────────────────────────

def process_file(filepath: str | Path, library_dir: Path, dry_run: bool = False,
                  gemini_names: dict | None = None, source_folder: Path | None = None,
                  stats: ImportStats | None = None, convert_flac: bool = False) -> None:
    """Process a single music file: parse, tag, fetch art, copy."""
    filepath = Path(filepath)
    ext = filepath.suffix.lower()

    if ext not in AUDIO_EXTENSIONS:
        return

    print(f"\n  {filepath.name}")

    # 1. Try existing tags first as a reference
    existing_artist = ""
    existing_title = ""
    try:
        mf_read = mediafile.MediaFile(str(filepath))
        existing_artist = (mf_read.artist or "").strip()
        existing_title = (mf_read.title or "").strip()
    except Exception as e:
        print(f"    Warning: couldn't read existing tags: {e}")

    # 2. Parse filename — prefer Gemini result if available
    if gemini_names and str(filepath) in gemini_names:
        artist, title, mix = gemini_names[str(filepath)]
        print(f"    Gemini:  {artist} — {title}" + (f" ({mix})" if mix else ""))
    else:
        artist, title, mix = parse_filename(filepath)

    # If filename parse found no artist, try the existing tag
    if not artist and existing_artist:
        artist = existing_artist
        print(f"    Artist from tags: {artist}")

    print(f"    Parsed: {artist} — {title}" + (f" ({mix})" if mix else ""))

    if not title:
        print("    Skipping: couldn't parse title")
        if stats:
            stats.skipped_parse_fail += 1
        return

    # 3. Search for cover art + genre (Spotify first, Discogs fallback)
    art_data = None
    genre = UNSORTED_GENRE
    search_artist = artist if artist else title
    spotify = spotify_search(search_artist, title)
    if spotify:
        if spotify["art_url"]:
            art_data = fetch_art(spotify["art_url"])
            if art_data:
                print(f"    Cover art: Spotify")
                if stats:
                    stats.cover_spotify += 1
        if spotify["genre"]:
            genre = spotify["genre"]
            print(f"    Genre: {genre}")
        # Fill in artist from Spotify if we don't have one and the title matches
        if not artist and spotify.get("spotify_artist") and spotify.get("spotify_title"):
            sp_title = spotify["spotify_title"].lower()
            if title.lower() in sp_title or sp_title in title.lower():
                artist = spotify["spotify_artist"]
                print(f"    Artist from Spotify: {artist}")
            else:
                print(f"    Spotify match title mismatch: \"{spotify['spotify_title']}\" — not using artist")
    else:
        print("    Spotify: no match")

    # Discogs fallback for art if Spotify didn't have it
    if not art_data:
        discogs_url = discogs_search_art(search_artist, title)
        if discogs_url:
            art_data = fetch_art(discogs_url)
            if art_data:
                print(f"    Cover art: Discogs")
                if stats:
                    stats.cover_discogs += 1

    if not art_data:
        print("    Cover art: none found")
        if stats:
            stats.cover_none += 1

    # 3. Build destination path — preserve subfolder structure from source
    safe_artist = safe_filename(artist) if artist else "Unknown Artist"
    safe_title = safe_filename(title)
    if mix:
        dest_name = f"{safe_artist} - {safe_title} ({safe_filename(mix)}){ext}"
    else:
        dest_name = f"{safe_artist} - {safe_title}{ext}"

    # Preserve subfolder structure: if file was in source/SubFolder/track.mp3
    # it goes to library/SubFolder/Artist - Title.mp3
    if source_folder:
        rel_parent = filepath.parent.relative_to(source_folder)
        dest_dir = library_dir / rel_parent
    else:
        dest_dir = library_dir

    dest_path = dest_dir / dest_name

    if dest_dir != library_dir:
        print(f"    -> {rel_parent / dest_name}")
    else:
        print(f"    -> {dest_name}")

    if dry_run:
        print("    (dry run — not copying)")
        if stats:
            stats.imported += 1
            stats.genres[genre] = stats.genres.get(genre, 0) + 1
        return

    # 4. Copy file (never touch original)
    dest_dir.mkdir(parents=True, exist_ok=True)
    if dest_path.exists():
        print("    Already exists — skipping")
        if stats:
            stats.skipped_existing += 1
        return
    shutil.copy2(filepath, dest_path)
    if stats:
        stats.imported += 1
        stats.total_bytes_copied += filepath.stat().st_size
        stats.genres[genre] = stats.genres.get(genre, 0) + 1

    # 4b. Convert FLAC to MP3 in-place if requested
    if convert_flac and ext == ".flac":
        mp3_dest = dest_path.with_suffix(".mp3")
        if mp3_dest.exists():
            print(f"    MP3 already exists — skipping conversion")
        else:
            print(f"    Converting to 320kbps MP3...", end="", flush=True)
            flac_size = dest_path.stat().st_size
            if convert_flac_to_mp3(dest_path, mp3_dest):
                mp3_size = mp3_dest.stat().st_size
                print(f"  {C_DIM}[{flac_size//(1024*1024)}MB → {mp3_size//(1024*1024)}MB]{C_RESET}")
                dest_path.unlink()
                dest_path = mp3_dest
            else:
                print(f"  {C_RED}failed — keeping FLAC{C_RESET}")

    # 5. Write tags to the COPY — always strip old art, write fresh
    try:
        mf = mediafile.MediaFile(str(dest_path))
        if artist:
            mf.artist = artist
        mf.title = title + (f" ({mix})" if mix else "")
        mf.genre = genre
        # Always remove existing art (promo images, low-res junk, etc.)
        mf.art = None
        # Then embed the fresh art we found
        if art_data:
            mf.art = art_data
        mf.save()
    except Exception as e:
        print(f"    Warning: couldn't write tags: {e}")
        if stats:
            stats.errors += 1


def process_folder(folder: str | Path, library_dir: Path, dry_run: bool = False,
                    use_gemini: bool = False, convert_flac: bool = False) -> None:
    """Process all music files in a folder (recursive)."""
    folder = Path(folder)
    files = find_audio_files(folder)

    if not files:
        print(f"No music files found in {folder}")
        return

    print(f"Found {len(files)} music file(s) in {folder}")
    if dry_run:
        print("DRY RUN — no files will be copied or modified\n")

    # Run Gemini AI name fixing before processing (better names = better Spotify search)
    gemini_names = {}
    if use_gemini:
        print("\n── Gemini AI Name Fixing ──")
        gemini_names = gemini_fix_names([str(f) for f in files])
        print(f"  Gemini fixed {len(gemini_names)}/{len(files)} filenames\n")

    stats = ImportStats(total_files=len(files), start_time=time.time())

    for i, f in enumerate(files, 1):
        _progress(i, len(files))
        process_file(f, library_dir, dry_run, gemini_names, source_folder=folder, stats=stats, convert_flac=convert_flac)

    print_import_summary(stats)
    print(f"Library: {library_dir}")


def fix_covers(library_dir: str | Path, dry_run: bool = False) -> None:
    """Re-scan library files that have no album cover and try to fetch one."""
    library_dir = Path(library_dir)
    files = find_audio_files(library_dir)

    if not files:
        print(f"No music files found in {library_dir}")
        return

    # Find files missing art
    missing = []
    for f in files:
        try:
            mf = mediafile.MediaFile(str(f))
            if not mf.art:
                missing.append(f)
        except Exception as e:
            print(f"    Warning: couldn't read {f.name}: {e}")
            missing.append(f)

    print(f"Found {len(missing)} file(s) missing cover art (out of {len(files)} total)")
    if not missing:
        print("All files have covers!")
        return
    if dry_run:
        print("DRY RUN — no files will be modified\n")

    for i, filepath in enumerate(missing, 1):
        _progress(i, len(missing))
        artist, title, _mix = parse_library_filename(filepath)

        print(f"\n  {filepath.name}")

        art_data, source = search_cover_art(artist, title)
        if art_data:
            print(f"    Cover art: {source}")
        else:
            print(f"    Still no cover found")
            continue

        if dry_run:
            print(f"    (dry run — not writing)")
            continue

        # Write art directly to the library file
        try:
            mf = mediafile.MediaFile(str(filepath))
            mf.art = art_data
            mf.save()
            print(f"    Cover embedded!")
        except Exception as e:
            print(f"    Warning: couldn't write art: {e}")

    print(f"\nDone! Library: {library_dir}")


def fix_tags(library_dir: str | Path, dry_run: bool = False) -> None:
    """Update album/year/genre tags from Spotify without changing title/artist/duration.

    Safe to update: album, album artist, year, genre, cover art
    Never touches: title, artist, duration, BPM, key, track number
    """
    library_dir = Path(library_dir)
    files = find_audio_files(library_dir)

    if not files:
        print(f"No music files found in {library_dir}")
        return

    print(f"Found {len(files)} file(s) in {library_dir}")
    if dry_run:
        print("DRY RUN — no files will be modified\n")

    updated = 0
    skipped = 0

    for i, filepath in enumerate(files, 1):
        _progress(i, len(files))
        artist, title_clean, _mix = parse_library_filename(filepath)

        print(f"\n  {filepath.name}")

        # Search Spotify — use clean title (without mix) for better results
        search_artist = artist if artist else title_clean
        spotify = spotify_search(search_artist, title_clean)

        if not spotify:
            print(f"    Spotify: no match — skipping")
            skipped += 1
            continue

        # Read current tags
        try:
            mf = mediafile.MediaFile(str(filepath))
        except Exception as e:
            print(f"    Error reading file: {e}")
            skipped += 1
            continue

        # Determine what needs updating
        changes = []

        if spotify.get("album_name") and mf.album != spotify["album_name"]:
            changes.append(("album", mf.album, spotify["album_name"]))

        if spotify.get("album_artist") and mf.albumartist != spotify["album_artist"]:
            changes.append(("album artist", mf.albumartist, spotify["album_artist"]))

        if spotify.get("year") and spotify["year"] != "0000":
            try:
                year = int(spotify["year"])
                if mf.year != year:
                    changes.append(("year", mf.year, year))
            except (ValueError, TypeError):
                pass

        if spotify.get("genre") and mf.genre != spotify["genre"]:
            changes.append(("genre", mf.genre, spotify["genre"]))

        # Check if art is missing or suspiciously small (likely an ad/placeholder)
        art_data = None
        if spotify.get("art_url"):
            needs_art = not mf.art
            if mf.art and len(mf.art) < ART_JUNK_THRESHOLD:
                needs_art = True
                changes.append(("art", f"replacing junk ({len(mf.art)} bytes)", "Spotify art"))
            elif needs_art:
                changes.append(("art", "missing", "Spotify art"))

            if needs_art:
                art_data = fetch_art(spotify["art_url"])

        if not changes:
            print(f"    Tags already good")
            continue

        for field, old, new in changes:
            print(f"    {field}: {old or '(empty)'} → {new}")

        if dry_run:
            print(f"    (dry run — not writing)")
            continue

        # Write only the safe tags
        try:
            if spotify.get("album_name"):
                mf.album = spotify["album_name"]
            if spotify.get("album_artist"):
                mf.albumartist = spotify["album_artist"]
            if spotify.get("year") and spotify["year"] != "0000":
                try:
                    mf.year = int(spotify["year"])
                except (ValueError, TypeError):
                    pass
            if spotify.get("genre"):
                mf.genre = spotify["genre"]
            if art_data:
                mf.art = art_data
            mf.save()
            print(f"    Tags updated!")
            updated += 1
        except Exception as e:
            print(f"    Warning: couldn't write tags: {e}")
            skipped += 1

    print(f"\nDone! Updated {updated} file(s), skipped {skipped}. Library: {library_dir}")


def remove_duplicates(library_dir: str | Path, dry_run: bool = False) -> None:
    """Find and remove duplicate tracks, keeping the highest-quality version.

    Duplicates are identified by normalised artist + title (case-insensitive,
    punctuation-stripped). When duplicates exist, the best format/size wins:
    FLAC > AIFF/AIF/WAV > MP3/M4A > OGG/OPUS, then largest file as tiebreaker.
    """
    library_dir = Path(library_dir)
    files = find_audio_files(library_dir)

    if not files:
        print(f"No music files found in {library_dir}")
        return

    def normalise_key(filepath):
        """Extract a normalised (artist, title) key from a library filename."""
        artist, title, _mix = parse_library_filename(filepath)
        # Normalise: lowercase, strip punctuation, collapse whitespace
        def norm(s):
            s = s.lower()
            s = re.sub(r"[^\w\s]", "", s)
            s = re.sub(r"\s+", " ", s).strip()
            return s
        return (norm(artist), norm(title))

    def quality_score(filepath):
        """Lower score = higher quality. Used to pick the keeper."""
        fmt = FORMAT_RANK.get(filepath.suffix.lower(), 99)
        size = filepath.stat().st_size
        # Negate size so larger = better (lower score when negated)
        return (fmt, -size)

    # Group files by normalised key
    groups: dict = {}
    for f in files:
        key = normalise_key(f)
        groups.setdefault(key, []).append(f)

    # Find groups with more than one file
    dupes = {k: v for k, v in groups.items() if len(v) > 1}

    if not dupes:
        print(f"No duplicates found in {library_dir} ({len(files)} tracks scanned)")
        return

    total_dupes = sum(len(v) - 1 for v in dupes.values())
    print(f"Found {len(dupes)} duplicate group(s) — {total_dupes} file(s) to remove\n")
    if dry_run:
        print("DRY RUN — no files will be deleted\n")

    removed = 0
    freed = 0

    sorted_dupes = sorted(dupes.items())
    for i, ((artist, title), group) in enumerate(sorted_dupes, 1):
        _progress(i, len(sorted_dupes))
        # Sort by quality — first entry is the keeper
        group.sort(key=quality_score)
        keeper = group[0]
        to_delete = group[1:]

        display_artist = artist.title() if artist else "Unknown Artist"
        display_title = title.title()
        print(f"  {display_artist} — {display_title}")
        print(f"    {C_GREEN}keep{C_RESET}   {keeper.name}  {C_DIM}({keeper.suffix[1:].upper()}, {keeper.stat().st_size // 1024}KB){C_RESET}")

        for f in to_delete:
            size_kb = f.stat().st_size // 1024
            print(f"    {C_RED}delete{C_RESET} {f.name}  {C_DIM}({f.suffix[1:].upper()}, {size_kb}KB){C_RESET}")
            if not dry_run:
                freed += f.stat().st_size
                f.unlink()
                removed += 1
            else:
                freed += f.stat().st_size
        print()

    freed_mb = freed / (1024 * 1024)
    if dry_run:
        print(f"Would remove {total_dupes} file(s), freeing ~{freed_mb:.1f}MB")
    else:
        print(f"Removed {removed} file(s), freed {freed_mb:.1f}MB. Library: {library_dir}")


# ── CLEAN SOURCE FOLDER ─────────────────────────────────────────────────────

def clean_source_folder(source_dir: str | Path, library_dir: str | Path,
                         dry_run: bool = False) -> None:
    """Find source files already in the library and offer to delete them.

    Matches are identified by normalised artist + title (same algorithm as
    remove_duplicates). Requires explicit 'YES' confirmation before deleting.
    """
    source_dir = Path(source_dir).resolve()
    library_dir = Path(library_dir).resolve()

    # Safety: refuse if source folder is the library or inside it
    try:
        source_dir.relative_to(library_dir)
        print(f"{C_RED}Error: source folder is inside the library — refusing to delete.{C_RESET}")
        return
    except ValueError:
        pass  # not relative — that's fine

    source_files = find_audio_files(source_dir)
    if not source_files:
        print(f"  No audio files found in {source_dir}")
        return

    library_files = find_audio_files(library_dir)

    def norm(s: str) -> str:
        s = s.lower()
        s = re.sub(r"[^\w\s]", "", s)
        s = re.sub(r"\s+", " ", s).strip()
        return s

    # Build library lookup: normalised (artist, title) keys and stem set
    lib_keys: set[tuple[str, str]] = set()
    lib_stems: set[str] = set()
    for lf in library_files:
        artist, title, _mix = parse_library_filename(lf)
        lib_keys.add((norm(artist), norm(title)))
        lib_stems.add(norm(lf.stem))

    # Find source files matching something in the library
    matches: list[tuple[Path, str]] = []  # (source_file, match_description)
    for sf in source_files:
        artist, title, _mix = parse_filename(sf)
        if not title:
            continue  # skip unparseable files

        key = (norm(artist), norm(title))
        if key in lib_keys:
            match_desc = f"{artist} — {title}" if artist else title
            matches.append((sf, match_desc))
        elif norm(sf.stem) in lib_stems:
            matches.append((sf, sf.stem))

    if not matches:
        print(f"  No source files found in library (scanned {len(source_files)} files)")
        return

    total_bytes = sum(sf.stat().st_size for sf, _ in matches)
    total_mb = total_bytes / (1024 * 1024)
    size_str = f"{total_mb / 1024:.2f} GB" if total_mb >= 1024 else f"{total_mb:.1f} MB"

    print(f"\n  Scanning {source_dir}...")
    print(f"  Found {len(matches)} files already in library ({size_str})\n")
    for sf, match_desc in matches:
        size_kb = sf.stat().st_size // 1024
        print(f"    {C_DIM}{sf.name}{C_RESET}  →  matches: {match_desc}  {C_DIM}({size_kb} KB){C_RESET}")
    print()

    if dry_run:
        print(f"  {C_DIM}DRY RUN — would delete {len(matches)} files, freeing {size_str}{C_RESET}")
        return

    confirm = _prompt(f"  {C_YELLOW}Type YES to delete {len(matches)} source files: {C_RESET}").strip()
    if confirm != "YES":
        print(f"  {C_DIM}Cancelled.{C_RESET}")
        return

    deleted = 0
    freed = 0
    for i, (sf, _) in enumerate(matches, 1):
        _progress(i, len(matches))
        try:
            freed += sf.stat().st_size
            sf.unlink()
            deleted += 1
        except Exception as e:
            print(f"  {C_RED}Error deleting {sf.name}: {e}{C_RESET}")

    freed_mb = freed / (1024 * 1024)
    freed_str = f"{freed_mb / 1024:.2f} GB" if freed_mb >= 1024 else f"{freed_mb:.1f} MB"
    print(f"  {C_GREEN}Deleted {deleted} files, freed {freed_str}{C_RESET}")


# ── FLAC TO MP3 CONVERSION ──────────────────────────────────────────────────

def check_ffmpeg() -> bool:
    """Return True if FFmpeg is installed and accessible."""
    try:
        subprocess.run(["ffmpeg", "-version"], capture_output=True, timeout=10)
        return True
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


def convert_flac_to_mp3(src: Path, dest: Path) -> bool:
    """Convert a FLAC file to 320kbps CBR MP3 using FFmpeg.

    Preserves all metadata tags and cover art via -map_metadata 0.
    Returns True on success. Never touches the source file.
    """
    try:
        result = subprocess.run(
            [
                "ffmpeg", "-i", str(src),
                "-codec:a", "libmp3lame", "-b:a", "320k",
                "-map_metadata", "0", "-id3v2_version", "3",
                "-y",
                str(dest),
            ],
            capture_output=True, timeout=300,
        )
        if result.returncode != 0:
            return False
        if not dest.exists() or dest.stat().st_size == 0:
            return False
        # Quick sanity check: mediafile can open the output
        try:
            mediafile.MediaFile(str(dest))
        except Exception:
            dest.unlink(missing_ok=True)
            return False
        return True
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return False


def batch_convert_flac(library_dir: str | Path, dry_run: bool = False,
                        keep_original: bool = False) -> None:
    """Convert all FLAC files in the library to 320kbps MP3.

    Skips files where an MP3 copy already exists. Deletes the FLAC after a
    verified successful conversion (unless keep_original=True).
    """
    library_dir = Path(library_dir)

    if not check_ffmpeg():
        print(f"{C_RED}FFmpeg not found. Install with: brew install ffmpeg{C_RESET}")
        return

    flac_files = [f for f in find_audio_files(library_dir) if f.suffix.lower() == ".flac"]
    if not flac_files:
        print(f"  No FLAC files found in {library_dir}")
        return

    total = len(flac_files)
    print(f"  Converting FLACs to 320kbps MP3...")
    if dry_run:
        print(f"  DRY RUN — no files will be converted\n")

    converted = 0
    skipped = 0
    total_saved = 0

    for i, flac_path in enumerate(flac_files, 1):
        _progress(i, total)
        mp3_path = flac_path.with_suffix(".mp3")
        flac_size = flac_path.stat().st_size
        flac_mb = flac_size / (1024 * 1024)

        if mp3_path.exists():
            print(f"  {i}/{total}  {C_DIM}{flac_path.name} → MP3 already exists, skipping{C_RESET}")
            skipped += 1
            continue

        if dry_run:
            est_mp3_mb = flac_mb * 0.28
            print(f"  {i}/{total}  {flac_path.name} → .mp3  {C_DIM}[{flac_mb:.1f} MB → ~{est_mp3_mb:.1f} MB]{C_RESET}")
            converted += 1
            total_saved += int(flac_size * 0.72)
            continue

        print(f"  {i}/{total}  {flac_path.name} → .mp3", end="", flush=True)
        if convert_flac_to_mp3(flac_path, mp3_path):
            mp3_size = mp3_path.stat().st_size
            saved_mb = (flac_size - mp3_size) / (1024 * 1024)
            print(f"  {C_DIM}[{flac_mb:.1f} MB → {mp3_size / (1024*1024):.1f} MB]{C_RESET}  {C_GREEN}✓{C_RESET}")
            if not keep_original:
                flac_path.unlink()
            total_saved += flac_size - mp3_size
            converted += 1
        else:
            print(f"  {C_RED}✗ failed{C_RESET}")
            mp3_path.unlink(missing_ok=True)
            skipped += 1

    saved_mb = total_saved / (1024 * 1024)
    saved_str = f"{saved_mb / 1024:.2f} GB" if saved_mb >= 1024 else f"{saved_mb:.1f} MB"
    if dry_run:
        print(f"\n  Would convert {converted} file(s), estimated savings: {saved_str}")
    else:
        print(f"\n  Converted {converted} file(s), saved {saved_str}. Skipped {skipped}.")


# ── GENRE ORGANIZATION VIA GEMINI AI ─────────────────────────────────────────

ELECTRONIC_GENRES = [
    "House", "Deep House", "Tech House", "Progressive House", "Afro House",
    "Techno", "Hard Techno", "Minimal Techno", "Acid Techno",
    "Drum & Bass", "Liquid DnB", "Jump Up", "Neurofunk",
    "Trance", "Progressive Trance", "Psytrance", "Uplifting Trance",
    "Breaks", "Breakbeat", "UK Garage", "Speed Garage",
    "Dubstep", "Riddim", "Future Bass",
    "Electro", "Electronica", "Downtempo", "Ambient",
    "Disco", "Nu-Disco", "Italo Disco",
    "Hardcore", "Hardstyle",
    "Other",
]
_GENRE_LOOKUP = {g.lower(): g for g in ELECTRONIC_GENRES}

GEMINI_GENRE_PROMPT = f"""You are a music genre classifier for an electronic/dance music DJ library.

Classify each track into exactly ONE genre from this list:
{", ".join(ELECTRONIC_GENRES)}

Each track has: artist, title, mix, and a "hints" field containing genre data from Spotify and Discogs APIs.

IMPORTANT — classification priority:
1. When hints are provided, use them as your PRIMARY signal. Map the hint genres/styles to the closest match in the list above.
2. Use your own music knowledge only to break ties or when hints are empty.
3. Hints like "Tribal, Techno" from Discogs are very reliable — trust them.

Rules:
- Pick exactly ONE genre from the list for each track
- Prefer specific subgenres over broad parent genres (e.g. "Deep House" over "House")
- Use "Other" only when no genre in the list fits and hints are empty
- Return ONLY a JSON array in input order

Example:
Input: [{{"artist": "Fisher", "title": "Losing It", "mix": "", "hints": "spotify: Tech House"}}, {{"artist": "Leod", "title": "Vortex", "mix": "", "hints": "discogs: Techno, Tribal"}}]
Output: [{{"genre": "Tech House"}}, {{"genre": "Techno"}}]

Classify these tracks:
"""


def gemini_classify_genres(tracks: list[tuple[str, str, str]],
                            hints: list[str] | None = None) -> list[str]:
    """Classify tracks into electronic music genres using Gemini AI.

    Takes a list of (artist, title, mix) tuples and optional genre hints
    from Spotify/Discogs APIs. Returns a list of genre strings (same length
    as input). Tracks that fail classification get "Other".
    """
    if not GEMINI_API_KEY:
        print("  Gemini API key not set — cannot classify genres")
        return []

    results: list[str] = ["Other"] * len(tracks)
    total_batches = (len(tracks) + GEMINI_BATCH_SIZE - 1) // GEMINI_BATCH_SIZE

    for i in range(0, len(tracks), GEMINI_BATCH_SIZE):
        batch = tracks[i:i + GEMINI_BATCH_SIZE]
        batch_hints = hints[i:i + GEMINI_BATCH_SIZE] if hints else [""] * len(batch)
        batch_dicts = [
            {"artist": a, "title": t, "mix": m, "hints": h}
            for (a, t, m), h in zip(batch, batch_hints)
        ]
        batch_num = i // GEMINI_BATCH_SIZE + 1

        print(f"  Batch {batch_num}/{total_batches}: {len(batch)} tracks...", end="", flush=True)

        try:
            resp = requests.post(
                f"https://generativelanguage.googleapis.com/v1beta/models/"
                f"{GEMINI_MODEL}:generateContent",
                params={"key": GEMINI_API_KEY},
                headers={"Content-Type": "application/json"},
                json={
                    "contents": [{"parts": [{"text": GEMINI_GENRE_PROMPT + json.dumps(batch_dicts)}]}],
                    "generationConfig": {
                        "responseMimeType": "application/json",
                        "temperature": 0.1,
                    },
                },
                timeout=30,
            )
            resp.raise_for_status()

            text = resp.json()["candidates"][0]["content"]["parts"][0]["text"]
            parsed = json.loads(text)

            if not isinstance(parsed, list) or len(parsed) != len(batch):
                print(f"  {C_RED}mismatch ({len(parsed) if isinstance(parsed, list) else '?'}"
                      f" results for {len(batch)} tracks){C_RESET}")
                continue

            for j, entry in enumerate(parsed):
                if isinstance(entry, dict) and "genre" in entry:
                    raw = entry["genre"].strip()
                    canonical = _GENRE_LOOKUP.get(raw.lower())
                    results[i + j] = canonical if canonical else "Other"

            print(f"  {C_GREEN}classified{C_RESET}")

        except requests.exceptions.HTTPError as e:
            print(f"  {C_RED}API error: {e}{C_RESET}")
            if hasattr(e, 'response') and e.response is not None:
                try:
                    print(f"    {e.response.json().get('error', {}).get('message', '')}")
                except Exception:
                    pass
        except (json.JSONDecodeError, KeyError, IndexError) as e:
            print(f"  {C_RED}parse error: {e}{C_RESET}")
        except Exception as e:
            print(f"  {C_RED}failed: {e}{C_RESET}")

    return results


def ai_genre_tag(library_dir: str | Path, dry_run: bool = False,
                  organize: bool = False) -> None:
    """Classify library tracks by genre using Gemini AI and update tags.

    If organize=True, also moves files into genre subfolders under library_dir.
    """
    library_dir = Path(library_dir)

    if not GEMINI_API_KEY:
        print(f"  {C_RED}Gemini API key not set — configure in Settings (s){C_RESET}")
        return

    files = find_audio_files(library_dir)
    if not files:
        print(f"  No audio files found in {library_dir}")
        return

    # Parse all library filenames into (artist, title, mix) tuples
    tracks: list[tuple[str, str, str]] = []
    for f in files:
        artist, title, mix = parse_library_filename(f)
        tracks.append((artist, title, mix))

    total = len(files)
    print(f"\n  AI Genre Tagging (Gemini + Spotify/Discogs)")

    # Enrich tracks with genre hints from Spotify and Discogs
    print(f"  Collecting genre data from APIs for {total} tracks...\n")
    hints: list[str] = []
    spotify_genre_cache: dict[str, str | None] = {}  # artist name → genre

    for i, (artist, title, mix) in enumerate(tracks, 1):
        _progress(i, total)
        hint_parts: list[str] = []

        # Spotify — cache by artist (genres are artist-level)
        sp_genre = None
        cache_key = artist.lower().strip() if artist else ""
        if cache_key and cache_key in spotify_genre_cache:
            sp_genre = spotify_genre_cache[cache_key]
        else:
            sp = spotify_search(artist if artist else title, title)
            sp_genre = sp.get("genre") if sp else None
            if cache_key:
                spotify_genre_cache[cache_key] = sp_genre

        if sp_genre:
            hint_parts.append(f"spotify: {sp_genre}")

        # Discogs fallback — only when Spotify has no genre
        if not sp_genre:
            dc_genre = discogs_search_genre(artist, title)
            if dc_genre:
                hint_parts.append(f"discogs: {dc_genre}")

        hints.append(" | ".join(hint_parts))

        if i % 20 == 0 or i == total:
            enriched = sum(1 for h in hints if h)
            print(f"    {i}/{total} looked up ({enriched} with genre data)")

    enriched_count = sum(1 for h in hints if h)
    print(f"\n  API coverage: {enriched_count}/{total} tracks have genre hints")

    total_batches = (total + GEMINI_BATCH_SIZE - 1) // GEMINI_BATCH_SIZE
    print(f"  Classifying {total} tracks in {total_batches} batch(es)...\n")

    # Classify via Gemini with API hints
    genres = gemini_classify_genres(tracks, hints=hints)
    if not genres:
        return

    # Genre breakdown
    genre_counts: dict[str, int] = {}
    for g in genres:
        genre_counts[g] = genre_counts.get(g, 0) + 1

    print(f"\n  Genre breakdown:")
    for genre, count in sorted(genre_counts.items(), key=lambda x: -x[1]):
        bar = "█" * min(count, 40)
        print(f"    {genre:<22} {count:>4}  {C_DIM}{bar}{C_RESET}")

    # Write genre tags
    if dry_run:
        print(f"\n  {C_DIM}DRY RUN — no files will be modified{C_RESET}")
    else:
        print(f"\n  Writing genre tags...")
        tagged = 0
        for i, (filepath, genre) in enumerate(zip(files, genres), 1):
            _progress(i, len(files))
            try:
                mf = mediafile.MediaFile(str(filepath))
                if mf.genre != genre:
                    mf.genre = genre
                    mf.save()
                    tagged += 1
            except Exception as e:
                print(f"    {C_RED}Error tagging {filepath.name}: {e}{C_RESET}")
        print(f"  Updated {tagged} file(s)")

    # Organize into genre subfolders
    if not organize:
        return

    if dry_run:
        print(f"\n  {C_DIM}DRY RUN — would organize into genre folders:{C_RESET}")
    else:
        print(f"\n  Organizing into genre folders...")

    moved = 0
    skipped = 0
    undo_actions: list[dict] = []
    # Reset progress counter so the organize phase has its own clock/ETA
    if _active_loader is not None:
        _active_loader.reset_progress()
    for i, (filepath, genre) in enumerate(zip(files, genres), 1):
        _progress(i, len(files))
        genre_dir = library_dir / safe_filename(genre)
        dest = genre_dir / filepath.name

        # Already in the correct genre folder
        if filepath.parent == genre_dir:
            continue

        # Name collision at destination
        if dest.exists():
            print(f"    {C_YELLOW}skip{C_RESET} {filepath.name} — already exists in {genre}/")
            skipped += 1
            continue

        if dry_run:
            print(f"    {C_DIM}{filepath.name} → {genre}/{C_RESET}")
            moved += 1
        else:
            genre_dir.mkdir(parents=True, exist_ok=True)
            shutil.move(str(filepath), str(dest))
            undo_actions.append({"type": "move", "src": str(filepath), "dest": str(dest)})
            moved += 1

    if dry_run:
        print(f"\n  Would move {moved} file(s) into genre folders")
    else:
        _save_undo("Genre organize", undo_actions)
        print(f"  Moved {moved} file(s), skipped {skipped}")


# ── FAKE BITRATE DETECTION ─────────────────────────────────────────────────

# Lossy formats to scan by default
LOSSY_EXTENSIONS = {".mp3", ".m4a", ".ogg", ".opus"}
# Lossless formats scanned only with --lossless flag
LOSSLESS_EXTENSIONS = {".flac", ".wav", ".aiff", ".aif"}

# Expected frequency cutoffs (kHz) for each bitrate tier.
# Based on LAME encoder defaults; ranges account for encoder variance.
# (min_ok, suspect_below, fake_below)
BITRATE_THRESHOLDS = {
    320: (19.5, 19.5, 18.0),
    256: (19.0, 19.0, 17.0),
    224: (18.5, 18.5, 16.5),
    192: (17.0, 17.0, 15.5),
    160: (16.5, 16.5, 15.0),
    128: (15.5, 15.5, 14.0),
}

# For lossless files, we expect full-spectrum audio
LOSSLESS_THRESHOLDS = (20.0, 20.0, 18.0)

# Reverse lookup: cutoff → approximate true source bitrate
CUTOFF_TO_BITRATE = [
    (20.0, "320kbps"),
    (19.0, "256kbps"),
    (17.5, "192kbps"),
    (16.5, "160kbps"),
    (15.5, "128kbps"),
    (14.0, "96kbps"),
    (0.0,  "<96kbps"),
]


def _check_numpy():
    """Check if numpy is available. Returns the module or None."""
    try:
        import numpy
        return numpy
    except ImportError:
        return None


def _get_duration_secs(filepath: Path) -> float | None:
    """Get audio duration in seconds via ffprobe."""
    try:
        result = subprocess.run(
            ["ffprobe", "-v", "quiet", "-show_entries", "format=duration",
             "-of", "csv=p=0", str(filepath)],
            capture_output=True, text=True, timeout=15,
        )
        return float(result.stdout.strip())
    except (subprocess.TimeoutExpired, FileNotFoundError, ValueError):
        return None


def _get_sample_rate(filepath: Path) -> int | None:
    """Get the sample rate of an audio file via ffprobe."""
    try:
        result = subprocess.run(
            ["ffprobe", "-v", "quiet", "-select_streams", "a:0",
             "-show_entries", "stream=sample_rate",
             "-of", "csv=p=0", str(filepath)],
            capture_output=True, text=True, timeout=15,
        )
        return int(result.stdout.strip())
    except (subprocess.TimeoutExpired, FileNotFoundError, ValueError):
        return None


def decode_to_pcm(filepath: Path, offset_secs: float, duration_secs: float,
                   sample_rate: int = 44100) -> bytes | None:
    """Decode a segment of audio to raw PCM via FFmpeg.

    Returns raw bytes (32-bit float, little-endian, mono) or None on failure.
    """
    try:
        result = subprocess.run(
            ["ffmpeg", "-ss", str(offset_secs), "-i", str(filepath),
             "-t", str(duration_secs),
             "-f", "f32le", "-acodec", "pcm_f32le",
             "-ac", "1", "-ar", str(sample_rate),
             "-v", "quiet", "pipe:1"],
            capture_output=True, timeout=30,
        )
        if result.returncode != 0 or len(result.stdout) == 0:
            return None
        return result.stdout
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return None


def _estimate_true_bitrate(cutoff_khz: float) -> str:
    """Reverse-lookup: given a frequency cutoff, estimate the true source bitrate."""
    for threshold, label in CUTOFF_TO_BITRATE:
        if cutoff_khz >= threshold:
            return label
    return "<96kbps"


def detect_spectral_cutoff(filepath: Path, np) -> tuple[float, float, bool] | None:
    """Detect fake bitrates by comparing energy between frequency bands.

    Uses Welch's method across 3 segments. The core metric is the energy
    ratio between the 12-16kHz band (present in any 192+kbps encode) and
    the 16-20kHz band (only fully present in true 320kbps / lossless).
    A large ratio means the high-frequency band has much less energy,
    indicating a likely transcode.

    Returns (band_ratio_db, cutoff_khz, success) or None on failure.
    np is the numpy module (passed to avoid repeated imports).
    """
    duration = _get_duration_secs(filepath)
    if duration is None or duration < 15:
        return None

    sample_rate = _get_sample_rate(filepath) or 44100
    fft_size = 8192
    hop = fft_size // 2  # 50% overlap

    # Sample 3 segments at 25%, 50%, 75% of track
    segment_duration = 10.0
    positions = [0.25, 0.50, 0.75]
    segment_results = []

    for pos in positions:
        offset = max(0, duration * pos - segment_duration / 2)
        if offset + segment_duration > duration:
            offset = max(0, duration - segment_duration)

        pcm = decode_to_pcm(filepath, offset, segment_duration, sample_rate)
        if pcm is None or len(pcm) < fft_size * 4:
            continue

        samples = np.frombuffer(pcm, dtype=np.float32)

        # Check for silence / near-zero energy
        if np.max(np.abs(samples)) < 1e-6:
            continue

        # Welch's method: split into overlapping chunks, window, FFT, average power
        window = np.hanning(fft_size)
        num_chunks = (len(samples) - fft_size) // hop + 1
        if num_chunks < 1:
            continue

        power_sum = np.zeros(fft_size // 2 + 1)
        for i in range(num_chunks):
            chunk = samples[i * hop : i * hop + fft_size]
            if len(chunk) < fft_size:
                break
            spectrum = np.fft.rfft(chunk * window)
            power_sum += np.abs(spectrum) ** 2
        power_avg = power_sum / num_chunks

        # Convert to dB
        power_db = 10 * np.log10(power_avg + 1e-10)

        freq_per_bin = sample_rate / fft_size

        # Skip segments where the mid-range is too quiet (breakdown/silence)
        bin_2k = int(2000 / freq_per_bin)
        bin_8k = int(8000 / freq_per_bin)
        ref_level = np.mean(power_db[bin_2k:bin_8k + 1])
        if ref_level < -60:
            continue

        # Band A: 12-16kHz (mid-high — present in 192+kbps encodes)
        bin_12k = int(12000 / freq_per_bin)
        bin_16k = int(16000 / freq_per_bin)
        band_a = np.mean(power_db[bin_12k:bin_16k + 1])

        # Band B: 16-20kHz (high — only fully present in true 320kbps/lossless)
        bin_20k = int(20000 / freq_per_bin)
        band_b = np.mean(power_db[bin_16k:bin_20k + 1])

        # Skip if band_a itself is very quiet relative to mid-range
        if band_a < ref_level - 40:
            continue

        ratio = band_a - band_b  # positive = 16-20kHz is quieter

        # Also find a display cutoff: where the smoothed spectrum drops
        # 30dB below the 2-8kHz reference (for display purposes only)
        smooth_bins = max(1, int(500 / freq_per_bin))
        kernel = np.ones(smooth_bins) / smooth_bins
        smooth_db = np.convolve(power_db, kernel, mode='same')
        drop_threshold = ref_level - 30
        cutoff_bin = int(5000 / freq_per_bin)
        for b in range(min(len(smooth_db) - 2, int(21000 / freq_per_bin)),
                       int(5000 / freq_per_bin), -1):
            if smooth_db[b] > drop_threshold:
                cutoff_bin = b
                break
        cutoff_khz = cutoff_bin * freq_per_bin / 1000.0

        segment_results.append((ratio, cutoff_khz))

    if not segment_results:
        return None

    # Take median of both metrics across segments
    segment_results.sort(key=lambda x: x[0])
    mid = len(segment_results) // 2
    median_ratio = segment_results[mid][0]

    cutoff_values = sorted(r[1] for r in segment_results)
    median_cutoff = cutoff_values[len(cutoff_values) // 2]

    return (median_ratio, median_cutoff, True)


def classify_quality(band_ratio: float, cutoff_khz: float,
                     claimed_bitrate: int, is_lossless: bool,
                     is_vbr: bool) -> tuple[str, int, str]:
    """Classify audio quality based on the energy ratio between frequency bands.

    band_ratio: dB difference between 12-16kHz and 16-20kHz bands.
      Low (< 8dB) = healthy high-frequency content = true high bitrate.
      High (> 15dB) = very little high-frequency content = likely transcode.

    Returns (verdict, confidence_score, estimated_true_bitrate).
    verdict is one of: "OK", "SUSPECT", "LIKELY FAKE"
    confidence_score is 0-100 (100 = definitely OK, 0 = definitely fake).
    """
    # Thresholds for 320kbps / lossless files
    # Based on real-world calibration: true 320kbps files show 3-8dB ratio,
    # transcodes from 128kbps show 15-30dB+, from 192kbps show 10-15dB.
    if is_lossless:
        ok_below = 8.0       # lossless should have even less roll-off
        suspect_above = 8.0
        fake_above = 14.0
    elif claimed_bitrate >= 256:
        ok_below = 10.0
        suspect_above = 10.0
        fake_above = 15.0
    elif claimed_bitrate >= 192:
        ok_below = 14.0
        suspect_above = 14.0
        fake_above = 20.0
    else:
        # 128kbps and below — high-frequency roll-off is expected
        ok_below = 20.0
        suspect_above = 20.0
        fake_above = 30.0

    # VBR tolerance
    if is_vbr and not is_lossless:
        ok_below += 2.0
        suspect_above += 2.0
        fake_above += 2.0

    # Confidence scoring based on band ratio
    if band_ratio <= ok_below:
        # Good: high-frequency content is present
        confidence = 95
    elif band_ratio <= suspect_above + 2:
        # Borderline: some roll-off but could be mastering choice
        ratio = (band_ratio - ok_below) / max(suspect_above + 2 - ok_below, 0.1)
        confidence = 70 - int(ratio * 30)  # 70 → 40
    elif band_ratio <= fake_above:
        # Suspicious: significant high-frequency loss
        ratio = (band_ratio - suspect_above) / max(fake_above - suspect_above, 0.1)
        confidence = 40 - int(ratio * 15)  # 40 → 25
    else:
        # Very likely fake: extreme high-frequency loss
        confidence = max(5, 25 - int((band_ratio - fake_above) / 2))

    confidence = max(0, min(100, confidence))

    # Verdict
    if confidence >= 70:
        verdict = "OK"
    elif confidence >= 40:
        verdict = "SUSPECT"
    else:
        verdict = "LIKELY FAKE"

    true_bitrate = _estimate_true_bitrate(cutoff_khz)

    return (verdict, confidence, true_bitrate)


def analyze_bitrate_quality(library_dir: str | Path,
                            include_lossless: bool = False,
                            target_paths: list[Path] | None = None) -> None:
    """Scan files for fake bitrates using spectral analysis.

    Analyzes lossy files by default. With include_lossless=True, also checks
    FLAC/WAV/AIFF for transcode artifacts.

    If target_paths is provided, only those files/folders are scanned instead
    of the full library.
    """
    np = _check_numpy()
    if np is None:
        print(f"  {C_RED}numpy is required for fake bitrate detection.{C_RESET}")
        print(f"  {C_DIM}Install with: pip install numpy{C_RESET}")
        return

    if not check_ffmpeg():
        print(f"  {C_RED}FFmpeg not found. Install with: brew install ffmpeg{C_RESET}")
        return

    library_dir = Path(library_dir)
    extensions = set(LOSSY_EXTENSIONS)
    if include_lossless:
        extensions |= LOSSLESS_EXTENSIONS

    # Collect files: from target_paths if given, otherwise full library
    if target_paths:
        files = []
        for tp in target_paths:
            tp = Path(tp)
            if tp.is_file() and tp.suffix.lower() in extensions:
                files.append(tp)
            elif tp.is_dir():
                files.extend(f for f in find_audio_files(tp)
                             if f.suffix.lower() in extensions)
            elif tp.is_file():
                print(f"  {C_YELLOW}Skipping {tp.name} — not a supported audio format{C_RESET}")
        files.sort()
    else:
        all_files = find_audio_files(library_dir)
        files = [f for f in all_files if f.suffix.lower() in extensions]

    if not files:
        label = "audio" if include_lossless else "lossy audio"
        print(f"  No {label} files found in {library_dir}")
        return

    total = len(files)
    mode = "audio" if include_lossless else "lossy"
    print(f"\n  Analyzing {mode} file quality ({total} files)...\n")

    results_ok = []
    results_suspect = []
    results_fake = []
    skipped = 0
    start_time = time.time()

    for i, filepath in enumerate(files, 1):
        _progress(i, total)
        name = filepath.name
        suffix = filepath.suffix.lower()
        is_lossless = suffix in LOSSLESS_EXTENSIONS

        # Get claimed bitrate
        try:
            mf = mediafile.MediaFile(str(filepath))
            claimed_bitrate = int(mf.bitrate / 1000) if mf.bitrate else 0
            # Detect VBR: mediafile doesn't expose this directly, so we
            # use a heuristic — non-standard bitrates are likely VBR
            is_vbr = claimed_bitrate not in (128, 160, 192, 224, 256, 320)
        except Exception:
            claimed_bitrate = 0
            is_vbr = False

        if is_lossless:
            bitrate_label = "lossless"
        elif claimed_bitrate > 0:
            bitrate_label = f"{claimed_bitrate}kbps"
        else:
            bitrate_label = "???kbps"

        # Run spectral analysis
        result = detect_spectral_cutoff(filepath, np)

        if result is None:
            print(f"  {C_DIM}{i:>4}/{total}  {name:<50} {bitrate_label:<10} skipped (too short or unreadable){C_RESET}")
            skipped += 1
            continue

        band_ratio, cutoff_khz, _ = result
        verdict, confidence, true_br = classify_quality(
            band_ratio, cutoff_khz, claimed_bitrate, is_lossless, is_vbr)

        # Format output
        cutoff_str = f"{cutoff_khz:.1f}kHz"
        entry = (filepath, bitrate_label, cutoff_str, confidence, verdict, true_br)

        if verdict == "OK":
            color = C_GREEN
            verdict_str = f"[{confidence:>2}] OK"
            results_ok.append(entry)
        elif verdict == "SUSPECT":
            color = C_YELLOW
            verdict_str = f"[{confidence:>2}] SUSPECT"
            results_suspect.append(entry)
        else:
            color = C_RED
            true_label = f" (true ~{true_br})" if not is_lossless else f" (from ~{true_br})"
            verdict_str = f"[{confidence:>2}] LIKELY FAKE{true_label}"
            results_fake.append(entry)

        # Truncate name to fit
        max_name = 45
        display_name = name if len(name) <= max_name else name[:max_name - 2] + ".."
        print(f"  {color}{i:>4}/{total}  {display_name:<{max_name}}  {bitrate_label:<10} cutoff: {cutoff_str:<9} {verdict_str}{C_RESET}")

    elapsed = time.time() - start_time

    # Summary
    print(f"\n  {'─' * 60}")
    print(f"  Results ({elapsed:.1f}s):")
    print(f"    {C_GREEN}OK:          {len(results_ok):>4} tracks{C_RESET}")
    if results_suspect:
        print(f"    {C_YELLOW}SUSPECT:     {len(results_suspect):>4} tracks  (review recommended){C_RESET}")
    if results_fake:
        print(f"    {C_RED}LIKELY FAKE: {len(results_fake):>4} tracks  (probably transcoded from lower quality){C_RESET}")
    if skipped:
        print(f"    {C_DIM}Skipped:     {skipped:>4} tracks  (too short or unreadable){C_RESET}")

    if results_suspect or results_fake:
        print(f"\n  {C_DIM}Note: SUSPECT files may be legitimate — bass-heavy tracks and vinyl")
        print(f"  rips can show lower cutoffs. Verify with Spek (https://www.spek.cc).{C_RESET}")

    # Save report
    if results_suspect or results_fake:
        report_path = Path(library_dir) / ".bitrate_report.txt"
        try:
            lines = []
            lines.append("CrateMate — Bitrate Quality Report")
            lines.append(f"Generated: {time.strftime('%Y-%m-%d %H:%M:%S')}")
            lines.append(f"Library: {library_dir}")
            lines.append(f"Files scanned: {total}")
            lines.append("")

            if results_fake:
                lines.append(f"LIKELY FAKE ({len(results_fake)} files):")
                for fp, br, co, conf, _, true_br in results_fake:
                    lines.append(f"  {fp.name:<55} claimed: {br:<10} cutoff: {co:<9} confidence: {conf:<3} est. source: ~{true_br}")
                lines.append("")

            if results_suspect:
                lines.append(f"SUSPECT ({len(results_suspect)} files):")
                for fp, br, co, conf, _, true_br in results_suspect:
                    lines.append(f"  {fp.name:<55} claimed: {br:<10} cutoff: {co:<9} confidence: {conf:<3} est. source: ~{true_br}")
                lines.append("")

            lines.append("Note: SUSPECT files may be legitimate. Bass-heavy tracks, vinyl rips,")
            lines.append("and certain mastering styles can produce lower frequency cutoffs.")
            lines.append("Verify with a spectrogram tool like Spek (https://www.spek.cc).")

            report_path.write_text("\n".join(lines) + "\n")
            print(f"\n  Report saved to: {report_path}")
        except OSError:
            pass


# ── UNDO LOG ────────────────────────────────────────────────────────────────

UNDO_FILE = CONFIG_DIR / "undo_log.json"


def _save_undo(operation: str, actions: list[dict]) -> None:
    """Save an undo log entry. Each entry records the operation name and a list
    of reversible actions with 'type', 'src', and 'dest' fields."""
    if not actions:
        return
    entry = {
        "operation": operation,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "actions": actions,
    }
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    UNDO_FILE.write_text(json.dumps(entry, indent=2) + "\n")


def _load_undo() -> dict | None:
    """Load the last undo log entry, or None if no log exists."""
    try:
        return json.loads(UNDO_FILE.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def _clear_undo() -> None:
    """Remove the undo log file."""
    try:
        UNDO_FILE.unlink(missing_ok=True)
    except Exception:
        pass


def undo_last_operation() -> None:
    """Reverse the last undoable operation."""
    entry = _load_undo()
    if not entry:
        print(f"  {C_DIM}Nothing to undo.{C_RESET}")
        return

    op = entry.get("operation", "unknown")
    ts = entry.get("timestamp", "")
    actions = entry.get("actions", [])

    print(f"\n  Last operation: {C_BOLD}{op}{C_RESET}  {C_DIM}({ts}){C_RESET}")
    print(f"  {len(actions)} action(s) to reverse:\n")

    # Preview
    for a in actions[:10]:
        if a["type"] == "move":
            print(f"    {C_DIM}{Path(a['dest']).name} → {Path(a['src']).parent}{C_RESET}")
        elif a["type"] == "rename":
            print(f"    {C_DIM}{Path(a['dest']).name} → {Path(a['src']).name}{C_RESET}")
        elif a["type"] == "copy":
            print(f"    {C_DIM}delete {Path(a['dest']).name}{C_RESET}")
    if len(actions) > 10:
        print(f"    {C_DIM}... and {len(actions) - 10} more{C_RESET}")
    print()

    confirm = _prompt(f"  {C_YELLOW}Type YES to undo: {C_RESET}").strip()
    if confirm != "YES":
        print(f"  {C_DIM}Cancelled.{C_RESET}")
        return

    undone = 0
    errors = 0
    for i, a in enumerate(actions, 1):
        _progress(i, len(actions))
        try:
            src = Path(a["src"])
            dest = Path(a["dest"])
            if a["type"] in ("move", "rename"):
                # Reverse: move dest back to src
                if dest.exists():
                    src.parent.mkdir(parents=True, exist_ok=True)
                    shutil.move(str(dest), str(src))
                    undone += 1
                else:
                    print(f"    {C_RED}Not found: {dest.name}{C_RESET}")
                    errors += 1
            elif a["type"] == "copy":
                # Reverse: delete the copy
                if dest.exists():
                    dest.unlink()
                    undone += 1
                else:
                    print(f"    {C_RED}Not found: {dest.name}{C_RESET}")
                    errors += 1
        except Exception as e:
            print(f"    {C_RED}Error: {e}{C_RESET}")
            errors += 1

    _clear_undo()
    print(f"\n  {C_GREEN}Undone {undone} action(s){C_RESET}" +
          (f", {C_RED}{errors} error(s){C_RESET}" if errors else ""))


# ── BATCH RENAME LIBRARY FILES VIA GEMINI ────────────────────────────────────

def batch_rename_library(library_dir: str | Path, dry_run: bool = False) -> None:
    """Re-parse library filenames using Gemini AI and rename files with cleaner names."""
    library_dir = Path(library_dir)

    if not GEMINI_API_KEY:
        print(f"  {C_RED}Gemini API key not set — configure in Settings (s){C_RESET}")
        return

    files = find_audio_files(library_dir)
    if not files:
        print(f"  No audio files found in {library_dir}")
        return

    print(f"\n  Batch Rename via Gemini AI")
    print(f"  Sending {len(files)} filenames to Gemini...\n")

    # Send filenames through Gemini
    filepaths_str = [str(f) for f in files]
    gemini_names = gemini_fix_names(filepaths_str)

    if not gemini_names:
        print(f"  {C_DIM}Gemini returned no results.{C_RESET}")
        return

    # Build rename plan
    renames: list[tuple[Path, Path]] = []  # (old_path, new_path)
    skipped_same = 0
    skipped_conflict = 0

    for filepath in files:
        key = str(filepath)
        if key not in gemini_names:
            continue

        artist, title, mix = gemini_names[key]
        if not title:
            continue

        ext = filepath.suffix.lower()
        safe_artist = safe_filename(artist) if artist else "Unknown Artist"
        safe_title = safe_filename(title)
        if mix:
            new_name = f"{safe_artist} - {safe_title} ({safe_filename(mix)}){ext}"
        else:
            new_name = f"{safe_artist} - {safe_title}{ext}"

        new_path = filepath.parent / new_name

        # Skip if name is unchanged
        if new_path == filepath:
            skipped_same += 1
            continue

        # Skip if destination already exists (different file)
        if new_path.exists() and new_path != filepath:
            print(f"    {C_YELLOW}skip{C_RESET} {filepath.name} — {new_name} already exists")
            skipped_conflict += 1
            continue

        renames.append((filepath, new_path))

    if not renames:
        print(f"  No renames needed ({skipped_same} already clean"
              + (f", {skipped_conflict} conflicts" if skipped_conflict else "") + ")")
        return

    # Show plan
    print(f"  {len(renames)} file(s) to rename:\n")
    for old, new in renames[:20]:
        print(f"    {C_DIM}{old.name}{C_RESET}")
        print(f"      → {C_GREEN}{new.name}{C_RESET}")
    if len(renames) > 20:
        print(f"    {C_DIM}... and {len(renames) - 20} more{C_RESET}")
    print()

    if dry_run:
        print(f"  {C_DIM}DRY RUN — no files will be renamed{C_RESET}")
        return

    confirm = _prompt(f"  {C_YELLOW}Type YES to rename {len(renames)} files: {C_RESET}").strip()
    if confirm != "YES":
        print(f"  {C_DIM}Cancelled.{C_RESET}")
        return

    # Execute renames and build undo log
    undo_actions: list[dict] = []
    renamed = 0
    for i, (old_path, new_path) in enumerate(renames, 1):
        _progress(i, len(renames))
        try:
            # Also update tags to match new filename
            artist, title, mix = gemini_names[str(old_path)]
            shutil.move(str(old_path), str(new_path))
            undo_actions.append({"type": "rename", "src": str(old_path), "dest": str(new_path)})
            renamed += 1

            # Update tags on the renamed file
            try:
                mf = mediafile.MediaFile(str(new_path))
                if artist:
                    mf.artist = artist
                if title:
                    mf.title = title
                mf.save()
            except Exception:
                pass  # tag update is best-effort

        except Exception as e:
            print(f"    {C_RED}Error renaming {old_path.name}: {e}{C_RESET}")

    _save_undo("Batch rename (Gemini)", undo_actions)
    print(f"\n  {C_GREEN}Renamed {renamed} file(s){C_RESET}")
    if skipped_same:
        print(f"  {C_DIM}{skipped_same} already had clean names{C_RESET}")


# ── CLI ─────────────────────────────────────────────────────────────────────

# ANSI color codes
C_RESET = "\033[0m"
C_BOLD = "\033[1m"
C_DIM = "\033[2m"
C_CYAN = "\033[36m"
C_MAGENTA = "\033[35m"
C_YELLOW = "\033[33m"
C_GREEN = "\033[32m"
C_RED = "\033[31m"
C_WHITE = "\033[97m"
C_PEACH = "\033[38;5;216m"
C_PEACH_DK = "\033[38;5;173m"
C_BROWN = "\033[38;5;137m"
C_BROWN_LT = "\033[38;5;180m"
C_GRAY = "\033[38;5;244m"
C_BLUE = "\033[38;5;69m"
C_BLUE_LT = "\033[38;5;111m"

VERSION = "v1.4.1"

# ── SPLASH LOGO ────────────────────────────────────────────────────────────

_LOGO_CRATE = [
    " ██████ ██████   █████  ████████ ████████",
    "██      ██   ██ ██   ██    ██    ██      ",
    "██      ██████  ███████    ██    █████   ",
    "██      ██   ██ ██   ██    ██    ██      ",
    " ██████ ██   ██ ██   ██    ██    ████████",
]
_LOGO_MATE = [
    "██   ██  █████  ████████ ████████",
    "███ ███ ██   ██    ██    ██      ",
    "██ █ ██ ███████    ██    █████   ",
    "██   ██ ██   ██    ██    ██      ",
    "██   ██ ██   ██    ██    ████████",
]


# ── WAVEFORM LOADER ─────────────────────────────────────────────────────────
#
# Industry-standard approach (same pattern as tqdm / rich / alive-progress):
# All terminal I/O is serialized through a single lock.  When the loader is
# active it replaces sys.stdout with a thin wrapper (_WaveStdout) that:
#   1. Acquires the lock
#   2. Hides the waveform (cursor-jump + erase)
#   3. Writes the application output at the scroll position
#   4. Redraws the waveform on the last line
#   5. Releases the lock
# The background animation thread uses the same lock, so the two can never
# interleave.  Result: rock-solid output with a pinned, glitch-free waveform.

# Block characters for different waveform heights (index 0 = empty, 7 = full)
_WAVE_BLOCKS = [" ", "▁", "▂", "▃", "▄", "▅", "▆", "▇"]

# Color gradient for the waveform bars (peach/warm palette matching the app)
_WAVE_COLORS = [
    "\033[38;5;95m",   # dark brown
    "\033[38;5;131m",  # brown
    "\033[38;5;137m",  # warm brown
    "\033[38;5;173m",  # peach dark
    "\033[38;5;216m",  # peach
    "\033[38;5;217m",  # peach light
    "\033[38;5;216m",  # peach
    "\033[38;5;173m",  # peach dark
]


def _get_term_height():
    """Get terminal height, default 24."""
    try:
        return os.get_terminal_size().lines
    except Exception:
        return 24


class _WaveStdout:
    """Stdout wrapper that serializes writes with the waveform animation.

    Every write():
      lock → hide waveform → write content → redraw waveform → unlock

    This guarantees that application print() output and the background
    animation thread never interleave, eliminating visual glitches.
    """

    def __init__(self, original, lock: threading.Lock, loader: "WaveformLoader"):
        self._original = original
        self._lock = lock
        self._loader = loader

    # ── file-like interface expected by print() / sys.stdout ──

    def write(self, text: str) -> int:
        if not text:
            return 0
        with self._lock:
            h = _get_term_height()
            raw = self._original
            # Save cursor, jump to bottom line, erase it (hide waveform)
            raw.write(f"\033[s\033[{h};1H\033[2K\033[u")
            # Write the actual application content in the scroll region
            n = raw.write(text)
            # Redraw the waveform on the bottom line
            wave = self._loader._render_wave()
            raw.write(f"\033[s\033[{h};1H{wave}\033[u")
            raw.flush()
            return n

    def flush(self):
        self._original.flush()

    # Forward everything else to the real stdout
    def __getattr__(self, name):
        return getattr(self._original, name)


class WaveformLoader:
    """A threaded waveform animation pinned to the bottom of the terminal.

    Uses a scroll-region to reserve the last line for the waveform, and a
    stdout wrapper + shared lock to serialize ALL terminal writes.  This is
    the same approach used by tqdm, rich, and alive-progress.
    """

    def __init__(self, message: str = "Working...", bars: int = 32):
        self._message = message
        self._bars = bars
        self._running = False
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()
        self._frame = 0
        self._original_stdout = None
        # Progress tracking — set via set_progress() so a numeric counter and
        # ETA can be rendered alongside the animated waveform.
        self._current = 0
        self._total = 0
        self._progress_started_at = 0.0

    def _format_eta(self) -> str:
        """Estimate remaining time based on elapsed/current ratio."""
        if self._total <= 0 or self._current <= 0:
            return ""
        elapsed = time.time() - self._progress_started_at
        if elapsed < 0.5:
            return ""
        per_item = elapsed / self._current
        remaining = max(0, self._total - self._current) * per_item
        if remaining < 1:
            return "almost done"
        if remaining < 60:
            return f"~{int(remaining)}s left"
        if remaining < 3600:
            mins = int(remaining // 60)
            secs = int(remaining % 60)
            return f"~{mins}m {secs:02d}s left"
        hrs = int(remaining // 3600)
        mins = int((remaining % 3600) // 60)
        return f"~{hrs}h {mins:02d}m left"

    def _progress_str(self) -> tuple[str, int]:
        """Build the progress counter chunk and its visible width.
        Returns ("", 0) when no total has been set."""
        if self._total <= 0:
            return "", 0
        counter = f"{self._current}/{self._total}"
        eta = self._format_eta()
        if eta:
            visible = f"  {counter} · {eta}"
            colored = f"  {C_WHITE}{counter}{C_RESET} {C_DIM}· {eta}{C_RESET}"
        else:
            visible = f"  {counter}"
            colored = f"  {C_WHITE}{counter}{C_RESET}"
        return colored, len(visible)

    def _render_wave(self) -> str:
        """Render one frame of the waveform animation.

        Called from both the animation thread and the stdout wrapper,
        always under self._lock.
        """
        w = get_term_width()
        t = self._frame * 0.15  # time progression

        # Build waveform bars
        wave_chars: list[str] = []
        for i in range(self._bars):
            # Combine multiple sine waves for an organic, audio-like feel
            x = i / self._bars
            v = (
                math.sin(t + x * 8.0) * 0.35 +
                math.sin(t * 1.7 + x * 12.0) * 0.25 +
                math.sin(t * 0.6 + x * 4.0) * 0.25 +
                math.sin(t * 2.3 + x * 16.0) * 0.15
            )
            # Normalize to 0–7 range for block character index
            level = int((v + 1.0) * 3.5)
            level = max(0, min(7, level))

            color = _WAVE_COLORS[level]
            block = _WAVE_BLOCKS[level]
            wave_chars.append(f"{color}{block}")

        wave_str = "".join(wave_chars) + C_RESET

        # Build the status line:
        #   "  ♪ message  47/200 · ~12s left  ▁▂▃▄▅▆▇..  ♪"
        progress_colored, progress_vis = self._progress_str()
        prefix = f"  {C_DIM}♪{C_RESET} {C_PEACH}{self._message}{C_RESET}{progress_colored}  "
        prefix_vis = len(f"  ♪ {self._message}") + progress_vis + 2
        wave_vis = self._bars
        suffix = f"  {C_DIM}♪{C_RESET}"
        suffix_vis = 3

        total_vis = prefix_vis + wave_vis + suffix_vis
        if total_vis < w:
            pad = " " * (w - total_vis)
            return f"{prefix}{wave_str}{pad}{suffix}"
        # Terminal is too narrow — drop the trailing waveform symbol
        if prefix_vis + wave_vis < w:
            return f"{prefix}{wave_str}"
        # Still too narrow — drop the wave entirely so the counter stays visible
        return prefix.rstrip()

    def _animate(self):
        """Background thread: redraw the waveform at ~15 fps."""
        raw = self._original_stdout
        while self._running:
            with self._lock:
                h = _get_term_height()
                wave = self._render_wave()
                # Jump to bottom line, clear it, draw waveform, return cursor
                raw.write(f"\033[s\033[{h};1H\033[2K{wave}\033[u")
                raw.flush()
                self._frame += 1
            time.sleep(0.066)  # ~15 fps

    def start(self):
        """Start the waveform animation."""
        global _active_loader
        if self._running:
            return
        self._running = True
        self._frame = 0

        # Save the real stdout before replacing it
        self._original_stdout = sys.stdout

        # Set scroll region: lines 1..(h-1), reserving line h for waveform
        h = _get_term_height()
        raw = self._original_stdout
        raw.write(f"\033[1;{h - 1}r")   # restrict scrolling to top region
        raw.write(f"\033[{h - 1};1H")   # position cursor at bottom of scroll region
        raw.flush()

        # Draw the initial waveform frame
        wave = self._render_wave()
        raw.write(f"\033[s\033[{h};1H\033[2K{wave}\033[u")
        raw.flush()

        # Replace sys.stdout with the serialized wrapper
        sys.stdout = _WaveStdout(raw, self._lock, self)

        # Register as the active loader so module-level progress/prompt helpers
        # can find us. Only one loader is expected to be active at a time.
        _active_loader = self

        # Start the animation thread
        self._thread = threading.Thread(target=self._animate, daemon=True)
        self._thread.start()

    def stop(self):
        """Stop the waveform animation and restore the terminal."""
        global _active_loader
        if not self._running:
            return
        self._running = False
        if self._thread:
            self._thread.join(timeout=1.0)
            self._thread = None

        # Restore real stdout
        raw = self._original_stdout
        if raw:
            sys.stdout = raw

            # Restore full scroll region and clean up the bottom line
            h = _get_term_height()
            raw.write(f"\033[1;{h}r")       # full scroll region
            raw.write(f"\033[{h};1H\033[2K") # clear waveform line
            raw.write(f"\033[{h - 1};1H")    # cursor back above
            raw.flush()

        self._original_stdout = None
        if _active_loader is self:
            _active_loader = None

    def update_message(self, message: str):
        """Update the status message shown alongside the waveform."""
        with self._lock:
            self._message = message

    def set_progress(self, current: int, total: int) -> None:
        """Update the progress counter rendered alongside the waveform.
        First call (re)starts the ETA clock so the estimate reflects the
        active operation rather than the time the loader was created."""
        with self._lock:
            if self._total <= 0 or total != self._total:
                self._progress_started_at = time.time()
            self._current = current
            self._total = total

    def reset_progress(self) -> None:
        """Clear the progress counter (returns the loader to indeterminate mode)."""
        with self._lock:
            self._current = 0
            self._total = 0
            self._progress_started_at = 0.0

    def pause(self):
        """Temporarily stop the animation so input() and other terminal-
        sensitive operations can run cleanly. Use with resume() or via the
        paused() context manager."""
        self.stop()

    def resume(self):
        """Restart the animation after pause(). Preserves message and progress."""
        self.start()

    def paused(self):
        """Context manager that pauses the loader on enter and resumes on exit."""
        return _LoaderPause(self)

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *args):
        self.stop()


# ── ACTIVE-LOADER HELPERS ───────────────────────────────────────────────────
#
# Long-running operations are wrapped in `with WaveformLoader(...)` from the
# interactive menu, but the actual functions are the ones that know their
# progress and need to ask the user questions. These module-level helpers let
# any function update the visible progress counter and pause the animation
# around input() prompts, without having to thread a loader reference through
# every call site.

_active_loader: "WaveformLoader | None" = None


class _LoaderPause:
    """Context manager that pauses a loader on enter and resumes on exit."""

    def __init__(self, loader: "WaveformLoader | None"):
        self._loader = loader
        self._was_running = False

    def __enter__(self):
        if self._loader is not None and self._loader._running:
            self._was_running = True
            self._loader.pause()
        return self

    def __exit__(self, *args):
        if self._was_running and self._loader is not None:
            self._loader.resume()


def _progress(current: int, total: int, message: str | None = None) -> None:
    """Update the active loader's progress counter (no-op when no loader)."""
    if _active_loader is None:
        return
    _active_loader.set_progress(current, total)
    if message is not None:
        _active_loader.update_message(message)


def _loader_paused():
    """Context manager: pause the active loader for the duration of a block.
    Safe to use whether or not a loader is currently running."""
    return _LoaderPause(_active_loader)


def _prompt(text: str) -> str:
    """input() replacement that pauses the active loader so the prompt and
    user keystrokes aren't mangled by the animation thread."""
    with _loader_paused():
        try:
            return input(text)
        except EOFError:
            return ""


def _preview_waveform():
    """Run a simulated task with the waveform loader for preview purposes."""
    loader = WaveformLoader("Scanning library...")
    loader.start()
    try:
        time.sleep(2.0)

        loader.update_message("Fetching metadata from Spotify...")
        print(f"  {C_GREEN}✓{C_RESET} Library scanned — 42 files found")
        time.sleep(2.5)

        loader.update_message("Processing files...")
        print(f"  {C_GREEN}✓{C_RESET} Metadata fetched from Spotify")
        time.sleep(1.0)

        # Simulate per-file output scrolling above the waveform
        print()
        print(f"  {C_DIM}Processing tracks:{C_RESET}")
        artists = [
            "Mall Grab", "Peggy Gou", "Sam Alfred", "Ross From Friends",
            "DJ Seinfeld", "Palms Trax", "Chaos in the CBD", "Folamour",
        ]
        titles = [
            "You Thought (Original Mix)", "1+1=11 (Spray Remix)",
            "Suzuka (Extended)", "Talk to Me You'll Understand",
            "U (DJ Seinfeld Remix)", "Forever (Club Mix)",
            "Midnight in Peckham", "The Journey (Extended Mix)",
        ]
        for i, (a, t) in enumerate(zip(artists, titles), 1):
            loader.update_message(f"Processing {i}/8...")
            print(f"    {C_DIM}{i}/8{C_RESET}  {a} - {t}.flac")
            time.sleep(0.5)

        loader.update_message("Writing tags...")
        print()
        print(f"  {C_GREEN}✓{C_RESET} Tags written")
        time.sleep(1.5)

        loader.update_message("Done!")
        print(f"  {C_GREEN}✓ Complete — 8 files processed{C_RESET}")
        time.sleep(1.5)

    finally:
        loader.stop()

    print(f"\n  {C_DIM}Preview finished. The waveform will appear at the bottom")
    print(f"  of the terminal during real operations.{C_RESET}")


def _splash_animation(hold: bool = False):
    """Play startup splash: waveform bars rise, logo reveals, then clear."""
    w = get_term_width()
    h = _get_term_height()

    # Skip splash if terminal is too small
    if h < 20 or w < 50:
        return

    raw = sys.stdout
    raw.write("\033[?25l")  # hide cursor
    raw.write("\033[2J\033[H")  # clear screen
    raw.flush()

    num_bars = min(w - 4, 60)
    pad_bars = (w - num_bars) // 2

    # Logo positioning — center the whole group (logo + subtitle + gap + waveform)
    logo_lines = _LOGO_CRATE + [""] + _LOGO_MATE
    total_block = len(logo_lines) + 4  # logo + 1 subtitle + 3 gap to waveform
    logo_start = h // 2 - total_block // 2
    sub_row = logo_start + len(logo_lines) + 1
    wave_row = sub_row + 3  # well below subtitle — no overlap

    def _render_wave(t: float, reveal: float = 1.0) -> str:
        """Build one waveform frame as a string (no cursor moves)."""
        bars = []
        for i in range(num_bars):
            x = i / num_bars
            r = max(0.0, min(1.0, (reveal * 1.8 - x * 0.6) * 2.5)) if reveal < 1.0 else 1.0
            v = (
                math.sin(t + x * 8.0) * 0.35 +
                math.sin(t * 1.7 + x * 12.0) * 0.25 +
                math.sin(t * 0.6 + x * 4.0) * 0.25 +
                math.sin(t * 2.3 + x * 16.0) * 0.15
            )
            level = max(0, min(7, int((v + 1.0) * 3.5 * r)))
            bars.append(f"{_WAVE_COLORS[level]}{_WAVE_BLOCKS[level]}")
        return " " * pad_bars + "".join(bars) + C_RESET

    try:
        # ── Phase 1: Waveform bars rise from left to right (~2.4s) ──
        for frame in range(40):
            t = frame * 0.2
            progress = frame / 40
            line = _render_wave(t, reveal=progress)
            # Single write: move cursor, overwrite in-place (no erase-line flicker)
            raw.write(f"\033[{wave_row};1H{line}\033[K")
            raw.flush()
            time.sleep(0.06)

        # ── Phase 2: Logo appears line by line (~0.6s) ──
        for i, line in enumerate(logo_lines):
            row = logo_start + i
            if 1 <= row <= h:
                pad = (w - len(line)) // 2
                raw.write(f"\033[{row};1H" + " " * max(pad, 0) + f"{C_BLUE}{line}{C_RESET}")
            raw.flush()
            time.sleep(0.05)

        # ── Phase 3: Subtitle fades in ──
        time.sleep(0.25)
        subtitle = "clean beats, clean tags"
        if 1 <= sub_row <= h:
            pad = (w - len(subtitle)) // 2
            raw.write(f"\033[{sub_row};1H" + " " * max(pad, 0) + f"{C_DIM}{subtitle}{C_RESET}")
            raw.flush()

        # ── Phase 4: Hold with living waveform (~3.6s) ──
        for frame in range(40, 100):
            t = frame * 0.2
            line = _render_wave(t)
            raw.write(f"\033[{wave_row};1H{line}\033[K")
            raw.flush()
            time.sleep(0.06)

        # ── Phase 5: Fade out — waveform shrinks, text dims (~1.2s) ──
        for frame in range(20):
            fade = 1.0 - frame / 20
            t = (100 + frame) * 0.2

            # Redraw waveform with decreasing amplitude
            bars = []
            for i in range(num_bars):
                x = i / num_bars
                v = (
                    math.sin(t + x * 8.0) * 0.35 +
                    math.sin(t * 1.7 + x * 12.0) * 0.25 +
                    math.sin(t * 0.6 + x * 4.0) * 0.25 +
                    math.sin(t * 2.3 + x * 16.0) * 0.15
                )
                level = max(0, min(7, int((v + 1.0) * 3.5 * fade)))
                bars.append(f"{_WAVE_COLORS[level]}{_WAVE_BLOCKS[level]}")
            raw.write(f"\033[{wave_row};1H" + " " * pad_bars + "".join(bars) + C_RESET + "\033[K")
            raw.flush()
            time.sleep(0.06)

    except KeyboardInterrupt:
        pass
    finally:
        if hold:
            # Keep the final frame on screen (for recording)
            raw.write("\033[?25h")
            raw.flush()
        else:
            # Clear and restore cursor
            raw.write("\033[2J\033[H")
            raw.write("\033[?25h")
            raw.flush()


def get_term_width():
    """Get terminal width, default 80."""
    try:
        return os.get_terminal_size().columns
    except Exception:
        return 80


def _static_waveform(width: int = 24) -> str:
    """Generate a static waveform string for the header."""
    bars = []
    for i in range(width):
        x = i / width
        v = (
            math.sin(x * 10.0) * 0.35 +
            math.sin(x * 15.0) * 0.25 +
            math.sin(x * 5.0) * 0.25 +
            math.sin(x * 20.0) * 0.15
        )
        level = int((v + 1.0) * 3.5)
        level = max(0, min(7, level))
        color = _WAVE_COLORS[level]
        block = _WAVE_BLOCKS[level]
        bars.append(f"{color}{block}")
    return "".join(bars) + C_RESET


def show_header(library_dir, lib_count):
    """Show compact startup header with waveform and library info."""
    w = get_term_width()
    label = f"CrateMate {VERSION} "
    print(f"{C_DIM}──{C_RESET} {C_PEACH}{C_BOLD}CrateMate{C_RESET} {C_DIM}{VERSION} {'─' * max(w - len(label) - 4, 0)}{C_RESET}")
    wave = _static_waveform(24)
    print(f"  {wave}   {C_YELLOW}Library:{C_RESET} {C_WHITE}{lib_count}{C_RESET} {C_DIM}tracks{C_RESET}")
    print(f"  {'  ' * 12}   {C_DIM}{library_dir}{C_RESET}")


def pick_folder(prompt="Enter folder path"):
    """Ask user for a folder path, with native dialog or manual input."""
    # On macOS, use osascript for a native Finder folder picker.
    # Tkinter file dialogs on macOS corrupt terminal stdin, crashing
    # subsequent input() calls — no cleanup workaround is reliable.
    if sys.platform == "darwin":
        try:
            print(f"  {C_DIM}Opening folder picker...{C_RESET}")
            default_dir = str(Path.home() / "Downloads")
            safe_prompt = prompt.replace('\\', '\\\\').replace('"', '\\"')
            safe_dir = default_dir.replace('\\', '\\\\').replace('"', '\\"')
            script = (
                f'POSIX path of (choose folder with prompt "{safe_prompt}"'
                f' default location (POSIX file "{safe_dir}"))'
            )
            result = subprocess.run(
                ["osascript", "-e", script],
                capture_output=True, text=True, timeout=120,
            )
            if result.returncode == 0 and result.stdout.strip():
                return Path(result.stdout.strip())
            # User cancelled the dialog
            return None
        except Exception:
            pass
    else:
        # Non-macOS: tkinter is safe to use
        try:
            import tkinter as tk
            from tkinter import filedialog
            print(f"  {C_DIM}Opening folder picker...{C_RESET}")
            root = tk.Tk()
            root.withdraw()
            folder = filedialog.askdirectory(
                title=prompt,
                initialdir=str(Path.home() / "Downloads"),
            )
            root.quit()
            root.destroy()
            if folder:
                return Path(folder)
        except Exception:
            pass

    # Manual input fallback
    path = input(f"  {C_CYAN}{prompt}: {C_RESET}").strip()
    if not path:
        return None
    return Path(os.path.expanduser(path))


def ask_dry_run():
    """Ask user if they want a dry run first."""
    choice = input(f"  {C_YELLOW}Dry run first? (y/N): {C_RESET}").strip().lower()
    return choice in ("y", "yes")


def _mask_key(key: str) -> str:
    """Mask an API key for display, showing only the first 6 and last 4 chars."""
    if not key:
        return f"{C_RED}not set{C_RESET}"
    if len(key) <= 12:
        return key[:3] + "..."
    return f"{key[:6]}...{key[-4:]}"


def _read_env_file() -> dict[str, str]:
    """Read ~/.config/cratemate/.env into a dict."""
    env = {}
    for env_path in (CONFIG_DIR / ".env", _SCRIPT_DIR / ".env"):
        try:
            for line in env_path.read_text().splitlines():
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    env.setdefault(k.strip(), v.strip())  # primary wins
        except FileNotFoundError:
            pass
    return env


def _write_env_file(env: dict[str, str]) -> None:
    """Write a dict to ~/.config/cratemate/.env (the primary config location)."""
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    env_path = CONFIG_DIR / ".env"
    lines = [f"{k}={v}" for k, v in env.items()]
    env_path.write_text("\n".join(lines) + "\n")


def _update_api_key(env_key: str, label: str) -> None:
    """Prompt user to update an API key, save to .env, and reload the global."""
    global SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET, DISCOGS_USER_TOKEN, GEMINI_API_KEY

    current = os.environ.get(env_key, "")
    print(f"    {C_DIM}Current: {_mask_key(current)}{C_RESET}")
    new_val = input(f"    {C_CYAN}New {label} (enter to keep): {C_RESET}").strip()
    if not new_val:
        print(f"    {C_DIM}Unchanged.{C_RESET}")
        return

    # Read existing .env, seed with current runtime values to never lose keys
    env = {
        "SPOTIFY_CLIENT_ID": SPOTIFY_CLIENT_ID,
        "SPOTIFY_CLIENT_SECRET": SPOTIFY_CLIENT_SECRET,
        "DISCOGS_USER_TOKEN": DISCOGS_USER_TOKEN,
        "GEMINI_API_KEY": GEMINI_API_KEY,
    }
    env.update(_read_env_file())  # overlay with anything already in .env
    env[env_key] = new_val
    # Remove empty entries so we don't write blank keys
    env = {k: v for k, v in env.items() if v}
    _write_env_file(env)

    # Update runtime
    os.environ[env_key] = new_val
    if env_key == "SPOTIFY_CLIENT_ID":
        SPOTIFY_CLIENT_ID = new_val
    elif env_key == "SPOTIFY_CLIENT_SECRET":
        SPOTIFY_CLIENT_SECRET = new_val
    elif env_key == "DISCOGS_USER_TOKEN":
        DISCOGS_USER_TOKEN = new_val
    elif env_key == "GEMINI_API_KEY":
        GEMINI_API_KEY = new_val

    # Clear cached Spotify token if Spotify creds changed
    if env_key in ("SPOTIFY_CLIENT_ID", "SPOTIFY_CLIENT_SECRET"):
        global _spotify_token
        _spotify_token = None

    print(f"    {C_GREEN}Saved!{C_RESET}")


def settings_menu(library_dir: Path) -> Path:
    """Settings submenu. Returns the (possibly changed) library_dir."""
    while True:
        print()
        print(f"  {C_BOLD}Settings{C_RESET}")
        print(f"  {C_DIM}{'─' * 40}{C_RESET}")
        print(f"  {C_GREEN}1{C_RESET}  Library path     {C_DIM}{library_dir}{C_RESET}")
        print(f"  {C_GREEN}2{C_RESET}  Spotify ID       {C_DIM}{_mask_key(SPOTIFY_CLIENT_ID)}{C_RESET}")
        print(f"  {C_GREEN}3{C_RESET}  Spotify Secret   {C_DIM}{_mask_key(SPOTIFY_CLIENT_SECRET)}{C_RESET}")
        print(f"  {C_GREEN}4{C_RESET}  Discogs Token    {C_DIM}{_mask_key(DISCOGS_USER_TOKEN)}{C_RESET}")
        print(f"  {C_GREEN}5{C_RESET}  Gemini API Key   {C_DIM}{_mask_key(GEMINI_API_KEY)}{C_RESET}")
        print(f"  {C_DIM}{'─' * 40}{C_RESET}")
        print(f"  {C_GREEN}6{C_RESET}  Preview waveform loader")
        print(f"  {C_DIM}b{C_RESET}  Back")
        print()

        choice = input(f"  {C_CYAN}settings>{C_RESET} ").strip().lower()

        if choice in ("b", "back", "q", ""):
            break
        elif choice == "1":
            print(f"    {C_DIM}Current: {library_dir}{C_RESET}")
            new_path = input(f"    {C_CYAN}New library path (enter to keep): {C_RESET}").strip()
            if new_path:
                library_dir = set_library_dir(new_path)
                print(f"    {C_GREEN}Library set to: {library_dir} (saved){C_RESET}")
            else:
                print(f"    {C_DIM}Unchanged.{C_RESET}")
        elif choice == "2":
            _update_api_key("SPOTIFY_CLIENT_ID", "Client ID")
        elif choice == "3":
            _update_api_key("SPOTIFY_CLIENT_SECRET", "Client Secret")
        elif choice == "4":
            _update_api_key("DISCOGS_USER_TOKEN", "Discogs Token")
        elif choice == "5":
            _update_api_key("GEMINI_API_KEY", "Gemini Key")
        elif choice == "6":
            print()
            _preview_waveform()
            print()

    return library_dir


def show_menu():
    """Print the menu options."""
    print(f"  {C_DIM}Import{C_RESET}")
    print(f"  {C_GREEN}1{C_RESET}  Import folder")
    print(f"  {C_GREEN}2{C_RESET}  Clean up source folder")
    print()
    print(f"  {C_DIM}Library{C_RESET}")
    print(f"  {C_GREEN}3{C_RESET}  Fix missing covers")
    print(f"  {C_GREEN}4{C_RESET}  Fix tags from Spotify")
    print(f"  {C_GREEN}5{C_RESET}  Remove duplicates")
    print(f"  {C_GREEN}6{C_RESET}  Convert FLACs to MP3")
    print(f"  {C_GREEN}7{C_RESET}  Batch rename files {C_MAGENTA}(Gemini){C_RESET}")
    print()
    print(f"  {C_DIM}AI & Analysis{C_RESET}")
    print(f"  {C_GREEN}8{C_RESET}  AI genre tagging {C_MAGENTA}(Gemini){C_RESET}")
    print(f"  {C_GREEN}9{C_RESET}  Detect fake bitrates {C_RED}(experimental){C_RESET}")
    print()
    print(f"  {C_YELLOW}u{C_RESET}  Undo last    {C_YELLOW}s{C_RESET}  Settings    {C_RED}q{C_RESET}  Quit")
    print()


def interactive_menu():
    """Interactive CLI menu."""
    library_dir = LIBRARY_DIR

    # Count library files
    lib_count = 0
    if library_dir.exists():
        lib_count = sum(1 for f in library_dir.rglob("*")
                        if f.is_file() and f.suffix.lower() in AUDIO_EXTENSIONS)

    # Play splash animation on startup
    _splash_animation()

    show_header(library_dir, lib_count)
    print()
    show_menu()

    while True:
        choice = input(f"  {C_CYAN}>{C_RESET} ").strip().lower()

        if choice in ("q", "quit", "exit"):
            print(f"\n  {C_DIM}See you next session ♪{C_RESET}\n")
            break

        elif choice == "1":
            folder = pick_folder("Select folder to import")
            if not folder or not folder.is_dir():
                print(f"  {C_RED}Not a valid directory.{C_RESET}\n")
                continue
            gemini = input(f"  {C_YELLOW}Use Gemini AI for filename parsing? (y/N): {C_RESET}").strip().lower() in ("y", "yes")
            conv = input(f"  {C_YELLOW}Convert FLACs to 320kbps MP3? (y/N): {C_RESET}").strip().lower() in ("y", "yes")
            dry = ask_dry_run()
            print()
            with WaveformLoader("Importing..."):
                process_folder(folder, library_dir, dry_run=dry, use_gemini=gemini, convert_flac=conv)
            print()

        elif choice == "2":
            folder = pick_folder("Select source folder to clean up")
            if not folder or not folder.is_dir():
                print(f"  {C_RED}Not a valid directory.{C_RESET}\n")
                continue
            dry = ask_dry_run()
            print()
            with WaveformLoader("Scanning source folder..."):
                clean_source_folder(folder, library_dir, dry_run=dry)
            print()

        elif choice == "3":
            dry = ask_dry_run()
            print()
            with WaveformLoader("Fixing covers..."):
                fix_covers(library_dir, dry_run=dry)
            print()

        elif choice == "4":
            dry = ask_dry_run()
            print()
            with WaveformLoader("Fixing tags..."):
                fix_tags(library_dir, dry_run=dry)
            print()

        elif choice == "5":
            dry = ask_dry_run()
            print()
            with WaveformLoader("Scanning for duplicates..."):
                remove_duplicates(library_dir, dry_run=dry)
            print()

        elif choice == "6":
            if not check_ffmpeg():
                print(f"  {C_RED}FFmpeg not found. Install with: brew install ffmpeg{C_RESET}\n")
                continue
            dry = ask_dry_run()
            keep = input(f"  {C_YELLOW}Keep original FLAC files after conversion? (y/N): {C_RESET}").strip().lower() in ("y", "yes")
            print()
            with WaveformLoader("Converting FLACs..."):
                batch_convert_flac(library_dir, dry_run=dry, keep_original=keep)
            print()

        elif choice == "7":
            dry = ask_dry_run()
            print()
            with WaveformLoader("Renaming files..."):
                batch_rename_library(library_dir, dry_run=dry)
            print()

        elif choice == "8":
            dry = ask_dry_run()
            org = input(f"  {C_YELLOW}Organize files into genre folders? (y/N): {C_RESET}").strip().lower() in ("y", "yes")
            print()
            with WaveformLoader("Tagging genres..."):
                ai_genre_tag(library_dir, dry_run=dry, organize=org)
            print()

        elif choice == "9":
            if not check_ffmpeg():
                print(f"  {C_RED}FFmpeg not found. Install with: brew install ffmpeg{C_RESET}\n")
                continue
            if _check_numpy() is None:
                print(f"  {C_RED}numpy is required. Install with: pip install numpy{C_RESET}\n")
                continue
            scope = input(f"  {C_YELLOW}Scan entire library or specific files/folder? (L)ibrary / (f)iles: {C_RESET}").strip().lower()
            targets = None
            if scope in ("f", "files"):
                path_input = input(f"  {C_YELLOW}Enter file or folder path(s), comma-separated: {C_RESET}").strip()
                if not path_input:
                    print(f"  {C_DIM}No path entered.{C_RESET}\n")
                    continue
                targets = [Path(p.strip().strip("'\"")) for p in path_input.split(",") if p.strip()]
                invalid = [p for p in targets if not p.exists()]
                if invalid:
                    for p in invalid:
                        print(f"  {C_RED}Not found: {p}{C_RESET}")
                    print()
                    continue
            lossless = input(f"  {C_YELLOW}Also scan lossless files (FLAC/WAV/AIFF) for transcode artifacts? (y/N): {C_RESET}").strip().lower() in ("y", "yes")
            print()
            with WaveformLoader("Analyzing bitrates..."):
                analyze_bitrate_quality(library_dir, include_lossless=lossless, target_paths=targets)
            print()

        elif choice == "u":
            undo_last_operation()
            print()

        elif choice == "s":
            library_dir = settings_menu(library_dir)
            print()
            show_menu()

        elif choice == "?" or choice == "help":
            show_menu()

        else:
            print(f"  {C_DIM}? for menu{C_RESET}")


def _check_api_keys() -> None:
    """Print a warning if essential API keys are missing."""
    if not SPOTIFY_CLIENT_ID or not SPOTIFY_CLIENT_SECRET:
        print(f"  {C_YELLOW}⚠  Spotify API keys not set — cover art and genre lookups will be skipped.{C_RESET}")
        print(f"  {C_DIM}   Set them in Settings (s) or add to {CONFIG_DIR / '.env'}{C_RESET}")
        print()


def _first_run_setup() -> None:
    """One-time setup wizard shown on first launch when no config exists."""
    print(f"\n  {C_BOLD}Welcome to CrateMate!{C_RESET} Let's get you set up.\n")
    print(f"  {C_DIM}You can change any of these later in Settings (s).{C_RESET}\n")

    # 1. Library path
    print(f"  {C_BOLD}1. Library path{C_RESET} (where imported music goes)")
    print(f"     Default: {DEFAULT_LIBRARY_DIR}")
    raw = input(f"     {C_CYAN}> (enter to accept): {C_RESET}").strip()
    library_path = Path(os.path.expanduser(raw)).resolve() if raw else DEFAULT_LIBRARY_DIR
    set_library_dir(library_path)
    print(f"     {C_GREEN}✓ {library_path}{C_RESET}\n")

    # 2. Spotify
    print(f"  {C_BOLD}2. Spotify API keys{C_RESET} (for cover art + genre)")
    print(f"     {C_DIM}Get yours at: https://developer.spotify.com/dashboard{C_RESET}")
    sp_id = input(f"     {C_CYAN}Client ID (enter to skip): {C_RESET}").strip()
    sp_secret = input(f"     {C_CYAN}Client Secret (enter to skip): {C_RESET}").strip()

    # 3. Discogs
    print(f"\n  {C_BOLD}3. Discogs token{C_RESET} (optional, fallback cover art)")
    print(f"     {C_DIM}Get yours at: https://www.discogs.com/settings/developers{C_RESET}")
    discogs = input(f"     {C_CYAN}Token (enter to skip): {C_RESET}").strip()

    # 4. Gemini
    print(f"\n  {C_BOLD}4. Gemini API key{C_RESET} (optional, AI filename fixing)")
    print(f"     {C_DIM}Get yours at: https://aistudio.google.com/apikey{C_RESET}")
    gemini = input(f"     {C_CYAN}Key (enter to skip): {C_RESET}").strip()

    # Persist to ~/.config/cratemate/.env and update runtime globals
    global SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET, DISCOGS_USER_TOKEN, GEMINI_API_KEY
    env: dict[str, str] = {}
    for env_key, val, runtime_name in [
        ("SPOTIFY_CLIENT_ID", sp_id, None),
        ("SPOTIFY_CLIENT_SECRET", sp_secret, None),
        ("DISCOGS_USER_TOKEN", discogs, None),
        ("GEMINI_API_KEY", gemini, None),
    ]:
        if val:
            env[env_key] = val
            os.environ[env_key] = val
    if sp_id:
        SPOTIFY_CLIENT_ID = sp_id
    if sp_secret:
        SPOTIFY_CLIENT_SECRET = sp_secret
    if discogs:
        DISCOGS_USER_TOKEN = discogs
    if gemini:
        GEMINI_API_KEY = gemini
    if env:
        _write_env_file(env)

    print(f"\n  {C_GREEN}✓ Config saved to {CONFIG_DIR}{C_RESET}")
    print(f"  {C_DIM}Update anytime in Settings (s).{C_RESET}\n")


def main():
    parser = argparse.ArgumentParser(
        description="CrateMate — clean up music files and organize your library"
    )
    parser.add_argument("folder", nargs="?", help="Folder of music to import")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview what would happen without copying")
    parser.add_argument("--library", default=str(get_library_dir()),
                        help=f"Library directory (default: {get_library_dir()})")
    parser.add_argument("--gemini", action="store_true",
                        help="Use Gemini AI to fix messy filenames before processing")
    parser.add_argument("--fix-covers", action="store_true",
                        help="Re-scan library for files missing covers and try to fetch them")
    parser.add_argument("--fix-tags", action="store_true",
                        help="Update album/year/genre tags from Spotify (never changes title/artist/duration)")
    parser.add_argument("--remove-dupes", action="store_true",
                        help="Find and remove duplicate tracks, keeping highest quality format")
    parser.add_argument("--clean-source", metavar="FOLDER",
                        help="Delete source files that already have a copy in the library")
    parser.add_argument("--convert-flac", action="store_true",
                        help="Batch-convert all FLAC files in the library to 320kbps MP3")
    parser.add_argument("--mp3", action="store_true",
                        help="Convert FLAC files to MP3 during import (use with a folder argument)")
    parser.add_argument("--ai-genres", action="store_true",
                        help="Classify library tracks by genre using Gemini AI and update tags")
    parser.add_argument("--organize", action="store_true",
                        help="Move files into genre subfolders (use with --ai-genres)")
    parser.add_argument("--batch-rename", action="store_true",
                        help="Re-parse library filenames using Gemini AI and rename files")
    parser.add_argument("--detect-fakes", nargs="*", metavar="PATH",
                        help="Detect fake bitrates via spectral analysis. Pass file/folder paths, or omit to scan entire library")
    parser.add_argument("--lossless", action="store_true",
                        help="Also scan lossless files for transcode artifacts (use with --detect-fakes)")
    parser.add_argument("--undo", action="store_true",
                        help="Undo the last undoable operation (rename, genre organize)")
    parser.add_argument("--splash", action="store_true",
                        help="Play the splash animation and exit (for recording)")
    args = parser.parse_args()

    if args.splash:
        _splash_animation(hold=True)
        # Clear the splash before printing the menu
        sys.stdout.write("\033[2J\033[H")
        sys.stdout.flush()
        library_dir = LIBRARY_DIR
        lib_count = 0
        if library_dir.exists():
            lib_count = sum(1 for f in library_dir.rglob("*")
                            if f.is_file() and f.suffix.lower() in AUDIO_EXTENSIONS)
        show_header(library_dir, lib_count)
        print()
        show_menu()
        print(f"  {C_CYAN}>{C_RESET} ", end="", flush=True)
        time.sleep(5)
        return

    is_interactive = (not args.folder and not args.fix_covers and not args.fix_tags
                      and not args.remove_dupes and not args.clean_source
                      and not args.convert_flac and not args.ai_genres
                      and not args.batch_rename and not args.undo
                      and args.detect_fakes is None)

    # First-run: show setup wizard if no config exists at all (interactive mode only)
    if is_interactive:
        no_config = (not CONFIG_FILE.exists() and not (CONFIG_DIR / ".env").exists()
                     and not _OLD_CONFIG_FILE.exists())
        if no_config:
            _first_run_setup()

    _check_api_keys()

    # If no arguments at all, launch interactive menu
    if is_interactive:
        interactive_menu()
        return

    library_dir = Path(args.library)

    if args.remove_dupes:
        remove_duplicates(library_dir, args.dry_run)
        return

    if args.fix_tags:
        fix_tags(library_dir, args.dry_run)
        return

    if args.fix_covers:
        fix_covers(library_dir, args.dry_run)
        return

    if args.clean_source:
        source = Path(args.clean_source)
        if not source.is_dir():
            print(f"Not a directory: {source}")
            sys.exit(1)
        clean_source_folder(source, library_dir, args.dry_run)
        return

    if args.convert_flac:
        if not check_ffmpeg():
            print("FFmpeg not found. Install with: brew install ffmpeg")
            sys.exit(1)
        batch_convert_flac(library_dir, args.dry_run)
        return

    if args.ai_genres:
        ai_genre_tag(library_dir, args.dry_run, organize=args.organize)
        return

    if args.batch_rename:
        batch_rename_library(library_dir, args.dry_run)
        return

    if args.undo:
        undo_last_operation()
        return

    if args.detect_fakes is not None:
        targets = [Path(p) for p in args.detect_fakes] if args.detect_fakes else None
        analyze_bitrate_quality(library_dir, include_lossless=args.lossless,
                                target_paths=targets)
        return

    folder = Path(args.folder)
    if not folder.is_dir():
        print(f"Not a directory: {folder}")
        sys.exit(1)

    process_folder(folder, library_dir, args.dry_run, args.gemini, convert_flac=args.mp3)


if __name__ == "__main__":
    main()
