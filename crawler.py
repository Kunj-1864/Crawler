#!/usr/bin/env python3 
"""
crawler.py (Tor-aware + scheduler + dead-site handling + per-page versioning & archive)
With run-complete flag so scooper can detect crawl cycles.

Features:
- Auto-detect Tor SOCKS (9050 or 9150). Uses Tor if available.
- Optional Tor ControlPort NEWNYM support via `stem` (optional).
- Crawl sites defined in sites.yaml, save page HTML to Source/{site}/, maintain metadata.
- Scheduler: run once or loop every N minutes (--interval-minutes).
- Dead-site handling: when a site fails repeatedly, mark it dead and skip until retry window expires.
- Per-page versioning: keep up to MAX_VERSIONS_PER_PAGE versions for live pages (default 3).
- Archiving: pages that return ARCHIVE_STATUS_CODES (404,410) are marked archived and never pruned.
- Writes `run_complete.flag` (timestamp) at the end of each full cycle.
"""

import os
import time
import json
import random
import socket
import argparse
import hashlib
from pathlib import Path
from datetime import datetime, timezone, timedelta
from urllib.parse import urljoin, urlparse, urldefrag, urlencode, parse_qsl, urlunparse

import requests
import yaml
from bs4 import BeautifulSoup

# ---------------- Config (tweak as needed) ----------------
SOURCE_DIR = Path("Source")
SITES_YAML = Path("sites.yaml")
DEAD_FILE = Path("dead_sites.json")
RUN_COMPLETE_FILE = Path("run_complete.flag")
HEADERS = [
    {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/124.0 Safari/537.36"},
    {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:124.0) Gecko/20100101 Firefox/124.0"}
]
SKIP_EXT = {".zip", ".7z", ".rar", ".gz", ".bz2", ".xz", ".tar",
            ".pdf", ".png", ".jpg", ".jpeg", ".gif", ".mp4", ".mp3", ".exe"}
POLITE_DEFAULT = (6, 12)

# Versioning / Archiving
MAX_VERSIONS_PER_PAGE = 3              # default max versions to keep for live pages
ARCHIVE_STATUS_CODES = {404, 410}      # status codes that mark page as removed/archived

# ControlPort env (optional)
CONTROL_PASSWORD = os.environ.get("CONTROL_PASSWORD")
CONTROL_PORT = 9051
# ---------------------------------------------------------

def utc_now():
    return datetime.now(timezone.utc)

def utc_ts():
    return utc_now().strftime("%Y%m%dT%H%M%SZ")

def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def detect_tor_socks_port():
    for port in (9050, 9150):
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.6):
                return port
        except OSError:
            continue
    return None

def session_with_optional_tor():
    port = detect_tor_socks_port()
    s = requests.Session()
    s.headers.update(random.choice(HEADERS))
    if port:
        proxy = f"socks5h://127.0.0.1:{port}"
        s.proxies.update({"http": proxy, "https": proxy})
        print(f"✅ Using Tor SOCKS proxy at 127.0.0.1:{port}")
    else:
        print("⚠️ Tor SOCKS proxy not detected (no 9050/9150). Running without Tor.")
    return s

def normalize_url_for_filename(url: str) -> str:
    absu, _ = urldefrag(url)
    p = urlparse(absu)
    q = urlencode(sorted(parse_qsl(p.query, keep_blank_values=True)))
    norm = urlunparse((p.scheme, p.netloc, p.path or "/", p.params, q, ""))
    return norm

def safe_filename_from_url(url: str) -> str:
    h = hashlib.sha1(url.encode()).hexdigest()[:12]
    return f"{utc_ts()}_{h}.html"

def load_sites(path: Path):
    if not path.exists():
        raise SystemExit("sites.yaml not found")
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return [s for s in data.get("sites", []) if s.get("enabled", True)]

def ensure_site_dir(site_name: str):
    d = SOURCE_DIR / site_name
    d.mkdir(parents=True, exist_ok=True)
    return d

def metadata_path(site_dir: Path):
    return site_dir / "metadata.json"

def load_metadata(site_dir: Path):
    mp = metadata_path(site_dir)
    if not mp.exists():
        return {"pages": {}}
    try:
        return json.loads(mp.read_text(encoding="utf-8"))
    except Exception:
        return {"pages": {}}

def save_metadata(site_dir: Path, meta: dict):
    mp = metadata_path(site_dir)
    mp.write_text(json.dumps(meta, indent=2), encoding="utf-8")

# old is_html_response expected a requests response;
# since fetch_url now returns headers/content, we'll keep a small helper for headers
def is_html_content_type(headers):
    ctype = ""
    try:
        ctype = headers.get("Content-Type", "") if headers else ""
    except Exception:
        ctype = ""
    ctype = ctype.lower()
    return ("text/html" in ctype) or ("application/xhtml+xml" in ctype)

# ---------------- Robust streaming fetch ----------------
def fetch_url(session: requests.Session, url: str, attempts=3, base_delay=3,
              timeout=45, connect_timeout=10, read_chunk=8192, max_bytes=5 * 1024 * 1024) -> dict:
    """
    Robust fetch that streams the response, enforces:
      - connect timeout (connect_timeout)
      - overall timeout (timeout) from the start of the request
      - max bytes to read (max_bytes)
    Returns dict: {'status_code': int, 'headers': resp.headers, 'content': bytes, 'text': str or None}
    Raises requests.RequestException on failure.
    """
    last_exc = None
    delay = base_delay
    for attempt in range(1, attempts + 1):
        start = time.monotonic()
        try:
            resp = session.get(url, allow_redirects=True, stream=True, timeout=(connect_timeout, 5))
            status = resp.status_code

            # Read content incrementally with global timeout and max_bytes cap
            chunks = []
            total = 0
            for chunk in resp.iter_content(chunk_size=read_chunk):
                # check global timeout
                if time.monotonic() - start > timeout:
                    resp.close()
                    raise requests.Timeout(f"Overall read timeout after {timeout}s for {url}")

                if chunk:
                    chunks.append(chunk)
                    total += len(chunk)
                    if total >= max_bytes:
                        # reached cap: stop reading further
                        resp.close()
                        break
                # tiny sleep not necessary but gives event loop a breath for interrupts
            content = b"".join(chunks)
            # try to decode a text version (safe)
            text = None
            try:
                text = content.decode("utf-8", errors="replace")
            except Exception:
                text = None

            headers = {}
            try:
                # convert to plain dict (requests' headers is case-insensitive dict-like)
                headers = dict(resp.headers)
            except Exception:
                headers = {}

            # do not raise for 404/410 here - caller handles archiving
            if status >= 400 and status not in ARCHIVE_STATUS_CODES:
                # for other 4xx/5xx raise to trigger retry/backoff
                raise requests.HTTPError(f"HTTP {status} for {url}")

            return {"status_code": status, "headers": headers, "content": content, "text": text}
        except (requests.Timeout, requests.ConnectionError, requests.HTTPError, requests.RequestException) as e:
            last_exc = e
            if attempt < attempts:
                time.sleep(delay)
                delay *= 2
                continue
            # final attempt failed: raise
            raise
    if last_exc:
        raise last_exc
    raise requests.RequestException("Unknown fetch error")

# ---------------- Dead-site persistence ----------------
def load_deadlist() -> dict:
    if DEAD_FILE.exists():
        try:
            raw = DEAD_FILE.read_text(encoding="utf-8")
            d = json.loads(raw)
            # convert until -> datetime
            for k, v in d.items():
                if v.get("until"):
                    v["until"] = datetime.fromisoformat(v["until"])
            return d
        except Exception:
            return {}
    return {}

def save_deadlist(d: dict):
    # convert datetimes to iso
    serial = {}
    for k, v in d.items():
        serial[k] = v.copy()
        if isinstance(serial[k].get("until"), datetime):
            serial[k]["until"] = serial[k]["until"].isoformat()
    DEAD_FILE.write_text(json.dumps(serial, indent=2), encoding="utf-8")

def is_site_dead(deadlist: dict, site_key: str):
    entry = deadlist.get(site_key)
    if not entry:
        return False
    until = entry.get("until")
    if not until:
        return False
    if isinstance(until, str):
        until = datetime.fromisoformat(until)
    if utc_now() < until:
        return True
    deadlist.pop(site_key, None)
    save_deadlist(deadlist)
    return False

def mark_site_dead(deadlist: dict, site_key: str, reason: str, retry_hours: float):
    until = utc_now() + timedelta(hours=retry_hours)
    deadlist[site_key] = {
        "marked_at": utc_now().isoformat(),
        "until": until,
        "reason": reason
    }
    save_deadlist(deadlist)
    print(f"🛑 Marked '{site_key}' dead until {until.isoformat()} (reason: {reason})")

# ----------------- Versioning & pruning helpers ----------------
def prune_old_versions(site_dir: Path, page_meta: dict, max_versions: int):
    """
    Keep at most max_versions files for a non-archived page.
    If page is archived (page_meta.get('archived') is True), do not prune.
    Returns list of removed filenames.
    """
    if page_meta.get("archived"):
        return []
    files = page_meta.get("files", [])
    if len(files) <= max_versions:
        return []
    removed = []
    # files stored newest -> oldest. To remove oldest, pop from end.
    while len(files) > max_versions:
        fname = files.pop()  # remove oldest
        fpath = site_dir / fname
        try:
            if fpath.exists():
                fpath.unlink()
                removed.append(fname)
        except Exception as e:
            print(f"[PRUNE ERR] Could not delete {fpath}: {e}")
    page_meta["files"] = files
    return removed

# ----------------- Crawl single site -----------------
def crawl_site(site_conf: dict, session: requests.Session, deadlist: dict,
               seeds=None, max_pages=50, max_depth=2, polite=POLITE_DEFAULT,
               max_failures=3, retry_hours_default=24, default_max_versions=MAX_VERSIONS_PER_PAGE):
    site_name = site_conf.get("name") or site_conf.get("url")
    base = site_conf.get("url")
    if not base:
        print(f"[WARN] Site config for {site_name} missing 'url'; skipping")
        return

    parsed_base = urlparse(base)
    site_key = site_conf.get("key") or parsed_base.hostname or site_name

    if is_site_dead(deadlist, site_key):
        entry = deadlist.get(site_key)
        print(f"[SKIP-Dead] {site_key} (until {entry.get('until')})")
        return

    site_dir = ensure_site_dir(site_name)
    meta = load_metadata(site_dir)
    meta.setdefault("pages", {})

    print(f"== Crawling {site_name} -> {base} ==")
    seeds = seeds or [base]
    visited = set()
    to_visit = [(s, 0) for s in seeds]
    pages_fetched = 0
    consecutive_failures = 0
    retry_hours = site_conf.get("retry_hours", retry_hours_default)
    site_max_versions = site_conf.get("max_versions_per_page", default_max_versions)

    while to_visit and pages_fetched < max_pages:
        url, depth = to_visit.pop(0)
        if url in visited:
            continue
        visited.add(url)

        path = urlparse(url).path or ""
        ext = (Path(path).suffix or "").lower()
        if ext in SKIP_EXT:
            print(f"[SKIP EXT] {url}")
            continue

        time.sleep(random.uniform(*polite))
        try:
            result = fetch_url(session, url, attempts=3, base_delay=3, timeout=45)
        except Exception as e:
            print(f"[ERR] {site_name} {url} :: {e}")
            consecutive_failures += 1
            meta["pages"].setdefault(url, {})["last_error"] = str(e)
            save_metadata(site_dir, meta)
            if consecutive_failures >= max_failures:
                reason = f"{consecutive_failures} consecutive fetch errors (last: {e})"
                mark_site_dead(deadlist, site_key, reason, retry_hours)
                return
            continue

        status = result.get("status_code")
        headers = result.get("headers", {}) or {}
        content = result.get("content", b"")
        text = result.get("text", None)

        page_meta = meta["pages"].setdefault(url, {})

        if status in ARCHIVE_STATUS_CODES:
            # mark archived and save metadata (do not prune archived pages)
            page_meta.setdefault("files", page_meta.get("files", []))
            page_meta["archived"] = True
            page_meta["last_error"] = f"HTTP {status}"
            page_meta["last_seen_at"] = utc_now().isoformat()
            save_metadata(site_dir, meta)
            print(f"[ARCHIVED] {url} -> HTTP {status}. Page archived and will not be pruned.")
            consecutive_failures += 1
            if consecutive_failures >= max_failures:
                reason = f"{consecutive_failures} consecutive fetch errors (last: HTTP {status})"
                mark_site_dead(deadlist, site_key, reason, retry_hours)
                return
            continue

        # Success: reset failure counter
        consecutive_failures = 0

        # store content (note: this may be partial if max_bytes cap reached)
        url_norm = normalize_url_for_filename(url)
        fname = safe_filename_from_url(url_norm)
        fpath = site_dir / fname
        try:
            fpath.write_bytes(content)
        except Exception as e:
            print(f"[FILE ERR] Could not write {fpath}: {e}")
            page_meta["last_error"] = f"write_error:{e}"
            save_metadata(site_dir, meta)
            continue

        # update page metadata: maintain files list newest->oldest
        page_meta.setdefault("files", [])
        page_meta["files"].insert(0, fname)  # newest at front
        page_meta["content_hash"] = sha256_bytes(content or b"")
        page_meta["status_code"] = status
        page_meta["content_type"] = headers.get("Content-Type", "")
        page_meta["last_seen_at"] = utc_now().isoformat()
        page_meta["archived"] = False

        # prune old versions for this page if not archived
        removed_files = prune_old_versions(site_dir, page_meta, max_versions=site_max_versions)
        if removed_files:
            print(f"[PRUNED] Removed old files for {url}: {removed_files}")

        save_metadata(site_dir, meta)
        pages_fetched += 1
        print(f"[SAVED] {site_name} {url} -> {fpath.name}")

        # extract links if allowed depth and HTML content-type
        if depth < max_depth and is_html_content_type(headers):
            soup = BeautifulSoup(text or "", "lxml")
            links = set()
            for a in soup.find_all("a", href=True):
                href = a["href"].strip()
                if href.lower().startswith("javascript:"):
                    continue
                try:
                    joined = urljoin(url, href)
                except Exception:
                    continue
                parsed = urlparse(joined)
                if parsed.hostname == urlparse(base).hostname:
                    links.add(joined)
            for l in links:
                if l not in visited:
                    to_visit.append((l, depth + 1))

    print(f"Done: fetched {pages_fetched} pages for {site_name}")

# ----------------- Tor Control helpers (optional) -----------------
try:
    from stem import Signal
    from stem.control import Controller
    STEM_AVAILABLE = True
except Exception:
    STEM_AVAILABLE = False

def renew_tor_identity(password=None, port=CONTROL_PORT):
    if not STEM_AVAILABLE:
        print("⚠️ stem not installed -- cannot renew Tor identity programmatically")
        return False
    try:
        with Controller.from_port(port=port) as controller:
            if password:
                controller.authenticate(password=password)
            else:
                controller.authenticate()
            controller.signal(Signal.NEWNYM)
            time.sleep(2)
            print("🔁 Sent NEWNYM to Tor (requested new identity)")
            return True
    except Exception as e:
        print(f"❌ Could not renew Tor identity: {e}")
        return False

# ----------------- Main loop / scheduler -----------------
def write_run_complete_flag():
    try:
        RUN_COMPLETE_FILE.write_text(utc_now().isoformat(), encoding="utf-8")
        print(f"📣 Wrote run-complete flag to {RUN_COMPLETE_FILE}")
    except Exception as e:
        print(f"[WARN] Could not write run-complete flag: {e}")

def main_loop(interval_minutes: int, max_failures: int, retry_hours_default: float, single_run=False):
    session = session_with_optional_tor()
    deadlist = load_deadlist()

    while True:
        try:
            sites = load_sites(SITES_YAML)
        except SystemExit as e:
            print(e)
            return

        for s in sites:
            crawl_site(s, session, deadlist,
                       seeds=[s.get("url")], max_pages=s.get("max_pages", 50),
                       max_depth=s.get("max_depth", 2),
                       polite=tuple(s.get("polite", POLITE_DEFAULT)),
                       max_failures=s.get("max_failures", max_failures),
                       retry_hours_default=s.get("retry_hours", retry_hours_default),
                       default_max_versions=s.get("max_versions_per_page", MAX_VERSIONS_PER_PAGE))

        # write the run-complete flag so scooper can pick it up
        write_run_complete_flag()

        if single_run:
            print("Single run complete. Exiting.")
            return

        if interval_minutes <= 0:
            print("Interval not specified (<=0). Exiting after one run.")
            return

        next_run = utc_now() + timedelta(minutes=interval_minutes)
        print(f"Sleeping for {interval_minutes} minutes. Next run at {next_run.isoformat()}")
        time.sleep(interval_minutes * 60)

def parse_args():
    p = argparse.ArgumentParser(description="Tor-aware crawler with scheduling, dead-site handling, and per-page versioning")
    p.add_argument("--interval-minutes", type=int, default=0,
                   help="If >0, run crawling every N minutes. If 0 (default), run once and exit.")
    p.add_argument("--max-failures", type=int, default=3,
                   help="Number of consecutive failures before marking a site dead.")
    p.add_argument("--retry-hours", type=float, default=24.0,
                   help="How many hours to skip a site after marking it dead (can be overridden per-site in sites.yaml)")
    p.add_argument("--single-run", action="store_true",
                   help="Run one iteration regardless of --interval-minutes and exit (convenience).")
    return p.parse_args()

if __name__ == "__main__":
    args = parse_args()
    try:
        main_loop(interval_minutes=args.interval_minutes,
                  max_failures=args.max_failures,
                  retry_hours_default=args.retry_hours,
                  single_run=args.single_run)
    except KeyboardInterrupt:
        print("Interrupted by user — exiting gracefully.")
        # optionally update run_complete so scooper can run if desired:
        try:
            write_run_complete_flag()
        except Exception:
            pass
        raise
