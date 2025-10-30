#!/usr/bin/env python3
"""
crawler.py (Tor-aware + scheduler + dead-site handling + per-page versioning & archive)
With run-complete flag so scooper can detect crawl cycles.

Features:
- Auto-detect Tor SOCKS (9050 or 9150). Uses Tor if available.
- Optional Tor ControlPort NEWNYM support via `stem` (optional).
- Crawl sites defined in sites.yaml, save page HTML to Source/{site}/, maintain metadata.
- Scheduler: run once or loop every N minutes (--interval-minutes).
- Dead-site handling: when a site fails repeatedly, mark a site dead and skip until retry window expires.
- Per-page versioning: keep up to MAX_VERSIONS_PER_PAGE versions for live pages (default 3).
- Archiving: pages that return ARCHIVE_STATUS_CODES (404,410) are marked archived and never pruned.
- Writes `run_complete.flag` (timestamp) at the end of each full cycle.
"""
# NOTE: This is the full crawler file with small, safe additions:
#  - enable_global_socks_proxy() (opt-in --force-socks)
#  - session_with_optional_tor() warns if PySocks missing
#  - CLI flags: --force-socks, --no-tor, --tor-port

import os
import time
import json
import random
import socket
import argparse
import hashlib
import yaml
import requests
from datetime import datetime, timezone, timedelta
from urllib.parse import urljoin, urlparse, urldefrag, urlencode, parse_qsl, urlunparse
from pathlib import Path
from typing import Dict, List

# ----------------- Configuration defaults (can be tuned) -----------------
CONTROL_PASSWORD = os.environ.get("CONTROL_PASSWORD")
CONTROL_PORT = 9051

# These constants might have been present in original file; set sensible defaults if missing.
# (If your original file sets these earlier, these won't override.)
HEADERS = [
    {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0 Safari/537.36"},
    {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0 Safari/537.36"},
]
POLITE_DEFAULT = (1.0, 3.0)  # seconds: min, max
ARCHIVE_STATUS_CODES = {404, 410}
MAX_VERSIONS_PER_PAGE = 3

# Files / paths
ROOT = Path.cwd()
RUN_COMPLETE_FILE = ROOT / "run_complete.flag"
SOURCE_DIR = ROOT / "Source"

# ----------------- Small utilities -----------------
def utc_now():
    return datetime.now(timezone.utc)

def utc_ts():
    return utc_now().strftime("%Y%m%dT%H%M%SZ")

def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def safe_filename_from_url(url: str) -> str:
    h = hashlib.sha1(url.encode()).hexdigest()[:12]
    return f"{utc_ts()}_{h}.html"

def normalize_url_for_filename(url: str) -> str:
    absu, _ = urldefrag(url)
    p = urlparse(absu)
    q = urlencode(sorted(parse_qsl(p.query, keep_blank_values=True)))
    norm = urlunparse((p.scheme, p.netloc, p.path or "/", p.params, q, ""))
    return norm

def load_sites(path: Path):
    if not path.exists():
        raise SystemExit("sites.yaml not found")
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return [s for s in data.get("sites", []) if s.get("enabled", True)]

# ----------------- Tor / SOCKS helpers -----------------

def detect_tor_socks_port():
    """
    Probe the common Tor SOCKS ports (9050, 9150). Returns the first found port or None.
    """
    for port in (9050, 9150):
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.6):
                return port
        except OSError:
            continue
    return None

def enable_global_socks_proxy(host="127.0.0.1", port=9050):
    """
    Globally monkeypatch socket.socket to route all TCP through a SOCKS5 proxy.
    Use only if a library doesn't accept per-request proxies (--force-socks).
    Returns True if successful.
    """
    try:
        import socks  # PySocks
    except Exception:
        print("⚠️ pysocks not installed; run 'pip install pysocks' to enable global SOCKS monkeypatch")
        return False

    try:
        socks.set_default_proxy(socks.SOCKS5, host, port)
        # Replace socket.socket. This is powerful; only do it when user requests it.
        import socket as _socket
        _socket.socket = socks.socksocket
        print(f"🔀 Global socket monkeypatch enabled -> SOCKS5 {host}:{port}")
        return True
    except Exception as e:
        print(f"❌ Could not enable global SOCKS monkeypatch: {e}")
        return False

def session_with_optional_tor(no_tor=False, explicit_port: int = None):
    """
    Return a requests.Session. If Tor is present (or explicit_port provided) it will
    configure the session to use the Tor SOCKS proxy using socks5h:// so DNS is resolved
    through Tor.
    """
    port = explicit_port if explicit_port else detect_tor_socks_port()
    s = requests.Session()
    s.headers.update(random.choice(HEADERS))

    if no_tor:
        print("ℹ️ Running with --no-tor: not attempting to use Tor proxy.")
        return s

    if port:
        # warn if PySocks missing (requests needs PySocks to handle socks:// URLs)
        try:
            import socks  # just to check presence
        except Exception:
            print("⚠️ PySocks not installed: run 'pip install pysocks' so requests can use socks5 proxies")
            # still return session (without proxies)
            return s

        proxy = f"socks5h://127.0.0.1:{port}"
        s.proxies.update({"http": proxy, "https": proxy})
        print(f"✅ Using Tor SOCKS proxy at 127.0.0.1:{port}")
    else:
        print("⚠️ Tor SOCKS proxy not detected (no 9050/9150). Running without Tor.")
    return s

# ----------------- HTTP fetch helpers -----------------
def fetch_url(session: requests.Session, url: str, attempts=3, base_delay=3, allow_text=False, timeout=45):
    """
    Use provided requests.Session to fetch `url`. Retries on RequestException with exponential backoff.
    Does not raise for 404/410 (archive detection).
    """
    last = None
    delay = base_delay
    for i in range(1, attempts + 1):
        try:
            r = session.get(url, allow_redirects=True, timeout=timeout)
            # do not raise for 404/410 because we want to detect and archive
            if r.status_code >= 400 and r.status_code not in ARCHIVE_STATUS_CODES:
                r.raise_for_status()
            return r
        except requests.RequestException as e:
            last = e
            if i < attempts:
                time.sleep(delay)
                delay *= 2
            else:
                # final failure; return the last exception as an object for the caller to interpret
                raise last

# ----------------- Versioned file helpers -----------------
def prune_old_versions(site_dir: Path, page_meta: dict, max_versions: int):
    """
    Keep newest -> oldest ordering in page_meta['files'].
    Remove oldest beyond max_versions.
    """
    files = page_meta.get("files", [])
    removed = []
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
def is_site_dead(deadlist: dict, site_key: str):
    entry = deadlist.get(site_key)
    if not entry:
        return False
    until = entry.get("until")
    if not until:
        return False
    try:
        until_dt = datetime.fromisoformat(until)
    except Exception:
        return False
    return datetime.now(timezone.utc) < until_dt

def crawl_site(site_conf: dict, session: requests.Session, deadlist: dict,
               seeds=None, max_pages=50, max_depth=2, polite=POLITE_DEFAULT,
               max_failures=3, retry_hours_default=24, default_max_versions=MAX_VERSIONS_PER_PAGE):
    """
    Crawl a single site according to site_conf
    """
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

    # Seeds and crawling structures
    to_visit = []
    visited = set()
    pages_fetched = 0

    if not seeds:
        seeds = [base]
    for s in seeds:
        to_visit.append((s, 0))

    site_dir = SOURCE_DIR / site_key
    site_dir.mkdir(parents=True, exist_ok=True)

    metadata = {"pages": {}}
    metadata_path = site_dir / "metadata.json"
    if metadata_path.exists():
        try:
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        except Exception:
            metadata = {"pages": {}}

    failures = 0
    while to_visit and pages_fetched < max_pages:
        url, depth = to_visit.pop(0)
        if url in visited:
            continue
        if depth > max_depth:
            continue

        # polite sleep
        time.sleep(random.uniform(*polite))

        try:
            r = fetch_url(session, url)
        except Exception as e:
            print(f"[ERR] Failed to fetch {url}: {e}")
            failures += 1
            if failures >= max_failures:
                # mark site dead for retry_hours_default
                until_dt = utc_now() + timedelta(hours=retry_hours_default)
                deadlist[site_key] = {"until": until_dt.isoformat()}
                print(f"[DEAD] Marking {site_key} dead until {until_dt.isoformat()}")
                # persist deadlist if needed (left to main program)
                return
            continue

        visited.add(url)
        pages_fetched += 1

        if is_html_response(r):
            content = r.text
            fname = safe_filename_from_url(url)
            outpath = site_dir / fname
            outpath.write_text(content, encoding='utf-8', errors='replace')

            # update metadata
            page_key = normalize_url_for_filename(url)
            page_meta = metadata["pages"].get(page_key, {"files": []})
            page_meta["files"].insert(0, fname)
            page_meta["last_fetched"] = utc_now().isoformat()
            # archive detection
            if r.status_code in ARCHIVE_STATUS_CODES:
                page_meta["archived"] = True
            metadata["pages"][page_key] = page_meta

            # prune old versions
            prune_old_versions(site_dir, page_meta, default_max_versions)
            # save metadata per site
            try:
                metadata_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")
            except Exception as e:
                print(f"[WARN] Could not write metadata for {site_key}: {e}")

            # find links for same-host crawling (simple approach)
            try:
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(content, "html.parser")
                links = set()
                for a in soup.find_all("a", href=True):
                    href = a.get("href")
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
            except Exception:
                # If bs4 not installed or parsing failed, skip link extraction
                pass

    print(f"Done: fetched {pages_fetched} pages for {site_name}")

def is_html_response(resp):
    ctype = resp.headers.get("Content-Type", "").lower()
    return ("text/html" in ctype) or ("application/xhtml+xml" in ctype)

# ----------------- Tor Control helpers (optional) -----------------
try:
    from stem import Signal
    from stem.control import Controller
    STEM_AVAILABLE = True
except Exception:
    STEM_AVAILABLE = False

def renew_tor_identity(password=None, port=CONTROL_PORT):
    if not STEM_AVAILABLE:
        print("⚠️ stem not installed -- cannot renew Tor identity programmatically. "
              "You can either 'pip install stem' and enable Tor ControlPort in torrc, "
              "or restart the tor service (sudo systemctl restart tor) as a fallback.")
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

def parse_args():
    p = argparse.ArgumentParser(description="Dark Web Monitor crawler")
    p.add_argument("--interval-minutes", type=float, default=0,
                   help="Minutes between runs; 0 or negative disables loop (single run).")
    p.add_argument("--max-failures", type=int, default=3,
                   help="Number of consecutive failures before marking a site dead.")
    p.add_argument("--retry-hours", type=float, default=24.0,
                   help="How many hours to skip a site after marking it dead (can be overridden per-site in sites.yaml)")
    p.add_argument("--single-run", action="store_true",
                   help="Run one iteration regardless of --interval-minutes and exit (convenience).")
    p.add_argument("--force-socks", action="store_true",
                   help="Force monkeypatch sockets via pysocks (use when other libs don't honor proxies)")
    p.add_argument("--no-tor", action="store_true", help="Do not attempt to use Tor even if available")
    p.add_argument("--tor-port", type=int, default=None, help="Explicit Tor SOCKS port (overrides auto-detect)")
    return p.parse_args()

def main_loop(interval_minutes=0, max_failures=3, retry_hours_default=24.0, single_run=False):
    args = parse_args()

    # optionally enable global monkeypatch for libs that don't support proxies
    if args.force_socks:
        port = args.tor_port or detect_tor_socks_port() or 9050
        ok = enable_global_socks_proxy(port=port)
        if not ok:
            print("Warning: --force-socks requested but could not enable it; proceeding without global SOCKS monkeypatch.")

    # create session (will try Tor unless --no-tor)
    session = session_with_optional_tor(no_tor=args.no_tor, explicit_port=args.tor_port)

    # load site list
    sites = []
    try:
        sites = load_sites(ROOT / "sites.yaml")
    except SystemExit as e:
        print(e)
        return

    deadlist = {}
    # If a deadlist file exists, try to load it (optional)
    deadlist_file = ROOT / "deadlist.json"
    if deadlist_file.exists():
        try:
            deadlist = json.loads(deadlist_file.read_text(encoding="utf-8")) or {}
        except Exception:
            deadlist = {}

    while True:
        for s in sites:
            crawl_site(s, session, deadlist,
                       seeds=[s.get("url")], max_pages=s.get("max_pages", 50),
                       max_depth=s.get("max_depth", 2),
                       polite=tuple(s.get("polite", POLITE_DEFAULT)),
                       max_failures=s.get("max_failures", max_failures),
                       retry_hours_default=s.get("retry_hours", retry_hours_default),
                       default_max_versions=s.get("max_versions_per_page", MAX_VERSIONS_PER_PAGE))

        # persist deadlist
        try:
            deadlist_file.write_text(json.dumps(deadlist, indent=2), encoding="utf-8")
        except Exception:
            pass

        # write the run-complete flag so scooper can pick it up
        write_run_complete_flag()

        if single_run or args.single_run:
            print("Single run complete. Exiting.")
            return

        if interval_minutes <= 0:
            print("Interval not specified (<=0). Exiting after one run.")
            return

        next_run = utc_now() + timedelta(minutes=interval_minutes)
        print(f"Sleeping for {interval_minutes} minutes. Next run at {next_run.isoformat()}")
        time.sleep(interval_minutes * 60)

if __name__ == "__main__":
    args = parse_args()
    # Run main loop with parsed args
    main_loop(interval_minutes=args.interval_minutes,
              max_failures=args.max_failures,
              retry_hours_default=args.retry_hours,
              single_run=args.single_run)
