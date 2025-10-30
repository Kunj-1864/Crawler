#!/usr/bin/env python3
"""
crawler.py

Updated crawler for Ubuntu/Linux environments with requested behavior:
 - Logs/prints start and completion messages per-site.
 - Saves raw HTML into per-site folders: Source/<sitename>/html/<timestamp>.html
 - Writes a crawl-complete flag file: Source/crawl_complete_<timestamp>.json
 - Preserves original crawling logic (loads sites.yaml, extracts title, appends to results.json)
 - CLI: --interval-minutes and --newnym
"""
from pathlib import Path
import os
import sys
import json
import time
import argparse
import logging
from typing import Optional, List

# network
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# stem for Tor control (NEWNYM)
try:
    from stem import Signal
    from stem.control import Controller
    HAS_STEM = True
except Exception:
    HAS_STEM = False

# ----- Base paths and initialization -----
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "Source"
LOG_DIR = BASE_DIR / "logs"
RESULTS_FILE = BASE_DIR / "results.json"
SITES_FILE = BASE_DIR / "sites.yaml"
KEYWORDS_FILE = BASE_DIR / "keywords.txt"

DATA_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)
if not RESULTS_FILE.exists():
    RESULTS_FILE.write_text("[]", encoding="utf-8")

# ----- Logging -----
logfile = LOG_DIR / "crawler.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.FileHandler(logfile, encoding="utf-8"), logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("crawler")

# ----- Requests session configured for Tor -----
TOR_SOCKS = os.environ.get("TOR_SOCKS", "socks5h://127.0.0.1:9050")
REQUEST_TIMEOUT = (15, 30)  # connect, read

session = requests.Session()
# polite retries for transient errors
retry_strategy = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["HEAD", "GET", "OPTIONS"]
)
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("http://", adapter)
session.mount("https://", adapter)
session.proxies.update({
    "http": TOR_SOCKS,
    "https": TOR_SOCKS,
})

# ----- Utility helpers -----
def atomic_write_json(path: Path, data):
    tmp = path.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())
    tmp.replace(path)

def read_results():
    try:
        with RESULTS_FILE.open("r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:
        logger.exception("Failed to read results.json, returning empty list")
        return []

def save_result(entry: dict):
    results = read_results()
    results.append(entry)
    try:
        atomic_write_json(RESULTS_FILE, results)
    except Exception:
        logger.exception("Failed to write results.json")

# ----- Tor controller / NEWNYM support -----
def get_tor_controller(control_port: int = 9051) -> Optional[Controller]:
    if not HAS_STEM:
        return None
    # try unix socket locations used by Debian/Ubuntu
    possible_sockets = ["/run/tor/control", "/var/run/tor/control"]
    for sock in possible_sockets:
        if Path(sock).exists():
            try:
                return Controller.from_socket_file(path=sock)
            except Exception:
                logger.debug(f"Couldn't use socket {sock} as controller")
    # try TCP control port fallback
    try:
        return Controller.from_port(port=control_port)
    except Exception:
        return None

def tor_newnym() -> bool:
    ctrl = get_tor_controller()
    if not ctrl:
        logger.debug("stem not available or tor controller not reachable")
        return False
    try:
        ctrl.authenticate()
        ctrl.signal(Signal.NEWNYM)
        logger.info("Requested Tor NEWNYM (identity change)")
        return True
    except Exception:
        logger.exception("Failed to signal NEWNYM")
        return False

# ----- Basic crawler logic (keeps original behavior but wraps network ops) -----
import yaml
from bs4 import BeautifulSoup

def load_sites() -> List[dict]:
    if not SITES_FILE.exists():
        logger.warning("sites.yaml not found; no sites to crawl")
        return []
    try:
        with SITES_FILE.open("r", encoding="utf-8") as fh:
            data = yaml.safe_load(fh)
            if not data:
                return []
            # normalize to list of dicts or strings
            sites = []
            if isinstance(data, list):
                for item in data:
                    sites.append(item)
            elif isinstance(data, dict):
                # if top-level dict, convert to list
                for k, v in data.items():
                    if isinstance(v, dict):
                        sites.append(v)
                    else:
                        sites.append({"url": v, "name": k})
            else:
                logger.warning("sites.yaml format not recognized")
            return sites
    except Exception:
        logger.exception("Failed to load sites.yaml")
        return []

def fetch_url(url: str, allow_redirects: bool = True):
    try:
        r = session.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=allow_redirects)
        r.raise_for_status()
        return r
    except Exception as e:
        logger.warning(f"Failed to fetch {url}: {e}")
        return None

def save_html_for_site(site_name: str, html: bytes, timestamp: int) -> Optional[Path]:
    # create per-site folder: Source/<sitename>/html/
    safe_site = site_name.strip().replace("/", "_") or "site"
    site_dir = DATA_DIR / safe_site / "html"
    site_dir.mkdir(parents=True, exist_ok=True)
    filename = f"{timestamp}.html"
    path = site_dir / filename
    try:
        with path.open("wb") as fh:
            fh.write(html)
        return path
    except Exception:
        logger.exception("Failed to save HTML for site %s", site_name)
        return None

def extract_title(html: bytes) -> str:
    try:
        soup = BeautifulSoup(html, "lxml")
        t = soup.title.string if soup.title and soup.title.string else ""
        return t.strip()
    except Exception:
        return ""

def run_once() -> dict:
    """Run a single crawl over all sites. Returns a summary dict."""
    sites = load_sites()
    summary = {
        "timestamp": int(time.time()),
        "crawled": []
    }
    if not sites:
        logger.info("No sites found in sites.yaml, exiting run_once")
        return summary

    for site in sites:
        # site may be a string or mapping with url & id
        if isinstance(site, str):
            url = site
            name = url.replace("https://", "").replace("http://", "")
        elif isinstance(site, dict):
            url = site.get("url") or site.get("site")
            name = site.get("name") or site.get("id") or (url or "site").replace("https://", "").replace("http://", "")
        else:
            continue

        if not url:
            continue

        # Notify start of crawling (stdout + log)
        msg_start = f"Crawling: {url}"
        logger.info(msg_start)
        print(msg_start)

        resp = fetch_url(url)
        if resp is None:
            msg_fail = f"Failed to crawl: {url}"
            logger.warning(msg_fail)
            print(msg_fail)
            continue

        timestamp = int(time.time())
        file_path = save_html_for_site(name, resp.content, timestamp)
        title = extract_title(resp.content)

        entry = {
            "url": url,
            "saved": str(file_path) if file_path else None,
            "title": title,
            "timestamp": timestamp,
            "site_name": name
        }
        save_result(entry)

        msg_done = f"Crawled: {url} -> {file_path}"
        logger.info(msg_done)
        print(msg_done)

        summary["crawled"].append({"url": url, "saved": str(file_path) if file_path else None, "title": title, "timestamp": timestamp})

        # polite sleep between requests
        time.sleep(2)

    # write crawl-complete flag file with summary
    flag_filename = DATA_DIR / f"crawl_complete_{summary['timestamp']}.json"
    try:
        atomic_write_json(flag_filename, summary)
        logger.info(f"Wrote crawl-complete flag: {flag_filename}")
        print(f"Crawl completed. Flag written: {flag_filename}")
    except Exception:
        logger.exception("Failed to write crawl complete flag")

    return summary

# ----- CLI and main loop -----
def main():
    parser = argparse.ArgumentParser(description="Crawler (Tor-aware)")
    parser.add_argument("--interval-minutes", type=float, default=0, help="If >0, run periodically every N minutes")
    parser.add_argument("--newnym", action="store_true", help="Try NEWNYM before each run")
    args = parser.parse_args()

    interval = max(0, args.interval_minutes)

    try:
        while True:
            if args.newnym:
                tor_newnym()
            run_once()
            if interval <= 0:
                break
            logger.info(f"Sleeping for {interval} minutes")
            time.sleep(interval * 60)
    except KeyboardInterrupt:
        logger.info("Interrupted, exiting")

if __name__ == "__main__":
    main()
