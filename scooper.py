#!/usr/bin/env python3
"""
scooper.py

Updated scooper for Ubuntu/Linux environments.

 - Scans under Source/<site>/html/*.html to match the crawler's storage layout.
 - Preserves original scanning and reporting behavior.
 - CLI: --watch and --poll-interval
"""
from pathlib import Path
import argparse
import logging
import sys
import time
import json
import os
from typing import List

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "Source"
LOG_DIR = BASE_DIR / "logs"
RESULTS_FILE = BASE_DIR / "results.json"
KEYWORDS_FILE = BASE_DIR / "keywords.txt"

DATA_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)
if not RESULTS_FILE.exists():
    RESULTS_FILE.write_text("[]", encoding="utf-8")

# logging setup
logfile = LOG_DIR / "scooper.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.FileHandler(logfile, encoding="utf-8"), logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("scooper")

# ----- utilities -----
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

# ----- keyword loading -----
def load_keywords() -> List[str]:
    if not KEYWORDS_FILE.exists():
        logger.warning("keywords.txt missing, scooper will not find anything")
        return []
    try:
        with KEYWORDS_FILE.open("r", encoding="utf-8") as fh:
            kws = [l.strip() for l in fh if l.strip() and not l.strip().startswith("#")]
            return kws
    except Exception:
        logger.exception("Failed to read keywords.txt")
        return []

# ----- scanning logic -----
from bs4 import BeautifulSoup

def scan_file(path: Path, keywords: List[str]):
    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        logger.exception(f"Failed to read {path}")
        return []
    found = []
    lower = text.lower()
    for kw in keywords:
        if kw.lower() in lower:
            found.append(kw)
    return found

def run_once():
    keywords = load_keywords()
    if not keywords:
        return
    # iterate html files in Source/<site>/html/
    for html_file in DATA_DIR.glob("*/html/*.html"):
        if not html_file.is_file():
            continue
        logger.info(f"Scanning {html_file}")
        found = scan_file(html_file, keywords)
        if found:
            entry = {
                "file": str(html_file),
                "keywords": found,
                "timestamp": int(time.time())
            }
            logger.info(f"Found keywords {found} in {html_file}")
            save_result(entry)

def watch_loop(poll_interval: int):
    logger.info("Starting watch loop")
    try:
        while True:
            try:
                run_once()
            except Exception:
                logger.exception("Error in scooper run_once")
            time.sleep(poll_interval)
    except KeyboardInterrupt:
        logger.info("Interrupted, exiting")

# ----- CLI -----
def main():
    parser = argparse.ArgumentParser(description="Scooper: scan saved HTML for keywords")
    parser.add_argument("--watch", action="store_true", help="Watch mode: poll Source/ every N seconds")
    parser.add_argument("--poll-interval", type=int, default=10, help="Poll interval in seconds for watch mode")
    args = parser.parse_args()

    if args.watch:
        watch_loop(args.poll_interval)
    else:
        run_once()

if __name__ == "__main__":
    main()
