#!/usr/bin/env python3
"""
scooper.py (single results.json)

- Loads keywords from keywords.txt (one per line).
- Searches saved HTML files under Source/ using metadata.json to discover files.
- Can run once or watch for:
    * new keywords added -> search only added keywords and update results.json
    * crawler run_complete.flag updated -> search all keywords and replace those entries in results.json
- Saves combined results into a single results.json file (atomic replace).
"""

import argparse
import json
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Tuple
from bs4 import BeautifulSoup
import re
from urllib.parse import urlparse

# --------- Config defaults ----------
SOURCE_DIR = Path("Source")
DEAD_FILE = Path("dead_sites.json")
KEYWORDS_FILE = Path("keywords.txt")
RUN_COMPLETE_FILE = Path("run_complete.flag")
RESULTS_FILE = Path("results.json")
POLL_INTERVAL = 10   # seconds (when --watch)
SNIPPET_CHARS = 120
# ------------------------------------

def utc_ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

def load_keywords(path: Path) -> List[str]:
    if not path.exists():
        return []
    lines = [l.strip() for l in path.read_text(encoding="utf-8").splitlines()]
    kws = [l for l in lines if l and not l.startswith("#")]
    return kws

def load_deadlist(path: Path) -> Dict[str, dict]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}

def load_site_metadata(site_dir: Path) -> dict:
    meta_file = site_dir / "metadata.json"
    if not meta_file.exists():
        return {}
    try:
        return json.loads(meta_file.read_text(encoding="utf-8"))
    except Exception:
        return {}

def gather_files_to_search(source_dir: Path) -> List[Tuple[str, str, Path, dict]]:
    results = []
    if not source_dir.exists():
        return results
    for site_dir in source_dir.iterdir():
        if not site_dir.is_dir():
            continue
        site_name = site_dir.name
        meta = load_site_metadata(site_dir)
        pages = meta.get("pages", {}) if isinstance(meta, dict) else {}
        seen_files = set()
        for url, page_meta in pages.items():
            files = page_meta.get("files", []) or []
            for fname in files:
                fpath = site_dir / fname
                if fpath.exists():
                    results.append((site_name, url, fpath, page_meta))
                    seen_files.add(str(fpath))
        for f in site_dir.glob("*.html"):
            if str(f) not in seen_files:
                results.append((site_name, f"http://{site_name}/{f.name}", f, {}))
    return results

def extract_text_from_html(content: bytes) -> str:
    try:
        soup = BeautifulSoup(content, "lxml")
        text = soup.get_text(separator=" ", strip=True)
        return re.sub(r"\s+", " ", text)
    except Exception:
        try:
            return content.decode("utf-8", errors="ignore")
        except Exception:
            return ""

def find_snippets(text: str, keyword: str, chars: int = SNIPPET_CHARS) -> List[str]:
    snippets = []
    try:
        pattern = re.compile(re.escape(keyword), flags=re.IGNORECASE)
    except re.error:
        return []
    for m in pattern.finditer(text):
        start = max(0, m.start() - chars)
        end = min(len(text), m.end() + chars)
        snippet = text[start:end]
        # highlight matched part by uppercase for quick visibility
        snippet = snippet[:m.start()-start] + text[m.start():m.end()].upper() + snippet[m.end()-start:]
        snippets.append(snippet)
        if len(snippets) >= 5:
            break
    return snippets

def search_keywords_in_files(keywords: List[str], file_index: List[Tuple[str, str, Path, dict]]) -> Dict[str, List[dict]]:
    deadlist = load_deadlist(DEAD_FILE)
    results = {k: [] for k in keywords}
    for site, url, fpath, page_meta in file_index:
        archived = bool(page_meta.get("archived")) if isinstance(page_meta, dict) else False
        # determine site key (prefer hostname)
        parsed_key = site
        try:
            parsed = urlparse(url)
            hostname = parsed.hostname
            if hostname:
                parsed_key = hostname
        except Exception:
            parsed_key = site
        is_dead = False
        if parsed_key in deadlist:
            entry = deadlist.get(parsed_key, {})
            until = entry.get("until")
            if until:
                try:
                    if isinstance(until, str):
                        until_dt = datetime.fromisoformat(until)
                    else:
                        until_dt = until
                    if datetime.now(timezone.utc) < until_dt:
                        is_dead = True
                except Exception:
                    is_dead = True
            else:
                is_dead = True
        try:
            content = fpath.read_bytes()
        except Exception:
            continue
        text = extract_text_from_html(content)
        if not text:
            continue
        for kw in keywords:
            if not kw:
                continue
            snippets = find_snippets(text, kw)
            if snippets:
                results[kw].append({
                    "site": site,
                    "url": url,
                    "file": str(fpath),
                    "archived": archived,
                    "is_dead": is_dead,
                    "snippets": snippets,
                    "found_at": utc_ts()
                })
    return results

# ---------- results.json helpers ----------
def load_results_file(path: Path) -> dict:
    if not path.exists():
        # return initial structure
        return {"last_run": None, "keywords": {}, "history": []}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"last_run": None, "keywords": {}, "history": []}

def atomic_write_json(path: Path, data: dict):
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2), encoding="utf-8")
    # atomic replace
    tmp.replace(path)

def update_results_for_keywords(path: Path, new_results: Dict[str, List[dict]], trigger: str):
    """
    Load existing results.json, update entries for keywords in new_results.
    - For each keyword in new_results, replace its list with the new list and set last_searched_at.
    - Update last_run and append a short history entry.
    """
    current = load_results_file(path)
    now = utc_ts()
    for kw, hits in new_results.items():
        current["keywords"][kw] = {
            "last_searched_at": now,
            "hits": hits
        }
    current["last_run"] = now
    # add a small history record (max 100 entries)
    current.setdefault("history", [])
    current["history"].insert(0, {"ts": now, "trigger": trigger, "updated_keywords": list(new_results.keys())})
    if len(current["history"]) > 100:
        current["history"] = current["history"][:100]
    atomic_write_json(path, current)
    return path

# ---------- main flows ----------
def run_once_all(keywords_file: Path, source_dir: Path):
    keywords = load_keywords(keywords_file)
    if not keywords:
        print("No keywords found; nothing to do.")
        return None
    file_index = gather_files_to_search(source_dir)
    print(f"Searching {len(keywords)} keywords across {len(file_index)} files...")
    results = search_keywords_in_files(keywords, file_index)
    out = update_results_for_keywords(RESULTS_FILE, results, trigger="manual-run")
    print(f"Updated {out} with all keyword results.")
    return out

def run_once_for_list(keywords: List[str], source_dir: Path, trigger: str):
    if not keywords:
        print("No keywords supplied.")
        return None
    file_index = gather_files_to_search(source_dir)
    print(f"Searching {len(keywords)} keywords across {len(file_index)} files...")
    results = search_keywords_in_files(keywords, file_index)
    out = update_results_for_keywords(RESULTS_FILE, results, trigger=trigger)
    print(f"Updated {out} for keywords: {keywords}")
    return out

def watch_loop(keywords_file: Path, source_dir: Path, run_complete_file: Path, poll_interval: int):
    last_keywords = load_keywords(keywords_file)
    last_keywords_set = set(last_keywords)
    last_run_ts = None
    # initial run for existing keywords
    if last_keywords:
        print("Initial run for existing keywords...")
        run_once_all(keywords_file, source_dir)
    else:
        print("No initial keywords yet. Watching for additions...")

    while True:
        time.sleep(poll_interval)
        current_keywords = load_keywords(keywords_file)
        current_set = set(current_keywords)
        added = sorted(list(current_set - last_keywords_set))
        if added:
            print(f"Detected {len(added)} new keyword(s): {added}. Searching them now...")
            run_once_for_list(added, source_dir, trigger="new-keywords")
            last_keywords_set = current_set

        if run_complete_file.exists():
            mtime = run_complete_file.stat().st_mtime
            if last_run_ts is None or mtime > last_run_ts:
                print("Detected crawler run completion signal. Searching all keywords now...")
                all_kws = list(current_set)
                if all_kws:
                    run_once_for_list(all_kws, source_dir, trigger="crawler-run-complete")
                else:
                    print("No keywords to search.")
                last_run_ts = mtime

def parse_args():
    p = argparse.ArgumentParser(description="Scooper: keyword search over saved crawler content (single results.json)")
    p.add_argument("--keywords-file", type=str, default=str(KEYWORDS_FILE))
    p.add_argument("--source-dir", type=str, default=str(SOURCE_DIR))
    p.add_argument("--run-complete-file", type=str, default=str(RUN_COMPLETE_FILE))
    p.add_argument("--watch", action="store_true", help="Watch keywords file and run-complete file (polling).")
    p.add_argument("--once", action="store_true", help="Run one search for all keywords and exit.")
    p.add_argument("--poll-interval", type=int, default=POLL_INTERVAL, help="Poll interval in seconds when watching.")
    return p.parse_args()

def main():
    args = parse_args()
    keywords_file = Path(args.keywords_file)
    source_dir = Path(args.source_dir)
    run_complete_file = Path(args.run_complete_file)

    if args.once:
        run_once_all(keywords_file, source_dir)
        return

    if args.watch:
        print(f"Watching keywords at {keywords_file} and run signal at {run_complete_file}")
        print(f"Poll interval: {args.poll_interval}s")
        watch_loop(keywords_file, source_dir, run_complete_file, args.poll_interval)
        return

    # default: run once
    run_once_all(keywords_file, source_dir)

if __name__ == "__main__":
    main()
