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

import os
import time
import json
import argparse
from pathlib import Path
from typing import List

ROOT = Path.cwd()
SOURCE_DIR = ROOT / "Source"
RESULTS_FILE = ROOT / "results.json"
KEYWORDS_FILE = ROOT / "keywords.txt"
RUN_COMPLETE_FILE = ROOT / "run_complete.flag"

def normpath_str_to_path(s: str) -> Path:
    """
    Convert paths stored in results.json (may have backslashes from Windows)
    into OS-native Paths.
    """
    import os as _os
    normalized = s.replace("\\", _os.sep)
    # If the path is absolute, Path will interpret it accordingly; if it's relative keep it relative
    return Path(normalized)

def load_keywords(path: Path) -> List[str]:
    if not path.exists():
        return []
    return [l.strip() for l in path.read_text(encoding="utf-8").splitlines() if l.strip()]

def find_html_files_for_site(site_dir: Path):
    """
    Return all html files in site_dir (excluding metadata files).
    """
    if not site_dir.exists():
        return []
    files = []
    for f in site_dir.iterdir():
        if f.is_file() and f.suffix.lower() in ('.html', '.htm'):
            files.append(f)
    return sorted(files, reverse=True)

def search_file_for_keywords(path: Path, keywords: List[str], threshold=1):
    """
    Read file (text) and find keyword occurrences. Return list of hits.
    """
    text = ""
    try:
        text = path.read_text(encoding="utf-8", errors="replace").lower()
    except Exception:
        return []
    hits = []
    for kw in keywords:
        if kw.lower() in text:
            hits.append({"keyword": kw, "file": str(path), "snippet": extract_snippet(text, kw.lower())})
    return hits

def extract_snippet(text: str, kw: str, context=120):
    idx = text.find(kw)
    if idx == -1:
        return ""
    start = max(0, idx - context)
    end = min(len(text), idx + len(kw) + context)
    snip = text[start:end]
    # return a short cleaned up snippet
    return snip.replace("\n", " ").strip()

def run_search(query=None, qfile=None, threshold=1):
    keywords = []
    if qfile:
        keywords = load_keywords(Path(qfile))
    elif query:
        # single comma-separated list accepted
        if isinstance(query, str) and "," in query:
            keywords = [q.strip() for q in query.split(",") if q.strip()]
        else:
            keywords = [query.strip()]
    else:
        keywords = load_keywords(KEYWORDS_FILE)

    results = {}
    # If results file exists, load it to preserve unrelated keys
    if RESULTS_FILE.exists():
        try:
            results = json.loads(RESULTS_FILE.read_text(encoding="utf-8")) or {}
        except Exception:
            results = {}

    # iterate Source/* directories
    if not SOURCE_DIR.exists():
        print("No Source/ directory found. Run the crawler first.")
        return

    for site_dir in SOURCE_DIR.iterdir():
        if not site_dir.is_dir():
            continue
        html_files = find_html_files_for_site(site_dir)
        for f in html_files:
            hits = search_file_for_keywords(f, keywords, threshold=threshold)
            for h in hits:
                k = h["keyword"]
                results.setdefault(k, []).append({
                    "site": site_dir.name,
                    "file": str(f),
                    "snippet": h.get("snippet"),
                    "found_at": time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
                })

    # atomic write
    tmp = RESULTS_FILE.with_suffix(".tmp")
    try:
        tmp.write_text(json.dumps(results, indent=2), encoding="utf-8")
        tmp.replace(RESULTS_FILE)
        print(f"Wrote {RESULTS_FILE} ({len(results)} keywords)")
    except Exception as e:
        print(f"Could not write results.json: {e}")

def run_update():
    # Run crawler.py in same Python env to update Source/ ...
    print("Running crawler.py to update Source/ ...")
    import subprocess, sys
    rc = subprocess.call([sys.executable, "crawler.py"])
    if rc != 0:
        print("crawler.py exited with code", rc)
    else:
        print("Update complete.")

def monitor_and_run(poll_seconds=10):
    """
    Watch keywords.txt and run_complete.flag and trigger searches/updates accordingly.
    """
    last_keywords_mtime = None
    last_run_flag = None

    while True:
        # check keywords
        if KEYWORDS_FILE.exists():
            m = KEYWORDS_FILE.stat().st_mtime
            if last_keywords_mtime is None:
                last_keywords_mtime = m
            elif m != last_keywords_mtime:
                print("Keywords file changed. Running incremental search.")
                run_search()
                last_keywords_mtime = m

        # check run_complete flag (crawler finished a run)
        if RUN_COMPLETE_FILE.exists():
            m2 = RUN_COMPLETE_FILE.stat().st_mtime
            if last_run_flag is None:
                last_run_flag = m2
            elif m2 != last_run_flag:
                print("Detected new crawler run. Running full search.")
                run_search()
                last_run_flag = m2

        time.sleep(poll_seconds)

def parse_args():
    p = argparse.ArgumentParser(description="Scooper - search saved pages for keywords")
    p.add_argument("cmd", choices=["search", "update", "monitor"], help="command to run")
    p.add_argument("--query", help="single keyword or comma-separated keywords to search now")
    p.add_argument("--query-file", help="path to keywords file")
    p.add_argument("--threshold", type=int, default=1, help="occurrence threshold")
    return p.parse_args()

def main():
    args = parse_args()
    if args.cmd == "update":
        run_update()
    elif args.cmd == "search":
        if not args.query and not args.query_file:
            print("Provide --query or --query-file")
            return
        run_search(query=args.query, qfile=args.query_file, threshold=args.threshold)
    elif args.cmd == "monitor":
        monitor_and_run()
    else:
        print("Unknown command")

if __name__ == "__main__":
    main()
