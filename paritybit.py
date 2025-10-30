#!/usr/bin/env python3
"""
paritybit.py
Simple CLI orchestrator:
  - python paritybit.py update   # run crawler to refresh Source/
  - python paritybit.py search --query "adda,adda.io"
"""

import argparse, subprocess, sys, os
from pathlib import Path

def run_update():
    # run crawler.py in same Python environment
    print("Running crawler.py to update Source/ ...")
    rc = subprocess.call([sys.executable, "crawler.py"])
    if rc != 0:
        print("crawler.py exited with code", rc)
    else:
        print("Update complete.")

def run_search(query=None, qfile=None, threshold=84):
    cmd = [sys.executable, "scooper.py"]
    if query:
        cmd += ["--query", query]
    elif qfile:
        cmd += ["--query-file", qfile]
    cmd += ["--threshold", str(threshold)]
    print("Running scooper:", " ".join(cmd))
    rc = subprocess.call(cmd)
    if rc != 0:
        print("scooper.py exited with code", rc)

def main():
    ap = argparse.ArgumentParser()
    sp = ap.add_subparsers(dest="cmd")
    sp_update = sp.add_parser("update")
    sp_search = sp.add_parser("search")
    sp_search.add_argument("--query", help="Comma-separated queries")
    sp_search.add_argument("--query-file", help="File with queries")
    sp_search.add_argument("--threshold", type=int, default=84)
    args = ap.parse_args()

    if args.cmd == "update":
        run_update()
    elif args.cmd == "search":
        if not args.query and not args.query_file:
            print("Provide --query or --query-file")
            return
        run_search(query=args.query, qfile=args.query_file, threshold=args.threshold)
    else:
        ap.print_help()

if __name__ == "__main__":
    main()
