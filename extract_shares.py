#!/usr/bin/env python3
"""
extract_shares.py
Extract shares outstanding from SEC companyfacts JSONs.
Usage:
  python3 extract_shares.py <cf_dir_or_dot> <tickers_json_unused> <out_dir>

Writes:
  <out_dir>/latest-shares.jsonl  (JSON Lines: one record per line)
  <out_dir>/meta.json
"""

import os
import sys
import glob
import json
from datetime import datetime, timezone

def find_companyfacts_files(cf_hint):
    """Try several plausible locations and return list of CIK*.json paths."""
    candidates = []
    search_roots = []
    if cf_hint:
        search_roots += [cf_hint, os.path.join(cf_hint, "companyfacts")]
    # also fall back to common roots
    search_roots += [".", "cf", os.path.join("cf", "companyfacts")]

    seen = set()
    for root in search_roots:
        root = os.path.abspath(root)
        if not os.path.isdir(root):
            continue
        for path in glob.glob(os.path.join(root, "CIK*.json")):
            if path not in seen:
                seen.add(path)
                candidates.append(path)

    print(f"[DEBUG] Search roots tried: {search_roots}")
    print(f"[DEBUG] Found {len(candidates)} JSON files")
    if candidates[:5]:
        print("[DEBUG] Sample files:", candidates[:5])
    return candidates

def extract_shares_from_file(path):
    """Return a list of rows (dicts) extracted from one CIK JSON file."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            j = json.load(f)
    except Exception as e:
        print(f"[WARN] Failed to parse {path}: {e}")
        return []

    cik = str(j.get("cik", "")).zfill(10)
    ticker = (j.get("ticker") or "").upper()
    facts = j.get("facts", {})
    out = []

    # candidate facts
    dei = facts.get("dei", {})
    gaap = facts.get("us-gaap", {})
    candidates = []
    if "EntityCommonStockSharesOutstanding" in dei:
        candidates.append(dei["EntityCommonStockSharesOutstanding"])
    if "CommonStockSharesOutstanding" in gaap:
        candidates.append(gaap["CommonStockSharesOutstanding"])

    for fact in candidates:
        units = fact.get("units", {}) if isinstance(fact, dict) else {}
        for unit, arr in units.items():
            for rec in arr or []:
                val = rec.get("val")
                end = rec.get("end")
                if isinstance(val, (int, float)) and end:
                    out.append({
                        "symbol": ticker,
                        "cik": cik,
                        "shares_outstanding": val,
                        "shares_asof": end,
                        "source": "sec_companyfacts",
                        "fetch_ts": datetime.now(timezone.utc).strftime("%Y-%m-%d")
                    })
    return out

def main():
    # args
    cf_dir = sys.argv[1] if len(sys.argv) > 1 else "."
    # sys.argv[2] could be a tickers.json path (not required for raw extraction), ignore for now
    out_dir = sys.argv[3] if len(sys.argv) > 3 else "."
    os.makedirs(out_dir, exist_ok=True)

    out_file = os.path.join(out_dir, "latest-shares.jsonl")
    meta_file = os.path.join(out_dir, "meta.json")

    files = find_companyfacts_files(cf_dir)
    total_files, total_rows = 0, 0

    with open(out_file, "w", encoding="utf-8") as fout:
        for path in files:
            rows = extract_shares_from_file(path)
            total_files += 1
            for r in rows:
                fout.write(json.dumps(r) + "\n")
                total_rows += 1

    meta = {
        "source_zip": "https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip",
        "tickers_source": "https://www.sec.gov/files/company_tickers.json",
        "companies_scanned": total_files,
        "rows_written": total_rows,
        "generated_utc": datetime.now(timezone.utc).isoformat()
    }
    with open(meta_file, "w", encoding="utf-8") as f:
        json.dump(meta, f)

    print(f"[INFO] Finished. Companies scanned={total_files}, rows_written={total_rows}")
    print(f"[INFO] Outputs: {out_file}, {meta_file}")

if __name__ == "__main__":
    main()
