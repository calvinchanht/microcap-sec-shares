#!/usr/bin/env python3
"""
extract_shares.py
---------------
Extract shares outstanding from SEC companyfacts JSONs (unzipped from companyfacts.zip).
Writes to `latest-shares.jsonl` (JSON Lines: one record per line).
Also writes `meta.json` with counters.
"""

import os
import glob
import json
from datetime import datetime

CF_DIR = "cf"  # unzip destination
OUT_FILE = "latest-shares.jsonl"
META_FILE = "meta.json"

def find_companyfacts_files():
    # First try cf/ directly
    files = glob.glob(os.path.join(CF_DIR, "CIK*.json"))
    if not files:
        # fallback if SEC changes folder layout
        files = glob.glob(os.path.join(CF_DIR, "companyfacts", "CIK*.json"))
    print(f"[DEBUG] Found {len(files)} JSON files")
    if files[:5]:
        print("[DEBUG] Sample files:", files[:5])
    return files

def extract_shares_from_file(path):
    """Return list of share records from one CIK file"""
    try:
        with open(path, "r", encoding="utf-8") as f:
            j = json.load(f)
    except Exception as e:
        print(f"[WARN] Failed to parse {path}: {e}")
        return []

    cik = str(j.get("cik", "")).zfill(10)
    ticker = (j.get("ticker") or "").upper()
    out = []

    facts = j.get("facts", {})
    # Look in dei or us-gaap
    candidates = []
    if "dei" in facts and "EntityCommonStockSharesOutstanding" in facts["dei"]:
        candidates.append(facts["dei"]["EntityCommonStockSharesOutstanding"])
    if "us-gaap" in facts and "CommonStockSharesOutstanding" in facts["us-gaap"]:
        candidates.append(facts["us-gaap"]["CommonStockSharesOutstanding"])

    for fact in candidates:
        if not fact or "units" not in fact:
            continue
        for unit, arr in fact["units"].items():
            for rec in arr:
                val = rec.get("val")
                end = rec.get("end")
                if isinstance(val, (int, float)) and end:
                    out.append({
                        "symbol": ticker,
                        "cik": cik,
                        "shares_outstanding": val,
                        "shares_asof": end,
                        "source": "sec_companyfacts",
                        "fetch_ts": datetime.utcnow().strftime("%Y-%m-%d")
                    })
    return out

def main():
    files = find_companyfacts_files()
    total_files = 0
    total_rows = 0

    with open(OUT_FILE, "w", encoding="utf-8") as fout:
        for path in files:
            rows = extract_shares_from_file(path)
            total_files += 1
            if not rows:
                continue
            for r in rows:
                fout.write(json.dumps(r) + "\n")
                total_rows += 1

    meta = {
        "source_zip": "https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip",
        "tickers_source": "https://www.sec.gov/files/company_tickers.json",
        "companies_scanned": total_files,
        "rows_written": total_rows,
        "generated_utc": datetime.utcnow().isoformat()
    }
    with open(META_FILE, "w", encoding="utf-8") as f:
        json.dump(meta, f)

    print(f"[INFO] Finished. Companies scanned={total_files}, rows_written={total_rows}")

if __name__ == "__main__":
    main()
