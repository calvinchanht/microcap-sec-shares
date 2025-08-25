#!/usr/bin/env python3
# extract_shares.py — v0.4 (2025-08-25)
#
# What’s new in v0.4
# - Joins SEC companyfacts CIK files with the official ticker map
#   (company_tickers.json) so "symbol" is populated.
# - Robust path discovery: searches multiple common folders for CIK*.json.
# - UTC timestamps are timezone-aware; clearer debug output.
#
# Usage:
#   python3 extract_shares.py <cf_dir_or_dot> <tickers_json> <out_dir>
#
# Inputs:
#   <cf_dir_or_dot>   Path that *may* contain companyfacts JSON files.
#                     We’ll also try: <path>/companyfacts, ".", "cf",
#                     and "cf/companyfacts".
#   <tickers_json>    Path to SEC "company_tickers.json". If not provided
#                     or cannot be parsed, symbol fallback is empty.
#   <out_dir>         Output directory; will be created if missing.
#
# Outputs:
#   <out_dir>/latest-shares.jsonl  — JSON Lines, one record per line:
#       {"symbol": "NVDA", "cik": "0001045810", "shares_outstanding": 2460000000,
#        "shares_asof": "2024-01-28", "source": "sec_companyfacts", "fetch_ts": "2025-08-25"}
#
#   <out_dir>/meta.json            — run metadata

import os
import sys
import re
import glob
import json
from datetime import datetime, timezone
from typing import Dict, List, Any

# -------- helpers --------

def pad_cik10(cik: Any) -> str:
    s = re.sub(r"\D", "", str(cik or ""))
    return ("0000000000" + s)[-10:] if s else ""

def find_companyfacts_files(cf_hint: str) -> List[str]:
    """Try several plausible locations and return list of CIK*.json paths."""
    candidates: List[str] = []
    search_roots = []
    if cf_hint:
        search_roots += [cf_hint, os.path.join(cf_hint, "companyfacts")]
    # also fall back to common roots
    search_roots += [".", "cf", os.path.join("cf", "companyfacts")]

    seen = set()
    for root in search_roots:
        root_abs = os.path.abspath(root)
        if not os.path.isdir(root_abs):
            continue
        for path in glob.glob(os.path.join(root_abs, "CIK*.json")):
            if path not in seen:
                seen.add(path)
                candidates.append(path)

    print(f"[DEBUG] Search roots tried: {search_roots}")
    print(f"[DEBUG] Found {len(candidates)} JSON files")
    if candidates[:5]:
        print("[DEBUG] Sample files:", [os.path.relpath(p) for p in candidates[:5]])
    return candidates

def load_ticker_map(tickers_path: str) -> Dict[str, str]:
    """Read SEC company_tickers.json, return map cik10 -> TICKER."""
    if not tickers_path or not os.path.exists(tickers_path):
        print(f"[WARN] Tickers file not found: {tickers_path!r}")
        return {}
    try:
        with open(tickers_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        print(f"[WARN] Failed to parse tickers JSON: {e}")
        return {}

    cik2ticker: Dict[str, str] = {}

    # SEC sometimes publishes as an object with numeric keys ("0","1",...),
    # each value like: {"cik_str": 1045810, "ticker": "NVDA", "title": "..."}
    # It can also be an array. Handle either.
    values_iter = None
    if isinstance(data, dict):
        values_iter = data.values()
    elif isinstance(data, list):
        values_iter = data
    else:
        print("[WARN] Unexpected tickers JSON shape; expected dict or list.")
        return {}

    seen = 0
    for rec in values_iter:
        if not isinstance(rec, dict):
            continue
        cik_raw = rec.get("cik_str")
        ticker = (rec.get("ticker") or "").strip().upper()
        if not cik_raw or not ticker:
            continue
        cik10 = pad_cik10(cik_raw)
        if cik10 and ticker:
            cik2ticker[cik10] = ticker
            seen += 1

    print(f"[DEBUG] Ticker map loaded entries={len(cik2ticker)} (seen={seen})")
    # optional sanity peek
    peek = list(cik2ticker.items())[:5]
    if peek:
        print("[DEBUG] Ticker map sample:", peek)
    return cik2ticker

def extract_shares_from_file(path: str, ticker_map: Dict[str, str]) -> List[dict]:
    """Return rows extracted from one CIK JSON file."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            j = json.load(f)
    except Exception as e:
        print(f"[WARN] Failed to parse {os.path.relpath(path)}: {e}")
        return []

    # CIK and ticker
    cik_file = ""
    m = re.search(r"CIK(\d+)\.json$", os.path.basename(path))
    if m:
        cik_file = pad_cik10(m.group(1))

    cik_json = pad_cik10(j.get("cik"))
    cik = cik_json or cik_file

    ticker_json = (j.get("ticker") or "").strip().upper()
    ticker = ticker_json or (ticker_map.get(cik, "") if cik else "")

    # facts
    facts = j.get("facts", {})
    out: List[dict] = []

    # candidate facts
    dei = facts.get("dei", {}) if isinstance(facts, dict) else {}
    gaap = facts.get("us-gaap", {}) if isinstance(facts, dict) else {}
    candidates = []
    if isinstance(dei.get("EntityCommonStockSharesOutstanding"), dict):
        candidates.append(dei["EntityCommonStockSharesOutstanding"])
    if isinstance(gaap.get("CommonStockSharesOutstanding"), dict):
        candidates.append(gaap["CommonStockSharesOutstanding"])

    for fact in candidates:
        units = fact.get("units", {}) if isinstance(fact, dict) else {}
        for unit, arr in units.items():
            if not isinstance(arr, list):
                continue
            for rec in arr:
                if not isinstance(rec, dict):
                    continue
                val = rec.get("val")
                end = rec.get("end")
                if isinstance(val, (int, float)) and end:
                    # Only emit rows with a symbol (keeps dataset tidy/usable)
                    if not ticker:
                        continue
                    out.append({
                        "symbol": ticker,
                        "cik": cik,
                        "shares_outstanding": val,
                        "shares_asof": end,
                        "source": "sec_companyfacts",
                        "fetch_ts": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                    })
    return out

# -------- main --------

def main():
    # args
    cf_dir = sys.argv[1] if len(sys.argv) > 1 else "."
    tickers_json = sys.argv[2] if len(sys.argv) > 2 else ""
    out_dir = sys.argv[3] if len(sys.argv) > 3 else "."
    os.makedirs(out_dir, exist_ok=True)

    out_file = os.path.join(out_dir, "latest-shares.jsonl")
    meta_file = os.path.join(out_dir, "meta.json")

    files = find_companyfacts_files(cf_dir)
    ticker_map = load_ticker_map(tickers_json)

    total_files, total_rows = 0, 0
    with open(out_file, "w", encoding="utf-8") as fout:
        for i, path in enumerate(files, 1):
            rows = extract_shares_from_file(path, ticker_map)
            total_files += 1
            for r in rows:
                fout.write(json.dumps(r) + "\n")
                total_rows += 1
            if i % 1000 == 0:
                print(f"[INFO] Processed {i}/{len(files)} files... rows_so_far={total_rows}")

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
