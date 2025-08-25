#!/usr/bin/env python3
# extract_shares.py — v0.5 (2025-08-25)
#
# What’s new in v0.5
# - In addition to full JSONL, emits compact per-symbol snapshots:
#     public/latest-by-symbol.csv
#     public/latest-by-symbol.json
# - Picks the most recent shares_asof per symbol (ties broken by later file rows).
# - Robust path discovery for CIK JSONs; joins SEC ticker map so "symbol" is filled.

import os
import sys
import re
import glob
import json
from datetime import datetime, timezone
from typing import Dict, List, Any, Tuple

def pad_cik10(cik: Any) -> str:
    s = re.sub(r"\D", "", str(cik or ""))
    return ("0000000000" + s)[-10:] if s else ""

def find_companyfacts_files(cf_hint: str) -> List[str]:
    candidates: List[str] = []
    search_roots = []
    if cf_hint:
        search_roots += [cf_hint, os.path.join(cf_hint, "companyfacts")]
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
    if isinstance(data, dict):
        values_iter = data.values()
    elif isinstance(data, list):
        values_iter = data
    else:
        print("[WARN] Unexpected tickers JSON shape; expected dict or list.")
        return {}

    for rec in values_iter:
        if not isinstance(rec, dict):
            continue
        cik_raw = rec.get("cik_str")
        ticker = (rec.get("ticker") or "").strip().upper()
        if not cik_raw or not ticker:
            continue
        cik10 = pad_cik10(cik_raw)
        if cik10:
            cik2ticker[cik10] = ticker

    print(f"[DEBUG] Ticker map entries={len(cik2ticker)}")
    return cik2ticker

def extract_rows_from_file(path: str, ticker_map: Dict[str, str]) -> List[dict]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            j = json.load(f)
    except Exception as e:
        print(f"[WARN] Failed to parse {os.path.relpath(path)}: {e}")
        return []

    m = re.search(r"CIK(\d+)\.json$", os.path.basename(path))
    cik_file = pad_cik10(m.group(1)) if m else ""
    cik_json = pad_cik10(j.get("cik"))
    cik = cik_json or cik_file

    ticker_json = (j.get("ticker") or "").strip().upper()
    ticker = ticker_json or (ticker_map.get(cik, "") if cik else "")

    facts = j.get("facts", {}) if isinstance(j, dict) else {}
    dei = facts.get("dei", {}) if isinstance(facts, dict) else {}
    gaap = facts.get("us-gaap", {}) if isinstance(facts, dict) else {}

    out = []
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
                    if not ticker:
                        # skip rows without a ticker (keeps downstream join simple)
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

def main():
    cf_dir = sys.argv[1] if len(sys.argv) > 1 else "."
    tickers_json = sys.argv[2] if len(sys.argv) > 2 else ""
    out_dir = sys.argv[3] if len(sys.argv) > 3 else "."
    os.makedirs(out_dir, exist_ok=True)

    out_jsonl = os.path.join(out_dir, "latest-shares.jsonl")
    out_csv   = os.path.join(out_dir, "latest-by-symbol.csv")
    out_json  = os.path.join(out_dir, "latest-by-symbol.json")
    meta_file = os.path.join(out_dir, "meta.json")

    files = find_companyfacts_files(cf_dir)
    ticker_map = load_ticker_map(tickers_json)

    per_symbol: Dict[str, Tuple[str, float, str]] = {}  # symbol -> (asof, shares, cik)
    total_files, total_rows = 0, 0

    with open(out_jsonl, "w", encoding="utf-8") as fj:
        for i, path in enumerate(files, 1):
            rows = extract_rows_from_file(path, ticker_map)
            total_files += 1
            for r in rows:
                fj.write(json.dumps(r) + "\n")
                total_rows += 1
                sym = r["symbol"]
                asof = r["shares_asof"]
                shares = float(r["shares_outstanding"])
                cik = r["cik"]
                best = per_symbol.get(sym)
                if (best is None) or (asof > best[0]):
                    per_symbol[sym] = (asof, shares, cik)
            if i % 1000 == 0:
                print(f"[INFO] Processed {i}/{len(files)} files... rows_so_far={total_rows}, unique_symbols={len(per_symbol)}")

    # Write compact snapshots
    symbols_sorted = sorted(per_symbol.keys())
    with open(out_csv, "w", encoding="utf-8") as fc:
        fc.write("symbol,cik,shares_outstanding,shares_asof,source,fetch_ts\n")
        for sym in symbols_sorted:
            asof, shares, cik = per_symbol[sym]
            fc.write(f"{sym},{cik},{shares},{asof},sec_companyfacts,{datetime.now(timezone.utc).strftime('%Y-%m-%d')}\n")

    with open(out_json, "w", encoding="utf-8") as fj2:
        json.dump(
            {sym: {"cik": per_symbol[sym][2],
                   "shares_outstanding": per_symbol[sym][1],
                   "shares_asof": per_symbol[sym][0]}
             for sym in symbols_sorted},
            fj2)

    meta = {
        "source_zip": "https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip",
        "tickers_source": "https://www.sec.gov/files/company_tickers.json",
        "companies_scanned": total_files,
        "rows_written": total_rows,
        "symbols_emitted": len(per_symbol),
        "generated_utc": datetime.now(timezone.utc).isoformat()
    }
    with open(meta_file, "w", encoding="utf-8") as f:
        json.dump(meta, f)

    print(f"[INFO] Finished. Companies scanned={total_files}, rows_written={total_rows}, symbols_emitted={len(per_symbol)}")
    print(f"[INFO] Outputs:\n  {out_jsonl}\n  {out_csv}\n  {out_json}\n  {meta_file}")

if __name__ == "__main__":
    main()
