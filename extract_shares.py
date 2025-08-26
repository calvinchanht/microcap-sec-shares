# extract_shares.py — v1.4.0
# - Adds SIC to outputs when present in companyfacts JSON (top-level "sic")
# - Keeps tolerant discovery (cf/ or cf/**/CIK*.json)
# - Emits:
#     public/latest-shares.jsonl   (history; now includes "sic")
#     public/latest-by-symbol.csv  (latest per symbol; header adds "sic")
#     public/meta.json             (counters & sources)
#
# Usage:
#   python3 extract_shares.py <companyfacts_root> <tickers.json> <out_dir>

import json, os, sys, glob, datetime
from datetime import datetime as dt

def utc_date():
    return dt.now(datetime.UTC).date().isoformat()

def is_share_unit(k: str) -> bool:
    k = (k or "").lower()
    return "share" in k  # accept "shares", "pureShares", etc.

def load_ticker_map(tickers_path: str):
    with open(tickers_path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    # Shapes:
    #  A) {"0":{"cik_str":1045810,"ticker":"NVDA","title":"NVIDIA CORP"}, ...}
    #  B) [{"cik_str":..., "ticker":...}, ...]
    tmap = {}
    if isinstance(raw, dict):
        for _, rec in raw.items():
            if not isinstance(rec, dict):
                continue
            cik = str(rec.get("cik_str") or rec.get("cik") or rec.get("cikStr") or "").strip()
            tic = str(rec.get("ticker") or "").strip().upper()
            if not cik or not tic:
                continue
            cik10 = ("0000000000"+ "".join([c for c in cik if c.isdigit()]))[-10:]
            tmap[cik10] = tic
    elif isinstance(raw, list):
        for rec in raw:
            cik = str(rec.get("cik_str") or rec.get("cik") or rec.get("cikStr") or "").strip()
            tic = str(rec.get("ticker") or "").strip().upper()
            if not cik or not tic:
                continue
            cik10 = ("0000000000"+ "".join([c for c in cik if c.isdigit()]))[-10:]
            tmap[cik10] = tic
    else:
        pass
    return tmap

def iter_companyfacts_files(root: str):
    pattern = os.path.join(root, "**", "CIK*.json")
    for path in glob.iglob(pattern, recursive=True):
        if os.path.isfile(path):
            yield path

def extract_series(j: dict):
    # Prefer dei.EntityCommonStockSharesOutstanding; fallback to us-gaap.CommonStockSharesOutstanding
    facts = j.get("facts") or {}
    series = None
    candidates = [
        ("dei", "EntityCommonStockSharesOutstanding"),
        ("us-gaap", "CommonStockSharesOutstanding"),
    ]
    for ns, key in candidates:
        nsobj = facts.get(ns) or {}
        node = nsobj.get(key)
        if node and isinstance(node, dict) and "units" in node:
            series = node["units"]
            break
    if not series:
        return []

    rows = []
    for unit_key, arr in series.items():
        if not is_share_unit(unit_key):
            continue
        if not isinstance(arr, list):
            continue
        for rec in arr:
            end = rec.get("end") or rec.get("instant") or ""
            try:
                val = float(rec.get("val"))
            except Exception:
                continue
            if not end:
                continue
            rows.append((end, val))
    return rows

def main():
    if len(sys.argv) != 4:
        print("Usage: python3 extract_shares.py <companyfacts_root> <tickers.json> <out_dir>")
        sys.exit(2)

    cf_root, tickers_path, out_dir = sys.argv[1], sys.argv[2], sys.argv[3]
    os.makedirs(out_dir, exist_ok=True)

    tmap = load_ticker_map(tickers_path)
    fetch_ts = utc_date()

    files = list(iter_companyfacts_files(cf_root))
    print(f"[DEBUG] Found {len(files)} JSON files")
    if files:
        print("[DEBUG] Sample files:", [os.path.relpath(p, cf_root) for p in files[:5]])

    jsonl_path = os.path.join(out_dir, "latest-shares.jsonl")
    csv_path   = os.path.join(out_dir, "latest-by-symbol.csv")
    meta_path  = os.path.join(out_dir, "meta.json")

    total_rows = 0
    latest_per_cik = {}  # cik10 -> (end, val)
    sic_by_cik = {}      # cik10 -> sic (string), when present

    with open(jsonl_path, "w", encoding="utf-8") as jsonl:
        for fp in files:
            fname = os.path.basename(fp)
            cik10 = ("".join([c for c in fname if c.isdigit()]))[-10:]
            try:
                with open(fp, "r", encoding="utf-8") as f:
                    j = json.load(f)
            except Exception:
                continue

            embedded = str(j.get("cik") or "").strip()
            if embedded:
                cik10 = ("0000000000" + "".join([c for c in embedded if c.isdigit()]))[-10:]

            # Capture sic if present at top level
            sic = str(j.get("sic") or "").strip()
            if sic:
                sic_by_cik[cik10] = sic

            series = extract_series(j)
            if not series:
                continue

            series.sort(key=lambda x: x[0])
            for end, val in series:
                rec = {
                    "symbol": tmap.get(cik10, ""),
                    "cik": cik10,
                    "sic": sic,  # may be ""
                    "shares_outstanding": val,
                    "shares_asof": end,
                    "source": "sec_companyfacts",
                    "fetch_ts": fetch_ts,
                }
                jsonl.write(json.dumps(rec) + "\n")
                total_rows += 1

            end, val = series[-1]
            cur = latest_per_cik.get(cik10)
            if (not cur) or (end > cur[0]):
                latest_per_cik[cik10] = (end, val)

    # latest-by-symbol.csv (add "sic" column after cik)
    with open(csv_path, "w", encoding="utf-8") as csvf:
        csvf.write("symbol,cik,sic,shares_outstanding,shares_asof,source,fetch_ts\n")
        emitted = 0
        for cik10, (end, val) in latest_per_cik.items():
            sym = tmap.get(cik10, "")
            if not sym:
                continue
            sic = sic_by_cik.get(cik10, "")
            line = f"{sym},{cik10},{sic},{val},{end},sec_companyfacts,{fetch_ts}\n"
            csvf.write(line)
            emitted += 1

    meta = {
        "source_zip": "https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip",
        "tickers_source": "https://www.sec.gov/files/company_tickers.json",
        "companies_scanned": len(files),
        "rows_written": total_rows,
        "symbols_emitted": sum(1 for _ in latest_per_cik),
        "generated_utc": dt.now(datetime.UTC).isoformat(),
    }
    with open(meta_path, "w", encoding="utf-8") as mf:
        json.dump(meta, mf)

    print(f"[INFO] Finished. Companies scanned={len(files)}, rows_written={total_rows}")

if __name__ == "__main__":
    main()
