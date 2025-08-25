#!/usr/bin/env python3
import sys, os, json, re
from glob import glob

def pad10(s):
    s = re.sub(r'\D','', str(s or ''))
    return ('0000000000' + s)[-10:] if s else ''

def choose_latest(arr):
    best = None  # (end, val)
    for d in arr or []:
        end = d.get('end')
        val = d.get('val')
        if not end or not isinstance(val, (int, float)):  # skip non-numeric
            continue
        if best is None or end > best[0]:
            best = (end, float(val))
    return best

def extract_one_companyfacts(path):
    with open(path, 'r') as f:
        j = json.load(f)
    facts = (j.get('facts') or {})
    dei   = (facts.get('dei') or {})
    gaap  = (facts.get('us-gaap') or {})

    # Prefer dei:EntityCommonStockSharesOutstanding, fallback to us-gaap:CommonStockSharesOutstanding
    out_candidates = []
    for family, key in (('dei','EntityCommonStockSharesOutstanding'),
                        ('us-gaap','CommonStockSharesOutstanding')):
        node = (dei if family=='dei' else gaap).get(key)
        if not node: 
            continue
        units = node.get('units') or {}
        # pick unit names containing "share", else all
        unit_keys = [k for k in units.keys() if 'share' in k.lower()] or list(units.keys())
        for uk in unit_keys:
            latest = choose_latest(units.get(uk))
            if latest:
                out_candidates.append(latest)

    if not out_candidates:
        return None  # no usable value
    # choose the overall most recent by end-date
    return max(out_candidates, key=lambda x: x[0])  # (asof, value)

def load_ticker_map(tickers_json_path):
    with open(tickers_json_path, 'r') as f:
        data = json.load(f)
    # Handles both dict indexed by ints {"0":{...}} and array forms
    iterable = data.values() if isinstance(data, dict) else data
    cik_to_ticker = {}
    seen = set()
    for rec in iterable:
        cik10 = pad10(rec.get('cik_str'))
        tkr   = (rec.get('ticker') or '').upper()
        if cik10 and tkr and tkr not in seen:
            cik_to_ticker[cik10] = tkr
            seen.add(tkr)
    return cik_to_ticker

def main():
    if len(sys.argv) < 4:
        print("Usage: extract_shares.py <companyfacts_dir> <tickers.json> <out_dir>", file=sys.stderr)
        sys.exit(2)

    cf_dir = sys.argv[1]
    tickers_json = sys.argv[2]
    out_dir = sys.argv[3]
    os.makedirs(out_dir, exist_ok=True)

    cik_to_ticker = load_ticker_map(tickers_json)
    files = glob(os.path.join(cf_dir, "CIK*.json"))
    total = len(files)
    wrote = 0

    out_path = os.path.join(out_dir, "latest-shares.jsonl")
    meta_path = os.path.join(out_dir, "meta.json")

    with open(out_path, 'w') as OUT:
        for i, path in enumerate(files, 1):
            cik10 = pad10(os.path.basename(path)[3:13])  # "CIK0000123456.json" → "0000123456"
            tkr = cik_to_ticker.get(cik10)
            if not tkr:
                continue
            res = extract_one_companyfacts(path)
            if not res:
                continue
            asof, val = res
            if val <= 0:
                continue
            OUT.write(json.dumps({
                "cik": cik10,
                "ticker": tkr,
                "shares": int(val),
                "asof": asof
            }) + "\n")
            wrote += 1
            if i % 1000 == 0:
                print(f"processed {i}/{total} (wrote {wrote})")

    meta = {
        "source_zip": "https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip",
        "tickers_source": "https://www.sec.gov/files/company_tickers.json",
        "companies_scanned": total,
        "rows_written": wrote
    }
    with open(meta_path, 'w') as M:
        json.dump(meta, M)

    print(f"Done. wrote={wrote} rows to {out_path}")

if __name__ == "__main__":
    main()
