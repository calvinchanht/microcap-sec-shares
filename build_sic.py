# build_sic.py — v1.0.0
# Purpose:
#   Fetch SIC for *eligible* listed tickers (same filter as fetchListingsFast v2)
#   and publish: public/latest-sic-by-symbol.csv + public/sic-meta.json
#
# Inputs:
#   python3 build_sic.py nasdaqlisted.txt otherlisted.txt company_tickers.json public
#
# Notes:
#   - Polite throttle (~8 req/s) with simple retry on 429/5xx.
#   - Single deployment: writes into ./public alongside shares artifacts.
#   - Filtering matches your Apps Script (Nasdaq/NYSE/NYSE American + excludes).
#
# Output files:
#   public/latest-sic-by-symbol.csv   (symbol,cik,sic,sic_desc,source,fetch_ts)
#   public/sic-meta.json              (counters and timestamps)
#
# Required env:
#   UA = descriptive "User-Agent" (e.g., 'Mozilla/5.0 MicroCap you@example.com')

import csv, json, re, sys, time, os
from datetime import datetime, timezone
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

SEC_BASE = "https://data.sec.gov"

ALLOWED_EXCH = {"Nasdaq", "NYSE", "NYSE American"}

# ----- Filters (mirror fetchListingsFast v2) -----

EXCLUDE_NAME_PATTERNS = re.compile(
    r"(ETF|ETNs?|Closed[-\s]?End|CEF|Unit(s)?\b|Warrant(s)?\b|Right(s)?\b|"
    r"Preferred( Stock)?|Preference|Trust\b|Limited Partnership| L\.P\.|\bLP\b|"
    r"Notes?\b|Baby Bond|Senior Notes?|Subordinated Notes?|Perpetual Note|Note )",
    re.IGNORECASE,
)

EXCLUDE_SYMBOL_SUFFIX = re.compile(r"[-\.](WS|W|WT|RT|U|P|PR|[A-Z]\d?)$", re.IGNORECASE)

def is_excluded(name: str, symbol: str) -> bool:
    n = name or ""
    s = symbol or ""
    if EXCLUDE_NAME_PATTERNS.search(n):
        # Keep ADR common shares: allow "American Depositary Shares" unless it also says Preferred/Notes.
        if re.search(r"American Depositary Shares?", n, re.IGNORECASE) and not re.search(r"Preferred|Notes?", n, re.IGNORECASE):
            pass
        else:
            return True
    if EXCLUDE_SYMBOL_SUFFIX.search(s):
        return True
    return False

# ----- Parse listings -----

def parse_nasdaq_listed(path: str):
    # nasdaqlisted.txt: pipe-delimited; last line is 'File Creation Time'
    out = []
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        lines = [ln.strip() for ln in f if ln.strip()]
    hdr = [h.strip() for h in lines[0].split("|")]
    idx = {h:i for i,h in enumerate(hdr)}
    for ln in lines[1:]:
        if ln.startswith("File Creation Time"):  # trailer
            break
        c = ln.split("|")
        sym = c[idx["Symbol"]].strip()
        name = c[idx["Security Name"]].strip()
        is_test = (c[idx["Test Issue"]] == "Y")
        is_etf  = (c[idx["ETF"]] == "Y")
        exchange = "Nasdaq"
        if not sym or not name: 
            continue
        out.append({"symbol": sym, "name": name, "exchange": exchange, "is_test": is_test, "is_etf": is_etf})
    return out

def parse_other_listed(path: str):
    # otherlisted.txt: pipe-delimited; last line is 'File Creation Time'
    # Exchange codes: N=NYSE, A=NYSE American, P=NYSE Arca
    code_map = {"N": "NYSE", "A": "NYSE American", "P": "NYSE Arca"}
    out = []
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        lines = [ln.strip() for ln in f if ln.strip()]
    hdr = [h.strip() for h in lines[0].split("|")]
    idx = {h:i for i,h in enumerate(hdr)}
    for ln in lines[1:]:
        if ln.startswith("File Creation Time"):
            break
        c = ln.split("|")
        sym = c[idx["Symbol"]].strip()
        exch_code = c[idx["Exchange"]].strip()
        name = c[idx["Name"]].strip()
        is_etf  = (c[idx["ETF"]] == "Y")
        is_test = (c[idx["Test Issue"]] == "Y")
        exchange = code_map.get(exch_code, "")
        if not sym or not name or not exchange:
            continue
        out.append({"symbol": sym, "name": name, "exchange": exchange, "is_test": is_test, "is_etf": is_etf})
    return out

def filtered_eligible(nas_rows, oth_rows):
    base = nas_rows + oth_rows
    elig = []
    for r in base:
        if r["exchange"] not in ALLOWED_EXCH:
            continue
        if r["is_etf"] or r["is_test"]:
            continue
        if is_excluded(r["name"], r["symbol"]):
            continue
        elig.append(r)
    # de-dupe by symbol keeping first occurrence
    seen = set()
    out = []
    for r in elig:
        u = r["symbol"].upper()
        if u in seen:
            continue
        seen.add(u)
        out.append(r)
    return out

# ----- SEC tickers mapping -----

def load_tickers_map(tickers_json_path: str):
    # company_tickers.json can be an object-of-objects: {"0":{"cik_str":..., "ticker":...}, ...}
    with open(tickers_json_path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    t2c = {}  # ticker -> cik10
    if isinstance(raw, dict):
        for _, rec in raw.items():
            if not isinstance(rec, dict):
                continue
            cik = str(rec.get("cik_str") or rec.get("cik") or rec.get("cikStr") or "").strip()
            tic = str(rec.get("ticker") or "").strip().upper()
            if not cik or not tic: 
                continue
            cik10 = ("0000000000" + "".join(ch for ch in cik if ch.isdigit()))[-10:]
            t2c[tic] = cik10
    elif isinstance(raw, list):
        for rec in raw:
            cik = str(rec.get("cik_str") or rec.get("cik") or rec.get("cikStr") or "").strip()
            tic = str(rec.get("ticker") or "").strip().upper()
            if not cik or not tic: 
                continue
            cik10 = ("0000000000" + "".join(ch for ch in cik if ch.isdigit()))[-10:]
            t2c[tic] = cik10
    return t2c

# ----- SEC fetch with throttle -----

def http_get_json(url: str, ua: str, tries=3, backoff=2.0):
    last = None
    for i in range(tries):
        req = Request(url, headers={
            "User-Agent": ua,
            "Accept-Encoding": "gzip, deflate",
        })
        try:
            with urlopen(req, timeout=30) as resp:
                data = resp.read()
                # Python handles gzip automatically if Accept-Encoding present; fallback json decode
                return json.loads(data.decode("utf-8", errors="ignore"))
        except HTTPError as e:
            last = e
            code = getattr(e, "code", 0)
            if code in (429, 500, 502, 503, 504):
                time.sleep(backoff * (i+1))
                continue
            break
        except URLError as e:
            last = e
            time.sleep(backoff * (i+1))
    if last:
        raise last
    raise RuntimeError("Unknown HTTP failure")

def fetch_sic_for_cik(cik10: str, ua: str):
    url = f"{SEC_BASE}/submissions/CIK{cik10}.json"
    j = http_get_json(url, ua)
    sic = str(j.get("sic") or "").strip()
    desc = str(j.get("sicDescription") or "").strip()
    return sic, desc

# ----- Main -----

def main():
    if len(sys.argv) != 5:
        print("Usage: python3 build_sic.py nasdaqlisted.txt otherlisted.txt company_tickers.json out_dir")
        sys.exit(2)

    nasdaq_path, other_path, tickers_path, out_dir = sys.argv[1:]
    ua = os.environ.get("UA", "").strip() or "Mozilla/5.0 MicroCap (contact@example.com)"
    os.makedirs(out_dir, exist_ok=True)

    nas = parse_nasdaq_listed(nasdaq_path)
    oth = parse_other_listed(other_path)
    elig = filtered_eligible(nas, oth)

    t2c = load_tickers_map(tickers_path)
    fetch_ts = datetime.now(timezone.utc).date().isoformat()

    # Build (symbol, cik) pairs for eligible list
    pairs = []
    missing_cik = []
    for r in elig:
        sym = r["symbol"].upper()
        cik10 = t2c.get(sym, "")
        if not cik10:
            missing_cik.append(sym)
            continue
        pairs.append((sym, cik10))

    # De-duplicate by symbol (in case of duplicates)
    seen_sym = set()
    uniq_pairs = []
    for sym, cik in pairs:
        if sym in seen_sym:
            continue
        seen_sym.add(sym)
        uniq_pairs.append((sym, cik))

    # Throttled fetch
    per_sec = 8.0
    interval = 1.0 / per_sec
    rows = []
    req_count = 0
    ok = 0
    miss = 0
    last_time = 0.0

    for i, (sym, cik10) in enumerate(uniq_pairs, 1):
        # throttle
        now = time.time()
        dt = now - last_time
        if dt < interval:
            time.sleep(interval - dt)
        last_time = time.time()

        try:
            sic, desc = fetch_sic_for_cik(cik10, ua)
            rows.append([sym, cik10, sic, desc, "sec_submissions", fetch_ts])
            ok += 1
        except Exception as e:
            miss += 1
        req_count += 1

    # Write CSV
    out_csv = os.path.join(out_dir, "latest-sic-by-symbol.csv")
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["symbol","cik","sic","sic_desc","source","fetch_ts"])
        w.writerows(rows)

    # Meta
    meta = {
        "eligible_symbols": len(elig),
        "pairs_with_cik": len(uniq_pairs),
        "requests_made": req_count,
        "rows_written": len(rows),
        "missing_cik_symbols": len(missing_cik),
        "generated_utc": datetime.now(timezone.utc).isoformat(),
    }
    with open(os.path.join(out_dir, "sic-meta.json"), "w", encoding="utf-8") as mf:
        json.dump(meta, mf)

    print(f"[INFO] SIC done. eligible={len(elig)} with_cik={len(uniq_pairs)} rows={len(rows)} miss_cik={len(missing_cik)}")

if __name__ == "__main__":
    main()
