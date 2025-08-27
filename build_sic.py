# build_sic.py — v1.1.0
# Purpose: Build a compact CSV of {symbol,cik,sic} for US listings using the same
#          filters as fetchListingsFast (Nasdaq, NYSE, NYSE American; exclude ETFs,
#          test issues, rights/warrants/units, extra fund-like vehicles).
#
# Changes (v1.1.0):
#   - Robust parser for otherlisted.txt:
#       * Accepts 'ACT Symbol' / 'CQS Symbol' / 'NASDAQ Symbol' / 'Symbol'
#       * Logs detected header mapping for debugging
#       * Reads ETF and Test Issue flags when available (not just empty flags)
#   - Extra debug prints for both source files and a few sample parsed rows
#
# Inputs (CLI):
#   python3 build_sic.py nasdaqlisted.txt otherlisted.txt company_tickers.json out_dir
#
# Outputs (in out_dir):
#   latest-sic-by-symbol.csv   (symbol,cik,sic,source,fetch_ts)
#   meta-sic.json              (counters & timing)
#
# Notes:
#   - Polite throttle ~8 req/sec to SEC submissions endpoint.
#   - Robust JSON shape handling for company_tickers.json (object or list).
#   - Listing filters mirror Apps Script fetchListingsFast + extra fund-like screens.
#
# License: MIT

import csv, json, os, re, sys, time
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Optional

try:
    import requests
except ImportError:
    print("[ERROR] 'requests' is required. Please pip install requests.")
    sys.exit(2)

SEC_SUBMISSIONS = "https://data.sec.gov/submissions/CIK{cik10}.json"

# ---------- helpers ----------

def utc_date() -> str:
    return datetime.now(timezone.utc).date().isoformat()

def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def pad_cik10(v: str) -> str:
    s = "".join(c for c in str(v) if c.isdigit())
    return ("0000000000" + s)[-10:] if s else ""

def norm_symbol(s: str) -> str:
    return (s or "").strip().upper().replace(" ", "")

def is_right_warrant_unit(name: str, symbol: str) -> bool:
    n = (name or "").lower()
    sym = (symbol or "").upper()
    if re.search(r'(^|[\s\-])(warrant|warrants|right|unit|units|ws|wt|wts)(s)?($|[\s\-/])', n, re.I):
        return True
    if re.search(r'[-\.](WS|W|WT|RT|U)$', sym):
        return True
    return False

EXTRA_FUNDLIKE_PAT = re.compile(
    r'(closed[- ]?end|etn\b|fund\b|trust\b|preferred\b|preference\b|limited partnership|l\.?p\.?\b)',
    re.I
)

def is_extra_fundlike(name: str) -> bool:
    n = (name or "")
    return bool(EXTRA_FUNDLIKE_PAT.search(n))

# ---------- file parsers ----------

def parse_nasdaqlisted(path: str) -> List[Tuple[str, str, str, str]]:
    """
    Returns list of (symbol, exchange, security_name, flags) where flags = 'ETF|Test'
    """
    out = []
    with open(path, "r", encoding="utf-8-sig", errors="ignore") as f:
        lines = [ln.rstrip("\r\n") for ln in f if ln.strip()]
    if not lines:
        print("[DEBUG] nasdaqlisted.txt is empty?")
        return out
    hdr = lines[0].split("|")
    idx = {h: i for i, h in enumerate(hdr)}
    print("[DEBUG] nasdaqlisted header:", hdr)

    def col(cols, key, default=""):
        return cols[idx[key]] if key in idx and idx[key] < len(cols) else default

    for ln in lines[1:]:
        if ln.startswith("File Creation Time"):
            break
        cols = ln.split("|")
        symbol = col(cols, "Symbol")
        name   = col(cols, "Security Name")
        etf    = col(cols, "ETF", "N")
        test   = col(cols, "Test Issue", "N")
        out.append((symbol, "Nasdaq", name, f"{etf}|{test}"))

    print(f"[DEBUG] nasdaqlisted parsed rows={len(out)} (sample={out[:3]})")
    return out

def parse_otherlisted(path: str) -> List[Tuple[str, str, str, str]]:
    """
    Returns list of (symbol, exchange, name, flags) with robust header detection:
      - Symbol header candidates: 'Symbol', 'ACT Symbol', 'CQS Symbol', 'NASDAQ Symbol'
      - Name header candidates:   'Security Name', 'Company Name', 'Security'
      - Flags when present: 'ETF', 'Test Issue'
      - Exchange code: 'N'->NYSE, 'A'->NYSE American, 'P'->NYSE Arca
    """
    out = []
    with open(path, "r", encoding="utf-8-sig", errors="ignore") as f:
        lines = [ln.rstrip("\r\n") for ln in f if ln.strip()]
    if not lines:
        print("[DEBUG] otherlisted.txt is empty?")
        return out
    hdr = lines[0].split("|")
    idx = {h: i for i, h in enumerate(hdr)}
    print("[DEBUG] otherlisted header:", hdr)

    # header candidates
    sym_candidates  = ["Symbol", "ACT Symbol", "CQS Symbol", "NASDAQ Symbol"]
    name_candidates = ["Security Name", "Company Name", "Security"]
    exch_key = "Exchange"

    def find_key(cands):
        for k in cands:
            if k in idx:
                return k
        return None

    sym_key  = find_key(sym_candidates)
    name_key = find_key(name_candidates)
    if sym_key is None:
        raise KeyError(f"Could not find a symbol column in otherlisted header. Header={hdr}")

    # helper to fetch col safely
    def col(cols, key, default=""):
        return cols[idx[key]] if key in idx and idx[key] < len(cols) else default

    # process rows
    for ln in lines[1:]:
        if ln.startswith("File Creation Time"):
            break
        cols = ln.split("|")
        symbol = col(cols, sym_key)
        name   = col(cols, name_key, "")
        exch_code = col(cols, exch_key, "")
        etf    = col(cols, "ETF", "N")
        test   = col(cols, "Test Issue", "N")

        exchange = "NYSE" if exch_code == "N" else "NYSE American" if exch_code == "A" else "NYSE Arca" if exch_code == "P" else ""
        out.append((symbol, exchange, name, f"{etf}|{test}"))

    print(f"[DEBUG] otherlisted parsed rows={len(out)} (sample={out[:3]})")
    return out

# ---------- mapping & filters ----------

def load_ticker_map(tickers_path: str) -> Dict[str, str]:
    # returns cik10 -> ticker
    with open(tickers_path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    tmap = {}
    if isinstance(raw, dict):
        for rec in raw.values():
            if not isinstance(rec, dict):
                continue
            cik = rec.get("cik_str") or rec.get("cik") or rec.get("cikStr")
            tic = rec.get("ticker")
            if not cik or not tic:
                continue
            tmap[pad_cik10(str(cik))] = norm_symbol(str(tic))
    elif isinstance(raw, list):
        for rec in raw:
            cik = rec.get("cik_str") or rec.get("cik") or rec.get("cikStr")
            tic = rec.get("ticker")
            if not cik or not tic:
                continue
            tmap[pad_cik10(str(cik))] = norm_symbol(str(tic))
    print(f"[DEBUG] ticker map size={len(tmap)}")
    return tmap

def build_eligible_symbols(nas_rows, oth_rows) -> List[Tuple[str, str]]:
    """
    Returns [(symbol, exchange)] filtered to:
      - Exchange ∈ {Nasdaq, NYSE, NYSE American} (exclude NYSE Arca)
      - Exclude ETFs (flag Y), test issues (flag Y), rights/warrants/units
      - Extra: exclude ETNs/CEFs/funds/trusts/LPs/preferred
    """
    out = []
    drop_etf, drop_test, drop_rwu, drop_funds = 0, 0, 0, 0

    for symbol, exchange, name, flags in nas_rows + oth_rows:
        if exchange not in ("Nasdaq", "NYSE", "NYSE American"):
            continue
        sym = norm_symbol(symbol)
        if not sym:
            continue

        etf_flag, test_flag = "N", "N"
        if flags:
            parts = flags.split("|")
            etf_flag = (parts[0] if len(parts) > 0 else "N") or "N"
            test_flag = (parts[1] if len(parts) > 1 else "N") or "N"

        if etf_flag.upper() == "Y":
            drop_etf += 1
            continue
        if test_flag.upper() == "Y":
            drop_test += 1
            continue
        if is_right_warrant_unit(name, sym):
            drop_rwu += 1
            continue
        if is_extra_fundlike(name):
            drop_funds += 1
            continue

        out.append((sym, exchange))

    # de-duplicate by symbol
    seen = set()
    uniq = []
    for sym, ex in out:
        if sym in seen:
            continue
        uniq.append((sym, ex))
        seen.add(sym)

    print(f"[DEBUG] filters: drop ETF={drop_etf}, test={drop_test}, rwu={drop_rwu}, funds={drop_funds}")
    print(f"[DEBUG] eligible unique={len(uniq)} (sample={uniq[:5]})")
    return uniq

def make_symbol_to_cik(eligible: List[Tuple[str, str]], cik_to_ticker: Dict[str, str]) -> Dict[str, str]:
    ticker_to_cik = {v: k for k, v in cik_to_ticker.items()}
    out = {}
    for sym, _ in eligible:
        cik = ticker_to_cik.get(sym)
        if cik:
            out[sym] = cik
    return out

def http_get_json(url: str, ua: str, timeout: int = 20) -> Optional[dict]:
    tries = 6
    backoff = 2.0
    for _ in range(tries):
        try:
            r = requests.get(url, headers={
                "User-Agent": ua,
                "Accept-Encoding": "identity",
            }, timeout=timeout)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (403, 429, 502, 503, 504):
                time.sleep(backoff)
                backoff *= 1.6
                continue
            time.sleep(0.5)
        except Exception:
            time.sleep(backoff)
            backoff *= 1.6
    return None

# ---------- main ----------

def main():
    if len(sys.argv) != 5:
        print("Usage: python3 build_sic.py nasdaqlisted.txt otherlisted.txt company_tickers.json out_dir")
        sys.exit(2)

    nasdaq_path, other_path, tickers_path, out_dir = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
    os.makedirs(out_dir, exist_ok=True)
    ua = os.environ.get("UA") or "Mozilla/5.0 MicroCap (contact@example.com)"

    print("[INFO] Loading listings…")
    nas_rows = parse_nasdaqlisted(nasdaq_path)
    oth_rows = parse_otherlisted(other_path)
    print(f"[INFO] nasdaqlisted rows={len(nas_rows)}, otherlisted rows={len(oth_rows)}")

    eligible = build_eligible_symbols(nas_rows, oth_rows)
    print(f"[INFO] Eligible after filters={len(eligible)}")

    print("[INFO] Loading SEC company_tickers.json…")
    cik_to_ticker = load_ticker_map(tickers_path)
    sym_to_cik = make_symbol_to_cik(eligible, cik_to_ticker)
    print(f"[INFO] Symbols with CIK mapped={len(sym_to_cik)}; missing={len(eligible)-len(sym_to_cik)}")

    out_csv = os.path.join(out_dir, "latest-sic-by-symbol.csv")
    meta_path = os.path.join(out_dir, "meta-sic.json")
    fetch_ts = utc_date()

    print("[INFO] Fetching submissions (sic)… (throttle ~8/sec)")
    ok = 0
    miss = 0
    last_tick = time.perf_counter()
    per_request_delay = 0.13  # ~7.7 req/sec

    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["symbol","cik","sic","source","fetch_ts"])
        for sym, cik in sym_to_cik.items():
            now = time.perf_counter()
            elapsed = now - last_tick
            if elapsed < per_request_delay:
                time.sleep(per_request_delay - elapsed)
            last_tick = time.perf_counter()

            url = SEC_SUBMISSIONS.format(cik10=cik)
            j = http_get_json(url, ua=ua)
            if not j:
                miss += 1
                continue
            sic = str(j.get("sic") or "").strip()
            w.writerow([sym, cik, sic, "sec_submissions", fetch_ts])
            ok += 1

    meta = {
        "eligible": len(eligible),
        "symbols_mapped": len(sym_to_cik),
        "rows_written": ok,
        "rows_missing": miss,
        "generated_utc": utc_iso(),
        "sources": {
            "nasdaqlisted": "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqlisted.txt",
            "otherlisted": "https://www.nasdaqtrader.com/dynamic/symdir/otherlisted.txt",
            "company_tickers": "https://www.sec.gov/files/company_tickers.json",
            "submissions": "https://data.sec.gov/submissions/CIK##########.json"
        }
    }
    with open(meta_path, "w", encoding="utf-8") as mf:
        json.dump(meta, mf)

    print(f"[INFO] Done. ok={ok} miss={miss}. Wrote {out_csv}")

if __name__ == "__main__":
    main()
