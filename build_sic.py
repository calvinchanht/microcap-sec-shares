# build_sic.py — v1.0.0
# Purpose: Build a compact CSV of {symbol,cik,sic} for US listings using the same
#          filters as fetchListingsFast (Nasdaq, NYSE, NYSE American; exclude ETFs,
#          test issues, rights/warrants/units, and extra fund-like vehicles).
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
    # ADRs are allowed; we do NOT filter on 'Depositary' to keep ADRs.
    n = (name or "")
    return bool(EXTRA_FUNDLIKE_PAT.search(n))

def parse_nasdaqlisted(path: str) -> List[Tuple[str, str, str, str]]:
    # returns list of (symbol, exchange, security_name, etf_flag)
    out = []
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        lines = [ln.strip() for ln in f if ln.strip()]
    if not lines:
        return out
    hdr = lines[0].split("|")
    idx = {h: i for i, h in enumerate(hdr)}
    for ln in lines[1:]:
        if ln.startswith("File Creation Time"):
            break
        cols = ln.split("|")
        symbol = cols[idx["Symbol"]]
        name   = cols[idx["Security Name"]]
        etf    = cols[idx.get("ETF", "")] if "ETF" in idx else "N"
        test   = cols[idx.get("Test Issue", "")] if "Test Issue" in idx else "N"
        # Market Category is present but not critical here.
        out.append((symbol, "Nasdaq", name, f"{etf}|{test}"))
    return out

def parse_otherlisted(path: str) -> List[Tuple[str, str, str, str]]:
    # returns list of (symbol, exchange, name, flags)
    out = []
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        lines = [ln.strip() for ln in f if ln.strip()]
    if not lines:
        return out
    hdr = lines[0].split("|")
    idx = {h: i for i, h in enumerate(hdr)}
    for ln in lines[1:]:
        if ln.startswith("File Creation Time"):
            break
        cols = ln.split("|")
        symbol = cols[idx["Symbol"]]
        name   = cols[idx["Name"]]
        exch_code = cols[idx["Exchange"]]
        exchange = "NYSE" if exch_code == "N" else "NYSE American" if exch_code == "A" else "NYSE Arca" if exch_code == "P" else ""
        out.append((symbol, exchange, name, ""))  # flags blank; otherlisted has ETF elsewhere
    return out

def load_ticker_map(tickers_path: str) -> Dict[str, str]:
    # returns cik10 -> ticker
    with open(tickers_path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    tmap = {}
    if isinstance(raw, dict):
        # shape: {"0": {"cik_str":..., "ticker":...}, ...}
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
    return tmap

def build_eligible_symbols(nas_rows, oth_rows) -> List[Tuple[str, str]]:
    """
    Returns [(symbol, exchange)] filtered to:
      - Exchange ∈ {Nasdaq, NYSE, NYSE American} (exclude NYSE Arca)
      - Exclude ETFs (in nasdaqlisted via ETF=Y), test issues, rights/warrants/units
      - Extra: exclude ETNs/CEFs/funds/trusts/LPs/preferred
    """
    out = []
    # Nasdaq file includes ETF/test flags; otherlisted doesn’t give explicit ETF/test in the same way.
    for symbol, exchange, name, flags in nas_rows + oth_rows:
        if exchange not in ("Nasdaq", "NYSE", "NYSE American"):
            continue
        sym = norm_symbol(symbol)
        if not sym:
            continue

        etf_flag, test_flag = "N", "N"
        if flags:
            parts = flags.split("|")
            etf_flag = parts[0] if len(parts) > 0 else "N"
            test_flag = parts[1] if len(parts) > 1 else "N"

        if etf_flag == "Y" or test_flag == "Y":
            continue
        if is_right_warrant_unit(name, sym):
            continue
        if is_extra_fundlike(name):
            continue

        out.append((sym, exchange))
    # de-duplicate by symbol (keep first occurrence)
    seen = set()
    uniq = []
    for sym, ex in out:
        if sym in seen:
            continue
        uniq.append((sym, ex))
        seen.add(sym)
    return uniq

def make_symbol_to_cik(eligible: List[Tuple[str, str]], cik_to_ticker: Dict[str, str]) -> Dict[str, str]:
    # Invert cik->ticker to ticker->cik, and map only eligible symbols
    ticker_to_cik = {v: k for k, v in cik_to_ticker.items()}
    out = {}
    for sym, _ in eligible:
        cik = ticker_to_cik.get(sym)
        if cik:
            out[sym] = cik
    return out

def http_get_json(url: str, ua: str, timeout: int = 20) -> Optional[dict]:
    # modest retry/backoff esp. for 403/429
    tries = 6
    backoff = 2.0
    for i in range(tries):
        try:
            r = requests.get(url, headers={
                "User-Agent": ua,
                "Accept-Encoding": "identity",
            }, timeout=timeout)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (403, 429, 502, 503, 504):
                # backoff
                time.sleep(backoff)
                backoff *= 1.6
                continue
            # other errors: short pause and continue
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
    print(f"[INFO] company_tickers entries={len(cik_to_ticker)}")

    sym_to_cik = make_symbol_to_cik(eligible, cik_to_ticker)
    print(f"[INFO] Symbols with CIK mapped={len(sym_to_cik)}; missing={len(eligible)-len(sym_to_cik)}")

    out_csv = os.path.join(out_dir, "latest-sic-by-symbol.csv")
    meta_path = os.path.join(out_dir, "meta-sic.json")
    fetch_ts = utc_date()

    print("[INFO] Fetching submissions (sic)… (throttle ~8/sec)")
    ok = 0
    miss = 0
    last_tick = time.perf_counter()
    per_request_delay = 0.13  # ~7.7 req/sec → under 8/sec target

    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["symbol","cik","sic","source","fetch_ts"])
        for sym, cik in sym_to_cik.items():
            # throttle
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
            # Emit even if sic is empty; Apps Script can skip blanks.
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
