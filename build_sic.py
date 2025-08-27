#!/usr/bin/env python3
# build_sic.py — v1.3.0
# Robust SIC fetcher via Cloudflare Worker
# - Headers: explicit UA/Accept/Accept-Encoding(identity)
# - Retries with backoff; throttles ~7–8 req/s
# - Accepts both /sec/submissions/* and /submissions/* via Worker
# - Writes:
#     public/latest-sic-by-symbol.csv
#     public/sic-meta.json
#
# Usage:
#   python3 build_sic.py nasdaqlisted.txt otherlisted.txt tickers.json public
#
# Env:
#   UA= "Mozilla/5.0 MicroCap you@domain"
#   SEC_SIC_PROXY_BASE="https://<your-worker>.workers.dev/sec"    # recommended
#   (If SEC_SIC_PROXY_BASE is unset, defaults to https://www.sec.gov (less reliable from CI))

import csv, os, sys, json, time, gzip, io
import urllib.request as urlreq
import urllib.error as urlerr
from datetime import datetime, timezone

UA = os.environ.get("UA", "Mozilla/5.0 MicroCap microcap@gmail.com")
SEC_BASE = os.environ.get("SEC_SIC_PROXY_BASE", "https://microcapsec.calvinchanht.workers.dev/")  # prefer your Worker /sec

# ---------- helpers ----------
def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()

def norm_ticker(s):
    return (s or "").strip().upper().replace(" ", "").replace(".", "-")

def parse_pipe_file(path):
    """Return (header:list, rows:list[list]) for a '|' separated file."""
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        lines = [ln.strip() for ln in f if ln.strip()]
    header = lines[0].split("|")
    rows = [ln.split("|") for ln in lines[1:] if not ln.startswith("File Creation Time")]
    return header, rows

def parse_nasdaq_listed(path):
    hdr, rows = parse_pipe_file(path)
    idx = {h:i for i,h in enumerate(hdr)}
    out = []
    for c in rows:
        try:
            sym = c[idx["Symbol"]].strip()
            name = c[idx["Security Name"]].strip()
            test = c[idx["Test Issue"]].strip().upper() == "Y"
            etf  = c[idx["ETF"]].strip().upper() == "Y"
        except KeyError:
            # Unexpected header; skip row
            continue
        out.append({"symbol": sym, "name": name, "exchange": "Nasdaq", "is_test": test, "is_etf": etf})
    return out

def parse_other_listed(path):
    hdr, rows = parse_pipe_file(path)
    idx = {h:i for i,h in enumerate(hdr)}
    out = []
    for c in rows:
        try:
            sym = c[idx["ACT Symbol"]].strip() if "ACT Symbol" in idx else c[idx["Symbol"]].strip()
            name = c[idx["Security Name"]].strip()
            exchCode = c[idx["Exchange"]].strip() if "Exchange" in idx else ""
            etf  = c[idx["ETF"]].strip().upper() == "Y"
            test = c[idx["Test Issue"]].strip().upper() == "Y"
        except KeyError:
            continue
        exch = "NYSE" if exchCode == "N" else "NYSE American" if exchCode == "A" else "NYSE Arca" if exchCode=="P" else ""
        out.append({"symbol": sym, "name": name, "exchange": exch, "is_test": test, "is_etf": etf})
    return out

def looks_like_rwu(symbol, name):
    if not symbol and not name: return False
    if symbol and any(symbol.upper().endswith(suf) for suf in ("-W", "-WS", "-WT", "-RT", "-U")):
        return True
    if name and any(tok in name.upper() for tok in (" WARRANT", " RIGHT", " UNIT", " UNITS", " WTS", " WT ")):
        return True
    return False

def looks_like_pref_trust_lp(symbol, name):
    txt = f"{symbol} {name}".upper()
    # crude filters for prefs/trusts/LPs (reduce noise)
    bad = (" PFD ", " PREFER", " TR ", " TRUST", " DEPOSITARY SHARE", " LP ", " L.P.", " LIMITED PARTNERSHIP", "NOTE DUE")
    return any(b in txt for b in bad)

def looks_like_adr(name):
    if not name: return False
    up = name.upper()
    return " ADR " in up or up.endswith(" ADR") or "DEPOSITARY SH" in up or "AMERICAN DEPOS" in up

def filtered_eligible(nas, oth):
    base = nas + [r for r in oth if r["exchange"] in ("NYSE","NYSE American")]
    keep = []
    for r in base:
        if r["exchange"] not in ("Nasdaq","NYSE","NYSE American"): continue
        if r["is_test"] or r["is_etf"]: continue
        if looks_like_rwu(r["symbol"], r["name"]): continue
        if looks_like_pref_trust_lp(r["symbol"], r["name"]): continue
        if looks_like_adr(r["name"]): continue
        keep.append(r)
    return keep

def load_tickers_map(tickers_json_path):
    with open(tickers_json_path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    tmap = {}  # cik10 -> ticker
    if isinstance(raw, dict):
        # {"0":{"cik_str":..., "ticker":...}, ...}
        for _, rec in raw.items():
            if not isinstance(rec, dict): continue
            cik = str(rec.get("cik_str") or rec.get("cik") or rec.get("cikStr") or "").strip()
            tic = norm_ticker(rec.get("ticker"))
            if not cik or not tic: continue
            cik10 = ("0000000000" + "".join(ch for ch in cik if ch.isdigit()))[-10:]
            tmap[cik10] = tic
    elif isinstance(raw, list):
        for rec in raw:
            cik = str(rec.get("cik_str") or rec.get("cik") or rec.get("cikStr") or "").strip()
            tic = norm_ticker(rec.get("ticker"))
            if not cik or not tic: continue
            cik10 = ("0000000000" + "".join(ch for ch in cik if ch.isdigit()))[-10:]
            tmap[cik10] = tic
    return tmap

def request_json(url, timeout=20):
    """GET JSON with strict headers, identity encoding, return parsed or raise."""
    headers = {
        "User-Agent": UA,
        "Accept": "application/json",
        "Accept-Encoding": "identity",  # avoid gzip/br to simplify
        "Connection": "close",
    }
    req = urlreq.Request(url, headers=headers, method="GET")
    with urlreq.urlopen(req, timeout=timeout) as resp:
        if resp.status != 200:
            raise urlerr.HTTPError(url, resp.status, f"HTTP {resp.status}", hdrs=resp.headers, fp=None)
        raw = resp.read()
        if not raw:
            raise ValueError("empty body")
        # Assume utf-8
        text = raw.decode("utf-8", errors="replace")
        return json.loads(text)

def fetch_submissions_json(cik10, base):
    """Try both /sec/submissions and /submissions paths against the proxy base."""
    paths = [
        f"{base.rstrip('/')}/submissions/CIK{cik10}.json",      # if base already ends with /sec
        f"{base.rstrip('/')}/sec/submissions/CIK{cik10}.json",  # in case base is just the worker root
    ]
    last_err = None
    for attempt in range(1, 5):  # up to 4 tries with backoff
        for u in paths:
            try:
                return request_json(u)
            except urlerr.HTTPError as he:
                # Bubble up 403/404 after retries; log as warn
                last_err = he
            except Exception as e:
                last_err = e
        # simple backoff
        time.sleep(0.5 * attempt)
    if last_err: raise last_err
    raise RuntimeError("failed to fetch submissions json")

# ---------- main ----------
def main():
    if len(sys.argv) != 5:
        print("Usage: python3 build_sic.py nasdaqlisted.txt otherlisted.txt tickers.json public")
        sys.exit(2)

    nasdaq_path, other_path, tickers_path, out_dir = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
    os.makedirs(out_dir, exist_ok=True)

    print("[INFO] Reading NasdaqTrader lists…")
    nas = parse_nasdaq_listed(nasdaq_path)
    oth = parse_other_listed(other_path)
    print(f"[DEBUG] parse_nasdaq_listed: rows={len(nas)}")
    print(f"[DEBUG] parse_other_listed: rows={len(oth)}")

    eligible = filtered_eligible(nas, oth)
    print(f"[DEBUG] filtered_eligible: before={len(nas)+len(oth)} after={len(eligible)}")

    cik_to_ticker = load_tickers_map(tickers_path)
    print(f"[DEBUG] load_tickers_map: size={len(cik_to_ticker)}")

    # Build ticker->cik map for eligible set
    pairs = []
    missing = 0
    for r in eligible:
        sym = norm_ticker(r["symbol"])
        # invert map: cik map is cik10->ticker, so search by value is O(1) if we prebuild
        # but we only need cik for eligible; build a small lookup
        # Build once:
        # (small trick: create a dict ticker->cik for eligible on demand to avoid a 10k scan each loop)
        # We'll construct a small set of eligible tickers and then backfill cik.
        pairs.append((sym, None))
    # build ticker->cik once
    ticker_to_cik = {tic: cik for cik, tic in cik_to_ticker.items()}
    fixed_pairs = []
    for sym, _ in pairs:
        cik = ticker_to_cik.get(sym)
        if cik:
            fixed_pairs.append((sym, cik))
        else:
            missing += 1
    print(f"[INFO] Eligible={len(eligible)} pairs_with_cik={len(fixed_pairs)} missing_cik={missing} via={SEC_BASE}")

    # Output files
    out_csv = os.path.join(out_dir, "latest-sic-by-symbol.csv")
    out_meta = os.path.join(out_dir, "sic-meta.json")

    # Write header early so file always exists
    with open(out_csv, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["symbol","cik","sic","sic_desc","source","fetch_ts"])

    ok = 0
    miss = 0
    wrote = 0
    start = time.time()

    with open(out_csv, "a", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        for i, (sym, cik10) in enumerate(fixed_pairs, 1):
            # throttle ~0.13s/request ≈ 7.6 rps
            time.sleep(0.13)
            try:
                j = fetch_submissions_json(cik10, SEC_BASE)
                sic = str(j.get("sic") or "").strip()
                desc = (j.get("sicDescription") or j.get("sic_description") or "").strip()
                if not sic:
                    miss += 1
                    print(f"[WARN] No SIC for {sym} CIK{cik10}")
                    continue
                w.writerow([sym, cik10, sic, desc, "sec_submissions", datetime.utcnow().date().isoformat()])
                ok += 1
                wrote += 1
            except urlerr.HTTPError as he:
                miss += 1
                print(f"[WARN] HTTP {he.code} for {SEC_BASE}/submissions/CIK{cik10}.json (try over): {sym}")
            except Exception as e:
                miss += 1
                print(f"[WARN] SIC fail {sym} CIK{cik10}: {e}")

    meta = {
        "eligible_symbols": len(eligible),
        "pairs_with_cik": len(fixed_pairs),
        "requests_made": ok + miss,
        "rows_written": wrote,
        "missing_cik_symbols": missing,
        "generated_utc": utc_now_iso(),
    }
    with open(out_meta, "w", encoding="utf-8") as mf:
        json.dump(meta, mf)

    dur = time.time() - start
    print(f"[INFO] SIC done. ok={ok} miss={miss} rows={wrote} in {dur:.1f}s")

if __name__ == "__main__":
    main()
