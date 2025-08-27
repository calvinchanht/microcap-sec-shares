#!/usr/bin/env python3
# build_sic.py — v1.4.0
# Robust SIC fetcher via Cloudflare Worker
# - Headers: explicit UA/Accept/Accept-Encoding(identity)
# - Retries with backoff + jitter; gentle throttle (~5–6 rps)
# - Accepts both /sec/submissions/* and /submissions/* via Worker
# - Writes:
#     public/latest-sic-by-symbol.csv
#     public/sic-meta.json
#
# Usage:
#   python3 build_sic.py nasdaqlisted.txt otherlisted.txt tickers.json public
#
# Env:
#   UA="Mozilla/5.0 MicroCap you@domain"
#   SEC_SIC_PROXY_BASE="https://<your-worker>.workers.dev/sec"  # preferred
#   (Alternatively) SIC_PROXY_BASE="https://<your-worker>.workers.dev/sec"

import csv, os, sys, json, time, random
import urllib.request as urlreq
import urllib.error as urlerr
from datetime import datetime, timezone

UA = os.environ.get("UA", "Mozilla/5.0 MicroCap microcap@gmail.com")
SEC_BASE = (
    os.environ.get("SEC_SIC_PROXY_BASE")
    or os.environ.get("SIC_PROXY_BASE")
    or "https://microcapsec.calvinchanht.workers.dev/sec"
)

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

def request_json(url, timeout=25):
    """GET JSON with strict headers, identity encoding, return parsed or raise."""
    headers = {
        "User-Agent": UA,
        "Accept": "application/json",
        "Accept-Encoding": "identity",
        "Connection": "close",
    }
    req = urlreq.Request(url, headers=headers, method="GET")
    with urlreq.urlopen(req, timeout=timeout) as resp:
        if resp.status != 200:
            raise urlerr.HTTPError(url, resp.status, f"HTTP {resp.status}", hdrs=resp.headers, fp=None)
        raw = resp.read()
        if not raw:
            raise ValueError("empty body")
        # Be paranoid: reject obvious HTML
        if raw[:32].lstrip().startswith(b"<!DOCTYPE") or raw[:1] == b"<":
            raise ValueError("non_json_like_body")
        text = raw.decode("utf-8", errors="replace")
        try:
            return json.loads(text)
        except json.JSONDecodeError as je:
            raise ValueError(f"json_decode_error: {je}")

def fetch_submissions_json(cik10, base):
    """Try both /sec/submissions and /submissions paths against the proxy base."""
    b = base.rstrip("/")
    bases = [b]
    if not b.endswith("/sec"):
        bases.append(b + "/sec")
    paths = [f"{bb}/submissions/CIK{cik10}.json" for bb in bases]

    last_err = None
    for attempt in range(1, 5):  # up to 4 tries with backoff + jitter
        for u in paths:
            try:
                return request_json(u)
            except Exception as e:
                last_err = e
        time.sleep(0.4 * attempt + random.uniform(0, 0.2))
    if last_err: raise last_err
    raise RuntimeError("failed to fetch submissions json")

# ---------- main ----------
def main():
    if len(sys.argv) != 5:
        print("Usage: python3 build_sic.py nasdaqlisted.txt otherlisted.txt tickers.json public")
        sys.exit(2)

    nasdaq_path, other_path, tickers_path, out_dir = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
    os.makedirs(out_dir, exist_ok=True)

    print(f"[INFO] Using UA={UA}")
    print(f"[INFO] Using SEC base={SEC_BASE}")

    print("[INFO] Reading NasdaqTrader lists…")
    nas = parse_nasdaq_listed(nasdaq_path)
    oth = parse_other_listed(other_path)
    print(f"[DEBUG] parse_nasdaq_listed: rows={len(nas)}")
    print(f"[DEBUG] parse_other_listed: rows={len(oth)}")

    eligible = filtered_eligible(nas, oth)
    print(f"[DEBUG] filtered_eligible: before={len(nas)+len(oth)} after={len(eligible)}")

    cik_to_ticker = load_tickers_map(tickers_path)
    print(f"[DEBUG] load_tickers_map: size={len(cik_to_ticker)}")

    # Build ticker->cik map and select eligible pairs
    ticker_to_cik = {tic: cik for cik, tic in cik_to_ticker.items()}
    fixed_pairs = []
    missing = 0
    for r in eligible:
        sym = norm_ticker(r["symbol"])
        cik = ticker_to_cik.get(sym)
        if cik:
            fixed_pairs.append((sym, cik))
        else:
            missing += 1

    print(f"[INFO] Eligible={len(eligible)} pairs_with_cik={len(fixed_pairs)} missing_cik={missing} via={SEC_BASE}")

    out_csv = os.path.join(out_dir, "latest-sic-by-symbol.csv")
    out_meta = os.path.join(out_dir, "sic-meta.json")

    # Write header early so file always exists
    with open(out_csv, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["symbol","cik","sic","sic_desc","source","fetch_ts"])

    ok = 0
    wrote = 0
    errs = {"http403":0, "http404":0, "empty":0, "non_json":0, "other":0}
    start = time.time()

    with open(out_csv, "a", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        for i, (sym, cik10) in enumerate(fixed_pairs, 1):
            # ~5–6 rps with light jitter
            time.sleep(0.18 + random.uniform(0, 0.05))
            try:
                j = fetch_submissions_json(cik10, SEC_BASE)
                sic = str(j.get("sic") or "").strip()
                desc = (j.get("sicDescription") or j.get("sic_description") or "").strip()
                if not sic:
                    errs["empty"] += 1
                    print(f"[WARN] No SIC for {sym} CIK{cik10}")
                    continue
                w.writerow([sym, cik10, sic, desc, "sec_submissions", datetime.now(timezone.utc).date().isoformat()])
                ok += 1
                wrote += 1
            except urlerr.HTTPError as he:
                if he.code == 403:
                    errs["http403"] += 1
                elif he.code == 404:
                    errs["http404"] += 1
                else:
                    errs["other"] += 1
                print(f"[WARN] HTTP {he.code} for submissions CIK{cik10} (try over): {sym}")
            except ValueError as ve:
                msg = str(ve)
                if "non_json_like_body" in msg or "json_decode_error" in msg:
                    errs["non_json"] += 1
                elif "empty body" in msg:
                    errs["empty"] += 1
                else:
                    errs["other"] += 1
                print(f"[WARN] SIC fail {sym} CIK{cik10}: {msg}")
            except Exception as e:
                errs["other"] += 1
                print(f"[WARN] SIC fail {sym} CIK{cik10}: {e}")

    dur = time.time() - start
    meta = {
        "eligible_symbols": len(eligible),
        "pairs_with_cik": len(fixed_pairs),
        "requests_made": ok + sum(errs.values()),
        "rows_written": wrote,
        "missing_cik_symbols": missing,
        "errors": errs,
        "generated_utc": utc_now_iso(),
        "duration_sec": round(dur, 1),
    }
    with open(out_meta, "w", encoding="utf-8") as mf:
        json.dump(meta, mf)

    print(f"[INFO] SIC done. ok={ok} miss={sum(errs.values())} rows={wrote} in {dur:.1f}s")

if __name__ == "__main__":
    main()
