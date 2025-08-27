# build_sic.py — v1.2.0
# - NEW: Proxy-aware (SIC_PROXY_BASE); default hits data.sec.gov
# - FIX: robust headers for otherlisted.txt (ACT Symbol, Security Name, etc.)
# - Better error logging; writes CSV even if empty
# - Same filter as fetchListingsFast v2 to keep universe lean

import csv, json, os, re, sys, time
from datetime import datetime, timezone
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

ALLOWED_EXCH = {"Nasdaq", "NYSE", "NYSE American"}

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
        if re.search(r"American Depositary Shares?", n, re.IGNORECASE) and not re.search(r"Preferred|Notes?", n, re.IGNORECASE):
            pass
        else:
            return True
    if EXCLUDE_SYMBOL_SUFFIX.search(s):
        return True
    return False

def split_header(line): return [h.strip() for h in line.split("|")]
def split_row(line):    return [c.strip() for c in line.split("|")]
def make_idx(header):   return {h.lower(): i for i, h in enumerate(header)}
def pick_idx(idx_map, *syn): 
    for s in syn:
        k = s.lower()
        if k in idx_map: return idx_map[k]
    return None

def parse_nasdaq_listed(path):
    out = []
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        lines = [ln.strip() for ln in f if ln.strip()]
    hdr = split_header(lines[0]); idx = make_idx(hdr)
    i_sym  = pick_idx(idx, "Symbol")
    i_name = pick_idx(idx, "Security Name")
    i_test = pick_idx(idx, "Test Issue")
    i_etf  = pick_idx(idx, "ETF")
    if None in (i_sym, i_name, i_test, i_etf):
        print("[ERROR] nasdaqlisted headers:", hdr); raise KeyError("nasdaqlisted.txt header mismatch")
    for ln in lines[1:]:
        if ln.startswith("File Creation Time"): break
        c = split_row(ln)
        if len(c) < len(hdr): continue
        out.append({
            "symbol": c[i_sym], "name": c[i_name], "exchange": "Nasdaq",
            "is_test": (c[i_test] == "Y"), "is_etf": (c[i_etf] == "Y")
        })
    print(f"[DEBUG] parse_nasdaq_listed: rows={len(out)}")
    return out

def parse_other_listed(path):
    code_map = {"N": "NYSE", "A": "NYSE American", "P": "NYSE Arca"}
    out = []
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        lines = [ln.strip() for ln in f if ln.strip()]
    hdr = split_header(lines[0]); idx = make_idx(hdr)
    i_sym  = pick_idx(idx, "ACT Symbol", "Symbol", "CQS Symbol", "NASDAQ Symbol")
    i_name = pick_idx(idx, "Security Name", "Name")
    i_exch = pick_idx(idx, "Exchange")
    i_etf  = pick_idx(idx, "ETF")
    i_test = pick_idx(idx, "Test Issue")
    if None in (i_sym, i_name, i_exch, i_etf, i_test):
        print("[ERROR] otherlisted headers:", hdr); raise KeyError("otherlisted.txt header mismatch")
    for ln in lines[1:]:
        if ln.startswith("File Creation Time"): break
        c = split_row(ln)
        if len(c) < len(hdr): continue
        exch = code_map.get(c[i_exch], "")
        if not exch: continue
        out.append({
            "symbol": c[i_sym], "name": c[i_name], "exchange": exch,
            "is_test": (c[i_test] == "Y"), "is_etf": (c[i_etf] == "Y")
        })
    print(f"[DEBUG] parse_other_listed: rows={len(out)}")
    return out

def filtered_eligible(nas, oth):
    base = nas + oth
    elig = []
    for r in base:
        if r["exchange"] not in ALLOWED_EXCH: continue
        if r["is_etf"] or r["is_test"]:       continue
        if is_excluded(r["name"], r["symbol"]): continue
        elig.append(r)
    seen, out = set(), []
    for r in elig:
        u = r["symbol"].upper()
        if u in seen: continue
        seen.add(u); out.append(r)
    print(f"[DEBUG] filtered_eligible: before={len(base)} after={len(out)}")
    return out

def load_tickers_map(tickers_json_path):
    with open(tickers_json_path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    t2c = {}
    if isinstance(raw, dict):
        for _, rec in raw.items():
            if not isinstance(rec, dict): continue
            cik = str(rec.get("cik_str") or rec.get("cik") or rec.get("cikStr") or "").strip()
            tic = str(rec.get("ticker") or "").strip().upper()
            if not cik or not tic: continue
            cik10 = ("0000000000" + "".join(ch for ch in cik if ch.isdigit()))[-10:]
            t2c[tic] = cik10
    elif isinstance(raw, list):
        for rec in raw:
            cik = str(rec.get("cik_str") or rec.get("cik") or rec.get("cikStr") or "").strip()
            tic = str(rec.get("ticker") or "").strip().upper()
            if not cik or not tic: continue
            cik10 = ("0000000000" + "".join(ch for ch in cik if ch.isdigit()))[-10:]
            t2c[tic] = cik10
    print(f"[DEBUG] load_tickers_map: size={len(t2c)}")
    return t2c

def base_url():
    b = os.environ.get("SIC_PROXY_BASE", "").strip()
    if b:
        return b.rstrip("/")
    return "https://data.sec.gov"

def http_get_json(url, ua, tries=4, backoff=1.5):
    last = None
    for i in range(tries):
        try:
            req = Request(url, headers={"User-Agent": ua, "Accept-Encoding": "gzip, deflate"})
            with urlopen(req, timeout=30) as resp:
                data = resp.read()
                return json.loads(data.decode("utf-8", errors="ignore"))
        except HTTPError as e:
            last = e
            code = getattr(e, "code", 0)
            print(f"[WARN] HTTP {code} for {url} (try {i+1}/{tries})")
            if code in (429, 500, 502, 503, 504):
                time.sleep(backoff * (i+1)); continue
            break
        except URLError as e:
            last = e
            print(f"[WARN] URLError for {url}: {e} (try {i+1}/{tries})")
            time.sleep(backoff * (i+1))
    if last: raise last
    raise RuntimeError("http_get_json unknown failure")

def fetch_sic_for_cik(cik10, ua):
    # If using Worker (…/sec), keep path after /sec identical to SEC.
    url = f"{base_url()}/submissions/CIK{cik10}.json"
    j = http_get_json(url, ua)
    return str(j.get("sic") or "").strip(), str(j.get("sicDescription") or "").strip()

def main():
    if len(sys.argv) != 5:
        print("Usage: python3 build_sic.py nasdaqlisted.txt otherlisted.txt company_tickers.json out_dir")
        sys.exit(2)

    nasdaq_path, other_path, tickers_path, out_dir = sys.argv[1:]
    ua = os.environ.get("UA", "").strip() or "Mozilla/5.0 MicroCap (contact@example.com)"
    os.makedirs(out_dir, exist_ok=True)

    print("[INFO] Reading NasdaqTrader lists…")
    nas = parse_nasdaq_listed(nasdaq_path)
    oth = parse_other_listed(other_path)
    elig = filtered_eligible(nas, oth)

    t2c = load_tickers_map(tickers_path)
    fetch_ts = datetime.now(timezone.utc).date().isoformat()

    pairs, missing = [], []
    for r in elig:
        sym = r["symbol"].upper()
        cik10 = t2c.get(sym, "")
        if not cik10: missing.append(sym); continue
        if not re.fullmatch(r"\d{10}", cik10): continue
        pairs.append((sym, cik10))

    seen_sym, uniq = set(), []
    for sym, cik in pairs:
        if sym in seen_sym: continue
        seen_sym.add(sym); uniq.append((sym, cik))

    print(f"[INFO] Eligible={len(elig)} pairs_with_cik={len(uniq)} missing_cik={len(missing)} via={base_url()}")

    per_sec = 8.0
    interval = 1.0 / per_sec
    rows, ok, miss = [], 0, 0
    last_t = 0.0

    for i, (sym, cik10) in enumerate(uniq, 1):
        # throttle
        now = time.time()
        dt = now - last_t
        if dt < interval: time.sleep(interval - dt)
        last_t = time.time()

        try:
            sic, desc = fetch_sic_for_cik(cik10, ua)
            rows.append([sym, cik10, sic, desc, "sec_submissions", fetch_ts])
            ok += 1
        except Exception as e:
            print(f"[WARN] SIC fail {sym} CIK{cik10}: {e}")
            miss += 1

    out_csv = os.path.join(out_dir, "latest-sic-by-symbol.csv")
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["symbol","cik","sic","sic_desc","source","fetch_ts"])
        w.writerows(rows)

    meta = {
        "eligible_symbols": len(elig),
        "pairs_with_cik": len(uniq),
        "requests_made": len(uniq),
        "rows_written": len(rows),
        "missing_cik_symbols": len(missing),
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "via": base_url(),
    }
    with open(os.path.join(out_dir, "sic-meta.json"), "w", encoding="utf-8") as mf:
        json.dump(meta, mf)

    print(f"[INFO] SIC done. ok={ok} miss={miss} rows={len(rows)}")

if __name__ == "__main__":
    main()
