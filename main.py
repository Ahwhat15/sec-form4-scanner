import os
import time
import sqlite3
import logging
import requests
import xml.etree.ElementTree as ET
from html import escape
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

TELEGRAM_TOKEN   = os.environ["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]
CET = ZoneInfo("Europe/Oslo")

# ── Filters ───────────────────────────────────────────────────────────────────
MIN_TRANSACTION_VALUE     = 100_000
INCLUDE_TRANSACTION_CODES = {"P"}
WATCHLIST_EXPIRY_DAYS     = 30

# ── Catalyst scanner ──────────────────────────────────────────────────────────
# SIC codes for pharma / biotech / medtech — only these sectors get 8-K scanned
BIOTECH_SICS = {
    "2833", "2834", "2835", "2836",          # pharmaceuticals
    "2860", "2861", "2865", "2869",          # industrial chemicals / biotech
    "3841", "3842", "3843", "3844", "3845",  # medical devices
    "5047",                                  # medical equipment wholesale
    "8011", "8049", "8071", "8099",          # health services
    "8731",                                  # commercial physical research
}

# 8-K item numbers that indicate regulatory/corporate events worth scanning
CATALYST_ITEMS = {"8.01", "7.01"}

# FDA keyword patterns — phrases only, no ambiguous abbreviations
# Each tuple: (search_phrase, display_label)
FDA_KEYWORDS = [
    # Drug applications — phrase match only to avoid "NDA" = non-disclosure agreement
    ("new drug application",         "NDA"),
    ("biologics license application", "BLA"),
    ("supplemental new drug",        "sNDA"),
    ("supplemental biologics",       "sBLA"),
    ("premarket approval",           "PMA"),
    ("510(k)",                       "510(k)"),
    # Regulatory milestones
    ("PDUFA",                        "PDUFA date"),
    ("prescription drug user fee",   "PDUFA"),
    ("complete response letter",     "CRL"),
    ("resubmission",                 "resubmission"),
    ("resubmit",                     "resubmit"),
    ("clinical hold",                "clinical hold"),
    ("partial clinical hold",        "partial clinical hold"),
    # Positive designations
    ("breakthrough therapy",         "breakthrough therapy"),
    ("fast track designation",       "fast track"),
    ("priority review",              "priority review"),
    ("accelerated approval",         "accelerated approval"),
    ("orphan drug designation",      "orphan drug"),
    # FDA actions
    ("fda approval",                 "FDA approval"),
    ("fda approved",                 "FDA approved"),
    ("fda clearance",                "FDA clearance"),
    ("fda granted",                  "FDA granted"),
    ("fda accepted",                 "FDA accepted"),
    ("fda rejected",                 "FDA rejected"),
    ("fda issued",                   "FDA issued"),
    ("advisory committee",           "AdCom"),
]

# Insider buy lookback for standalone catalyst alerts (days)
CATALYST_INSIDER_LOOKBACK = 90

# ── Signal thresholds ─────────────────────────────────────────────────────────
RSI_PERIOD          = 14
EMA_PERIOD          = 20
RSI_MIN             = 45        # momentum positive
RSI_MAX             = 70        # not overbought — blocks exhausted moves
MAX_ABOVE_INSIDER   = 0.08      # within 8% of insider buy price
MAX_FILING_AGE_DAYS = 5         # fresh filing — within 5 trading days
MIN_INSIDER_QUALITY = 500_000   # DIR/OFF or transaction >= $500k
MAX_INSIDER_PRICE   = 500       # filter data errors
MIN_AVG_DAILY_VOL   = 100_000   # filter illiquid tickers
MAX_TRANSACTION_VALUE = 50_000_000   # filter data errors (e.g. $307M SVRE)
MAX_TXN_AGE_DAYS    = 30        # skip transactions older than 30 days
MICRO_CAP_VOL_MAX   = 300_000   # flag micro-cap momentum
FUND_KEYWORDS       = {         # skip self-purchases by funds/ETFs
    "fund", "trust", "etf", "inc.", "lp", "llc", "management",
    "capital", "asset", "partners", "investments", "advisors",
    "group", "global", "corp.", "s.a.", "limited",
}

# ── EDGAR ─────────────────────────────────────────────────────────────────────
EDGAR_HEADERS = {"User-Agent": "VMc1Investments scanner@vmc1.no"}
EFTS_URL      = "https://efts.sec.gov/LATEST/search-index"

# ── Database ──────────────────────────────────────────────────────────────────
DB_PATH = "/data/watchlist.db"

# ── Paperclip VMc1 ────────────────────────────────────────────────────────────
PAPERCLIP_BASE_URL = os.environ.get("PAPERCLIP_BASE_URL", "")
PAPERCLIP_CEO_API_KEY = os.environ.get("PAPERCLIP_CEO_API_KEY", "")
VMC1_COMPANY_ID    = "dc2df96a-a846-4634-a9a0-24e593916c75"
VMC1_CEO_AGENT_ID  = "3db60f1f-86fd-461e-a7bd-96392fa2c893"


# ═══════════════════════════════════════════════════════════════════════════════
# DATABASE
# ═══════════════════════════════════════════════════════════════════════════════

def db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def db_init():
    with db_connect() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS watchlist (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker        TEXT NOT NULL,
                company       TEXT,
                insider_name  TEXT,
                insider_role  TEXT,
                buy_price     REAL NOT NULL,
                shares        REAL,
                value         REAL,
                filed_date    TEXT NOT NULL,
                txn_date      TEXT,
                added_at      TEXT NOT NULL,
                expires_at    TEXT NOT NULL,
                alerted       INTEGER DEFAULT 0
            )
        """)
        conn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_watchlist_ticker_date
            ON watchlist(ticker, txn_date)
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS catalyst_watchlist (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker       TEXT NOT NULL,
                company      TEXT,
                cik          TEXT,
                event_type   TEXT,
                event_desc   TEXT,
                filed_date   TEXT NOT NULL,
                accession    TEXT UNIQUE,
                insider_buy  INTEGER DEFAULT 0,
                alerted      INTEGER DEFAULT 0,
                added_at     TEXT NOT NULL
            )
        """)
    log.info("DB initialised")


def db_add_ticker(t: dict):
    expires = (datetime.now(CET) + timedelta(days=WATCHLIST_EXPIRY_DAYS)).strftime("%Y-%m-%d")
    try:
        with db_connect() as conn:
            conn.execute("""
                INSERT OR IGNORE INTO watchlist
                (ticker, company, insider_name, insider_role, buy_price, shares,
                 value, filed_date, txn_date, added_at, expires_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """, (
                t["ticker"], t["company"], t["name"],
                ("DIR" if t["is_director"] else "OFF" if t["is_officer"] else "INS"),
                t["price"], t["shares"], t["value"],
                datetime.now(CET).strftime("%Y-%m-%d"),
                t["date"],
                datetime.now(CET).isoformat(),
                expires,
            ))
    except Exception as e:
        log.warning(f"DB insert failed for {t['ticker']}: {e}")


def db_get_active() -> list:
    today = datetime.now(CET).strftime("%Y-%m-%d")
    with db_connect() as conn:
        rows = conn.execute("""
            SELECT * FROM watchlist
            WHERE expires_at >= ? AND alerted = 0
            ORDER BY value DESC
        """, (today,)).fetchall()
    return [dict(r) for r in rows]


def db_mark_alerted(row_id: int):
    with db_connect() as conn:
        conn.execute("UPDATE watchlist SET alerted = 1 WHERE id = ?", (row_id,))


def db_expire_old():
    today = datetime.now(CET).strftime("%Y-%m-%d")
    with db_connect() as conn:
        n = conn.execute(
            "DELETE FROM watchlist WHERE expires_at < ?", (today,)
        ).rowcount
    if n:
        log.info(f"Expired {n} watchlist entries")


def db_watchlist_count() -> int:
    with db_connect() as conn:
        return conn.execute(
            "SELECT COUNT(*) FROM watchlist WHERE alerted = 0"
        ).fetchone()[0]


def db_get_recently_added(minutes: int = 30) -> list:
    """Return watchlist rows added within the last N minutes — for spot checks."""
    cutoff = (datetime.now(CET) - timedelta(minutes=minutes)).isoformat()
    with db_connect() as conn:
        rows = conn.execute("""
            SELECT * FROM watchlist
            WHERE added_at >= ? AND alerted = 0
            ORDER BY value DESC
        """, (cutoff,)).fetchall()
    return [dict(r) for r in rows]


def db_get_conviction_score(ticker: str) -> int:
    """Count distinct insider buy events for this ticker in the watchlist."""
    with db_connect() as conn:
        return conn.execute(
            "SELECT COUNT(*) FROM watchlist WHERE ticker = ? AND alerted = 0",
            (ticker,)
        ).fetchone()[0]


def db_get_ticker_history(ticker: str, lookback_days: int = WATCHLIST_EXPIRY_DAYS) -> dict:
    """
    Aggregate insider buy history for a ticker within the lookback window,
    regardless of whether individual rows have already been alerted.

    This reflects the INSIDER'S sustained buying behaviour over time —
    not just which rows happen to be unalerted on tonight's run.
    Used for conviction tier / Elite determination.
    """
    cutoff = (datetime.now(CET) - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    with db_connect() as conn:
        row = conn.execute("""
            SELECT
                COUNT(*)                                  AS event_count,
                COALESCE(SUM(value), 0)                   AS total_value,
                MAX(CASE WHEN insider_role = 'DIR' THEN 1 ELSE 0 END) AS has_dir
            FROM watchlist
            WHERE ticker = ? AND filed_date >= ?
        """, (ticker, cutoff)).fetchone()
    return {
        "event_count": row["event_count"] or 0,
        "total_value": row["total_value"] or 0,
        "has_dir":     bool(row["has_dir"]),
    }


# ═══════════════════════════════════════════════════════════════════════════════
# TELEGRAM
# ═══════════════════════════════════════════════════════════════════════════════

def send_telegram(text: str) -> bool:
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    for attempt in range(3):
        try:
            r = requests.post(
                url,
                json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"},
                timeout=15,
            )
            r.raise_for_status()
            log.info("Telegram sent OK")
            return True
        except Exception as e:
            log.warning(f"Telegram attempt {attempt+1} failed: {e}")
            time.sleep(3)
    log.error("Telegram failed after 3 attempts")
    return False


# ═══════════════════════════════════════════════════════════════════════════════
# PAPERCLIP VMc1 INTEGRATION
# ═══════════════════════════════════════════════════════════════════════════════

def notify_paperclip_ceo(ticker: str, signals: list[tuple], market_data: dict) -> bool:
    """
    Create ONE task per ticker per run, consolidating all watchlist entries.
    Includes conviction score and micro-cap flag in the briefing.
    """
    if not PAPERCLIP_BASE_URL or not PAPERCLIP_CEO_API_KEY:
        log.warning("Paperclip env vars not set — skipping CEO notification")
        return False

    # Use the highest-value row as the primary signal
    row, sig = max(signals, key=lambda x: x[0]["value"])

    conviction      = len(signals)
    vol_ratio       = sig["volume"] / max(sig["avg_volume"], 1)
    upside_pct      = ((sig["price"] - row["buy_price"]) / row["buy_price"]) * 100
    days_since      = (datetime.now(CET) - datetime.fromisoformat(row["added_at"])).days
    is_micro_cap    = sig["avg_volume"] < MICRO_CAP_VOL_MAX
    avg_vol_k       = sig["avg_volume"] / 1000

    # Conviction label — 5 unique tiers
    agg_value  = sum(r["value"] for r, _ in signals)
    has_dir_pc = any(r.get("insider_role") == "DIR" for r, _ in signals)
    is_elite = conviction >= 3 and agg_value >= 1_000_000 and has_dir_pc
    if is_elite:
        conviction_label = "💎 ELITE — 3+ buys, DIR, ≥$1M"
        strategy_note   = "Highest conviction signal. Director accumulating heavily at scale."
        position_size   = 200
    elif conviction >= 3:
        conviction_label = "🔥 HIGH — 3+ separate buy events"
        strategy_note   = "Multi-day accumulation pattern. Strong insider intent."
        position_size   = 200
    elif conviction == 2:
        conviction_label = "🟠 ELEVATED — 2 separate buy events"
        strategy_note   = "Repeated buying confirms insider confidence."
        position_size   = 175
    elif row.get("insider_role") == "DIR" or agg_value >= 1_000_000:
        conviction_label = "🔺 STANDARD T1 — DIR/CEO or ≥$1M single buy"
        strategy_note   = "Director or large single buy. Volume requirement waived."
        position_size   = 150
    else:
        conviction_label = "🔵 STANDARD T2 — single INS buy, vol confirmed"
        strategy_note   = "Single insider buy with volume confirmation."
        position_size   = 100

    # Micro-cap flag
    micro_cap_note = ""
    if is_micro_cap:
        micro_cap_note = f"""
### ⚠️ MICRO-CAP MOMENTUM FLAG
Average daily volume is {avg_vol_k:.0f}k shares (below {MICRO_CAP_VOL_MAX/1000:.0f}k threshold).
- **Treat as momentum trade, not value investment**
- **Max position: $100** (half normal VMc1 size)
- **Exit window: 1-5 days maximum**
- **Scale out in thirds**: 1/3 at +15%, 1/3 at +30%, trail remainder
- **Hard stop**: entry day low
- **Do NOT average down** if price returns to insider level
- Research Agent: verify avg daily dollar volume and check for pump-and-dump indicators
"""

    # All buy events for this ticker
    buy_events = "\n".join([
        f"  - {r['insider_name']} [{r['insider_role']}]: "
        f"{r['shares']:,.0f} sh @ ${r['buy_price']:.2f} = ${r['value']:,.0f} on {r['txn_date']}"
        for r, _ in sorted(signals, key=lambda x: x[0]["txn_date"] or "")
    ])

    task_title = f"[INSIDER SIGNAL] ${ticker} — {conviction_label} | ${position_size} position"

    task_body = f"""## VMc1 Insider Flow Signal — Action Required

**Ticker:** ${ticker}
**Company:** {row['company']}
**Signal Date:** {datetime.now(CET).strftime('%Y-%m-%d')}
**Conviction:** {conviction_label}

---

### All Insider Buy Events ({conviction} total)
{buy_events}

---

### Technical Confirmations (4/4) ✅
- Price ${sig['price']:.2f} > Highest insider buy ${row['buy_price']:.2f} ({upside_pct:+.1f}%) ✅
- RSI {sig['rsi']} in range {RSI_MIN}–{RSI_MAX} ✅
- Price > 20 EMA (${sig['ema20']:.2f}) ✅
- Volume {vol_ratio:.1f}x above 20-day average ✅
- Avg daily volume: {avg_vol_k:.0f}k shares
- Signal fired {days_since}d after first filing
{micro_cap_note}
---

### Strategy Note
{strategy_note}

---

### Requested Actions

1. **Research Agent** — Pull fundamentals, recent news, sector context, insider history for ${ticker}. Score conviction (1–10). Flag any red flags (dilution, debt, litigation).

2. **Backtest Agent** — Run insider-buy + 4-confirmation strategy on ${ticker} historically. Report win rate, avg return, max drawdown.

3. **Risk Management Agent** — Suggested position: **${position_size}** based on conviction tier. {'$100 max for micro-cap momentum — override conviction sizing.' if is_micro_cap else ''} Stop = below ${row['buy_price']:.2f} (insider buy price). Target = 3:1 R:R. Approve or reject.

4. **Execution Agent** — If Risk approves, place paper trade on Alpaca. Report entry, stop, target, size.

This is a paper trade — no real capital at risk.
"""

    url     = f"{PAPERCLIP_BASE_URL}/api/companies/{VMC1_COMPANY_ID}/issues"
    headers = {
        "Authorization": f"Bearer {PAPERCLIP_CEO_API_KEY}",
        "Content-Type":  "application/json",
    }
    payload = {
        "title":       task_title,
        "description": task_body,
        "assigneeAgentId": VMC1_CEO_AGENT_ID,
        "priority":    "urgent",
        "status":      "todo",
    }

    try:
        r = requests.post(url, json=payload, headers=headers, timeout=15)
        r.raise_for_status()
        issue    = r.json()
        issue_id = issue.get("issuePrefix", "VMC") + "-" + str(issue.get("number", "?"))
        log.info(f"Paperclip task created: {issue_id} for ${ticker} (conviction={conviction})")

        # Wake the CEO
        wake_url = (f"{PAPERCLIP_BASE_URL}/api/companies/{VMC1_COMPANY_ID}"
                    f"/agents/{VMC1_CEO_AGENT_ID}/heartbeat")
        requests.post(wake_url, headers=headers, timeout=10)
        log.info("CEO agent heartbeat triggered")
        return True

    except Exception as e:
        log.error(f"Paperclip notification failed for ${ticker}: {e}")
        return False


# ═══════════════════════════════════════════════════════════════════════════════
# MARKET DATA
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_market_data(ticker: str) -> dict | None:
    try:
        import yfinance as yf
        tk        = yf.Ticker(ticker)
        tk._history = None
        hist      = tk.history(period="60d", interval="1d", auto_adjust=True)
        if hist.empty or len(hist) < EMA_PERIOD + 1:
            return None

        closes  = hist["Close"].tolist()
        volumes = hist["Volume"].tolist()
        price   = closes[-1]
        volume  = volumes[-1]

        # 20-day EMA
        ema = closes[0]
        k   = 2 / (EMA_PERIOD + 1)
        for c in closes[1:]:
            ema = c * k + ema * (1 - k)

        # RSI-14
        gains, losses = [], []
        for i in range(1, RSI_PERIOD + 1):
            d = closes[-RSI_PERIOD + i] - closes[-RSI_PERIOD + i - 1]
            (gains if d >= 0 else losses).append(abs(d))
        avg_gain = sum(gains) / RSI_PERIOD if gains else 0
        avg_loss = sum(losses) / RSI_PERIOD if losses else 1e-9
        rsi      = 100 - (100 / (1 + avg_gain / avg_loss))

        avg_vol = sum(volumes[-EMA_PERIOD:]) / EMA_PERIOD

        return {"price": price, "rsi": rsi, "ema20": ema,
                "volume": volume, "avg_volume": avg_vol}
    except Exception as e:
        log.debug(f"Market data failed for {ticker}: {e}")
        return None


def check_signal(data: dict, insider_buy_price: float, conviction: int = 1,
                 is_director: bool = False, value: float = 0,
                 filed_date: str = "") -> dict:
    """High-probability signal: quality over quantity."""
    already_moved = (data["price"] - insider_buy_price) / max(insider_buy_price, 0.01)
    filing_age    = get_filing_age_trading_days(filed_date)
    high_quality  = is_director or value >= MIN_INSIDER_QUALITY

    price_reclaim  = data["price"] > insider_buy_price
    close_to_entry = already_moved <= MAX_ABOVE_INSIDER
    ema_ok         = data["price"] > data["ema20"]
    fresh_filing   = filing_age <= MAX_FILING_AGE_DAYS
    quality_ok     = high_quality
    sane_price     = insider_buy_price <= MAX_INSIDER_PRICE
    liquid         = data["avg_volume"] >= MIN_AVG_DAILY_VOL

    # CEO/large buy RSI override:
    # If Director AND total value >= $5M, waive RSI floor only
    # Insider conviction at this scale overrides short-term momentum weakness
    ceo_large_buy = is_director and value >= 5_000_000
    if ceo_large_buy:
        rsi_ok = data["rsi"] <= RSI_MAX   # ceiling only — no floor
    else:
        rsi_ok = RSI_MIN <= data["rsi"] <= RSI_MAX

    # Volume confirmation: today's volume >= average daily volume
    volume_ok = data["volume"] >= data["avg_volume"]

    return {
        "signal":        all([price_reclaim, close_to_entry, rsi_ok, ema_ok,
                              fresh_filing, quality_ok, sane_price, liquid, volume_ok]),
        "high_quality":  high_quality,
        "ceo_large_buy": ceo_large_buy,
        "price_reclaim": price_reclaim,
        "close_to_entry": close_to_entry,
        "rsi_ok":        rsi_ok,
        "ema_ok":        ema_ok,
        "fresh_filing":  fresh_filing,
        "quality_ok":    quality_ok,
        "sane_price":    sane_price,
        "liquid":        liquid,
        "volume_ok":     volume_ok,
        "already_moved": already_moved,
        "filing_age":    filing_age,
        "price":         data["price"],
        "rsi":           round(data["rsi"], 1),
        "ema20":         round(data["ema20"], 2),
        "volume":        data["volume"],
        "avg_volume":    data["avg_volume"],
    }


# ═══════════════════════════════════════════════════════════════════════════════
# EDGAR SCAN
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_all_filing_index(start_date: str, end_date: str) -> list[dict]:
    all_hits  = []
    offset    = 0
    total_exp = None
    while True:
        params = {"forms": "4", "dateRange": "custom",
                  "startdt": start_date, "enddt": end_date, "from": offset}
        try:
            r = requests.get(EFTS_URL, params=params, headers=EDGAR_HEADERS, timeout=30)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            log.error(f"EFTS request failed at offset {offset}: {e}")
            break
        hits_obj = data.get("hits", {})
        if total_exp is None:
            t         = hits_obj.get("total", {})
            total_exp = t.get("value", 0) if isinstance(t, dict) else int(t or 0)
            log.info(f"EFTS total Form 4 filings in range: {total_exp}")
        batch = hits_obj.get("hits", [])
        if not batch:
            break
        all_hits.extend(batch)
        offset += len(batch)
        log.info(f"Indexed {offset}/{total_exp} filings")
        if offset >= total_exp or offset >= 10_000:
            break
        time.sleep(0.35)
    return all_hits


def parse_filing_meta(hit: dict):
    hit_id = hit.get("_id", "")
    src    = hit.get("_source", {})
    if ":" in hit_id:
        accession    = hit_id.split(":")[0]
        xml_filename = hit_id.split(":")[1]
    else:
        accession    = hit_id
        xml_filename = None
    accession   = src.get("adsh") or accession
    ciks        = src.get("ciks", [])
    company_cik = ciks[-1].lstrip("0") if ciks else None
    return accession, company_cik, xml_filename, src


def is_self_purchase(issuer_name: str, reporter_name: str) -> bool:
    """
    Returns True if the reporter appears to be a fund buying its own units
    e.g. 'RBC Global Asset Management' buying 'RBC BlueBay Enhanced Income'.
    Heuristic: check if 3+ words overlap between issuer and reporter names.
    """
    if not issuer_name or not reporter_name:
        return False
    issuer_words   = set(issuer_name.lower().split()) - FUND_KEYWORDS - {"the", "of", "and"}
    reporter_words = set(reporter_name.lower().split()) - FUND_KEYWORDS - {"the", "of", "and"}
    overlap = issuer_words & reporter_words
    return len(overlap) >= 2


def is_stale_transaction(txn_date: str, max_days: int = MAX_TXN_AGE_DAYS) -> bool:
    """Returns True if the transaction date is older than max_days."""
    if not txn_date:
        return False
    try:
        # Handle dates with timezone suffix like "2026-05-27-05:00"
        clean = txn_date[:10]
        txn_dt = datetime.strptime(clean, "%Y-%m-%d").replace(tzinfo=CET)
        age = (datetime.now(CET) - txn_dt).days
        return age > max_days
    except Exception:
        return False


def get_filing_age_trading_days(filed_date: str) -> int:
    """Approximate trading days since filing date, skipping weekends."""
    if not filed_date:
        return 999
    try:
        filed_dt = datetime.strptime(filed_date[:10], "%Y-%m-%d").replace(tzinfo=CET)
        now      = datetime.now(CET)
        days     = 0
        current  = filed_dt
        while current.date() < now.date():
            current += timedelta(days=1)
            if current.weekday() < 5:
                days += 1
        return days
    except Exception:
        return 999


def parse_form4_xml(accession: str, company_cik: str,
                    xml_filename: str, src: dict) -> list[dict]:
    if not accession or not company_cik or not xml_filename:
        return []
    if not xml_filename.lower().endswith(".xml"):
        return []
    acc_nodash = accession.replace("-", "")
    xml_url    = (f"https://www.sec.gov/Archives/edgar/data/"
                  f"{company_cik}/{acc_nodash}/{xml_filename}")
    try:
        r = requests.get(xml_url, headers=EDGAR_HEADERS, timeout=20)
        if r.status_code != 200:
            return []
        root = ET.fromstring(r.content)
    except Exception as e:
        log.debug(f"XML fetch/parse failed {accession}: {e}")
        return []

    def txt(el, path):
        node = el.find(path)
        return node.text.strip() if node is not None and node.text else ""

    issuer_name    = txt(root, "issuer/issuerName")
    issuer_ticker  = txt(root, "issuer/issuerTradingSymbol")
    reporter_name  = txt(root, "reportingOwner/reportingOwnerId/rptOwnerName")
    reporter_title = txt(root, "reportingOwner/reportingOwnerRelationship/officerTitle")
    is_director    = txt(root, "reportingOwner/reportingOwnerRelationship/isDirector") == "1"
    is_officer     = txt(root, "reportingOwner/reportingOwnerRelationship/isOfficer") == "1"
    if not issuer_name:
        names = src.get("display_names", [])
        issuer_name = names[-1].split("  (CIK")[0] if names else ""

    # Skip fund self-purchases
    if is_self_purchase(issuer_name, reporter_name):
        log.debug(f"Skipping self-purchase: {reporter_name} buying {issuer_name}")
        return []

    results = []
    for txn in root.findall(".//nonDerivativeTransaction"):
        code = txt(txn, "transactionCoding/transactionCode")
        if code not in INCLUDE_TRANSACTION_CODES:
            continue
        try:
            shares = float(txt(txn, "transactionAmounts/transactionShares/value") or 0)
            price  = float(txt(txn, "transactionAmounts/transactionPricePerShare/value") or 0)
            value  = shares * price
        except ValueError:
            continue
        txn_date_val = txt(txn, "transactionDate/value")
        if value < MIN_TRANSACTION_VALUE:
            continue
        if value > MAX_TRANSACTION_VALUE:
            log.info(f"Filtered large transaction: ${issuer_ticker} ${value:,.0f} (exceeds ${MAX_TRANSACTION_VALUE:,.0f} cap)")
            send_telegram(
                f"⚠️ <b>Large Transaction Filtered</b>\n"
                f"  ${escape(issuer_ticker)} — {escape(issuer_name)}\n"
                f"  💰 {shares:,.0f} sh @ ${price:.2f} = <b>${value:,.0f}</b>\n"
                f"  📅 {txn_date_val}\n"
                f"  <i>Exceeded ${MAX_TRANSACTION_VALUE:,.0f} cap — verify manually</i>"
            )
            continue
        if is_stale_transaction(txn_date_val):
            log.debug(f"Skipping stale transaction {txn_date_val} for {issuer_ticker}")
            continue
        results.append({
            "ticker":      issuer_ticker or "N/A",
            "company":     issuer_name,
            "name":        reporter_name,
            "title":       reporter_title,
            "is_director": is_director,
            "is_officer":  is_officer,
            "shares":      shares,
            "price":       price,
            "value":       value,
            "date":        txn_date_val,
        })
    return results


# ═══════════════════════════════════════════════════════════════════════════════
# CATALYST SCANNER  (8-K regulatory events + insider cross-reference)
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_8k_filings(start_date: str, end_date: str) -> list[dict]:
    """Fetch 8-K filings from biotech/pharma SIC codes only."""
    all_hits  = []
    offset    = 0
    total_exp = None

    while True:
        params = {
            "forms":     "8-K",
            "dateRange": "custom",
            "startdt":   start_date,
            "enddt":     end_date,
            "from":      offset,
        }
        try:
            r = requests.get(EFTS_URL, params=params, headers=EDGAR_HEADERS, timeout=30)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            log.error(f"8-K EFTS request failed at offset {offset}: {e}")
            break

        hits_obj = data.get("hits", {})
        if total_exp is None:
            t         = hits_obj.get("total", {})
            total_exp = t.get("value", 0) if isinstance(t, dict) else int(t or 0)
            log.info(f"8-K total filings in range: {total_exp}")

        batch = hits_obj.get("hits", [])
        if not batch:
            break

        # Filter to biotech/pharma SIC codes before adding
        for hit in batch:
            src  = hit.get("_source", {})
            sics = src.get("sics", [])
            if any(s in BIOTECH_SICS for s in sics):
                all_hits.append(hit)

        offset += len(batch)
        if offset >= total_exp or offset >= 10_000:
            break
        time.sleep(0.35)

    log.info(f"8-K filings in biotech/pharma SICs: {len(all_hits)}")
    return all_hits


def fetch_8k_text(accession: str, company_cik: str, filename: str) -> str:
    """Fetch the text content of an 8-K document."""
    if not filename or not filename.lower().endswith(".htm"):
        return ""
    acc_nodash = accession.replace("-", "")
    url = (f"https://www.sec.gov/Archives/edgar/data/"
           f"{company_cik}/{acc_nodash}/{filename}")
    try:
        r = requests.get(url, headers=EDGAR_HEADERS, timeout=20)
        if r.status_code != 200:
            return ""
        # Strip HTML tags for keyword search
        text = r.text
        import re
        text = re.sub(r"<[^>]+>", " ", text)
        return text[:50_000]  # first 50k chars is enough
    except Exception as e:
        log.debug(f"8-K text fetch failed {accession}: {e}")
        return ""


def detect_fda_event(text: str) -> tuple[bool, str]:
    """
    Check if 8-K text contains FDA regulatory phrases.
    Uses phrase matching only — avoids false positives from
    abbreviations like NDA (non-disclosure agreement).
    Returns (is_fda_event, event_description).
    """
    if not text:
        return False, ""

    text_lower = text.lower()
    found_labels = []

    for phrase, label in FDA_KEYWORDS:
        if phrase.lower() in text_lower:
            if label not in found_labels:
                found_labels.append(label)

    if not found_labels:
        return False, ""

    return True, ", ".join(found_labels[:5])


def get_insider_buys_for_cik(cik: str) -> list[dict]:
    """Check if we have recent insider buys for this CIK in the watchlist."""
    cutoff = (datetime.now(CET) - timedelta(days=CATALYST_INSIDER_LOOKBACK)).strftime("%Y-%m-%d")
    with db_connect() as conn:
        rows = conn.execute("""
            SELECT ticker, insider_name, insider_role, buy_price, value, txn_date
            FROM watchlist
            WHERE added_at >= ?
            ORDER BY value DESC
        """, (cutoff,)).fetchall()
    # We don't store CIK in watchlist — match by ticker lookup if needed
    # Return all recent buys for caller to cross-reference by ticker
    return [dict(r) for r in rows]


def db_add_catalyst(ticker: str, company: str, cik: str,
                    event_desc: str, filed_date: str,
                    accession: str, has_insider: bool):
    try:
        with db_connect() as conn:
            conn.execute("""
                INSERT OR IGNORE INTO catalyst_watchlist
                (ticker, company, cik, event_type, event_desc, filed_date,
                 accession, insider_buy, alerted, added_at)
                VALUES (?,?,?,?,?,?,?,?,0,?)
            """, (
                ticker, company, cik,
                "FDA_REGULATORY",
                event_desc,
                filed_date,
                accession,
                1 if has_insider else 0,
                datetime.now(CET).isoformat(),
            ))
    except Exception as e:
        log.warning(f"Catalyst DB insert failed {ticker}: {e}")


def run_catalyst_scan(label: str = "morning"):
    """
    Scan 8-K filings from biotech/pharma companies for FDA regulatory events.
    Cross-references with insider watchlist for convergence signals.
    """
    now = datetime.now(CET)
    if now.weekday() >= 5:
        log.info(f"Weekend — skipping catalyst scan")
        return

    log.info(f"=== Catalyst 8-K scan starting ({label}) ===")
    end_date   = now.strftime("%Y-%m-%d")
    start_date = (now - timedelta(days=1)).strftime("%Y-%m-%d")

    hits = fetch_8k_filings(start_date, end_date)
    if not hits:
        log.info("No biotech/pharma 8-K filings found")
        return

    # Get current insider watchlist tickers for cross-reference
    recent_insider_tickers = set()
    cutoff = (now - timedelta(days=CATALYST_INSIDER_LOOKBACK)).strftime("%Y-%m-%d")
    with db_connect() as conn:
        rows = conn.execute(
            "SELECT DISTINCT ticker FROM watchlist WHERE added_at >= ?", (cutoff,)
        ).fetchall()
    recent_insider_tickers = {r[0] for r in rows}

    fda_events      = []
    convergence     = []  # FDA event + insider buy overlap
    seen_accessions = set()

    for hit in hits:
        src       = hit.get("_source", {})
        hit_id    = hit.get("_id", "")
        accession = src.get("adsh") or (hit_id.split(":")[0] if ":" in hit_id else hit_id)
        filename  = hit_id.split(":")[1] if ":" in hit_id else ""
        ciks      = src.get("ciks", [])
        company_cik = ciks[-1].lstrip("0") if ciks else ""
        items     = src.get("items", [])

        if accession in seen_accessions:
            continue
        seen_accessions.add(accession)

        # Quick filter: only process items 8.01 and 7.01
        if not any(item in CATALYST_ITEMS for item in items):
            continue

        # Get ticker from display_names
        display = src.get("display_names", [""])
        ticker  = ""
        company = ""
        if display:
            # Format: "Company Name  (TICK)  (CIK 0001234)"
            import re
            match = re.search(r'\(([A-Z]{1,5})(?:,|\s|\))', display[0])
            if match:
                ticker = match.group(1)
            company = display[0].split("  (")[0] if "  (" in display[0] else display[0]

        # Fetch and check document text for FDA keywords
        text = fetch_8k_text(accession, company_cik, filename)
        is_fda, event_desc = detect_fda_event(text)

        if not is_fda:
            continue

        has_insider = ticker in recent_insider_tickers
        db_add_catalyst(ticker, company, company_cik,
                        event_desc, src.get("file_date", end_date),
                        accession, has_insider)

        entry = {
            "ticker":      ticker,
            "company":     company,
            "event_desc":  event_desc,
            "filed_date":  src.get("file_date", end_date),
            "has_insider": has_insider,
            "items":       items,
        }
        fda_events.append(entry)
        if has_insider:
            convergence.append(entry)

        time.sleep(0.15)

    log.info(f"FDA 8-K events found: {len(fda_events)} "
             f"({len(convergence)} with insider convergence)")

    # ── Send convergence alerts first (highest priority) ──────────────────────
    for e in convergence:
        # Get insider detail from watchlist
        with db_connect() as conn:
            ins_rows = conn.execute(
                "SELECT insider_name, insider_role, buy_price, value, txn_date "
                "FROM watchlist WHERE ticker = ? AND alerted = 0 "
                "ORDER BY value DESC LIMIT 1",
                (e["ticker"],)
            ).fetchone()

        insider_line = ""
        if ins_rows:
            ins = dict(ins_rows)
            insider_line = (
                f"\n<b>Insider Buy on Watchlist</b>\n"
                f"  👤 {ins['insider_name']} [{ins['insider_role']}]\n"
                f"  💰 {ins['value']:,.0f} shares @ ${ins['buy_price']:.2f} on {ins['txn_date']}"
            )

        msg = (
            f"🔬 <b>VMc1 CATALYST CONVERGENCE — ${e['ticker']}</b>\n"
            f"<i>{e['company']}</i>\n\n"
            f"<b>FDA Regulatory Event</b>\n"
            f"  📋 Keywords: {e['event_desc']}\n"
            f"  📅 Filed: {e['filed_date']}\n"
            f"  📄 Items: {', '.join(e['items'])}"
            f"{insider_line}\n\n"
            f"<i>⚠️ Verify the 8-K content before acting — "
            f"could be approval, rejection, or routine update</i>"
        )
        send_telegram(msg)
        log.info(f"CONVERGENCE ALERT: ${e['ticker']} — {e['event_desc']}")

    # ── Send standalone FDA events digest ─────────────────────────────────────
    standalone = [e for e in fda_events if not e["has_insider"]]
    if standalone:
        lines = [
            f"🧬 <b>FDA Regulatory Events ({label})</b>",
            f"<i>{start_date} — {len(standalone)} events, no insider overlap</i>",
            "<i>Monitoring only — no insider buying detected</i>",
            "",
        ]
        for e in standalone[:10]:
            lines.append(
                f"<b>${e['ticker']}</b> {e['company'][:30]}\n"
                f"  📋 {e['event_desc']} | Items: {', '.join(e['items'])}"
            )
        if len(standalone) > 10:
            lines.append(f"\n<i>… and {len(standalone) - 10} more</i>")
        msg = "\n".join(lines)
        # Telegram limit is 4096 chars — truncate if needed
        if len(msg) > 4000:
            msg = msg[:4000] + "\n\n<i>... truncated — too many near signals</i>"
        send_telegram(msg)

    log.info(f"=== Catalyst scan complete ===")


# ═══════════════════════════════════════════════════════════════════════════════
# SCAN JOBS  (06:00 and 17:00 CET)
# ═══════════════════════════════════════════════════════════════════════════════

def run_scan(label: str = "morning"):
    now = datetime.now(CET)
    if now.weekday() >= 5:
        log.info(f"Weekend — skipping {label} scan ({now.strftime('%A')})")
        return

    log.info(f"=== SEC Form 4 {label} scan starting ===")
    end_date   = now.strftime("%Y-%m-%d")
    start_date = (now - timedelta(days=1)).strftime("%Y-%m-%d")
    log.info(f"Date range: {start_date} → {end_date}")

    raw_hits = fetch_all_filing_index(start_date, end_date)
    log.info(f"Found {len(raw_hits)} Form 4 filings to inspect")

    if not raw_hits:
        if label == "morning":
            send_telegram(
                f"📋 <b>SEC Form 4 Scan</b>\n<i>{start_date}</i>\n\nNo filings found."
            )
        log.info(f"=== {label} scan complete ===")
        return

    qualifying      = []
    seen_accessions = set()

    for i, hit in enumerate(raw_hits):
        accession, company_cik, xml_filename, src = parse_filing_meta(hit)
        if not accession or accession in seen_accessions:
            continue
        seen_accessions.add(accession)
        file_type = src.get("file_type", "")
        if file_type not in ("4", "") and not file_type.startswith("4"):
            continue
        txns = parse_form4_xml(accession, company_cik, xml_filename, src)
        qualifying.extend(txns)
        if (i + 1) % 100 == 0:
            log.info(f"  Processed {i+1}/{len(raw_hits)}, qualifying: {len(qualifying)}")
        time.sleep(0.1)

    log.info(f"Qualifying purchases: {len(qualifying)}")
    qualifying.sort(key=lambda x: x["value"], reverse=True)

    new_additions = 0
    for t in qualifying:
        if t["ticker"] not in ("N/A", "NONE", "") and t["price"] > 0.10:
            before = db_watchlist_count()
            db_add_ticker(t)
            after = db_watchlist_count()
            if after > before:
                new_additions += 1

    log.info(f"Added {new_additions} new tickers to watchlist")

    if not qualifying:
        log.info(f"=== {label} scan complete ===")
        return

    lines = [
        f"🔍 <b>SEC Form 4 — Insider Purchases</b> <i>({label})</i>",
        f"<i>{start_date} → {end_date}</i>",
        f"<i>{len(seen_accessions):,} filings · {len(qualifying)} qualifying · "
        f"+{new_additions} added to watchlist ({db_watchlist_count()} active)</i>",
        "",
    ]
    for t in qualifying[:15]:
        role    = "DIR" if t["is_director"] else ("OFF" if t["is_officer"] else "INS")
        company = t["company"][:28]
        title   = f" · {t['title'][:20]}" if t["title"] else ""
        lines.append(
            f"<b>${t['ticker']}</b>  {company}\n"
            f"  👤 {t['name']} [{role}]{title}\n"
            f"  {t['shares']:,.0f} sh @ ${t['price']:.2f} = "
            f"<b>${t['value']:,.0f}</b>  📅 {t['date']}"
        )
    if len(qualifying) > 15:
        lines.append(f"\n<i>… and {len(qualifying) - 15} more</i>")

    send_telegram("\n".join(lines))
    log.info(f"=== {label} scan complete ===")


# ═══════════════════════════════════════════════════════════════════════════════
# SPOT CHECK  (runs ~5 min after intraday scan, new tickers only)
# ═══════════════════════════════════════════════════════════════════════════════

def run_spot_check():
    """
    Check only tickers added in the last 30 minutes.
    Runs at 17:05 CET, right after the intraday scan.
    Catches same-day filings while the US market is still open.
    """
    now = datetime.now(CET)
    if now.weekday() >= 5:
        return

    new_rows = db_get_recently_added(minutes=30)
    if not new_rows:
        log.info("Spot check: no new tickers to check")
        return

    log.info(f"=== Spot check: {len(new_rows)} newly added tickers ===")

    # Group by ticker
    by_ticker: dict[str, list] = {}
    for row in new_rows:
        by_ticker.setdefault(row["ticker"], []).append(row)

    signals_by_ticker: dict[str, list[tuple]] = {}

    for ticker, rows in by_ticker.items():
        data = fetch_market_data(ticker)
        if not data:
            continue

        total_value = sum(r["value"] for r in rows)
        ticker_signals = []
        for row in rows:
            sig           = check_signal(data, row["buy_price"],
                                          conviction=len(rows),
                                          is_director=(row["insider_role"] == "DIR"),
                                          value=total_value,
                                          filed_date=row["filed_date"])
            confirmations = sum([sig["price_reclaim"], sig["close_to_entry"],
                                 sig["rsi_ok"], sig["ema_ok"], sig["fresh_filing"],
                                 sig["quality_ok"], sig["sane_price"], sig["liquid"]])
            log.info(
                f"  [spot] {ticker}: price={data['price']:.2f} "
                f"insider={row['buy_price']:.2f} RSI={sig['rsi']}{'*' if sig.get('ceo_large_buy') else ''} "
                f"moved={sig['already_moved']*100:.1f}% age={sig['filing_age']}d "
                f"confirms={confirmations}/8"
            )
            if sig["signal"]:
                ticker_signals.append((row, sig))
                db_mark_alerted(row["id"])

        if ticker_signals:
            signals_by_ticker[ticker] = ticker_signals
        time.sleep(0.3)

    if not signals_by_ticker:
        log.info("Spot check: no signals fired")
        return

    # Send alerts — same format as full watchlist check
    for ticker, ticker_signals in signals_by_ticker.items():
        row, sig   = max(ticker_signals, key=lambda x: x[0]["value"])
        history    = db_get_ticker_history(ticker)
        conviction = history["event_count"]
        agg_value  = history["total_value"]
        has_dir    = history["has_dir"]

        vol_ratio  = sig["volume"] / max(sig["avg_volume"], 1)
        upside_pct = ((sig["price"] - row["buy_price"]) / row["buy_price"]) * 100
        is_micro   = sig["avg_volume"] < MICRO_CAP_VOL_MAX

        is_elite = conviction >= 3 and agg_value >= 1_000_000 and has_dir
        if is_elite:
            badge = "💎 ELITE"
        elif conviction >= 3:
            badge = "🔥 HIGH"
        elif conviction == 2:
            badge = "🟠 ELEVATED"
        elif row["insider_role"] == "DIR" or row["value"] >= 1_000_000:
            badge = "🔺 STANDARD T1"
        else:
            badge = "🔵 STANDARD T2"

        if is_micro:
            micro_note = "\n  ⚠️ <b>MICRO-CAP MOMENTUM</b> — max $100, exit within 5 days"
        else:
            micro_note = ""

        # Trade levels
        entry     = sig["price"]
        stop      = round(row["buy_price"] * 0.98, 2)
        risk      = max(entry - stop, 0.01)
        t1        = round(entry + risk * 1.5, 2)
        t2        = round(entry + risk * 3.0, 2)
        t3        = round(entry + risk * 5.0, 2)

        if badge in ("💎 ELITE", "🔥 HIGH"):
            pos_size = 200
        elif badge == "🟠 ELEVATED":
            pos_size = 175
        elif badge == "🔺 STANDARD T1":
            pos_size = 150
        else:
            pos_size = 100
        if is_micro:
            pos_size = min(pos_size, 100)
        shares_to_buy = int(pos_size / entry) if entry > 0 else 0

        parts = [
            f"⚡ <b>VMc1 INTRADAY SIGNAL — ${ticker}</b>  {badge}",
            f"<i>{row['company']}</i>",
            "<i>Filed today — market still open</i>",
            "",
            f"<b>Insider Context</b>",
            f"  👤 {row['insider_name']} [{row['insider_role']}]",
            f"  💰 {row['shares']:,.0f} sh @ ${row['buy_price']:.2f} (${row['value']:,.0f}) on {row['txn_date']}",
        ]
        if micro_note:
            parts.append(micro_note)
        parts += [
            "",
            "<b>Signal Checks ✅ 9/9</b>",
            f"  📈 ${sig['price']:.2f} vs insider ${row['buy_price']:.2f} ({upside_pct:+.1f}%) ✅",
            f"  📊 RSI {sig['rsi']} ({RSI_MIN}–{RSI_MAX}) ✅",
            f"  〰️ Above 20 EMA (${sig['ema20']:.2f}) ✅",
            f"  ⏱ Filing age: {sig['filing_age']}d ✅",
            f"  🔊 Volume {'✅' if sig['volume_ok'] else '❌'} ({sig['volume']/max(sig['avg_volume'],1):.1f}x avg)",
            "",
            "<b>Trade Levels</b>",
            f"  🟢 Entry:  ${entry:.2f}  ({shares_to_buy} sh · ${pos_size} position)",
            f"  🔴 Stop:   ${stop:.2f}  (2% below insider ${row['buy_price']:.2f})",
            f"  🎯 T1:     ${t1:.2f}  (1.5R — sell ⅓, stop → breakeven)",
            f"  🎯 T2:     ${t2:.2f}  (3.0R — sell ⅓, trail rest)",
            f"  🎯 T3:     ${t3:.2f}  (5.0R — close last ⅓)",
            f"  ⚖️ Risk: ${risk:.2f}/sh · R:R to T2 = 3:1",
            "",
            "<i>⚡ Same-day signal — 🤖 VMc1 agents briefed</i>",
        ]
        msg = "\n".join(parts)
        send_telegram(msg)

        # Only wake CEO agent for Elite signals
        if badge == "💎 ELITE":
            paperclip_ok = notify_paperclip_ceo(ticker, ticker_signals,
                                                {"avg_volume": sig["avg_volume"]})
            log.info(f"SPOT SIGNAL: ${ticker} {badge} | Paperclip: {'OK' if paperclip_ok else 'FAILED'}")
        else:
            log.info(f"SPOT SIGNAL: ${ticker} {badge} | Paperclip: skipped (not Elite)")

    log.info(f"=== Spot check complete: {len(signals_by_ticker)} signals fired ===")


# ═══════════════════════════════════════════════════════════════════════════════
# WATCHLIST SIGNAL CHECK  (21:00 CET daily)
# ═══════════════════════════════════════════════════════════════════════════════

def run_watchlist_check(label: str = "close"):
    now = datetime.now(CET)
    if now.weekday() >= 5:
        log.info(f"Weekend — skipping {label} watchlist check")
        return

    db_expire_old()
    active = db_get_active()
    log.info(f"=== Watchlist check [{label}]: {len(active)} active positions ===")

    if not active:
        log.info("Watchlist empty — nothing to check")
        return

    # Group rows by ticker first
    by_ticker: dict[str, list] = {}
    for row in active:
        by_ticker.setdefault(row["ticker"], []).append(row)

    signals_by_ticker: dict[str, list[tuple]] = {}  # ticker → [(row, sig), ...]
    partial_by_ticker: dict[str, tuple]        = {}  # ticker → (best_row, best_sig, count)

    for ticker, rows in by_ticker.items():
        data = fetch_market_data(ticker)
        if not data:
            log.debug(f"No market data for {ticker}")
            continue

        total_value    = sum(r["value"] for r in rows)
        ticker_signals = []
        best_partial   = None

        for row in rows:
            sig           = check_signal(data, row["buy_price"],
                                          conviction=len(rows),
                                          is_director=(row["insider_role"] == "DIR"),
                                          value=total_value,
                                          filed_date=row["filed_date"])
            confirmations = sum([sig["price_reclaim"], sig["close_to_entry"],
                                 sig["rsi_ok"], sig["ema_ok"], sig["fresh_filing"],
                                 sig["quality_ok"], sig["sane_price"], sig["liquid"]])
            log.info(
                f"  {ticker}: price={data['price']:.2f} insider={row['buy_price']:.2f} "
                f"RSI={sig['rsi']} moved={sig['already_moved']*100:.1f}% "
                f"age={sig['filing_age']}d confirms={confirmations}/8"
            )

            if sig["signal"]:
                ticker_signals.append((row, sig))
                db_mark_alerted(row["id"])
            elif confirmations >= 6:
                if best_partial is None or confirmations > best_partial[2]:
                    best_partial = (row, sig, confirmations)

        if ticker_signals:
            signals_by_ticker[ticker] = ticker_signals
        elif best_partial:
            partial_by_ticker[ticker] = best_partial

        time.sleep(0.3)

    # ── Full signals — one Telegram + one Paperclip task per ticker ───────────
    for ticker, ticker_signals in signals_by_ticker.items():
        row, sig    = max(ticker_signals, key=lambda x: x[0]["value"])
        # Conviction reflects the INSIDER'S total buying history for this
        # ticker within the lookback window — not just rows unalerted tonight.
        history     = db_get_ticker_history(ticker)
        conviction  = history["event_count"]
        agg_value   = history["total_value"]
        has_dir     = history["has_dir"]

        vol_ratio   = sig["volume"] / max(sig["avg_volume"], 1)
        upside_pct  = ((sig["price"] - row["buy_price"]) / row["buy_price"]) * 100
        days_since  = (datetime.now(CET) -
                       datetime.fromisoformat(row["added_at"])).days
        is_micro    = sig["avg_volume"] < MICRO_CAP_VOL_MAX

        # Conviction badge — 5 unique tiers
        is_elite = conviction >= 3 and agg_value >= 1_000_000 and has_dir
        if is_elite:
            badge = "💎 ELITE"
        elif conviction >= 3:
            badge = "🔥 HIGH"
        elif conviction == 2:
            badge = "🟠 ELEVATED"
        elif row["insider_role"] == "DIR" or row["value"] >= 1_000_000:
            badge = "🔺 STANDARD T1"
        else:
            badge = "🔵 STANDARD T2"

        micro_note = "\n  ⚠️ <b>MICRO-CAP MOMENTUM</b> — max $100, exit within 5 days" \
                     if is_micro else ""

        # Trade levels
        entry     = sig["price"]
        stop      = round(row["buy_price"] * 0.98, 2)
        risk      = max(entry - stop, 0.01)
        t1        = round(entry + risk * 1.5, 2)
        t2        = round(entry + risk * 3.0, 2)
        t3        = round(entry + risk * 5.0, 2)

        if badge in ("💎 ELITE", "🔥 HIGH"):
            pos_size = 200
        elif badge == "🟠 ELEVATED":
            pos_size = 175
        elif badge == "🔺 STANDARD T1":
            pos_size = 150
        else:
            pos_size = 100
        if is_micro:
            pos_size = min(pos_size, 100)
        shares_to_buy = int(pos_size / entry) if entry > 0 else 0

        lines = [
            f"🚨 <b>VMc1 BUY SIGNAL — ${ticker}</b>  {badge}",
            f"<i>{row['company']}</i>",
            "",
            f"<b>Insider Context</b> ({conviction} buy event{'s' if conviction > 1 else ''})",
            f"  👤 {row['insider_name']} [{row['insider_role']}]",
            f"  💰 {row['shares']:,.0f} sh @ ${row['buy_price']:.2f} (${row['value']:,.0f}) on {row['txn_date']}",
            f"  📅 Signal fired {days_since}d after filing",
        ]
        if micro_note:
            lines.append(micro_note)
        lines += [
            "",
            "<b>Signal Checks ✅ 9/9</b>",
            f"  📈 ${sig['price']:.2f} vs insider ${row['buy_price']:.2f} ({upside_pct:+.1f}%) ✅",
            f"  📊 RSI {sig['rsi']} ({RSI_MIN}–{RSI_MAX}) ✅",
            f"  〰️ Above 20 EMA (${sig['ema20']:.2f}) ✅",
            f"  ⏱ Filing age: {sig['filing_age']}d ✅",
            f"  🔊 Volume {'✅' if sig['volume_ok'] else '❌'} ({sig['volume']/max(sig['avg_volume'],1):.1f}x avg)",
            "",
            "<b>Trade Levels</b>",
            f"  🟢 Entry:  ${entry:.2f}  ({shares_to_buy} sh · ${pos_size} position)",
            f"  🔴 Stop:   ${stop:.2f}  (2% below insider ${row['buy_price']:.2f})",
            f"  🎯 T1:     ${t1:.2f}  (1.5R — sell ⅓, stop → breakeven)",
            f"  🎯 T2:     ${t2:.2f}  (3.0R — sell ⅓, trail rest)",
            f"  🎯 T3:     ${t3:.2f}  (5.0R — close last ⅓)",
            f"  ⚖️ Risk: ${risk:.2f}/sh · R:R to T2 = 3:1",
            "",
            "<i>🤖 VMc1 agents briefed — Research → Risk → Execution underway</i>" if badge == "💎 ELITE" else "<i>📋 Review manually — Paperclip reserved for 💎 Elite signals</i>",
        ]
        msg = "\n".join(lines)
        send_telegram(msg)

        # Only wake CEO agent for Elite signals — controls Anthropic costs
        if badge == "💎 ELITE":
            paperclip_ok = notify_paperclip_ceo(ticker, ticker_signals,
                                                {"avg_volume": sig["avg_volume"]})
            log.info(f"SIGNAL FIRED: ${ticker} {badge} | Paperclip: {'OK' if paperclip_ok else 'FAILED'}")
        else:
            log.info(f"SIGNAL FIRED: ${ticker} {badge} | Paperclip: skipped (not Elite)")

    # ── Partial signals digest ────────────────────────────────────────────────
    if partial_by_ticker:
        lines = ["👀 <b>VMc1 Watchlist — Near Signals</b>", ""]
        for ticker, (row, sig, n) in sorted(
            partial_by_ticker.items(), key=lambda x: -x[1][2]
        ):
            vol_ratio  = sig["volume"] / max(sig["avg_volume"], 1)
            moved_pct  = sig["already_moved"] * 100
            sanity     = ""
            if not sig["sane_price"]:
                sanity += " ⚠️ price data error"
            if not sig["liquid"]:
                sanity += " ⚠️ illiquid"
            if not sig["close_to_entry"]:
                sanity += f" ⚠️ chasing (+{moved_pct:.0f}%)"
            checks = (
                f"{'✅' if sig['price_reclaim'] else '❌'} price "
                f"{'✅' if sig['close_to_entry'] else '❌'} entry(+{sig['already_moved']*100:.0f}%) "
                f"{'✅' if sig['rsi_ok'] else '❌'} RSI={sig['rsi']}{'*' if sig.get('ceo_large_buy') else ''} "
                f"{'✅' if sig['ema_ok'] else '❌'} EMA "
                f"{'✅' if sig['fresh_filing'] else '❌'} age={sig['filing_age']}d"
            )
            history    = db_get_ticker_history(ticker)
            conviction = history["event_count"]
            agg_value  = history["total_value"]
            has_dir    = history["has_dir"]
            conv_badge = " 💎" if (conviction >= 3 and agg_value >= 1_000_000 and has_dir) else " 🔥" if conviction >= 3 else " 🟠" if conviction == 2 else " 🔺" if (row["insider_role"] == "DIR" or row["value"] >= 1_000_000) else " 🔵"
            lines.append(
                f"<b>${ticker}</b>{conv_badge} {n}/9\n"
                f"  {checks}\n"
                f"  Price ${sig['price']:.2f} | Insider ${row['buy_price']:.2f} | "
                f"Vol {vol_ratio:.1f}x | +{moved_pct:.1f}%"
            )
        send_telegram("\n".join(lines))

    log.info(
        f"=== Watchlist check complete: {len(signals_by_ticker)} "
        f"tickers fired signals ==="
    )


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

# ── Schedule definition (all times in CET/CEST — DST handled automatically) ──
# Each entry: (hour, minute, job_function, label)
SCHEDULE = [
    (6,  0,  lambda: run_scan("morning"),            "morning scan"),
    (6,  15, lambda: run_catalyst_scan("morning"),   "morning catalyst"),
    (15, 0,  lambda: run_watchlist_check("pre-market"), "pre-market watchlist"),
    (17, 0,  lambda: run_scan("intraday"),           "intraday scan"),
    (17, 5,  run_spot_check,                         "spot check"),
    (17, 15, lambda: run_catalyst_scan("intraday"),  "intraday catalyst"),
    (21, 0,  lambda: run_watchlist_check("close"),   "close watchlist"),
]


def main():
    db_init()
    log.info("SEC Form 4 Scanner + Watchlist Monitor starting up")
    send_telegram(
        "✅ <b>SEC Form 4 Scanner</b> online\n"
        "📋 Watchlist monitor active\n"
        "🤖 VMc1 Paperclip integration enabled\n"
        "⚡ Intraday scan + spot check (17:00 / 17:05 CET)\n"
        "📊 Watchlist checks: 15:00 pre-market + 21:00 close\n"
        "🕐 All times CET/CEST — DST aware"
    )

    log.info(
        "Scheduled (CET/CEST): scan 06:00+17:00 | catalyst 06:15+17:15 | "
        "watchlist 15:00+21:00 | spot 17:05"
    )

    log.info("Running initial scan now...")
    run_scan("startup")

    # Track which jobs have already run today to avoid double-firing
    last_run: dict[str, str] = {}  # label → date string

    while True:
        now  = datetime.now(CET)
        date = now.strftime("%Y-%m-%d")
        hm   = (now.hour, now.minute)

        for hour, minute, job, label in SCHEDULE:
            if hm == (hour, minute) and last_run.get(label) != date:
                last_run[label] = date
                try:
                    log.info(f"Triggering: {label}")
                    job()
                except Exception as e:
                    log.error(f"Job failed [{label}]: {e}", exc_info=True)

        time.sleep(30)


if __name__ == "__main__":
    main()
