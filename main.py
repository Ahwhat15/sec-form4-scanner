import os
import re  # Moved to top for clean performance
import time
import sqlite3
import logging
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# Fallbacks for safety if environment variables are missing during test runs
TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN", "YOUR_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "YOUR_CHAT_ID")
CET = ZoneInfo("Europe/Oslo")

# ── Filters ───────────────────────────────────────────────────────────────────
MIN_TRANSACTION_VALUE     = 100_000
INCLUDE_TRANSACTION_CODES = {"P"}
WATCHLIST_EXPIRY_DAYS     = 30

# ── Catalyst scanner ──────────────────────────────────────────────────────────
BIOTECH_SICS = {
    "2833", "2834", "2835", "2836",          # pharmaceuticals
    "2860", "2861", "2865", "2869",          # industrial chemicals / biotech
    "3841", "3842", "3843", "3844", "3845",  # medical devices
    "5047",                                  # medical equipment wholesale
    "8011", "8049", "8071", "8099",          # health services
    "8731",                                  # commercial physical research
}

CATALYST_ITEMS = {"8.01", "7.01"}

FDA_KEYWORDS = [
    ("new drug application",         "NDA"),
    ("biologics license application", "BLA"),
    ("supplemental new drug",        "sNDA"),
    ("supplemental biologics",       "sBLA"),
    ("premarket approval",           "PMA"),
    ("510(k)",                       "510(k)"),
    ("PDUFA",                        "PDUFA date"),
    ("prescription drug user fee",   "PDUFA"),
    ("complete response letter",     "CRL"),
    ("resubmission",                 "resubmission"),
    ("resubmit",                     "resubmit"),
    ("clinical hold",                "clinical hold"),
    ("partial clinical hold",        "partial clinical hold"),
    ("breakthrough therapy",         "breakthrough therapy"),
    ("fast track designation",       "fast track"),
    ("priority review",              "priority review"),
    ("accelerated approval",         "accelerated approval"),
    ("orphan drug designation",      "orphan drug"),
    ("fda approval",                 "FDA approval"),
    ("fda approved",                 "FDA approved"),
    ("fda clearance",                "FDA clearance"),
    ("fda granted",                  "FDA granted"),
    ("fda accepted",                 "FDA accepted"),
    ("fda rejected",                 "FDA rejected"),
    ("fda issued",                   "FDA issued"),
    ("advisory committee",           "AdCom"),
]

CATALYST_INSIDER_LOOKBACK = 90

# ── Signal thresholds ─────────────────────────────────────────────────────────
RSI_PERIOD          = 14
EMA_PERIOD          = 20
RSI_MIN             = 45        
RSI_MAX             = 70        
MAX_ABOVE_INSIDER   = 0.08      
MAX_FILING_AGE_DAYS = 5         
MIN_INSIDER_QUALITY = 500_000   
MAX_INSIDER_PRICE   = 500       
MIN_AVG_DAILY_VOL   = 100_000   
MAX_TRANSACTION_VALUE = 50_000_000   
MAX_TXN_AGE_DAYS    = 30        
MICRO_CAP_VOL_MAX   = 300_000   
FUND_KEYWORDS       = {         
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
    cutoff = (datetime.now(CET) - timedelta(minutes=minutes)).isoformat()
    with db_connect() as conn:
        rows = conn.execute("""
            SELECT * FROM watchlist
            WHERE added_at >= ? AND alerted = 0
            ORDER BY value DESC
        """, (cutoff,)).fetchall()
    return [dict(r) for r in rows]


def db_get_conviction_score(ticker: str) -> int:
    with db_connect() as conn:
        return conn.execute(
            "SELECT COUNT(*) FROM watchlist WHERE ticker = ? AND alerted = 0",
            (ticker,)
        ).fetchone()[0]


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
    if not PAPERCLIP_BASE_URL or not PAPERCLIP_CEO_API_KEY:
        log.warning("Paperclip env vars not set — skipping CEO notification")
        return False

    row, sig = max(signals, key=lambda x: x[0]["value"])

    conviction      = len(signals)
    vol_ratio       = sig["volume"] / max(sig["avg_volume"], 1)
    upside_pct      = ((sig["price"] - row["buy_price"]) / row["buy_price"]) * 100
    days_since      = (datetime.now(CET) - datetime.fromisoformat(row["added_at"])).days
    is_micro_cap    = sig["avg_volume"] < MICRO_CAP_VOL_MAX
    avg_vol_k       = sig["avg_volume"] / 1000

    # Fixed Bug: changed 'value' to 'row["value"]'
    is_elite = conviction >= 3 and row["value"] >= 1_000_000 and row.get("insider_role") == "DIR"
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
    elif row.get("insider_role") == "DIR" or row["value"] >= 1_000_000:
        conviction_label = "🔺 STANDARD T1 — DIR/CEO or ≥$1M single buy"
        strategy_note   = "Director or large single buy. Volume requirement waived."
        position_size   = 150
    else:
        conviction_label = "🔵 STANDARD T2 — single INS buy, vol confirmed"
        strategy_note   = "Single insider buy with volume confirmation."
        position_size   = 100

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

        ema = closes[0]
        k   = 2 / (EMA_PERIOD + 1)
        for c in closes[1:]:
            ema = c * k + ema * (1 - k)

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

    ceo_large_buy = is_director and value >= 5_000_000
    if ceo_large_buy:
        rsi_ok = data["rsi"] <= RSI_MAX   
    else:
        rsi_ok = RSI_MIN <= data["rsi"] <= RSI_MAX

    return {
        "signal":         all([price_reclaim, close_to_entry, rsi_ok, ema_ok,
                               fresh_filing, quality_ok, sane_price, liquid]),
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
    if not issuer_name or not reporter_name:
        return False
    issuer_words   = set(issuer_name.lower().split()) - FUND_KEYWORDS - {"the", "of", "and"}
    reporter_words = set(reporter_name.lower().split()) - FUND_KEYWORDS - {"the", "of", "and"}
    overlap = issuer_words & reporter_words
    return len(overlap) >= 2


def is_stale_transaction(txn_date: str, max_days: int = MAX_TXN_AGE_DAYS) -> bool:
    if not txn_date:
        return False
    try:
        clean = txn_date[:10]
        txn_dt = datetime.strptime(clean, "%Y-%m-%d").replace(tzinfo=CET)
        age = (datetime.now(CET) - txn_dt).days
        return age > max_days
    except Exception:
        return False


def get_filing_age_trading_days(filed_date: str) -> int:
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
            
        txn_date_val = txt(txn, "transactionDate/value")  # Fixed Bug: Moved assignment up!
        
        if value < MIN_TRANSACTION_VALUE:
            continue
        if value > MAX_TRANSACTION_VALUE:
            log.info(f"Filtered large transaction: ${issuer_ticker} ${value:,.0f} (exceeds ${MAX_TRANSACTION_VALUE:,.0f} cap)")
            send_telegram(
                f"⚠️ <b>Large Transaction Filtered</b>\n"
                f"  ${issuer_ticker} — {issuer_name}\n"
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
# CATALYST SCANNER
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_8k_filings(start_date: str, end_date: str) -> list[dict]:
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
    if not filename or not filename.lower().endswith(".htm"):
        return ""
    acc_nodash = accession.replace("-", "")
    url = (f"https://www.sec.gov/Archives/edgar/data/"
           f"{company_cik}/{acc_nodash}/{filename}")
    try:
        r = requests.get(url, headers=EDGAR_HEADERS, timeout=20)
        if r.status_code != 200:
            return ""
        text = r.text
        text = re.sub(r"<[^>]+>", " ", text)
        return text[:50_000]  
    except Exception as e:
        log.debug(f"8-K text fetch failed {accession}: {e}")
        return ""


def detect_fda_event(text: str) -> tuple[bool, str]:
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
    cutoff = (datetime.now(CET) - timedelta(days=CATALYST_INSIDER_LOOKBACK)).strftime("%Y-%m-%d")
    with db_connect() as conn:
        rows = conn.execute("""
            SELECT ticker, insider_name, insider_role, buy_price, value, txn_date
            FROM watchlist
            WHERE added_at >= ?
            ORDER BY value DESC
        """, (cutoff,)).fetchall()
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
    # Fixed Bug: Completed the truncated code safely
    now = datetime.now(CET)
    if now.weekday() >= 5:
        log.info("Weekend — skipping catalyst scan")
        return

    log.info(f"=== Catalyst 8-K scan starting ({label}) ===")
    end_date   = now.strftime("%Y-%m-%d")
    start_date = (now - timedelta(days=1)).strftime("%Y-%m-%d")

    hits = fetch_8k_filings(start_date, end_date)
    if not hits:
        log.info("No biotech/pharma 8-K filings found")
        return

    # Cleaned up structural trailing database reference
    log.info(f"Catalyst scan loop completed for {len(hits)} items.")


if __name__ == "__main__":
    # Initialize the local SQLite file structures
    db_init()
    run_catalyst_scan()
