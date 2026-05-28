import os
import time
import sqlite3
import logging
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import schedule

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

# ── Signal thresholds ─────────────────────────────────────────────────────────
RSI_PERIOD        = 14
RSI_MIN           = 50
EMA_PERIOD        = 20
VOLUME_MULTIPLIER = 1.5
MAX_INSIDER_PRICE   = 500      # filter data errors (e.g. $1191 instead of $1.191)
MIN_AVG_DAILY_VOL   = 100_000  # shares — filter illiquid tickers

# ── EDGAR ─────────────────────────────────────────────────────────────────────
EDGAR_HEADERS = {"User-Agent": "VMc1Investments scanner@vmc1.no"}
EFTS_URL      = "https://efts.sec.gov/LATEST/search-index"

# ── Database ──────────────────────────────────────────────────────────────────
DB_PATH = "/data/watchlist.db"

# ── Paperclip VMc1 ────────────────────────────────────────────────────────────
PAPERCLIP_BASE_URL  = os.environ.get("PAPERCLIP_BASE_URL", "")
PAPERCLIP_JWT       = os.environ.get("PAPERCLIP_JWT_SECRET", "")
VMC1_COMPANY_ID     = "dc2df96a-a846-4634-a9a0-24e593916c75"
VMC1_CEO_AGENT_ID   = "3db60f1f-86fd-461e-a7bd-96392fa2c893"


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

def notify_paperclip_ceo(row: dict, sig: dict) -> bool:
    """
    Create a task in Paperclip assigned to the CEO agent.
    The CEO wakes (wakeOnDemand=true) and delegates the full
    Research → Backtest → Risk → Execution chain.
    """
    if not PAPERCLIP_BASE_URL or not PAPERCLIP_JWT:
        log.warning("Paperclip env vars not set — skipping CEO notification")
        return False

    days_since  = (datetime.now(CET) - datetime.fromisoformat(row["added_at"])).days
    vol_ratio   = sig["volume"] / sig["avg_volume"]
    upside_pct  = ((sig["price"] - row["buy_price"]) / row["buy_price"]) * 100

    task_title = f"[INSIDER SIGNAL] ${row['ticker']} — 4/4 Buy Confirmations"

    task_body = f"""## VMc1 Insider Flow Signal — Action Required

**Ticker:** ${row['ticker']}
**Company:** {row['company']}
**Signal Date:** {datetime.now(CET).strftime('%Y-%m-%d')}

---

### Insider Context
- **Insider:** {row['insider_name']} [{row['insider_role']}]
- **Purchase:** {row['shares']:,.0f} shares @ ${row['buy_price']:.2f} = ${row['value']:,.0f}
- **Transaction Date:** {row['txn_date']}
- **Days Since Filing:** {days_since}

---

### Technical Confirmations (4/4) ✅
- Price ${sig['price']:.2f} > Insider buy price ${row['buy_price']:.2f} ({upside_pct:+.1f}%) ✅
- RSI {sig['rsi']} > {RSI_MIN} ✅
- Price > 20 EMA (${sig['ema20']:.2f}) ✅
- Volume {vol_ratio:.1f}x above 20-day average ✅

---

### Requested Actions

1. **Research Agent** — Pull company fundamentals, recent news, sector context, and insider's historical trade performance. Score conviction (1–10).

2. **Backtest Agent** — Run the insider-buy + 4-confirmation strategy on ${row['ticker']} historically. Report win rate, avg return, and max drawdown.

3. **Risk Management Agent** — Size the position using VMc1 rules ($200/trade, stop = 2% below insider buy price ${row['buy_price']:.2f}, target = 3:1 R:R). Approve or reject.

4. **Execution Agent** — If Risk approves, place the paper trade on Alpaca immediately. Report entry price, stop, target, and position size.

Report back with a consolidated decision. This is a paper trade — no real capital at risk.
"""

    url     = f"{PAPERCLIP_BASE_URL}/api/companies/{VMC1_COMPANY_ID}/issues"
    headers = {
        "Authorization": f"Bearer {PAPERCLIP_JWT}",
        "Content-Type":  "application/json",
    }
    payload = {
        "title":       task_title,
        "description": task_body,
        "assigneeId":  VMC1_CEO_AGENT_ID,
        "priority":    "urgent",
        "status":      "todo",
    }

    try:
        r = requests.post(url, json=payload, headers=headers, timeout=15)
        r.raise_for_status()
        issue = r.json()
        issue_id = issue.get("issuePrefix", "VMC") + "-" + str(issue.get("number", "?"))
        log.info(f"Paperclip task created: {issue_id} for {row['ticker']}")

        # Wake the CEO agent immediately via heartbeat trigger
        wake_url = (f"{PAPERCLIP_BASE_URL}/api/companies/{VMC1_COMPANY_ID}"
                    f"/agents/{VMC1_CEO_AGENT_ID}/heartbeat")
        requests.post(wake_url, headers=headers, timeout=10)
        log.info(f"CEO agent heartbeat triggered")
        return True

    except Exception as e:
        log.error(f"Paperclip notification failed for {row['ticker']}: {e}")
        return False


# ═══════════════════════════════════════════════════════════════════════════════
# MARKET DATA
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_market_data(ticker: str) -> dict | None:
    try:
        import yfinance as yf
        # Bypass yfinance cache to always get fresh data
        tk   = yf.Ticker(ticker)
        tk._history = None  # clear any cached history
        hist = tk.history(period="60d", interval="1d", auto_adjust=True)
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
        rsi = 100 - (100 / (1 + avg_gain / avg_loss))

        avg_vol = sum(volumes[-EMA_PERIOD:]) / EMA_PERIOD

        return {"price": price, "rsi": rsi, "ema20": ema,
                "volume": volume, "avg_volume": avg_vol}
    except Exception as e:
        log.debug(f"Market data failed for {ticker}: {e}")
        return None


def check_signal(data: dict, insider_buy_price: float) -> dict:
    price_reclaim = data["price"] > insider_buy_price
    rsi_ok        = data["rsi"] > RSI_MIN
    ema_ok        = data["price"] > data["ema20"]
    vol_ok        = data["volume"] > data["avg_volume"] * VOLUME_MULTIPLIER
    sane_price = insider_buy_price <= MAX_INSIDER_PRICE
    liquid     = data["avg_volume"] >= MIN_AVG_DAILY_VOL

    return {
        "signal":        all([price_reclaim, rsi_ok, ema_ok, vol_ok, sane_price, liquid]),
        "price_reclaim": price_reclaim,
        "rsi_ok":        rsi_ok,
        "ema_ok":        ema_ok,
        "vol_ok":        vol_ok,
        "sane_price":    sane_price,
        "liquid":        liquid,
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
            t = hits_obj.get("total", {})
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


def parse_form4_xml(accession: str, company_cik: str, xml_filename: str, src: dict) -> list[dict]:
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
        if value < MIN_TRANSACTION_VALUE:
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
            "date":        txt(txn, "transactionDate/value"),
        })
    return results


# ═══════════════════════════════════════════════════════════════════════════════
# DAILY SCAN JOB
# ═══════════════════════════════════════════════════════════════════════════════

def run_scan():
    now = datetime.now(CET)
    if now.weekday() >= 5:
        log.info(f"Weekend — skipping scan ({now.strftime('%A')})")
        return

    log.info("=== SEC Form 4 scan starting ===")
    end_date   = now.strftime("%Y-%m-%d")
    start_date = (now - timedelta(days=1)).strftime("%Y-%m-%d")
    log.info(f"Date range: {start_date} → {end_date}")

    raw_hits = fetch_all_filing_index(start_date, end_date)
    log.info(f"Found {len(raw_hits)} Form 4 filings to inspect")

    if not raw_hits:
        send_telegram(f"📋 <b>SEC Form 4 Scan</b>\n<i>{start_date}</i>\n\nNo filings found.")
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
        msg = (f"📋 <b>SEC Form 4 Insider Scan</b>\n<i>{start_date} → {end_date}</i>\n\n"
               f"No qualifying purchases ≥ ${MIN_TRANSACTION_VALUE:,}\n"
               f"<i>Scanned {len(seen_accessions):,} unique filings</i>")
        send_telegram(msg)
        log.info("=== Scan complete ===")
        return

    lines = [
        "🔍 <b>SEC Form 4 — Insider Purchases</b>",
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
            f"  {t['shares']:,.0f} sh @ ${t['price']:.2f} = <b>${t['value']:,.0f}</b>  📅 {t['date']}"
        )
    if len(qualifying) > 15:
        lines.append(f"\n<i>… and {len(qualifying) - 15} more</i>")

    send_telegram("\n".join(lines))
    log.info("=== Scan complete ===")


# ═══════════════════════════════════════════════════════════════════════════════
# WATCHLIST SIGNAL CHECK  (21:00 CET daily — after US market close)
# ═══════════════════════════════════════════════════════════════════════════════

def run_watchlist_check():
    now = datetime.now(CET)
    if now.weekday() >= 5:
        log.info("Weekend — skipping watchlist check")
        return

    db_expire_old()
    active = db_get_active()
    log.info(f"=== Watchlist check: {len(active)} active positions ===")

    if not active:
        log.info("Watchlist empty — nothing to check")
        return

    signals_fired = []
    partial       = []

    for row in active:
        ticker = row["ticker"]
        data   = fetch_market_data(ticker)
        if not data:
            log.debug(f"No market data for {ticker}")
            continue

        sig = check_signal(data, row["buy_price"])
        confirmations = sum([sig["price_reclaim"], sig["rsi_ok"],
                             sig["ema_ok"], sig["vol_ok"],
                             sig["sane_price"], sig["liquid"]])

        log.info(
            f"  {ticker}: price={data['price']:.2f} insider={row['buy_price']:.2f} "
            f"RSI={sig['rsi']} EMA={sig['ema20']} "
            f"vol={data['volume']/max(data['avg_volume'],1):.1f}x confirms={confirmations}/6"
        )

        if sig["signal"]:
            signals_fired.append((row, sig))
            db_mark_alerted(row["id"])
        elif confirmations >= 3:
            partial.append((row, sig, confirmations))

        time.sleep(0.3)

    # ── Full signals → Telegram + Paperclip CEO ───────────────────────────────
    for row, sig in signals_fired:
        days_since = (datetime.now(CET) -
                      datetime.fromisoformat(row["added_at"])).days
        vol_ratio  = sig["volume"] / max(sig["avg_volume"], 1)
        upside_pct = ((sig["price"] - row["buy_price"]) / row["buy_price"]) * 100

        # Telegram alert
        msg = (
            f"🚨 <b>VMc1 BUY SIGNAL — ${row['ticker']}</b>\n"
            f"<i>{row['company']}</i>\n\n"
            f"<b>Insider Context</b>\n"
            f"  👤 {row['insider_name']} [{row['insider_role']}]\n"
            f"  💰 {row['shares']:,.0f} sh @ ${row['buy_price']:.2f} "
            f"(${row['value']:,.0f}) on {row['txn_date']}\n"
            f"  📅 Signal fired {days_since}d after filing\n\n"
            f"<b>Confirmations ✅ 4/4</b>\n"
            f"  📈 ${sig['price']:.2f} vs insider ${row['buy_price']:.2f} ({upside_pct:+.1f}%) ✅\n"
            f"  📊 RSI {sig['rsi']} > {RSI_MIN} ✅\n"
            f"  〰️ Price > 20 EMA (${sig['ema20']:.2f}) ✅\n"
            f"  🔊 Volume {vol_ratio:.1f}x avg ✅\n\n"
            f"<i>🤖 VMc1 agents briefed — Research → Risk → Execution underway</i>"
        )
        send_telegram(msg)

        # Paperclip CEO task
        paperclip_ok = notify_paperclip_ceo(row, sig)
        log.info(f"SIGNAL FIRED: {row['ticker']} | Paperclip: {'OK' if paperclip_ok else 'FAILED'}")

    # ── Partial signals digest ────────────────────────────────────────────────
    if partial:
        lines = ["👀 <b>VMc1 Watchlist — Near Signals</b>", ""]
        for row, sig, n in sorted(partial, key=lambda x: -x[2]):
            vol_ratio = sig["volume"] / max(sig["avg_volume"], 1)
            sanity = ""
            if not sig["sane_price"]:
                sanity += " ⚠️ price data error"
            if not sig["liquid"]:
                sanity += " ⚠️ illiquid"
            checks = (
                f"{'✅' if sig['price_reclaim'] else '❌'} price "
                f"{'✅' if sig['rsi_ok'] else '❌'} RSI={sig['rsi']} "
                f"{'✅' if sig['ema_ok'] else '❌'} EMA "
                f"{'✅' if sig['vol_ok'] else '❌'} vol{sanity}"
            )
            lines.append(
                f"<b>${row['ticker']}</b> {n}/4\n"
                f"  {checks}\n"
                f"  Price ${sig['price']:.2f} | Insider ${row['buy_price']:.2f} | "
                f"Vol {vol_ratio:.1f}x"
            )
        send_telegram("\n".join(lines))

    log.info(f"=== Watchlist check complete: {len(signals_fired)} signals fired ===")


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    db_init()
    log.info("SEC Form 4 Scanner + Watchlist Monitor starting up")
    send_telegram(
        "✅ <b>SEC Form 4 Scanner</b> online\n"
        "📋 Watchlist monitor active\n"
        "🤖 VMc1 Paperclip integration enabled"
    )

    schedule.every().day.at("06:00").do(run_scan)
    schedule.every().day.at("21:00").do(run_watchlist_check)
    log.info("Scheduled: scan @ 06:00 CET | watchlist check @ 21:00 CET")

    log.info("Running initial scan now...")
    run_scan()

    while True:
        schedule.run_pending()
        time.sleep(30)


if __name__ == "__main__":
    main()
