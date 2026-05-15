"""
SEC Form 4 Insider Purchase Scanner
Railway deployment — runs daily at 06:00 CET
Scans EDGAR for open-market insider purchases > $100k in last 24h
Sends ranked Telegram alert
"""

import os
import time
import logging
import datetime
import xml.etree.ElementTree as ET
from zoneinfo import ZoneInfo

import requests
import schedule

# ── Config ────────────────────────────────────────────────────────────────────
TELEGRAM_TOKEN  = os.environ["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]
MIN_VALUE       = int(os.getenv("MIN_VALUE", "100000"))
MAX_RESULTS     = int(os.getenv("MAX_RESULTS", "20"))
SCAN_HOUR_CET   = int(os.getenv("SCAN_HOUR_CET", "6"))
REQUEST_TIMEOUT = 20
CET             = ZoneInfo("Europe/Oslo")

HEADERS = {"User-Agent": "VMc1Investments admin@vmc1.com", "Accept-Encoding": "gzip, deflate"}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

def get_recent_form4_filings(start_dt: str, end_dt: str) -> list[dict]:
    filings = []
    url = (
        "https://efts.sec.gov/LATEST/search-index"
        f"?q=%22%22&forms=4"
        f"&dateRange=custom&startdt={start_dt}&enddt={end_dt}"
        "&hits.hits.total.value=true"
    )
    try:
        log.info("Querying EDGAR EFTS: %s", url)
        r = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        hits = data.get("hits", {}).get("hits", [])
        log.info("EFTS returned %d raw hits", len(hits))
        for h in hits:
            src = h.get("_source", {})
            accession = h.get("_id", "").replace("-", "")
            if accession:
                filings.append({
                    "accession": accession,
                    "accession_fmt": h.get("_id", ""),
                    "entity_name": src.get("entity_name", ""),
                    "file_date": src.get("file_date", ""),
                })
        return filings
    except Exception as e:
        log.warning("EFTS query failed (%s), falling back to RSS", e)

    rss_url = (
        "https://www.sec.gov/cgi-bin/browse-edgar"
        "?action=getcurrent&type=4&dateb=&owner=include&count=100"
        "&search_text=&output=atom"
    )
    try:
        log.info("Querying EDGAR RSS fallback")
        r = requests.get(rss_url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        root = ET.fromstring(r.content)
        ns = {"atom": "http://www.w3.org/2005/Atom"}
        for entry in root.findall("atom:entry", ns):
            updated = (entry.findtext("atom:updated", "", ns) or "")[:10]
            if updated < start_dt:
                continue
            link = entry.find("atom:link", ns)
            href = link.get("href", "") if link is not None else ""
            acc_fmt = ""
            if "/Archives/edgar/data/" in href:
                parts = href.split("/")
                for p in parts:
                    if len(p) == 20 and "-" in p:
                        acc_fmt = p
                        break
            if acc_fmt:
                filings.append({
                    "accession": acc_fmt.replace("-", ""),
                    "accession_fmt": acc_fmt,
                    "entity_name": entry.findtext("atom:company-name", "", ns),
                    "file_date": updated,
                })
        log.info("RSS fallback returned %d filings", len(filings))
    except Exception as e:
        log.error("RSS fallback also failed: %s", e)

    return filings


def fetch_filing_xml(accession: str, entity_name: str) -> str | None:
    acc_fmt = f"{accession[:10]}-{accession[10:12]}-{accession[12:]}"
    company_url = (
        f"https://efts.sec.gov/LATEST/search-index"
        f"?q=%22{acc_fmt}%22&forms=4"
    )
    try:
        r = requests.get(company_url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        hits = r.json().get("hits", {}).get("hits", [])
        if not hits:
            return None
        src = hits[0].get("_source", {})
        cik = str(src.get("entity_id", "")).zfill(10)
        if not cik or cik == "0000000000":
            return None
    except Exception as e:
        log.debug("CIK lookup failed for %s: %s", accession, e)
        return None

    index_url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession}/{acc_fmt}-index.json"
    try:
        r = requests.get(index_url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        index = r.json()
        xml_file = None
        for doc in index.get("directory", {}).get("item", []):
            name = doc.get("name", "")
            if name.endswith(".xml") and "xsl" not in name.lower():
                xml_file = name
                break
        if not xml_file:
            return None
        xml_url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession}/{xml_file}"
        r2 = requests.get(xml_url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        r2.raise_for_status()
        return r2.text
    except Exception as e:
        log.debug("XML fetch failed for %s: %s", accession, e)
        return None


def parse_form4_xml(xml_str: str) -> list[dict]:
    try:
        root = ET.fromstring(xml_str)
    except ET.ParseError as e:
        log.debug("XML parse error: %s", e)
        return []

    ticker   = (root.findtext(".//issuerTradingSymbol") or "").strip().upper()
    company  = (root.findtext(".//issuerName") or "").strip()
    owner    = (root.findtext(".//rptOwnerName") or "").strip()
    title    = (root.findtext(".//officerTitle") or "").strip()
    is_dir   = root.findtext(".//isDirector", "0") == "1"
    is_10pct = root.findtext(".//isTenPercentOwner", "0") == "1"
    role     = title if title else ("Director" if is_dir else ("10% Owner" if is_10pct else "Insider"))

    results = []
    for tx in root.findall(".//nonDerivativeTransaction"):
        code     = (tx.findtext(".//transactionCode") or "").strip()
        acquired = (tx.findtext(".//transactionAcquiredDisposedCode/value") or "").strip()
        if code != "P" or acquired != "A":
            continue
        try:
            shares = float(tx.findtext(".//transactionShares/value") or 0)
            price  = float(tx.findtext(".//transactionPricePerShare/value") or 0)
            value  = shares * price
        except (TypeError, ValueError):
            continue
        if value < MIN_VALUE:
            continue
        results.append({
            "ticker":  ticker or "???",
            "company": company,
            "owner":   owner,
            "role":    role,
            "shares":  int(shares),
            "price":   price,
            "value":   value,
        })
    return results


def send_telegram(text: str) -> bool:
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id":    TELEGRAM_CHAT_ID,
        "text":       text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    try:
        r = requests.post(url, json=payload, timeout=15)
        r.raise_for_status()
        log.info("Telegram sent OK")
        return True
    except Exception as e:
        log.error("Telegram failed: %s", e)
        return False


def format_message(filings: list[dict], start_dt: str, end_dt: str) -> str:
    now_cet = datetime.datetime.now(CET).strftime("%Y-%m-%d %H:%M CET")
    if not filings:
        return (
            f"📋 <b>SEC Form 4 — Insider Purchases</b>\n"
            f"📅 {start_dt} → {end_dt}\n\n"
            f"No qualifying open-market purchases (&gt;${MIN_VALUE:,}) found.\n\n"
            f"<i>Scanned at {now_cet}</i>"
        )
    sorted_f = sorted(filings, key=lambda x: x["value"], reverse=True)[:MAX_RESULTS]
    lines = [
        f"📋 <b>SEC Form 4 — Insider Purchases (Last 24h)</b>",
        f"📅 {start_dt} → {end_dt} | {len(filings)} qualifying trade(s)\n",
    ]
    for f in sorted_f:
        v = f["value"]
        val_str = f"${v/1_000_000:.2f}M" if v >= 1_000_000 else f"${v:,.0f}"
        lines.append(
            f"🟢 <b>${f['ticker']}</b> — {f['company']}\n"
            f"   👤 {f['owner']} ({f['role']})\n"
            f"   📈 {f['shares']:,} shares @ ${f['price']:.2f} = <b>{val_str}</b>\n"
        )
    if len(filings) > MAX_RESULTS:
        lines.append(f"<i>...and {len(filings) - MAX_RESULTS} more qualifying trades</i>\n")
    lines.append(f"<i>Scanned at {now_cet}</i>")
    return "\n".join(lines)


def run_scan():
    log.info("=== SEC Form 4 scan starting ===")
    now   = datetime.datetime.now(CET)
    start = (now - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    end   = now.strftime("%Y-%m-%d")
    raw_filings = get_recent_form4_filings(start, end)
    log.info("Found %d Form 4 filings to inspect", len(raw_filings))
    qualifying = []
    for i, filing in enumerate(raw_filings):
        acc = filing["accession"]
        log.debug("[%d/%d] Fetching %s (%s)", i + 1, len(raw_filings), acc, filing.get("entity_name", ""))
        xml_str = fetch_filing_xml(acc, filing.get("entity_name", ""))
        if not xml_str:
            continue
        parsed = parse_form4_xml(xml_str)
        qualifying.extend(parsed)
        time.sleep(0.15)
    log.info("Qualifying purchases: %d", len(qualifying))
    msg = format_message(qualifying, start, end)
    send_telegram(msg)
    log.info("=== Scan complete ===")


def startup_message():
    now_cet = datetime.datetime.now(CET).strftime("%Y-%m-%d %H:%M CET")
    send_telegram(
        f"🚀 <b>SEC Form 4 Scanner online</b>\n"
        f"Daily scan at <b>{SCAN_HOUR_CET:02d}:00 CET</b>\n"
        f"Filter: open-market purchases &gt; ${MIN_VALUE:,}\n"
        f"Started: {now_cet}"
    )


if __name__ == "__main__":
    log.info("SEC Form 4 Scanner starting up")
    startup_message()
    schedule.every().day.at(f"{SCAN_HOUR_CET:02d}:00").do(run_scan)
    log.info("Scheduled daily scan at %02d:00 CET", SCAN_HOUR_CET)
    log.info("Running initial scan now...")
    run_scan()
    while True:
        schedule.run_pending()
        time.sleep(60)
