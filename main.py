import os
import time
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

# ── Filters ──────────────────────────────────────────────────────────────────
MIN_TRANSACTION_VALUE     = 100_000   # USD
INCLUDE_TRANSACTION_CODES = {"P"}     # open-market purchases only

# ── EDGAR ────────────────────────────────────────────────────────────────────
EDGAR_HEADERS = {
    "User-Agent": "VMc1Investments scanner@vmc1.no",
}
EFTS_URL = "https://efts.sec.gov/LATEST/search-index"


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


def fetch_all_filing_index(start_date: str, end_date: str) -> list[dict]:
    """
    Paginate EDGAR EFTS to collect ALL Form 4 filing index entries.
    Each page returns up to 100 hits; we loop with 'from' offset until done.
    """
    all_hits   = []
    offset     = 0
    total_exp  = None

    while True:
        params = {
            "forms":     "4",
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
            log.error(f"EFTS request failed at offset {offset}: {e}")
            break

        hits_obj = data.get("hits", {})

        if total_exp is None:
            t        = hits_obj.get("total", {})
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
    """
    Extract CIK and accession number from an EFTS hit.

    Real _id format:  "0001628280-26-036379:wk-form4_1779220980.xml"
    CIK is in:        _source.ciks[]   (list, company CIK is usually index 1)
    Accession is the part before the colon in _id.
    """
    hit_id = hit.get("_id", "")
    src    = hit.get("_source", {})

    # Accession: everything before the colon
    accession = hit_id.split(":")[0] if ":" in hit_id else None

    # Also available directly in _source as 'adsh'
    accession = src.get("adsh") or accession

    # CIK: _source.ciks is a list; company CIK is typically index 1,
    # reporter CIK is index 0. We need the company CIK to build the URL.
    ciks = src.get("ciks", [])
    # The company (issuer) is usually the last CIK in the list
    company_cik = ciks[-1].lstrip("0") if ciks else None

    return accession, company_cik, src


def parse_form4_xml(accession: str, company_cik: str, src: dict) -> list[dict]:
    """
    Download and parse a Form 4 XML document.
    Returns qualifying open-market purchase transactions.
    """
    if not accession or not company_cik:
        return []

    acc_nodash = accession.replace("-", "")
    xml_url    = (
        f"https://www.sec.gov/Archives/edgar/data/"
        f"{company_cik}/{acc_nodash}/{accession}.xml"
    )

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

    # Fallback to EFTS display_names if XML issuer fields are empty
    if not issuer_name:
        names = src.get("display_names", [])
        issuer_name = names[-1].split("  (CIK")[0] if names else ""

    results = []
    for txn in root.findall(".//nonDerivativeTransaction"):
        code = txt(txn, "transactionCoding/transactionCode")
        if code not in INCLUDE_TRANSACTION_CODES:
            continue

        shares_str = txt(txn, "transactionAmounts/transactionShares/value")
        price_str  = txt(txn, "transactionAmounts/transactionPricePerShare/value")
        date_str   = txt(txn, "transactionDate/value")

        try:
            shares = float(shares_str) if shares_str else 0.0
            price  = float(price_str)  if price_str  else 0.0
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
            "date":        date_str,
            "url": (
                f"https://www.sec.gov/Archives/edgar/data/"
                f"{company_cik}/{acc_nodash}/{accession}-index.htm"
            ),
        })

    return results


def run_scan():
    now = datetime.now(CET)

    # SEC doesn't process filings on weekends
    if now.weekday() >= 5:
        log.info(f"Weekend — skipping scan ({now.strftime('%A')})")
        return

    log.info("=== SEC Form 4 scan starting ===")

    end_date   = now.strftime("%Y-%m-%d")
    start_date = (now - timedelta(days=1)).strftime("%Y-%m-%d")
    log.info(f"Date range: {start_date} → {end_date}")

    # ── Step 1: Full paginated filing index ──────────────────────────────────
    raw_hits = fetch_all_filing_index(start_date, end_date)
    log.info(f"Found {len(raw_hits)} Form 4 filings to inspect")

    if not raw_hits:
        send_telegram(
            f"📋 <b>SEC Form 4 Scan</b>\n<i>{start_date}</i>\n\nNo filings found."
        )
        log.info("=== Scan complete ===")
        return

    # ── Step 2: Parse each XML for qualifying purchases ──────────────────────
    qualifying = []
    errors     = 0
    seen_accessions = set()  # deduplicate (EFTS returns one hit per file in a filing)

    for i, hit in enumerate(raw_hits):
        accession, company_cik, src = parse_filing_meta(hit)

        if not accession or accession in seen_accessions:
            continue
        seen_accessions.add(accession)

        # Only process actual Form 4 documents, skip EX-24 power-of-attorney attachments
        file_type = src.get("file_type", "")
        if file_type not in ("4", "") and not file_type.startswith("4"):
            continue

        txns = parse_form4_xml(accession, company_cik, src)
        if txns is None:
            errors += 1
        else:
            qualifying.extend(txns)

        if (i + 1) % 100 == 0:
            log.info(f"  Processed {i+1}/{len(raw_hits)}, qualifying so far: {len(qualifying)}")

        time.sleep(0.1)

    log.info(f"Qualifying purchases: {len(qualifying)} (parse errors: {errors})")

    # ── Step 3: Send Telegram ────────────────────────────────────────────────
    qualifying.sort(key=lambda x: x["value"], reverse=True)

    if not qualifying:
        msg = (
            f"📋 <b>SEC Form 4 Insider Scan</b>\n"
            f"<i>{start_date} → {end_date}</i>\n\n"
            f"No qualifying open-market purchases ≥ ${MIN_TRANSACTION_VALUE:,}\n"
            f"<i>Scanned {len(seen_accessions):,} unique filings</i>"
        )
        send_telegram(msg)
        log.info("=== Scan complete ===")
        return

    lines = [
        "🔍 <b>SEC Form 4 — Insider Purchases</b>",
        f"<i>{start_date} → {end_date}</i>",
        f"<i>{len(seen_accessions):,} filings scanned · {len(qualifying)} qualifying buys</i>",
        "",
    ]

    for t in qualifying[:15]:
        role    = "DIR" if t["is_director"] else ("OFF" if t["is_officer"] else "INS")
        company = t["company"][:28] if t["company"] else "Unknown"
        title   = f" · {t['title'][:22]}" if t["title"] else ""
        lines.append(
            f"<b>${t['ticker']}</b>  {company}\n"
            f"  👤 {t['name']} [{role}]{title}\n"
            f"  {t['shares']:,.0f} sh @ ${t['price']:.2f} = <b>${t['value']:,.0f}</b>  📅 {t['date']}"
        )

    if len(qualifying) > 15:
        lines.append(f"\n<i>… and {len(qualifying) - 15} more qualifying purchases</i>")

    send_telegram("\n".join(lines))
    log.info("=== Scan complete ===")


def main():
    log.info("SEC Form 4 Scanner starting up")
    send_telegram("✅ <b>SEC Form 4 Scanner</b> redeployed — parse fix applied")

    schedule.every().day.at("06:00").do(run_scan)
    log.info("Scheduled daily scan at 06:00 CET")

    log.info("Running initial scan now...")
    run_scan()

    while True:
        schedule.run_pending()
        time.sleep(30)


if __name__ == "__main__":
    main()

