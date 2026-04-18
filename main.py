import os
import re
import time
import logging
import requests
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import xml.etree.ElementTree as ET

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)
logger = logging.getLogger(__name__)

TELEGRAM_TOKEN   = os.environ['TELEGRAM_TOKEN']
TELEGRAM_CHAT_ID = os.environ['TELEGRAM_CHAT_ID']

HEADERS  = {'User-Agent': 'SEC-Form4-Scanner Ahwhat15 ahwhat15@gmail.com'}
EFTS_URL = 'https://efts.sec.gov/LATEST/search-index'
EDGAR    = 'https://www.sec.gov'
MIN_VAL  = 100_000
CET      = ZoneInfo('Europe/Paris')


# ── Telegram ──────────────────────────────────────────────────────────────────

def _send(text: str) -> None:
    url = f'https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage'
    r = requests.post(url, json={
        'chat_id': TELEGRAM_CHAT_ID,
        'text': text,
        'parse_mode': 'HTML',
        'disable_web_page_preview': True,
    }, timeout=30)
    r.raise_for_status()


def send_telegram(text: str) -> None:
    """Send, splitting into ≤4000-char chunks if needed."""
    max_len = 4000
    if len(text) <= max_len:
        _send(text)
        logger.info('Telegram message sent (%d chars)', len(text))
        return

    lines, chunk = text.split('\n'), ''
    for line in lines:
        if len(chunk) + len(line) + 1 > max_len:
            _send(chunk.strip())
            time.sleep(1)
            chunk = ''
        chunk += line + '\n'
    if chunk.strip():
        _send(chunk.strip())
    logger.info('Telegram message sent in multiple chunks')


# ── EDGAR data fetch ───────────────────────────────────────────────────────────

def get_filings(start_date: str, end_date: str) -> list[dict]:
    """Return deduplicated Form 4 filing stubs from EFTS."""
    filings: list[dict] = []
    seen_acc: set[str]  = set()
    from_offset = 0
    page_size   = 40

    while True:
        params = {
            'q': '', 'forms': '4', 'dateRange': 'custom',
            'startdt': start_date, 'enddt': end_date,
            'from': from_offset,
        }
        try:
            r = requests.get(EFTS_URL, params=params, headers=HEADERS, timeout=30)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            logger.error('EFTS request failed: %s', e)
            break

        hits = data.get('hits', {}).get('hits', [])
        if not hits:
            break

        for hit in hits:
            src = hit.get('_source', {})
            acc = src.get('accession_no', '')
            if acc and acc not in seen_acc:
                seen_acc.add(acc)
                filings.append({
                    'id':           hit.get('_id', ''),
                    'accession_no': acc,
                    'entity_name':  src.get('entity_name', ''),
                })

        total = data.get('hits', {}).get('total', {}).get('value', 0)
        from_offset += page_size
        if from_offset >= min(total, 400):
            break
        time.sleep(0.12)

    logger.info('Found %d unique Form 4 filings', len(filings))
    return filings


def fetch_xml(filing_id: str) -> str | None:
    """Fetch the Form 4 XML document using the EFTS _id path."""
    # Direct XML hit
    if filing_id.lower().endswith('.xml'):
        try:
            r = requests.get(EDGAR + filing_id, headers=HEADERS, timeout=20)
            if r.ok:
                return r.text
        except Exception:
            pass

    # Derive directory and scan for XML
    dir_path = filing_id.rsplit('/', 1)[0] if '/' in filing_id else filing_id
    dir_url  = f'{EDGAR}{dir_path}/'
    try:
        r = requests.get(dir_url, headers=HEADERS, timeout=20)
        r.raise_for_status()
    except Exception as e:
        logger.debug('Dir fetch failed %s: %s', dir_url, e)
        return None

    links = re.findall(r'href="([^"]+\.xml)"', r.text, re.IGNORECASE)
    if not links:
        return None

    preferred = [l for l in links if any(k in l.lower() for k in ['form4', 'form-4', 'ownership', 'doc'])]
    target    = preferred[0] if preferred else links[0]
    xml_url   = (EDGAR + target) if target.startswith('/') else (dir_url + target)

    try:
        r = requests.get(xml_url, headers=HEADERS, timeout=20)
        r.raise_for_status()
        return r.text
    except Exception as e:
        logger.debug('XML fetch failed %s: %s', xml_url, e)
        return None


# ── Parsing ────────────────────────────────────────────────────────────────────

def parse_form4(xml_text: str) -> list[dict]:
    """Extract qualifying purchase (P) transactions from a Form 4 XML."""
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return []

    # Strip namespaces so paths work uniformly
    for el in root.iter():
        if '}' in el.tag:
            el.tag = el.tag.split('}', 1)[1]

    def txt(node, path: str, default: str = '') -> str:
        el = node.find(path)
        return (el.text or '').strip() if el is not None else default

    issuer   = txt(root, 'issuer/issuerName')
    ticker   = (txt(root, 'issuer/issuerTradingSymbol') or '?').upper()
    owner    = txt(root, 'reportingOwner/reportingOwnerId/rptOwnerName')
    is_off   = txt(root, 'reportingOwner/reportingOwnerRelationship/isOfficer') == '1'
    off_ttl  = txt(root, 'reportingOwner/reportingOwnerRelationship/officerTitle')
    is_dir   = txt(root, 'reportingOwner/reportingOwnerRelationship/isDirector') == '1'
    title    = off_ttl if (is_off and off_ttl) else ('Director' if is_dir else 'Insider')

    results: list[dict] = []
    for txn in root.findall('.//nonDerivativeTransaction'):
        if txt(txn, 'transactionCoding/transactionCode') != 'P':
            continue
        if txt(txn, 'transactionAmounts/transactionAcquiredDisposedCode') != 'A':
            continue
        try:
            shares = float(txt(txn, 'transactionAmounts/transactionShares', '0').replace(',', ''))
            price  = float(txt(txn, 'transactionAmounts/transactionPricePerShare', '0').replace(',', ''))
        except ValueError:
            continue

        value = shares * price
        if value < MIN_VAL:
            continue

        try:
            after  = float(txt(txn, 'postTransactionAmounts/sharesOwnedFollowingTransaction', '0').replace(',', ''))
            is_new = (after - shares) <= 0
        except ValueError:
            is_new = False

        results.append({
            'ticker':  ticker,
            'company': issuer,
            'owner':   owner,
            'title':   title,
            'shares':  shares,
            'price':   price,
            'value':   value,
            'is_new':  is_new,
        })

    return results


# ── Formatting ─────────────────────────────────────────────────────────────────

def format_message(purchases: list[dict], date_str: str) -> str:
    lines = [
        f'<b>🔍 SEC Form 4 Insider Purchases — {date_str}</b>',
        f'<i>{len(purchases)} qualifying purchase(s) above $100K — ranked by value</i>\n',
    ]
    for i, p in enumerate(purchases, 1):
        pos = '🆕 New position' if p['is_new'] else '➕ Added to position'
        lines.append(
            f'<b>{i}. ${p["ticker"]}</b> — {p["company"]}\n'
            f'   👤 {p["owner"]} | {p["title"]}\n'
            f'   💰 ${p["value"]:,.0f}  ({p["shares"]:,.0f} sh @ ${p["price"]:.2f})\n'
            f'   {pos}\n'
        )
    return '\n'.join(lines)


# ── Orchestration ──────────────────────────────────────────────────────────────

def run_scanner() -> None:
    logger.info('=== Form 4 scanner starting ===')
    now   = datetime.now(CET)
    end   = now.strftime('%Y-%m-%d')
    start = (now - timedelta(days=1)).strftime('%Y-%m-%d')

    filings   = get_filings(start, end)
    purchases: list[dict] = []

    for idx, filing in enumerate(filings):
        if not filing['id']:
            continue
        xml = fetch_xml(filing['id'])
        if xml:
            purchases.extend(parse_form4(xml))
        if idx % 25 == 0 and idx:
            logger.info('Progress: %d/%d filings, %d purchases', idx, len(filings), len(purchases))
        time.sleep(0.10)

    # Deduplicate by (ticker, owner, rounded value)
    seen: set = set()
    unique:    list[dict] = []
    for p in purchases:
        key = (p['ticker'], p['owner'], round(p['value'], -2))
        if key not in seen:
            seen.add(key)
            unique.append(p)

    unique.sort(key=lambda x: x['value'], reverse=True)
    logger.info('Qualifying purchases: %d', len(unique))

    if not unique:
        send_telegram(
            '📋 <b>SEC Form 4 Insider Scanner</b>\n'
            f'No qualifying insider purchases (> $100K) found for {end}.'
        )
    else:
        send_telegram(format_message(unique[:20], end))

    logger.info('=== Form 4 scanner done ===')


def next_run_seconds() -> float:
    """Seconds until next 06:00 CET, accounting for DST."""
    now    = datetime.now(CET)
    target = now.replace(hour=6, minute=0, second=0, microsecond=0)
    if now >= target:
        target += timedelta(days=1)
    return (target - now).total_seconds()


if __name__ == '__main__':
    while True:
        try:
            run_scanner()
        except Exception as exc:
            logger.exception('Unhandled scanner error: %s', exc)

        secs = next_run_seconds()
        logger.info('Next run in %.1f hours (06:00 CET)', secs / 3600)
        time.sleep(secs)
