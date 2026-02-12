import asyncio
import json
import re
import logging
from typing import List, Optional, Tuple
from datetime import date
from calendar import monthrange

import httpx

from .models import MarketContract

logger = logging.getLogger(__name__)

MARKET_VIEW_URL = "https://norexeco.com/market-view"
MARKET_API_URL = "https://norexeco.com/api/market/public/initial"

# Month abbreviation → number mapping
MONTH_MAP = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4,
    "MAY": 5, "JUN": 6, "JUL": 7, "AUG": 8,
    "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}

# Quarter → (start_month, end_month) mapping
QUARTER_MAP = {
    1: (1, 3),
    2: (4, 6),
    3: (7, 9),
    4: (10, 12),
}

# Norexco product codes → internal product types
# BHKP (Bleached Hardwood Kraft Pulp) is the hardwood equivalent of BEK
PRODUCT_MAP = {
    "NBSK": "NBSK",
    "BHKP": "BEK",
}

# Products we want to scrape (skip NBSKCIF, NBSKSH, BHKPCH, OCC, etc.)
TARGET_PRODUCTS = set(PRODUCT_MAP.keys())


def parse_contract_date(display_name: str) -> Tuple[Optional[date], str]:
    """
    Parse contract delivery date and period type from display name.

    Formats:
      - Monthly:   "NBSK MAR26"  → last day of Mar 2026, "Monthly"
      - Quarterly: "NBSK Q226"   → last day of Q2 2026 (Jun 30), "Quarterly"
      - Calendar:  "NBSK CAL27"  → last day of 2027 (Dec 31), "Calendar"

    Returns (contract_date, period_type) or (None, "") on failure.
    """
    name = display_name.strip().upper()

    # Monthly: ends with 3-letter month + 2-digit year (e.g., "MAR26")
    m = re.search(r'([A-Z]{3})(\d{2})$', name)
    if m:
        month_str, year_str = m.group(1), m.group(2)
        month = MONTH_MAP.get(month_str)
        if month:
            year = 2000 + int(year_str)
            _, last_day = monthrange(year, month)
            return date(year, month, last_day), "Monthly"

    # Quarterly: Q + quarter_number + 2-digit year (e.g., "Q226" = Q2 2026)
    m = re.search(r'Q(\d)(\d{2})$', name)
    if m:
        quarter, year_str = int(m.group(1)), m.group(2)
        if 1 <= quarter <= 4:
            year = 2000 + int(year_str)
            _, end_month = QUARTER_MAP[quarter]
            _, last_day = monthrange(year, end_month)
            return date(year, end_month, last_day), "Quarterly"

    # Calendar year: CAL + 2-digit year (e.g., "CAL27" = full year 2027)
    m = re.search(r'CAL(\d{2})$', name)
    if m:
        year = 2000 + int(m.group(1))
        return date(year, 12, 31), "Calendar"

    return None, ""


def _parse_trading_items(items: list) -> List[MarketContract]:
    """Parse a list of trading data items (dicts) into MarketContract objects."""
    contracts = []

    for item in items:
        product_code = item.get("productCode", "")
        if product_code not in TARGET_PRODUCTS:
            continue

        settlement_price = item.get("settlementPrice")
        if not settlement_price or settlement_price <= 0:
            continue

        display_name = item.get("contractDisplayName", "")
        contract_date, period_type = parse_contract_date(display_name)
        if not contract_date:
            logger.debug(f"Could not parse date from: {display_name}")
            continue

        product_type = PRODUCT_MAP[product_code]

        try:
            contract = MarketContract(
                ticker=display_name.replace(" ", "-"),
                product_type=product_type,
                contract_date=contract_date,
                period_type=period_type,
                price=float(settlement_price),
            )
            contracts.append(contract)
        except Exception as e:
            logger.debug(f"Skipping {display_name}: {e}")

    # Deduplicate by (product_type, contract_date), keeping first occurrence
    seen = set()
    unique = []
    for c in contracts:
        key = (c.product_type, c.contract_date)
        if key not in seen:
            seen.add(key)
            unique.append(c)

    return sorted(unique, key=lambda c: (c.product_type, c.contract_date))


class HybridScraper:
    """
    Scrapes Norexco market-view page for NBSK and BEK futures contracts.

    Strategies (tried in order):
    1. Direct API call to /api/market/public/initial (fastest)
    2. HTTP fetch of page HTML + extract JSON from Next.js SSR payload
    3. Playwright-based page rendering + JS evaluation (slowest)
    """

    def __init__(self, ref_loader=None):
        # ref_loader kept for backward compatibility but no longer needed
        self.ref_loader = ref_loader

    async def run(self) -> List[MarketContract]:
        # Strategy 1: Direct API (fastest)
        try:
            contracts = await self._fetch_via_api()
            if contracts:
                logger.info(f"API scrape: {len(contracts)} contracts")
                return contracts
            logger.info("API returned 0 contracts (may be outside trading hours)")
        except Exception as e:
            logger.warning(f"API scrape failed: {e}")

        # Strategy 2: SSR HTML extraction
        try:
            contracts = await self._fetch_via_html()
            if contracts:
                logger.info(f"HTML scrape: {len(contracts)} contracts")
                return contracts
        except Exception as e:
            logger.warning(f"HTML scrape failed: {e}")

        # Strategy 3: Playwright fallback (opt-in, heavy on memory)
        import os
        if os.environ.get("ENABLE_PLAYWRIGHT", "").lower() in ("1", "true", "yes"):
            try:
                contracts = await self._fetch_via_playwright()
                if contracts:
                    logger.info(f"Playwright scrape: {len(contracts)} contracts")
                    return contracts
            except Exception as e:
                logger.warning(f"Playwright scrape failed: {e}")
        else:
            logger.info("Playwright scrape skipped (ENABLE_PLAYWRIGHT not set)")

        logger.warning("All scrape strategies returned 0 contracts")
        return []

    async def _fetch_via_api(self) -> List[MarketContract]:
        """Direct call to Norexco's public market data API."""
        async with httpx.AsyncClient(follow_redirects=True, timeout=30) as client:
            resp = await client.get(MARKET_API_URL, headers={"Cache-Control": "no-store"})
            resp.raise_for_status()
            data = resp.json()

        items = data.get("tradingData", [])
        return _parse_trading_items(items)

    async def _fetch_via_html(self) -> List[MarketContract]:
        """Fetch page HTML and extract tradingData from Next.js SSR payload."""
        async with httpx.AsyncClient(follow_redirects=True, timeout=30) as client:
            resp = await client.get(MARKET_VIEW_URL)
            resp.raise_for_status()
            html = resp.text

        return self._extract_contracts_from_html(html)

    async def _fetch_via_playwright(self) -> List[MarketContract]:
        """Fallback: use Playwright to render page and extract data."""
        from playwright.async_api import async_playwright

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            try:
                page = await browser.new_page()
                await page.goto(MARKET_VIEW_URL, wait_until="networkidle", timeout=30000)

                # Extract the full HTML after JS rendering
                html = await page.content()
                contracts = self._extract_contracts_from_html(html)

                if not contracts:
                    contracts = await self._extract_via_js(page)

                return contracts
            finally:
                await browser.close()

    async def _extract_via_js(self, page) -> List[MarketContract]:
        """Try extracting data by evaluating JS in the page context."""
        try:
            data = await page.evaluate("""
                () => {
                    const scripts = document.querySelectorAll('script');
                    for (const s of scripts) {
                        const text = s.textContent || '';
                        if (text.includes('tradingData') && text.includes('settlementPrice')) {
                            return text;
                        }
                    }
                    return null;
                }
            """)
            if data:
                return self._parse_text_for_contracts(data)
        except Exception as e:
            logger.warning(f"JS evaluation failed: {e}")
        return []

    def _extract_contracts_from_html(self, html: str) -> List[MarketContract]:
        """Extract tradingData JSON from Next.js server-rendered HTML."""
        # Next.js streaming format uses escaped JSON inside self.__next_f.push() calls.
        # Unescape the double-escaped quotes before regex parsing.
        unescaped = html.replace('\\"', '"')
        return self._parse_text_for_contracts(unescaped)

    def _parse_text_for_contracts(self, text: str) -> List[MarketContract]:
        """Parse contract data from text using regex to find JSON fragments."""
        contracts = []

        pattern = r'\{[^{}]*"contractDisplayName"\s*:\s*"([^"]+)"[^{}]*"productCode"\s*:\s*"([^"]+)"[^{}]*"settlementPrice"\s*:\s*(\d+(?:\.\d+)?)[^{}]*\}'
        matches = re.finditer(pattern, text)

        for m in matches:
            display_name = m.group(1)
            product_code = m.group(2)
            settlement_price = float(m.group(3))

            if product_code not in TARGET_PRODUCTS:
                continue

            if settlement_price <= 0:
                continue

            contract_date, period_type = parse_contract_date(display_name)
            if not contract_date:
                logger.debug(f"Could not parse date from: {display_name}")
                continue

            product_type = PRODUCT_MAP[product_code]

            try:
                contract = MarketContract(
                    ticker=display_name.replace(" ", "-"),
                    product_type=product_type,
                    contract_date=contract_date,
                    period_type=period_type,
                    price=settlement_price,
                )
                contracts.append(contract)
            except Exception as e:
                logger.debug(f"Skipping {display_name}: {e}")

        # Deduplicate
        seen = set()
        unique = []
        for c in contracts:
            key = (c.product_type, c.contract_date)
            if key not in seen:
                seen.add(key)
                unique.append(c)

        return sorted(unique, key=lambda c: (c.product_type, c.contract_date))
