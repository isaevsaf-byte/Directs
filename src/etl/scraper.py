import asyncio
from typing import List, Optional
from datetime import date
from playwright.async_api import async_playwright, Page, Response
from .models import MarketContract
from .reference_data import ReferenceDataLoader

MARKET_VIEW_URL = "https://norexeco.com/market-view"

class HybridScraper:
    def __init__(self, ref_loader: ReferenceDataLoader):
        self.ref_loader = ref_loader
        self.contracts: List[MarketContract] = []

    async def run(self) -> List[MarketContract]:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            # Strategy 1: API Interception
            # We listen for specific JSON responses that look like market data
            page.on("response", self.handle_response)

            try:
                await page.goto(MARKET_VIEW_URL, wait_until="networkidle")
                
                # If API interception didn't yield results, fallback to DOM
                if not self.contracts:
                    print("API Interception failed or empty. Falling back to DOM scraping.")
                    await self.scrape_dom(page)
                
            except Exception as e:
                print(f"Scraping failed: {e}")
            finally:
                await browser.close()
        
        return self.contracts

    async def handle_response(self, response: Response):
        """
        Intercepts network responses to find the market data JSON.
        """
        if "market-data" in response.url and response.status == 200:
             try:
                 data = await response.json()
                 self.parse_api_data(data)
             except Exception as e:
                 print(f"Failed to parse API response: {e}")

    def parse_api_data(self, data: dict):
        """
        Parses the raw API JSON into MarketContract objects.
        This needs to be adjusted based on actual API shape.
        """
        # Placeholder logic - assumes a list of items
        items = data.get("contracts", [])
        for item in items:
            ticker = item.get("ticker", "")
            price = item.get("last_price", 0)
            
            # Skip if invalid or suspended (BHKP case)
            if "BHKP" in ticker:
                continue
                
            # Determine Product Type
            prod_type = "NBSK" if "NBSK" in ticker else "BEK" if "BEK" in ticker else None
            if not prod_type:
                continue

            # Resolve Date
            contract_date = self.ref_loader.get_delivery_date(ticker)
            if not contract_date:
                # Log warning or attempt regex parsing?
                # For now, skip to be safe as per "Reference Data Anchor" rule
                continue
                
            contract = MarketContract(
                ticker=ticker,
                product_type=prod_type,
                contract_date=contract_date,
                period_type="Monthly", # Simplification, logic needed to detect Q/Y
                price=price
            )
            self.contracts.append(contract)

    async def scrape_dom(self, page: Page):
        """
        Fallback: Scrapes the visible table on the page.
        """
        # Selectors for the new 2026 table layout
        # This is hypothetical until we inspect the actual DOM, but following the prompt's fallback instruction.
        rows = await page.query_selector_all("table.market-data tr")
        
        for row in rows:
            # Extract text from cells
            cells = await row.query_selector_all("td")
            if len(cells) < 4: 
                continue
                
            ticker = await cells[0].inner_text()
            price_text = await cells[2].inner_text()
            
            try:
                price = float(price_text.replace(",", ""))
            except:
                continue

            # Reuse parsing logic...
            # (In production I'd refactor the parsing into a shared method)
            if "BHKP" in ticker: continue
            
            prod_type = "NBSK" if "NBSK" in ticker else "BEK" if "BEK" in ticker else None
            if not prod_type: continue
            
            contract_date = self.ref_loader.get_delivery_date(ticker)
            if not contract_date: continue

            self.contracts.append(MarketContract(
                ticker=ticker,
                product_type=prod_type,
                contract_date=contract_date,
                period_type="Monthly",
                price=price
            ))
