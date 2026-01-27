import pandas as pd
from typing import Dict, Optional
from datetime import date, datetime
from io import StringIO
import requests
from .models import ReferenceData

REFERENCE_DATA_URL = "https://norexeco.com/reference-data-new"

class ReferenceDataLoader:
    def __init__(self, local_path: Optional[str] = None):
        self.local_path = local_path
        self.ticker_map: Dict[str, date] = {}

    def fetch(self):
        """
        Fetches the reference data from URL or local path and builds the lookup map.
        """
        if self.local_path:
            df = pd.read_csv(self.local_path)
        else:
            # In a real scenario, we'd handle request errors/retries here
            response = requests.get(REFERENCE_DATA_URL)
            response.raise_for_status()
            df = pd.read_csv(StringIO(response.text))

        self._process_dataframe(df)

    def _process_dataframe(self, df: pd.DataFrame):
        """
        Parses the raw dataframe into the ticker_map.
        Expected columns: 'Ticker Code', 'Product Name', 'Delivery Date', etc.
        """
        # clean column names just in case
        df.columns = [c.strip() for c in df.columns]

        for _, row in df.iterrows():
            ticker = str(row.get('Ticker', '')).strip()
            delivery_date_raw = str(row.get('Last Trading Day', ''))
            
            # Skip empty rows or invalid tickers
            if not ticker or not delivery_date_raw:
                continue
                
            try:
                # Assuming date format YYYY-MM-DD or similar standard
                dt = pd.to_datetime(delivery_date_raw).date()
                self.ticker_map[ticker] = dt
            except Exception as e:
                print(f"Warning: Could not parse date for ticker {ticker}: {e}")

    def get_delivery_date(self, ticker: str) -> Optional[date]:
        """
        Returns the parsed delivery date for a given ticker.
        """
        return self.ticker_map.get(ticker)
