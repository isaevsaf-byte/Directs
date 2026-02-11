from datetime import date
from typing import Literal, Optional
from pydantic import BaseModel, Field, field_validator, ValidationInfo
import re

class MarketContract(BaseModel):
    """
    Represents a single futures contract from Norexeco.
    Strictly enforces 2026 protocol: NBSK (DAP Europe) and BEK (Europe).
    """
    ticker: str = Field(..., description="The raw ticker code, e.g., 'NBSK-26Z' or 'BEK-26Q1'")
    product_type: Literal["NBSK", "BEK"] = Field(..., description="Product type derived from ticker")
    contract_date: date = Field(..., description="The delivery/settlement date of the contract")
    period_type: Literal["Monthly", "Quarterly", "Calendar"] = Field(..., description="Duration type of the contract")
    price: float = Field(..., gt=0, description="Settlement price or Last Traded Price")
    currency: str = Field("USD", description="Currency of the contract")
    
    @field_validator('product_type', mode='before')
    @classmethod
    def validate_product_type(cls, v: str, info: ValidationInfo) -> str:
        # If product_type is manually passed, validation happens here. 
        # But usually we derive it. If 'v' is passed, check it.
        if v not in ("NBSK", "BEK"):
             raise ValueError(f"Invalid product type: {v}. Must be NBSK or BEK.")
        return v

    @field_validator('ticker')
    @classmethod
    def validate_ticker(cls, v: str) -> str:
        v = v.upper().strip()

        # Accept NBSK, BEK, BHKP (hardwood), and EUCALYPTUS tickers
        if not re.search(r'(NBSK|BEK|BHKP|EUCALYPTUS)', v):
             raise ValueError(f"Unknown Contract Type in ticker: {v}")

        return v

class ReferenceData(BaseModel):
    """
    Model for rows from the Norexeco Reference Data CSV.
    Used to anchor tickers to specific dates.
    """
    ticker_code: str
    product_name: str
    settlement_type: str  # Should contain "Financial"
    delivery_date: date   # The crucial mapping anchor

class SpotPrice(BaseModel):
    """
    The T=0 fastmarkets spot price anchor.
    """
    date: date
    nbsk_price: float
    bek_price: float

    @field_validator('nbsk_price')
    @classmethod
    def validate_nbsk_range(cls, v: float) -> float:
        """NBSK PIX typically ranges 1000-2000 USD/tonne in 2024-2026"""
        if not (800 < v < 2500):
            raise ValueError(
                f"NBSK price {v} outside expected range (800-2500). "
                "Check data source configuration."
            )
        return v

    @field_validator('bek_price')
    @classmethod
    def validate_bek_range(cls, v: float) -> float:
        """BEK PIX typically ranges 800-1500 USD/tonne"""
        if not (500 < v < 2000):
            raise ValueError(
                f"BEK price {v} outside expected range (500-2000). "
                "Check data source configuration."
            )
        return v


class PriceValidationConfig(BaseModel):
    """Configuration for price sanity checks"""
    nbsk_min: float = 800.0
    nbsk_max: float = 2500.0
    bek_min: float = 500.0
    bek_max: float = 2000.0
    max_daily_change_pct: float = 5.0  # Max 5% daily move considered realistic

    def validate_price(self, price: float, product_type: str) -> bool:
        if product_type == "NBSK":
            return self.nbsk_min <= price <= self.nbsk_max
        elif product_type == "BEK":
            return self.bek_min <= price <= self.bek_max
        return False
