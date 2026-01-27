import sys
import os
from datetime import date, timedelta
import pandas as pd
import numpy as np

# ensure src is in path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__))))

from src.etl.models import MarketContract, ReferenceData
from src.math.spline import MaximumSmoothnessSpline, ContractBlock
from src.db.schema import init_db, SessionLocal, MarketSnapshot, engine
from src.db.access import MarketRepository
from sqlalchemy.orm import Session

def test_models():
    print("Testing Pydantic Models...")
    # Valid Case
    try:
        c = MarketContract(
            ticker="NBSK-26Z",
            product_type="NBSK",
            contract_date=date(2026, 12, 1),
            period_type="Monthly",
            price=1200.50
        )
        print("  [PASS] Valid Contract")
    except Exception as e:
        print(f"  [FAIL] Valid Contract: {e}")

    # Legacy BHKP Case (Should Fail)
    try:
        MarketContract(
            ticker="BHKP-OLD",
            product_type="BEK", # Passing BEK but ticker has BHKP
            contract_date=date(2026, 1, 1),
            period_type="Monthly",
            price=800
        )
        print("  [FAIL] BHKP Check (Expected failure)")
    except ValueError as e:
        if "Legacy Ticker Detected" in str(e):
             print("  [PASS] BHKP Legacy Check")
        else:
             print(f"  [FAIL] Wrong Error message: {e}")

def test_spline():
    print("\nTesting Spline Engine...")
    spot_date = date(2026, 1, 1)
    spot_price = 1000.0
    
    # 3 Monthly Blocks rising in price
    blocks = [
        ContractBlock(start_date=date(2026, 1, 1), end_date=date(2026, 1, 31), price=1000),
        ContractBlock(start_date=date(2026, 2, 1), end_date=date(2026, 2, 28), price=1050),
        ContractBlock(start_date=date(2026, 3, 1), end_date=date(2026, 3, 31), price=1100),
    ]
    
    spliner = MaximumSmoothnessSpline(spot_date, spot_price)
    curve = spliner.build_curve(blocks)
    
    if len(curve) > 0:
        print(f"  [PASS] Curve generated with {len(curve)} days")
        print(f"  Start Value: {curve.iloc[0]:.2f} (Expected {spot_price})")
        print(f"  End Value: {curve.iloc[-1]:.2f}")
        
        # Check arbitage integration for Month 1
        m1_dates = pd.date_range('2026-01-01', '2026-01-31')
        m1_avg = curve[m1_dates].mean()
        print(f"  Month 1 Average: {m1_avg:.2f} (Target 1000.0)")
        
        if abs(m1_avg - 1000) < 0.1:
            print("  [PASS] Arbitrage Constraint Holds")
        else:
            print("  [FAIL] Arbitrage Constraint Violation")
    else:
        print("  [FAIL] Curve generation failed")

def test_db():
    print("\nTesting Database...")
    # Clean DB
    MarketSnapshot.__table__.drop(engine)
    init_db()
    
    session = SessionLocal()
    repo = MarketRepository(session)
    
    snap = MarketSnapshot(
        snapshot_date=date.today(),
        contract_date=date(2026, 5, 1),
        product_type="NBSK",
        price=1050.25,
        is_interpolated=True
    )
    repo.save_snapshot([snap])
    
    retrieved = repo.get_latest_curve("NBSK")
    if len(retrieved) == 1 and retrieved[0].price == 1050.25:
         print("  [PASS] DB Roundtrip")
    else:
         print(f"  [FAIL] DB Roundtrip. Got {retrieved}")
    session.close()

if __name__ == "__main__":
    test_models()
    test_spline()
    test_db()
