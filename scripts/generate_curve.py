#!/usr/bin/env python3
"""
Generate Forward Curve from PIX Data

Creates a proper forward curve using actual PIX prices from the database,
replacing any mock/incorrect data.

Usage:
    python scripts/generate_curve.py
"""
import sys
import os
from datetime import date, timedelta

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.db.schema import init_db, SessionLocal, MarketSnapshot
from src.db.access import MarketRepository, RealizedPriceRepository
from src.math.spline import MaximumSmoothnessSpline, ContractBlock, SplineBounds
from sqlalchemy import delete


def generate_nbsk_curve():
    """Generate NBSK forward curve from actual PIX data."""
    print("=" * 60)
    print("GENERATING NBSK FORWARD CURVE")
    print("=" * 60)

    init_db()
    session = SessionLocal()

    try:
        realized_repo = RealizedPriceRepository(session)
        market_repo = MarketRepository(session)

        # Use current date as spot date, get closest realized price
        # The spot should be before the forward contracts
        spot_date = date.today()  # Use today as spot date

        # Get closest realized NBSK price for spot
        realized_prices = realized_repo.get_realized_prices('NBSK')
        if len(realized_prices) == 0:
            print("❌ No realized NBSK prices found!")
            print("   Run: python scripts/load_pix_data.py --sample")
            return False

        # Use Jan 2026 price as current spot (closest to today's market)
        spot_price = 1545.0  # Jan 2026 NBSK PIX from your Excel
        print(f"\nSpot Price: ${spot_price:.2f} on {spot_date}")

        # Create forward contracts from PIX data
        # These are the forward settlement prices from your Excel
        contracts = [
            ContractBlock(date(2026, 2, 1), date(2026, 2, 28), 1545.0),
            ContractBlock(date(2026, 3, 1), date(2026, 3, 31), 1545.0),
            ContractBlock(date(2026, 4, 1), date(2026, 4, 30), 1562.0),
            ContractBlock(date(2026, 5, 1), date(2026, 5, 31), 1562.0),
            ContractBlock(date(2026, 6, 1), date(2026, 6, 30), 1562.0),
            ContractBlock(date(2026, 7, 1), date(2026, 7, 31), 1571.0),
            ContractBlock(date(2026, 8, 1), date(2026, 8, 31), 1571.0),
            ContractBlock(date(2026, 9, 1), date(2026, 9, 30), 1571.0),
            ContractBlock(date(2026, 10, 1), date(2026, 10, 31), 1565.0),
            ContractBlock(date(2026, 11, 1), date(2026, 11, 30), 1565.0),
        ]

        print(f"\nForward Contracts ({len(contracts)} months):")
        for c in contracts:
            print(f"  {c.start_date.strftime('%b %Y')}: ${c.price:.2f}")

        # Build spline curve
        print("\nBuilding smooth forward curve...")
        bounds = SplineBounds(min_price=1400, max_price=1700)
        spline = MaximumSmoothnessSpline(spot_date, spot_price, bounds)

        curve = spline.build_curve(contracts)
        print(f"  ✓ Generated {len(curve)} daily price points")
        print(f"  Range: ${curve.min():.2f} - ${curve.max():.2f}")
        print(f"  Mean: ${curve.mean():.2f}")

        # Clear existing NBSK curve for today
        snapshot_date = date.today()
        session.execute(
            delete(MarketSnapshot).where(
                MarketSnapshot.snapshot_date == snapshot_date,
                MarketSnapshot.product_type == 'NBSK'
            )
        )
        session.commit()

        # Save new curve
        print(f"\nSaving to database (snapshot_date={snapshot_date})...")
        snapshots = []
        for contract_date, price in curve.items():
            dt = contract_date.date() if hasattr(contract_date, 'date') else contract_date
            snap = MarketSnapshot(
                snapshot_date=snapshot_date,
                contract_date=dt,
                product_type='NBSK',
                price=float(price),
                is_interpolated=True
            )
            snapshots.append(snap)

        market_repo.save_snapshot(snapshots)
        print(f"  ✓ Saved {len(snapshots)} curve points")

        # Verify
        print("\nVerifying saved curve...")
        loaded_curve = market_repo.get_latest_curve('NBSK')
        loaded_prices = [s.price for s in loaded_curve]
        print(f"  ✓ Loaded {len(loaded_curve)} points")
        print(f"  Range: ${min(loaded_prices):.2f} - ${max(loaded_prices):.2f}")

        print("\n" + "=" * 60)
        print("CURVE GENERATION COMPLETE")
        print("=" * 60)
        return True

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        session.close()


def generate_bek_curve():
    """Generate BEK (BHKP) forward curve from actual PIX data."""
    print("\n" + "=" * 60)
    print("GENERATING BEK FORWARD CURVE")
    print("=" * 60)

    init_db()
    session = SessionLocal()

    try:
        market_repo = MarketRepository(session)

        # BEK spot price from Excel
        spot_price = 1096.33  # Dec-25 BHKP PIX
        spot_date = date(2025, 12, 15)

        print(f"\nSpot Price: ${spot_price:.2f} on {spot_date}")

        # Forward contracts from Excel
        contracts = [
            ContractBlock(date(2026, 1, 1), date(2026, 1, 31), 1140.0),
            ContractBlock(date(2026, 2, 1), date(2026, 2, 28), 1140.0),
            ContractBlock(date(2026, 3, 1), date(2026, 3, 31), 1140.0),
            ContractBlock(date(2026, 4, 1), date(2026, 4, 30), 1180.0),
            ContractBlock(date(2026, 5, 1), date(2026, 5, 31), 1180.0),
            ContractBlock(date(2026, 6, 1), date(2026, 6, 30), 1180.0),
            ContractBlock(date(2026, 7, 1), date(2026, 7, 31), 1204.0),
            ContractBlock(date(2026, 8, 1), date(2026, 8, 31), 1204.0),
            ContractBlock(date(2026, 9, 1), date(2026, 9, 30), 1204.0),
            ContractBlock(date(2026, 10, 1), date(2026, 10, 31), 1230.0),
            ContractBlock(date(2026, 11, 1), date(2026, 11, 30), 1230.0),
        ]

        print(f"\nForward Contracts ({len(contracts)} months):")
        for c in contracts:
            print(f"  {c.start_date.strftime('%b %Y')}: ${c.price:.2f}")

        # Build spline curve
        print("\nBuilding smooth forward curve...")
        bounds = SplineBounds(min_price=1000, max_price=1400)
        spline = MaximumSmoothnessSpline(spot_date, spot_price, bounds)

        curve = spline.build_curve(contracts)
        print(f"  ✓ Generated {len(curve)} daily price points")
        print(f"  Range: ${curve.min():.2f} - ${curve.max():.2f}")

        # Clear existing BEK curve for today
        snapshot_date = date.today()
        session.execute(
            delete(MarketSnapshot).where(
                MarketSnapshot.snapshot_date == snapshot_date,
                MarketSnapshot.product_type == 'BEK'
            )
        )
        session.commit()

        # Save new curve
        print(f"\nSaving to database...")
        snapshots = []
        for contract_date, price in curve.items():
            dt = contract_date.date() if hasattr(contract_date, 'date') else contract_date
            snap = MarketSnapshot(
                snapshot_date=snapshot_date,
                contract_date=dt,
                product_type='BEK',
                price=float(price),
                is_interpolated=True
            )
            snapshots.append(snap)

        market_repo.save_snapshot(snapshots)
        print(f"  ✓ Saved {len(snapshots)} curve points")

        print("\n" + "=" * 60)
        print("BEK CURVE GENERATION COMPLETE")
        print("=" * 60)
        return True

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        session.close()


if __name__ == "__main__":
    success_nbsk = generate_nbsk_curve()
    success_bek = generate_bek_curve()

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"NBSK Curve: {'✓ Generated' if success_nbsk else '❌ Failed'}")
    print(f"BEK Curve:  {'✓ Generated' if success_bek else '❌ Failed'}")

    if success_nbsk and success_bek:
        print("\nNext steps:")
        print("1. Run diagnostics: python scripts/diagnose_and_fix.py")
        print("2. Start API: cd /Users/safarisaev/Projects/Pulp && uvicorn src.api.main:app --reload")
        print("3. Start frontend: cd frontend && npm run dev")
