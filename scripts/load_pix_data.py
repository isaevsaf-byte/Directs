#!/usr/bin/env python3
"""
PIX Data Loader Script

Loads actual NBSK and BHKP PIX prices from your Excel data into the database
for backtesting and accuracy tracking.

Usage:
    python scripts/load_pix_data.py --csv path/to/pix_data.csv
    python scripts/load_pix_data.py --manual  # Enter data interactively
    python scripts/load_pix_data.py --sample  # Load sample data from your Excel screenshot

Based on your Excel screenshot, this script pre-populates with actual PIX values.
"""
import sys
import os
from datetime import date
from typing import List, Dict

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.db.schema import init_db, SessionLocal, RealizedPrice, ForecastAccuracy
from src.db.access import RealizedPriceRepository, ForecastRepository


# Actual PIX data from your Excel screenshot
# Format: (year, month): {'nbsk': price, 'bek': price}
ACTUAL_PIX_DATA = {
    # 2025 data from your Excel
    (2025, 1): {'nbsk': 1480.5, 'bek': 1000.0},
    (2025, 2): {'nbsk': 1493.7, 'bek': 1066.54},
    (2025, 3): {'nbsk': 1532.1, 'bek': 1142.4},
    (2025, 4): {'nbsk': 1573.8, 'bek': 1195.5},
    (2025, 5): {'nbsk': 1597.1, 'bek': 1193.5},
    (2025, 6): {'nbsk': 1572.7, 'bek': 1137.8},
    (2025, 7): {'nbsk': 1527.8, 'bek': 1079.7},
    (2025, 8): {'nbsk': 1499.97, 'bek': 1013.37},
    (2025, 9): {'nbsk': 1495.91, 'bek': 1000.0},
    (2025, 10): {'nbsk': 1496.89, 'bek': 1051.6},
    (2025, 11): {'nbsk': 1497.58, 'bek': 1075.19},  # Dec-25 column shows updated Nov
    (2025, 12): {'nbsk': 1498.20, 'bek': 1096.33},

    # 2026 data from your Excel
    (2026, 1): {'nbsk': 1545.0, 'bek': 1140.0},
    (2026, 2): {'nbsk': 1545.0, 'bek': 1140.0},
    (2026, 3): {'nbsk': 1545.0, 'bek': 1140.0},
    (2026, 4): {'nbsk': 1562.0, 'bek': 1180.0},
    (2026, 5): {'nbsk': 1562.0, 'bek': 1180.0},
    (2026, 6): {'nbsk': 1562.0, 'bek': 1180.0},
    (2026, 7): {'nbsk': 1571.0, 'bek': 1204.0},
    (2026, 8): {'nbsk': 1571.0, 'bek': 1204.0},
    (2026, 9): {'nbsk': 1571.0, 'bek': 1204.0},
    (2026, 10): {'nbsk': 1565.0, 'bek': 1230.0},
    (2026, 11): {'nbsk': 1565.0, 'bek': 1230.0},
}


def load_sample_data():
    """Load the PIX data from your Excel screenshot into the database."""
    init_db()
    session = SessionLocal()
    repo = RealizedPriceRepository(session)

    loaded_count = 0
    errors = []

    print("Loading actual PIX prices into database...")
    print("-" * 50)

    for (year, month), prices in sorted(ACTUAL_PIX_DATA.items()):
        price_date = date(year, month, 15)  # Mid-month as reference

        # Load NBSK
        try:
            repo.save_realized_price(
                price_date=price_date,
                product_type="NBSK",
                price=prices['nbsk'],
                source="Fastmarkets PIX (Excel Import)"
            )
            print(f"  {price_date}: NBSK ${prices['nbsk']:.2f}")
            loaded_count += 1
        except Exception as e:
            if "UNIQUE constraint" in str(e):
                print(f"  {price_date}: NBSK already exists, skipping")
                session.rollback()
            else:
                errors.append(f"NBSK {price_date}: {e}")
                session.rollback()

        # Load BEK (BHKP)
        try:
            repo.save_realized_price(
                price_date=price_date,
                product_type="BEK",
                price=prices['bek'],
                source="Fastmarkets PIX (Excel Import)"
            )
            print(f"  {price_date}: BEK  ${prices['bek']:.2f}")
            loaded_count += 1
        except Exception as e:
            if "UNIQUE constraint" in str(e):
                print(f"  {price_date}: BEK already exists, skipping")
                session.rollback()
            else:
                errors.append(f"BEK {price_date}: {e}")
                session.rollback()

    session.close()

    print("-" * 50)
    print(f"Loaded {loaded_count} price records")
    if errors:
        print(f"Errors: {len(errors)}")
        for e in errors:
            print(f"  - {e}")

    return loaded_count


def load_from_csv(filepath: str):
    """
    Load PIX data from a CSV file.

    Expected CSV format:
    date,product,price
    2025-01-15,NBSK,1480.5
    2025-01-15,BEK,1000.0
    ...
    """
    import pandas as pd

    init_db()
    session = SessionLocal()
    repo = RealizedPriceRepository(session)

    df = pd.read_csv(filepath)
    df['date'] = pd.to_datetime(df['date']).dt.date

    loaded_count = 0
    for _, row in df.iterrows():
        try:
            repo.save_realized_price(
                price_date=row['date'],
                product_type=row['product'].upper(),
                price=float(row['price']),
                source=f"CSV Import: {filepath}"
            )
            loaded_count += 1
        except Exception as e:
            if "UNIQUE constraint" not in str(e):
                print(f"Error loading {row}: {e}")
            session.rollback()

    session.close()
    print(f"Loaded {loaded_count} records from {filepath}")
    return loaded_count


def verify_data():
    """Verify loaded data and show summary."""
    init_db()
    session = SessionLocal()
    repo = RealizedPriceRepository(session)

    print("\n" + "=" * 50)
    print("VERIFICATION: Loaded PIX Data Summary")
    print("=" * 50)

    for product in ["NBSK", "BEK"]:
        prices = repo.get_realized_prices(product)
        if len(prices) > 0:
            print(f"\n{product}:")
            print(f"  Records: {len(prices)}")
            print(f"  Date Range: {min(prices.index)} to {max(prices.index)}")
            print(f"  Price Range: ${min(prices.values):.2f} - ${max(prices.values):.2f}")
            print(f"  Mean Price: ${prices.mean():.2f}")
        else:
            print(f"\n{product}: No data loaded")

    session.close()


def diagnose_system():
    """Run diagnostics to identify data issues."""
    init_db()
    session = SessionLocal()

    from src.db.access import MarketRepository

    market_repo = MarketRepository(session)
    realized_repo = RealizedPriceRepository(session)

    print("\n" + "=" * 50)
    print("SYSTEM DIAGNOSTICS")
    print("=" * 50)

    # Check curve data
    latest_date = market_repo.get_latest_snapshot_date()
    if latest_date:
        curve = market_repo.get_latest_curve("NBSK")
        if curve:
            prices = [s.price for s in curve]
            print(f"\nCurrent Curve Data (as of {latest_date}):")
            print(f"  Price Range: ${min(prices):.2f} - ${max(prices):.2f}")
            print(f"  Mean: ${sum(prices)/len(prices):.2f}")

            # Check if prices are in expected range
            if min(prices) < 800 or max(prices) > 2500:
                print("  ⚠️  WARNING: Prices outside expected NBSK range (800-2500)")
            elif min(prices) < 1400:
                print("  ⚠️  WARNING: Prices seem LOW for 2025-2026 NBSK (expected ~1500)")
            else:
                print("  ✓  Prices appear reasonable")
    else:
        print("\n⚠️  No curve data in database!")

    # Check realized prices
    nbsk_prices = realized_repo.get_realized_prices("NBSK")
    if len(nbsk_prices) > 0:
        print(f"\nRealized PIX Data:")
        print(f"  NBSK Records: {len(nbsk_prices)}")
        print(f"  NBSK Range: ${min(nbsk_prices.values):.2f} - ${max(nbsk_prices.values):.2f}")
    else:
        print("\n⚠️  No realized PIX data loaded. Run: python scripts/load_pix_data.py --sample")

    # Compare curve vs realized
    if latest_date and curve and len(nbsk_prices) > 0:
        print("\nCurve vs Realized Comparison:")
        curve_mean = sum(prices) / len(prices)
        realized_mean = nbsk_prices.mean()
        diff = curve_mean - realized_mean
        diff_pct = (diff / realized_mean) * 100

        print(f"  Curve Mean: ${curve_mean:.2f}")
        print(f"  Realized Mean: ${realized_mean:.2f}")
        print(f"  Difference: ${diff:.2f} ({diff_pct:+.1f}%)")

        if abs(diff_pct) > 10:
            print("  ⚠️  SIGNIFICANT DISCREPANCY - Check data sources!")
        else:
            print("  ✓  Within acceptable range")

    session.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Load PIX price data into the database")
    parser.add_argument('--sample', action='store_true', help="Load sample data from Excel screenshot")
    parser.add_argument('--csv', type=str, help="Path to CSV file with PIX data")
    parser.add_argument('--verify', action='store_true', help="Verify loaded data")
    parser.add_argument('--diagnose', action='store_true', help="Run system diagnostics")

    args = parser.parse_args()

    if args.csv:
        load_from_csv(args.csv)
    elif args.sample:
        load_sample_data()

    if args.verify or args.sample or args.csv:
        verify_data()

    if args.diagnose:
        diagnose_system()

    if not any([args.sample, args.csv, args.verify, args.diagnose]):
        print("Usage:")
        print("  python scripts/load_pix_data.py --sample    # Load data from Excel screenshot")
        print("  python scripts/load_pix_data.py --csv FILE  # Load from CSV file")
        print("  python scripts/load_pix_data.py --verify    # Verify loaded data")
        print("  python scripts/load_pix_data.py --diagnose  # Run diagnostics")
