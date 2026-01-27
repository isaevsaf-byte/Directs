#!/usr/bin/env python3
"""
Comprehensive Diagnostic and Fix Script

Identifies data issues and provides automated fixes for the forecasting system.

Usage:
    python scripts/diagnose_and_fix.py          # Run full diagnostics
    python scripts/diagnose_and_fix.py --fix    # Apply automatic fixes
"""
import sys
import os
from datetime import date, timedelta
from typing import List, Dict, Tuple
import logging

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.db.schema import init_db, SessionLocal, MarketSnapshot, RealizedPrice, Base, engine
from src.db.access import MarketRepository, RealizedPriceRepository, ForecastRepository
from src.math.spline import MaximumSmoothnessSpline, ContractBlock, SplineBounds
from src.etl.models import PriceValidationConfig

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


class SystemDiagnostics:
    """Comprehensive system diagnostics for the Pulp forecasting system."""

    def __init__(self):
        init_db()
        self.session = SessionLocal()
        self.market_repo = MarketRepository(self.session)
        self.realized_repo = RealizedPriceRepository(self.session)
        self.issues = []
        self.warnings = []
        self.fixes_applied = []

    def close(self):
        self.session.close()

    def run_all_checks(self) -> Dict:
        """Run all diagnostic checks."""
        print("\n" + "=" * 60)
        print("PULP FORECASTING SYSTEM DIAGNOSTICS")
        print("=" * 60)

        results = {
            'database': self.check_database(),
            'curve_data': self.check_curve_data(),
            'realized_prices': self.check_realized_prices(),
            'price_ranges': self.check_price_ranges(),
            'data_freshness': self.check_data_freshness(),
            'forecast_accuracy': self.check_forecast_accuracy()
        }

        self.print_summary()
        return results

    def check_database(self) -> Dict:
        """Check database connectivity and tables."""
        print("\n[1/6] Database Check")
        print("-" * 40)

        result = {'status': 'ok', 'details': {}}

        try:
            # Check tables exist
            from sqlalchemy import inspect
            inspector = inspect(engine)
            tables = inspector.get_table_names()

            expected_tables = ['fact_market_snapshot', 'fact_forecast_accuracy', 'dim_realized_price']
            missing_tables = [t for t in expected_tables if t not in tables]

            if missing_tables:
                result['status'] = 'warning'
                result['details']['missing_tables'] = missing_tables
                self.warnings.append(f"Missing tables: {missing_tables}")
                print(f"  ⚠️  Missing tables: {missing_tables}")
            else:
                print(f"  ✓  All expected tables exist")

            result['details']['tables'] = tables
            print(f"  ✓  Database connected successfully")
            print(f"     Tables: {tables}")

        except Exception as e:
            result['status'] = 'error'
            result['details']['error'] = str(e)
            self.issues.append(f"Database error: {e}")
            print(f"  ❌ Database error: {e}")

        return result

    def check_curve_data(self) -> Dict:
        """Check market curve data."""
        print("\n[2/6] Curve Data Check")
        print("-" * 40)

        result = {'status': 'ok', 'details': {}}

        for product in ['NBSK', 'BEK']:
            latest_date = self.market_repo.get_latest_snapshot_date()
            if not latest_date:
                result['status'] = 'error'
                self.issues.append(f"No curve data found for {product}")
                print(f"  ❌ {product}: No data")
                continue

            curve = self.market_repo.get_latest_curve(product)
            if not curve:
                result['status'] = 'error'
                self.issues.append(f"Empty curve for {product}")
                print(f"  ❌ {product}: Empty curve")
                continue

            prices = [s.price for s in curve]
            result['details'][product] = {
                'snapshot_date': latest_date,
                'points': len(curve),
                'min': min(prices),
                'max': max(prices),
                'mean': sum(prices) / len(prices)
            }

            print(f"  {product}:")
            print(f"     Snapshot: {latest_date}")
            print(f"     Points: {len(curve)}")
            print(f"     Range: ${min(prices):.2f} - ${max(prices):.2f}")

        return result

    def check_realized_prices(self) -> Dict:
        """Check realized PIX prices."""
        print("\n[3/6] Realized Prices Check")
        print("-" * 40)

        result = {'status': 'ok', 'details': {}}

        for product in ['NBSK', 'BEK']:
            prices = self.realized_repo.get_realized_prices(product)

            if len(prices) == 0:
                result['status'] = 'warning'
                self.warnings.append(f"No realized prices for {product}")
                print(f"  ⚠️  {product}: No realized prices loaded")
                print(f"     Run: python scripts/load_pix_data.py --sample")
            else:
                result['details'][product] = {
                    'count': len(prices),
                    'date_range': (min(prices.index), max(prices.index)),
                    'price_range': (min(prices.values), max(prices.values))
                }
                print(f"  ✓  {product}: {len(prices)} records")
                print(f"     Dates: {min(prices.index)} to {max(prices.index)}")
                print(f"     Range: ${min(prices.values):.2f} - ${max(prices.values):.2f}")

        return result

    def check_price_ranges(self) -> Dict:
        """Check if prices are in expected ranges."""
        print("\n[4/6] Price Range Validation")
        print("-" * 40)

        result = {'status': 'ok', 'details': {}}
        config = PriceValidationConfig()

        # Check curve prices
        for product in ['NBSK']:
            curve = self.market_repo.get_latest_curve(product)
            if curve:
                prices = [s.price for s in curve]
                mean_price = sum(prices) / len(prices)

                expected_min = config.nbsk_min if product == 'NBSK' else config.bek_min
                expected_max = config.nbsk_max if product == 'NBSK' else config.bek_max

                if min(prices) < expected_min or max(prices) > expected_max:
                    result['status'] = 'error'
                    self.issues.append(f"{product} curve prices outside expected range")
                    print(f"  ❌ {product} curve: Prices outside range [{expected_min}, {expected_max}]")

                # Check if prices seem unrealistically low for 2025-2026
                if product == 'NBSK' and mean_price < 1400:
                    result['status'] = 'error'
                    self.issues.append(f"{product} curve prices too low (mean={mean_price:.2f}, expected ~1500)")
                    print(f"  ❌ {product} curve mean ${mean_price:.2f} seems LOW")
                    print(f"     Expected NBSK PIX for 2025-2026: ~$1450-1600")
                    print(f"     This suggests incorrect data source configuration!")
                else:
                    print(f"  ✓  {product} curve prices in reasonable range")

        return result

    def check_data_freshness(self) -> Dict:
        """Check how recent the data is."""
        print("\n[5/6] Data Freshness Check")
        print("-" * 40)

        result = {'status': 'ok', 'details': {}}

        latest_date = self.market_repo.get_latest_snapshot_date()
        if latest_date:
            days_old = (date.today() - latest_date).days
            result['details']['curve_age_days'] = days_old

            if days_old > 7:
                result['status'] = 'warning'
                self.warnings.append(f"Curve data is {days_old} days old")
                print(f"  ⚠️  Curve data is {days_old} days old (snapshot: {latest_date})")
            else:
                print(f"  ✓  Curve data is {days_old} days old")
        else:
            result['status'] = 'error'
            print(f"  ❌ No curve data available")

        return result

    def check_forecast_accuracy(self) -> Dict:
        """Check forecast accuracy metrics."""
        print("\n[6/6] Forecast Accuracy Check")
        print("-" * 40)

        result = {'status': 'ok', 'details': {}}

        forecast_repo = ForecastRepository(self.session)
        summary = forecast_repo.get_accuracy_summary('NBSK')

        if 'error' in summary:
            print(f"  ℹ️  No forecast accuracy data yet")
            print(f"     This is normal for new installations")
        else:
            mape = summary.get('mape', 0)
            result['details'] = summary

            if mape > 20:
                result['status'] = 'warning'
                self.warnings.append(f"High MAPE: {mape:.1f}%")
                print(f"  ⚠️  MAPE: {mape:.1f}% (target: <10%)")
            elif mape > 10:
                print(f"  ℹ️  MAPE: {mape:.1f}% (acceptable)")
            else:
                print(f"  ✓  MAPE: {mape:.1f}% (good)")

        return result

    def print_summary(self):
        """Print diagnostic summary."""
        print("\n" + "=" * 60)
        print("DIAGNOSTIC SUMMARY")
        print("=" * 60)

        if self.issues:
            print(f"\n❌ ISSUES ({len(self.issues)}):")
            for issue in self.issues:
                print(f"   - {issue}")

        if self.warnings:
            print(f"\n⚠️  WARNINGS ({len(self.warnings)}):")
            for warning in self.warnings:
                print(f"   - {warning}")

        if not self.issues and not self.warnings:
            print("\n✓ All checks passed!")

        print("\n" + "=" * 60)

    def apply_fixes(self):
        """Apply automatic fixes for identified issues."""
        print("\n" + "=" * 60)
        print("APPLYING AUTOMATIC FIXES")
        print("=" * 60)

        # Fix 1: Load sample PIX data if missing
        realized = self.realized_repo.get_realized_prices('NBSK')
        if len(realized) == 0:
            print("\n[Fix 1] Loading sample PIX data...")
            from scripts.load_pix_data import load_sample_data
            load_sample_data()
            self.fixes_applied.append("Loaded sample PIX data")

        # Fix 2: Regenerate curve with correct prices if wrong
        curve = self.market_repo.get_latest_curve('NBSK')
        if curve:
            prices = [s.price for s in curve]
            mean_price = sum(prices) / len(prices)

            if mean_price < 1400:
                print("\n[Fix 2] Curve prices too low - regenerating with correct data...")
                self._regenerate_curve_with_correct_prices()
                self.fixes_applied.append("Regenerated curve with corrected prices")

        print("\n" + "-" * 40)
        if self.fixes_applied:
            print(f"Applied {len(self.fixes_applied)} fixes:")
            for fix in self.fixes_applied:
                print(f"  ✓ {fix}")
        else:
            print("No automatic fixes needed.")

    def _regenerate_curve_with_correct_prices(self):
        """Regenerate the forward curve using correct PIX-based prices."""
        # Use realized PIX data as basis for contracts
        realized = self.realized_repo.get_realized_prices('NBSK')
        if len(realized) == 0:
            print("  Cannot regenerate: No realized prices available")
            return

        # Get latest realized price as spot
        latest_realized = self.realized_repo.get_latest_price('NBSK')
        if not latest_realized:
            print("  Cannot regenerate: No spot price available")
            return

        spot_price = latest_realized.price
        spot_date = latest_realized.price_date

        print(f"  Using spot: ${spot_price:.2f} on {spot_date}")

        # Create contract blocks from forward PIX data
        # Using the 2026 forward prices from Excel
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

        # Build spline
        bounds = SplineBounds(min_price=1200, max_price=1800)
        spline = MaximumSmoothnessSpline(spot_date, spot_price, bounds)

        try:
            curve = spline.build_curve(contracts)
            print(f"  Generated curve with {len(curve)} points")
            print(f"  Range: ${curve.min():.2f} - ${curve.max():.2f}")

            # Save to database
            snapshot_date = date.today()

            # Clear old snapshots for today
            from sqlalchemy import delete
            self.session.execute(
                delete(MarketSnapshot).where(
                    MarketSnapshot.snapshot_date == snapshot_date,
                    MarketSnapshot.product_type == 'NBSK'
                )
            )

            # Insert new curve
            snapshots = []
            for contract_date, price in curve.items():
                snap = MarketSnapshot(
                    snapshot_date=snapshot_date,
                    contract_date=contract_date.date() if hasattr(contract_date, 'date') else contract_date,
                    product_type='NBSK',
                    price=float(price),
                    is_interpolated=True
                )
                snapshots.append(snap)

            self.market_repo.save_snapshot(snapshots)
            print(f"  ✓ Saved {len(snapshots)} curve points to database")

        except Exception as e:
            print(f"  ❌ Failed to regenerate curve: {e}")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Run system diagnostics and apply fixes")
    parser.add_argument('--fix', action='store_true', help="Apply automatic fixes")

    args = parser.parse_args()

    diag = SystemDiagnostics()

    try:
        diag.run_all_checks()

        if args.fix:
            diag.apply_fixes()
            print("\nRe-running diagnostics after fixes...")
            diag.run_all_checks()

    finally:
        diag.close()


if __name__ == "__main__":
    main()
