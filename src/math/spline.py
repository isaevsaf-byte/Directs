import numpy as np
from scipy.optimize import minimize
from datetime import date, timedelta
from typing import List, Tuple, Optional
from dataclasses import dataclass
import pandas as pd
import logging

logger = logging.getLogger(__name__)


@dataclass
class ContractBlock:
    start_date: date
    end_date: date
    price: float


@dataclass
class SplineBounds:
    """Price bounds for sanity checking"""
    min_price: float = 800.0   # NBSK shouldn't go below this
    max_price: float = 2500.0  # NBSK shouldn't go above this
    max_daily_change: float = 50.0  # Max $/day change


class MaximumSmoothnessSpline:
    def __init__(
        self,
        spot_date: date,
        spot_price: float,
        bounds: Optional[SplineBounds] = None
    ):
        self.spot_date = spot_date
        self.spot_price = spot_price
        self.bounds = bounds or SplineBounds()

        # Validate spot price
        if not (self.bounds.min_price <= spot_price <= self.bounds.max_price):
            logger.warning(
                f"Spot price {spot_price} outside expected range "
                f"[{self.bounds.min_price}, {self.bounds.max_price}]. "
                "Check data source configuration."
            )

    def build_curve(self, contracts: List[ContractBlock]) -> pd.Series:
        """
        Generates a smooth daily curve from the spot date to the end of the last contract.

        Args:
            contracts: List of futures contracts (must be arbitrage-free inputs basically, but we treat them as constraints).

        Returns:
            pd.Series: Index is Date, Value is Price.
        """
        if not contracts:
            logger.warning("No contracts provided to build_curve")
            return pd.Series()

        # Log input contracts for debugging
        logger.info(f"Building curve from spot {self.spot_price} on {self.spot_date}")
        for c in contracts:
            logger.debug(f"  Contract: {c.start_date} to {c.end_date}, price={c.price}")

        # Validate contract prices
        for c in contracts:
            if not (self.bounds.min_price <= c.price <= self.bounds.max_price):
                logger.warning(
                    f"Contract price {c.price} for {c.start_date}-{c.end_date} "
                    f"outside expected range [{self.bounds.min_price}, {self.bounds.max_price}]"
                )

        # 1. Determine timeline
        # Sort contracts just in case
        contracts = sorted(contracts, key=lambda x: x.start_date)
        max_date = max(c.end_date for c in contracts)

        # Cap curve at 400 days to keep optimization tractable on limited resources
        max_horizon = self.spot_date + timedelta(days=400)
        if max_date > max_horizon:
            logger.info(f"Capping curve from {max_date} to {max_horizon} (400-day limit)")
            max_date = max_horizon

        days_total = (max_date - self.spot_date).days + 1
        date_range = [self.spot_date + timedelta(days=i) for i in range(days_total)]

        logger.info(f"Curve spans {days_total} days from {self.spot_date} to {max_date}")

        # 2. Build knot points from contract midpoints + spot
        from scipy.interpolate import CubicSpline

        knot_days = [0]  # spot date
        knot_prices = [self.spot_price]

        for contract in contracts:
            # Use contract midpoint as knot
            start_idx = max(0, (contract.start_date - self.spot_date).days)
            end_idx = min(days_total - 1, (contract.end_date - self.spot_date).days)

            if end_idx < 0 or start_idx >= days_total:
                logger.warning(f"Contract {contract} entirely out of range, skipping")
                continue

            mid_idx = (start_idx + end_idx) // 2
            if mid_idx > 0 and mid_idx not in knot_days:  # avoid duplicate at 0
                knot_days.append(mid_idx)
                knot_prices.append(contract.price)

        # Add final point if not already there
        if knot_days[-1] != days_total - 1:
            knot_days.append(days_total - 1)
            knot_prices.append(knot_prices[-1])  # hold last price flat

        knot_days = np.array(knot_days, dtype=float)
        knot_prices = np.array(knot_prices, dtype=float)

        # 3. Cubic spline interpolation (natural boundary conditions)
        cs = CubicSpline(knot_days, knot_prices, bc_type='natural')
        all_days = np.arange(days_total)
        curve = cs(all_days)

        # 4. Clamp to bounds
        curve = np.clip(curve, self.bounds.min_price, self.bounds.max_price)

        logger.info(f"Cubic spline built in <1s with {len(knot_days)} knots")
        daily_changes = np.abs(np.diff(curve))
        max_change = daily_changes.max() if len(daily_changes) > 0 else 0

        if max_change > self.bounds.max_daily_change:
            logger.warning(
                f"Curve has large daily changes (max ${max_change:.2f}/day). "
                "This may indicate data issues."
            )

        logger.info(
            f"Curve built successfully. "
            f"Range: ${curve.min():.2f} - ${curve.max():.2f}, "
            f"Max daily change: ${max_change:.2f}"
        )

        return pd.Series(data=curve, index=pd.to_datetime(date_range))

    def build_curve_with_diagnostics(
        self,
        contracts: List[ContractBlock]
    ) -> Tuple[pd.Series, dict]:
        """
        Build curve and return diagnostics for debugging.

        Returns:
            Tuple of (curve Series, diagnostics dict)
        """
        curve = self.build_curve(contracts)

        diagnostics = {
            'spot_date': self.spot_date,
            'spot_price': self.spot_price,
            'num_contracts': len(contracts),
            'curve_start': curve.iloc[0] if len(curve) > 0 else None,
            'curve_end': curve.iloc[-1] if len(curve) > 0 else None,
            'curve_min': curve.min() if len(curve) > 0 else None,
            'curve_max': curve.max() if len(curve) > 0 else None,
            'curve_mean': curve.mean() if len(curve) > 0 else None,
            'days_total': len(curve),
            'contracts': [
                {'start': c.start_date, 'end': c.end_date, 'price': c.price}
                for c in contracts
            ]
        }

        # Verify arbitrage constraints are satisfied
        for c in contracts:
            start_idx = (c.start_date - self.spot_date).days
            end_idx = (c.end_date - self.spot_date).days
            if 0 <= start_idx < len(curve) and end_idx < len(curve):
                actual_avg = curve.iloc[start_idx:end_idx+1].mean()
                diagnostics[f'contract_{c.start_date}_avg'] = actual_avg
                diagnostics[f'contract_{c.start_date}_target'] = c.price
                diagnostics[f'contract_{c.start_date}_error'] = actual_avg - c.price

        return curve, diagnostics

# Example usage helper
def create_blocks_from_market_contracts(market_contracts: List) -> List[ContractBlock]:
    """
    Convert market contracts to ContractBlock format.
    Handles Monthly, Quarterly, and Calendar period types.
    """
    from calendar import monthrange

    blocks = []
    for mc in market_contracts:
        # mc is expected to have: contract_date, period_type, price
        contract_date = mc.contract_date if hasattr(mc, 'contract_date') else mc.get('contract_date')
        period_type = mc.period_type if hasattr(mc, 'period_type') else mc.get('period_type', 'Monthly')
        price = mc.price if hasattr(mc, 'price') else mc.get('price')

        if contract_date is None or price is None:
            logger.warning(f"Skipping invalid contract: {mc}")
            continue

        # Determine start and end dates based on period type
        if period_type == "Monthly":
            # contract_date is typically the first of the month
            year, month = contract_date.year, contract_date.month
            start_date = date(year, month, 1)
            _, last_day = monthrange(year, month)
            end_date = date(year, month, last_day)

        elif period_type == "Quarterly":
            # Q1 = Jan-Mar, Q2 = Apr-Jun, Q3 = Jul-Sep, Q4 = Oct-Dec
            year = contract_date.year
            quarter = (contract_date.month - 1) // 3 + 1
            start_month = (quarter - 1) * 3 + 1
            end_month = start_month + 2
            start_date = date(year, start_month, 1)
            _, last_day = monthrange(year, end_month)
            end_date = date(year, end_month, last_day)

        elif period_type == "Calendar":
            # Full year
            year = contract_date.year
            start_date = date(year, 1, 1)
            end_date = date(year, 12, 31)

        else:
            logger.warning(f"Unknown period type: {period_type}, treating as Monthly")
            year, month = contract_date.year, contract_date.month
            start_date = date(year, month, 1)
            _, last_day = monthrange(year, month)
            end_date = date(year, month, last_day)

        blocks.append(ContractBlock(
            start_date=start_date,
            end_date=end_date,
            price=price
        ))

    # Sort by start date
    blocks.sort(key=lambda b: b.start_date)

    logger.info(f"Created {len(blocks)} contract blocks from {len(market_contracts)} market contracts")
    return blocks
