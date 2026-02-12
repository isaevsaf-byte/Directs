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

        # 2. Optimization Setup
        # Variable x: Daily prices from T=0 to T=End
        # We start with a flat guess equal to spot
        x0 = np.full(days_total, self.spot_price)

        # 3. Objective Function: Minimize Curvature (Squared Second Derivative)
        # sum((x[t] - 2*x[t-1] + x[t-2])^2)
        def objective(x):
            # Discrete 2nd derivative: [1, -2, 1] convolution
            diff2 = x[:-2] - 2*x[1:-1] + x[2:]
            return np.sum(diff2**2)

        # 4. Constraints
        constraints = []

        # Constraint A: Curve must start at Spot Price
        # x[0] == spot_price
        constraints.append({
            'type': 'eq',
            'fun': lambda x: x[0] - self.spot_price
        })

        # Constraint B: Arbitrage-Free (Average of daily values in period == Contract Price)
        # Note: In commodity markets, usually arithmetic average for swaps/futures settlement.
        for contact in contracts:
            # Find indices for this contract's period
            start_idx = (contact.start_date - self.spot_date).days
            end_idx = (contact.end_date - self.spot_date).days

            # Clip contracts that overlap the spot date (e.g., current month)
            if end_idx < 0 or start_idx >= days_total:
                logger.warning(f"Contract {contact} entirely out of range, skipping")
                continue
            if start_idx < 0:
                logger.info(f"Clipping contract {contact.start_date}-{contact.end_date} start from idx {start_idx} to 0")
                start_idx = 0
            if end_idx >= days_total:
                end_idx = days_total - 1

            # Helper to capture closure variables
            def make_constraint(s_idx, e_idx, target_price):
                # Using e_idx + 1 because python slice is exclusive at end
                return lambda x: np.mean(x[s_idx : e_idx + 1]) - target_price

            constraints.append({
                'type': 'eq',
                'fun': make_constraint(start_idx, end_idx, contact.price)
            })

        # 5. Set up bounds for each day
        # This prevents the optimizer from generating unrealistic prices
        price_bounds = [
            (self.bounds.min_price, self.bounds.max_price)
            for _ in range(days_total)
        ]

        # 6. Run Optimization
        # SLSQP is good for equality constraints
        result = minimize(
            objective,
            x0,
            constraints=constraints,
            bounds=price_bounds,
            method='SLSQP',
            options={'ftol': 1e-6, 'maxiter': 2000}
        )

        if not result.success:
            logger.error(f"Spline optimization failed: {result.message}")
            raise ValueError(f"Spline optimization failed: {result.message}")

        # 7. Post-optimization validation
        curve = result.x
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
