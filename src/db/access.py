from datetime import date, timedelta
from typing import List, Optional, Dict
from sqlalchemy.orm import Session
from .schema import MarketSnapshot, ForecastAccuracy, RealizedPrice, SessionLocal
from sqlalchemy import select, and_, desc, func
import pandas as pd


class MarketRepository:
    def __init__(self, session: Session):
        self.session = session

    def save_snapshot(self, snapshots: List[MarketSnapshot]):
        """
        Bulk saves a list of snapshot points.
        """
        self.session.add_all(snapshots)
        self.session.commit()

    def get_curve_by_date(self, snapshot_date: date, product_type: str) -> List[MarketSnapshot]:
        """
        Retrieves the full forward curve as it was known on 'snapshot_date'.
        This is the "Time Machine" query.
        """
        stmt = (
            select(MarketSnapshot)
            .where(
                and_(
                    MarketSnapshot.snapshot_date == snapshot_date,
                    MarketSnapshot.product_type == product_type
                )
            )
            .order_by(MarketSnapshot.contract_date)
        )
        return self.session.scalars(stmt).all()

    def get_latest_snapshot_date(self) -> Optional[date]:
        """
        Finds the most recent date we have data for.
        """
        stmt = select(MarketSnapshot.snapshot_date).order_by(desc(MarketSnapshot.snapshot_date)).limit(1)
        return self.session.scalar(stmt)

    def get_latest_curve(self, product_type: str) -> List[MarketSnapshot]:
        """
        Convenience method to get the most recent curve.
        """
        latest_date = self.get_latest_snapshot_date()
        if not latest_date:
            return []
        return self.get_curve_by_date(latest_date, product_type)

    def get_all_snapshot_dates(self, product_type: str = "NBSK") -> List[date]:
        """
        Get all unique snapshot dates in the database.
        """
        stmt = (
            select(MarketSnapshot.snapshot_date)
            .where(MarketSnapshot.product_type == product_type)
            .distinct()
            .order_by(MarketSnapshot.snapshot_date)
        )
        return list(self.session.scalars(stmt).all())

    def get_historical_curves(
        self,
        product_type: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> Dict[date, pd.Series]:
        """
        Get all historical curves as a dict of snapshot_date -> curve Series.
        Useful for backtesting.
        """
        snapshot_dates = self.get_all_snapshot_dates(product_type)

        if start_date:
            snapshot_dates = [d for d in snapshot_dates if d >= start_date]
        if end_date:
            snapshot_dates = [d for d in snapshot_dates if d <= end_date]

        curves = {}
        for snapshot_date in snapshot_dates:
            snapshots = self.get_curve_by_date(snapshot_date, product_type)
            if snapshots:
                curves[snapshot_date] = pd.Series(
                    {s.contract_date: s.price for s in snapshots}
                )

        return curves


class ForecastRepository:
    """Repository for forecast accuracy tracking"""

    def __init__(self, session: Session):
        self.session = session

    def save_forecast(
        self,
        prediction_date: date,
        target_date: date,
        product_type: str,
        predicted_price: float,
        model_version: str = "ensemble_v1",
        weights: Optional[Dict[str, float]] = None
    ):
        """Save a forecast prediction for later accuracy evaluation."""
        forecast = ForecastAccuracy(
            prediction_date=prediction_date,
            target_date=target_date,
            product_type=product_type,
            predicted_price=predicted_price,
            model_version=model_version,
            forecast_horizon_days=(target_date - prediction_date).days,
            futures_weight=weights.get('futures_curve') if weights else None,
            statistical_weight=weights.get('statistical') if weights else None,
            mean_reversion_weight=weights.get('mean_reversion') if weights else None
        )
        self.session.add(forecast)
        self.session.commit()

    def save_forecasts_bulk(self, forecasts: List[ForecastAccuracy]):
        """Bulk save forecast predictions."""
        self.session.add_all(forecasts)
        self.session.commit()

    def update_with_actual(self, target_date: date, product_type: str, actual_price: float):
        """
        Update forecast records with realized actual price and calculate errors.
        Should be called when PIX price is published.
        """
        stmt = (
            select(ForecastAccuracy)
            .where(
                and_(
                    ForecastAccuracy.target_date == target_date,
                    ForecastAccuracy.product_type == product_type,
                    ForecastAccuracy.actual_price.is_(None)
                )
            )
        )
        forecasts = self.session.scalars(stmt).all()

        for forecast in forecasts:
            forecast.actual_price = actual_price
            forecast.error = forecast.predicted_price - actual_price
            forecast.error_pct = (forecast.error / actual_price) * 100

        self.session.commit()
        return len(forecasts)

    def get_accuracy_summary(
        self,
        product_type: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> Dict:
        """
        Get summary statistics of forecast accuracy.
        """
        stmt = select(ForecastAccuracy).where(
            and_(
                ForecastAccuracy.product_type == product_type,
                ForecastAccuracy.actual_price.isnot(None)
            )
        )

        if start_date:
            stmt = stmt.where(ForecastAccuracy.prediction_date >= start_date)
        if end_date:
            stmt = stmt.where(ForecastAccuracy.prediction_date <= end_date)

        forecasts = self.session.scalars(stmt).all()

        if not forecasts:
            return {"error": "No forecast accuracy data available"}

        errors_pct = [abs(f.error_pct) for f in forecasts if f.error_pct is not None]
        errors = [f.error for f in forecasts if f.error is not None]

        return {
            "n_observations": len(forecasts),
            "mape": sum(errors_pct) / len(errors_pct) if errors_pct else None,
            "bias": sum(errors) / len(errors) if errors else None,
            "by_horizon": self._group_by_horizon(forecasts)
        }

    def _group_by_horizon(self, forecasts: List[ForecastAccuracy]) -> Dict:
        """Group accuracy metrics by forecast horizon."""
        horizons = {}

        for f in forecasts:
            if f.forecast_horizon_days is None or f.error_pct is None:
                continue

            # Bucket into horizon groups
            if f.forecast_horizon_days <= 7:
                bucket = "1_week"
            elif f.forecast_horizon_days <= 30:
                bucket = "1_month"
            elif f.forecast_horizon_days <= 90:
                bucket = "3_months"
            else:
                bucket = "6_months_plus"

            if bucket not in horizons:
                horizons[bucket] = {"errors": [], "count": 0}

            horizons[bucket]["errors"].append(abs(f.error_pct))
            horizons[bucket]["count"] += 1

        # Calculate MAPE for each bucket
        for bucket, data in horizons.items():
            if data["errors"]:
                data["mape"] = sum(data["errors"]) / len(data["errors"])
            del data["errors"]

        return horizons

    def get_pending_forecasts(self, product_type: str) -> List[ForecastAccuracy]:
        """Get forecasts that haven't been validated against actuals yet."""
        stmt = (
            select(ForecastAccuracy)
            .where(
                and_(
                    ForecastAccuracy.product_type == product_type,
                    ForecastAccuracy.actual_price.is_(None),
                    ForecastAccuracy.target_date <= date.today()
                )
            )
            .order_by(ForecastAccuracy.target_date)
        )
        return list(self.session.scalars(stmt).all())


class RealizedPriceRepository:
    """Repository for actual PIX prices"""

    def __init__(self, session: Session):
        self.session = session

    def save_realized_price(
        self,
        price_date: date,
        product_type: str,
        price: float,
        source: str = "Fastmarkets PIX"
    ):
        """Save an actual realized PIX price."""
        realized = RealizedPrice(
            price_date=price_date,
            product_type=product_type,
            price=price,
            source=source
        )
        self.session.add(realized)
        self.session.commit()

    def get_realized_prices(
        self,
        product_type: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> pd.Series:
        """Get realized prices as a Series for backtesting."""
        stmt = select(RealizedPrice).where(
            RealizedPrice.product_type == product_type
        )

        if start_date:
            stmt = stmt.where(RealizedPrice.price_date >= start_date)
        if end_date:
            stmt = stmt.where(RealizedPrice.price_date <= end_date)

        stmt = stmt.order_by(RealizedPrice.price_date)
        prices = self.session.scalars(stmt).all()

        return pd.Series(
            {p.price_date: p.price for p in prices}
        )

    def get_latest_price(self, product_type: str) -> Optional[RealizedPrice]:
        """Get the most recent realized price."""
        stmt = (
            select(RealizedPrice)
            .where(RealizedPrice.product_type == product_type)
            .order_by(desc(RealizedPrice.price_date))
            .limit(1)
        )
        return self.session.scalar(stmt)
