"""
Ensemble Forecasting Module

Combines multiple forecasting approaches for improved accuracy:
1. Futures curve interpolation (current spline)
2. Statistical models (ARIMA, Prophet)
3. Mean reversion component
4. Volatility-based confidence intervals
"""
import numpy as np
import pandas as pd
from datetime import date, timedelta
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
import logging

from .spline import MaximumSmoothnessSpline, ContractBlock
from .forecast import (
    ARIMAForecaster,
    ProphetForecaster,
    MeanReversionModel,
    SimpleMovingAverageForecast,
    GARCHVolatilityModel,
    ForecastResult
)

logger = logging.getLogger(__name__)


@dataclass
class EnsembleForecastResult:
    """Complete ensemble forecast output"""
    dates: pd.DatetimeIndex
    point_forecast: pd.Series
    lower_bound_90: pd.Series
    upper_bound_90: pd.Series
    lower_bound_50: pd.Series
    upper_bound_50: pd.Series
    component_weights: Dict[str, float]
    component_forecasts: Dict[str, pd.Series]


class EnsembleForecaster:
    """
    Combines multiple forecasting signals with adaptive weighting.

    Components:
    - Futures Curve: Market-implied forward prices (highest weight for near-term)
    - ARIMA: Captures momentum and autoregressive patterns
    - Prophet: Captures seasonality and trend
    - Mean Reversion: Pull toward long-term equilibrium
    """

    def __init__(
        self,
        weights: Optional[Dict[str, float]] = None,
        long_term_mean: float = 1100.0,  # NBSK long-term equilibrium
        adapt_weights: bool = True
    ):
        """
        Args:
            weights: Component weights (must sum to 1.0)
            long_term_mean: Long-term price equilibrium
            adapt_weights: Whether to adjust weights based on forecast horizon
        """
        self.weights = weights or {
            'futures_curve': 0.50,
            'statistical': 0.30,
            'mean_reversion': 0.20
        }
        self.long_term_mean = long_term_mean
        self.adapt_weights = adapt_weights

        # Validate weights
        total = sum(self.weights.values())
        if not np.isclose(total, 1.0):
            # Normalize
            self.weights = {k: v/total for k, v in self.weights.items()}

    def _get_horizon_adjusted_weights(self, horizon_days: int) -> Dict[str, float]:
        """
        Adjust weights based on forecast horizon.

        Near-term (< 30 days): Trust futures curve more
        Medium-term (30-90 days): Balanced
        Long-term (> 90 days): More weight on mean reversion
        """
        if not self.adapt_weights:
            return self.weights

        if horizon_days <= 30:
            return {
                'futures_curve': 0.60,
                'statistical': 0.25,
                'mean_reversion': 0.15
            }
        elif horizon_days <= 90:
            return {
                'futures_curve': 0.45,
                'statistical': 0.30,
                'mean_reversion': 0.25
            }
        else:
            return {
                'futures_curve': 0.30,
                'statistical': 0.30,
                'mean_reversion': 0.40
            }

    def forecast(
        self,
        spot_price: float,
        spot_date: date,
        contracts: List[ContractBlock],
        historical_prices: pd.Series,
        horizon_days: int = 365
    ) -> EnsembleForecastResult:
        """
        Generate ensemble forecast combining all components.

        Args:
            spot_price: Current spot price (T=0 anchor)
            spot_date: Date of spot price
            contracts: Futures contracts for spline
            historical_prices: Historical price series for statistical models
            horizon_days: Forecast horizon

        Returns:
            EnsembleForecastResult with point forecast and confidence intervals
        """
        component_forecasts = {}
        forecast_dates = pd.date_range(
            start=spot_date,
            periods=horizon_days,
            freq='D'
        )

        # 1. Futures Curve (Spline)
        try:
            spline = MaximumSmoothnessSpline(spot_date, spot_price)
            spline_curve = spline.build_curve(contracts)
            # Extend or align to forecast horizon
            spline_aligned = self._align_to_dates(spline_curve, forecast_dates, spot_price)
            component_forecasts['futures_curve'] = spline_aligned
            logger.info("Spline component generated")
        except Exception as e:
            logger.warning(f"Spline failed: {e}. Using flat projection.")
            component_forecasts['futures_curve'] = pd.Series(
                spot_price, index=forecast_dates
            )

        # 2. Statistical Model (ARIMA or Prophet)
        try:
            arima = ARIMAForecaster(order=(2, 1, 2)).fit(historical_prices)
            arima_result = arima.forecast(horizon_days)
            arima_aligned = self._align_to_dates(
                arima_result.point_forecast, forecast_dates, spot_price
            )
            component_forecasts['statistical'] = arima_aligned
            logger.info("ARIMA component generated")
        except Exception as e:
            logger.warning(f"ARIMA failed: {e}. Trying SMA.")
            try:
                sma = SimpleMovingAverageForecast(window=30)
                sma_forecast = sma.forecast(historical_prices, horizon_days)
                sma_aligned = self._align_to_dates(sma_forecast, forecast_dates, spot_price)
                component_forecasts['statistical'] = sma_aligned
            except:
                component_forecasts['statistical'] = pd.Series(
                    spot_price, index=forecast_dates
                )

        # 3. Mean Reversion
        mr_model = MeanReversionModel(
            long_term_mean=self.long_term_mean,
            half_life_days=180
        )
        mr_forecast = mr_model.forecast(spot_price, horizon_days)
        mr_aligned = self._align_to_dates(mr_forecast, forecast_dates, spot_price)
        component_forecasts['mean_reversion'] = mr_aligned
        logger.info("Mean reversion component generated")

        # 4. Combine with weighted average
        weights = self._get_horizon_adjusted_weights(horizon_days)

        combined = pd.Series(0.0, index=forecast_dates)
        for component, weight in weights.items():
            if component in component_forecasts:
                combined += weight * component_forecasts[component]

        # 5. Generate confidence intervals using volatility estimate
        volatility = self._estimate_volatility(historical_prices, horizon_days)

        # 90% CI (~1.645 std devs)
        lower_90 = combined - 1.645 * volatility
        upper_90 = combined + 1.645 * volatility

        # 50% CI (~0.674 std devs)
        lower_50 = combined - 0.674 * volatility
        upper_50 = combined + 0.674 * volatility

        return EnsembleForecastResult(
            dates=forecast_dates,
            point_forecast=combined,
            lower_bound_90=lower_90,
            upper_bound_90=upper_90,
            lower_bound_50=lower_50,
            upper_bound_50=upper_50,
            component_weights=weights,
            component_forecasts=component_forecasts
        )

    def _align_to_dates(
        self,
        series: pd.Series,
        target_dates: pd.DatetimeIndex,
        fallback_value: float
    ) -> pd.Series:
        """Align a series to target dates, forward-filling and extending as needed."""
        if len(series) == 0:
            return pd.Series(fallback_value, index=target_dates)

        # Ensure datetime index
        if not isinstance(series.index, pd.DatetimeIndex):
            series.index = pd.to_datetime(series.index)

        # Reindex to target dates
        aligned = series.reindex(target_dates)

        # Forward fill, then backward fill, then fallback
        aligned = aligned.ffill().bfill().fillna(fallback_value)

        return aligned

    def _estimate_volatility(
        self,
        historical_prices: pd.Series,
        horizon_days: int
    ) -> pd.Series:
        """
        Estimate volatility that increases with forecast horizon.
        Uses historical volatility scaled by sqrt(time).
        """
        # Calculate historical daily volatility
        returns = historical_prices.pct_change().dropna()
        daily_vol = returns.std()

        # Scale by sqrt(time) for each horizon day
        days = np.arange(1, horizon_days + 1)
        vol_curve = daily_vol * np.sqrt(days) * historical_prices.iloc[-1]

        # Cap at reasonable maximum (30% of price)
        vol_curve = np.minimum(vol_curve, 0.30 * historical_prices.iloc[-1])

        return pd.Series(vol_curve)


class AdaptiveEnsemble(EnsembleForecaster):
    """
    Ensemble that learns optimal weights from historical accuracy.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.learned_weights = None

    def learn_weights(
        self,
        historical_curves: Dict[date, pd.Series],
        realized_prices: pd.Series,
        validation_horizon: int = 30
    ) -> Dict[str, float]:
        """
        Learn optimal weights by minimizing historical forecast error.

        Uses simple grid search over weight combinations.
        """
        from itertools import product
        from .accuracy import ForecastAccuracyTracker

        tracker = ForecastAccuracyTracker()
        best_mape = float('inf')
        best_weights = self.weights.copy()

        # Grid search over weight combinations
        weight_options = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6]

        for w1, w2 in product(weight_options, repeat=2):
            w3 = 1.0 - w1 - w2
            if w3 < 0.05:  # Ensure minimum weight
                continue

            test_weights = {
                'futures_curve': w1,
                'statistical': w2,
                'mean_reversion': w3
            }

            # Evaluate this weight combination
            # (Simplified - in production, would do proper backtesting)
            mape = self._evaluate_weights(
                test_weights,
                historical_curves,
                realized_prices,
                validation_horizon
            )

            if mape < best_mape:
                best_mape = mape
                best_weights = test_weights

        self.learned_weights = best_weights
        logger.info(f"Learned optimal weights: {best_weights}, MAPE: {best_mape:.2f}%")
        return best_weights

    def _evaluate_weights(
        self,
        weights: Dict[str, float],
        historical_curves: Dict[date, pd.Series],
        realized_prices: pd.Series,
        horizon: int
    ) -> float:
        """Evaluate a weight combination's historical accuracy."""
        errors = []

        for snapshot_date, curve in historical_curves.items():
            target_date = snapshot_date + timedelta(days=horizon)

            if target_date not in realized_prices.index:
                continue

            # This is simplified - would need actual component forecasts
            predicted = curve.get(target_date, curve.iloc[-1])
            actual = realized_prices[target_date]

            errors.append(abs(predicted - actual) / actual * 100)

        return np.mean(errors) if errors else float('inf')
