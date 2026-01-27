"""
Predictive Forecasting Models for Pulp Prices

Implements statistical and ML models for actual price prediction,
complementing the spline-based curve interpolation.
"""
import numpy as np
import pandas as pd
from datetime import date, timedelta
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


@dataclass
class ForecastResult:
    """Container for forecast output"""
    dates: pd.DatetimeIndex
    point_forecast: pd.Series
    lower_bound: pd.Series  # e.g., 5th percentile
    upper_bound: pd.Series  # e.g., 95th percentile
    model_name: str
    confidence_level: float = 0.90


class ARIMAForecaster:
    """
    ARIMA-based forecasting for pulp prices.
    Good for capturing trend and autoregressive patterns.
    """

    def __init__(self, order: Tuple[int, int, int] = (2, 1, 2)):
        """
        Args:
            order: (p, d, q) - AR order, differencing, MA order
        """
        self.order = order
        self.model = None
        self.fitted = None

    def fit(self, historical_prices: pd.Series) -> 'ARIMAForecaster':
        """
        Fit ARIMA model to historical price data.

        Args:
            historical_prices: Series with datetime index and price values
        """
        try:
            from statsmodels.tsa.arima.model import ARIMA

            # Ensure proper datetime index
            prices = historical_prices.copy()
            if not isinstance(prices.index, pd.DatetimeIndex):
                prices.index = pd.to_datetime(prices.index)

            prices = prices.asfreq('D', method='ffill')  # Fill gaps

            self.model = ARIMA(prices, order=self.order)
            self.fitted = self.model.fit()
            logger.info(f"ARIMA{self.order} fitted. AIC: {self.fitted.aic:.2f}")

        except ImportError:
            logger.warning("statsmodels not installed. ARIMA forecasting unavailable.")
            raise
        except Exception as e:
            logger.error(f"ARIMA fitting failed: {e}")
            raise

        return self

    def forecast(self, horizon_days: int, confidence: float = 0.90) -> ForecastResult:
        """
        Generate forecast for specified horizon.

        Args:
            horizon_days: Number of days to forecast
            confidence: Confidence level for prediction intervals
        """
        if self.fitted is None:
            raise ValueError("Model not fitted. Call fit() first.")

        forecast = self.fitted.get_forecast(steps=horizon_days)
        pred_mean = forecast.predicted_mean
        conf_int = forecast.conf_int(alpha=1 - confidence)

        return ForecastResult(
            dates=pred_mean.index,
            point_forecast=pred_mean,
            lower_bound=conf_int.iloc[:, 0],
            upper_bound=conf_int.iloc[:, 1],
            model_name=f"ARIMA{self.order}",
            confidence_level=confidence
        )


class GARCHVolatilityModel:
    """
    GARCH model for volatility forecasting.
    Useful for generating realistic confidence intervals.
    """

    def __init__(self, p: int = 1, q: int = 1):
        self.p = p
        self.q = q
        self.model = None
        self.fitted = None

    def fit(self, historical_prices: pd.Series) -> 'GARCHVolatilityModel':
        """Fit GARCH model to price returns."""
        try:
            from arch import arch_model

            # Convert prices to returns
            returns = historical_prices.pct_change().dropna() * 100  # Scale to %

            self.model = arch_model(
                returns,
                vol='Garch',
                p=self.p,
                q=self.q,
                mean='AR',
                lags=1
            )
            self.fitted = self.model.fit(disp='off')
            logger.info(f"GARCH({self.p},{self.q}) fitted.")

        except ImportError:
            logger.warning("arch package not installed. GARCH unavailable.")
            raise

        return self

    def forecast_volatility(self, horizon_days: int) -> pd.Series:
        """Forecast volatility (std dev of returns) for horizon."""
        if self.fitted is None:
            raise ValueError("Model not fitted. Call fit() first.")

        forecast = self.fitted.forecast(horizon=horizon_days)
        # variance -> std dev
        vol_forecast = np.sqrt(forecast.variance.dropna().iloc[-1])
        return vol_forecast


class ProphetForecaster:
    """
    Facebook Prophet for trend and seasonality detection.
    Good for capturing yearly patterns in pulp markets.
    """

    def __init__(self, yearly_seasonality: bool = True,
                 changepoint_prior_scale: float = 0.05):
        self.yearly_seasonality = yearly_seasonality
        self.changepoint_prior_scale = changepoint_prior_scale
        self.model = None

    def fit(self, historical_prices: pd.Series) -> 'ProphetForecaster':
        """Fit Prophet model."""
        try:
            from prophet import Prophet

            # Prophet requires specific column names
            df = historical_prices.reset_index()
            df.columns = ['ds', 'y']
            df['ds'] = pd.to_datetime(df['ds'])

            self.model = Prophet(
                yearly_seasonality=self.yearly_seasonality,
                weekly_seasonality=False,
                daily_seasonality=False,
                changepoint_prior_scale=self.changepoint_prior_scale
            )

            # Suppress verbose output
            self.model.fit(df)
            logger.info("Prophet model fitted.")

        except ImportError:
            logger.warning("prophet not installed. Prophet forecasting unavailable.")
            raise

        return self

    def forecast(self, horizon_days: int) -> ForecastResult:
        """Generate Prophet forecast."""
        if self.model is None:
            raise ValueError("Model not fitted. Call fit() first.")

        future = self.model.make_future_dataframe(periods=horizon_days)
        forecast = self.model.predict(future)

        # Get only the forecast portion
        forecast = forecast.tail(horizon_days)

        return ForecastResult(
            dates=pd.DatetimeIndex(forecast['ds']),
            point_forecast=pd.Series(forecast['yhat'].values, index=forecast['ds']),
            lower_bound=pd.Series(forecast['yhat_lower'].values, index=forecast['ds']),
            upper_bound=pd.Series(forecast['yhat_upper'].values, index=forecast['ds']),
            model_name="Prophet",
            confidence_level=0.80  # Prophet default
        )


class MeanReversionModel:
    """
    Mean reversion model for commodity prices.
    Pulp prices tend to revert to long-term production cost levels.
    """

    def __init__(self, long_term_mean: float = 1100.0, half_life_days: int = 180):
        """
        Args:
            long_term_mean: Long-term equilibrium price (production cost proxy)
            half_life_days: How fast prices revert to mean
        """
        self.long_term_mean = long_term_mean
        self.half_life_days = half_life_days
        self.theta = np.log(2) / half_life_days  # Mean reversion speed

    def forecast(self, current_price: float, horizon_days: int) -> pd.Series:
        """
        Ornstein-Uhlenbeck mean reversion forecast.

        P(t) = mu + (P(0) - mu) * exp(-theta * t)
        """
        days = np.arange(1, horizon_days + 1)
        deviation = current_price - self.long_term_mean
        forecast = self.long_term_mean + deviation * np.exp(-self.theta * days)

        dates = pd.date_range(start=date.today(), periods=horizon_days, freq='D')
        return pd.Series(forecast, index=dates)


class SimpleMovingAverageForecast:
    """
    Simple MA-based forecast as a baseline.
    Uses recent average with trend adjustment.
    """

    def __init__(self, window: int = 30):
        self.window = window

    def forecast(self, historical_prices: pd.Series, horizon_days: int) -> pd.Series:
        """Generate forecast based on recent moving average and trend."""
        recent = historical_prices.tail(self.window)
        ma = recent.mean()

        # Calculate trend (daily change)
        trend = (recent.iloc[-1] - recent.iloc[0]) / len(recent)

        # Project forward
        dates = pd.date_range(
            start=historical_prices.index[-1] + timedelta(days=1),
            periods=horizon_days,
            freq='D'
        )
        forecast_values = ma + trend * np.arange(1, horizon_days + 1)

        return pd.Series(forecast_values, index=dates)


def select_best_model(
    historical_prices: pd.Series,
    validation_period: int = 30
) -> str:
    """
    Automatically select the best forecasting model based on validation.

    Args:
        historical_prices: Full price history
        validation_period: Days to holdout for validation

    Returns:
        Name of best performing model
    """
    train = historical_prices.iloc[:-validation_period]
    test = historical_prices.iloc[-validation_period:]

    models = {}
    errors = {}

    # Test each model
    try:
        arima = ARIMAForecaster().fit(train)
        arima_forecast = arima.forecast(validation_period)
        arima_pred = arima_forecast.point_forecast
        arima_pred.index = test.index
        errors['ARIMA'] = np.mean(np.abs((test - arima_pred) / test)) * 100
    except:
        pass

    try:
        prophet = ProphetForecaster().fit(train)
        prophet_forecast = prophet.forecast(validation_period)
        prophet_pred = prophet_forecast.point_forecast
        prophet_pred.index = test.index
        errors['Prophet'] = np.mean(np.abs((test - prophet_pred) / test)) * 100
    except:
        pass

    # Simple MA as baseline
    sma = SimpleMovingAverageForecast().forecast(train, validation_period)
    sma.index = test.index
    errors['SMA'] = np.mean(np.abs((test - sma) / test)) * 100

    if not errors:
        return 'SMA'

    best_model = min(errors, key=errors.get)
    logger.info(f"Model selection: {errors}. Best: {best_model}")
    return best_model
