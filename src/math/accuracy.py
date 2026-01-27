"""
Forecast Accuracy Tracking Module

Provides tools for backtesting predictions against realized prices
and calculating accuracy metrics (MAPE, RMSE, directional accuracy).
"""
import numpy as np
import pandas as pd
from datetime import date, timedelta
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass


@dataclass
class AccuracyResult:
    """Result of an accuracy calculation"""
    prediction_date: date
    target_date: date
    predicted_price: float
    actual_price: float
    error: float
    error_pct: float
    direction_correct: bool


class ForecastAccuracyTracker:
    """
    Compares historical predictions against realized PIX prices
    to measure and improve forecast accuracy.
    """

    def __init__(self):
        self.results: List[AccuracyResult] = []

    def calculate_mape(self, predictions: pd.Series, actuals: pd.Series) -> float:
        """
        Mean Absolute Percentage Error
        Lower is better. < 5% is excellent, < 10% is good, > 20% needs improvement.
        """
        aligned = pd.concat([predictions, actuals], axis=1, keys=['pred', 'actual']).dropna()
        if len(aligned) == 0:
            return float('nan')

        mape = np.mean(np.abs((aligned['actual'] - aligned['pred']) / aligned['actual'])) * 100
        return mape

    def calculate_rmse(self, predictions: pd.Series, actuals: pd.Series) -> float:
        """
        Root Mean Square Error
        Penalizes large errors more heavily.
        """
        aligned = pd.concat([predictions, actuals], axis=1, keys=['pred', 'actual']).dropna()
        if len(aligned) == 0:
            return float('nan')

        rmse = np.sqrt(np.mean((aligned['actual'] - aligned['pred']) ** 2))
        return rmse

    def calculate_directional_accuracy(self, predictions: pd.Series, actuals: pd.Series) -> float:
        """
        Percentage of times the predicted direction of change was correct.
        > 50% is better than random, > 60% is good.
        """
        pred_changes = predictions.diff().dropna()
        actual_changes = actuals.diff().dropna()

        aligned = pd.concat([pred_changes, actual_changes], axis=1, keys=['pred', 'actual']).dropna()
        if len(aligned) == 0:
            return float('nan')

        correct_direction = (np.sign(aligned['pred']) == np.sign(aligned['actual']))
        return correct_direction.mean() * 100

    def calculate_bias(self, predictions: pd.Series, actuals: pd.Series) -> float:
        """
        Systematic over/under prediction.
        Positive = overestimating, Negative = underestimating.
        """
        aligned = pd.concat([predictions, actuals], axis=1, keys=['pred', 'actual']).dropna()
        if len(aligned) == 0:
            return float('nan')

        bias = np.mean(aligned['pred'] - aligned['actual'])
        return bias

    def backtest_curve(
        self,
        historical_curves: Dict[date, pd.Series],
        realized_prices: pd.Series,
        horizon_days: List[int] = [7, 14, 30, 60, 90]
    ) -> pd.DataFrame:
        """
        For each historical curve snapshot, measure prediction accuracy
        at various forecast horizons.

        Args:
            historical_curves: Dict mapping snapshot_date -> forward curve (Series with date index)
            realized_prices: Series of actual realized PIX prices (date index)
            horizon_days: List of forecast horizons to evaluate

        Returns:
            DataFrame with accuracy metrics by snapshot date and horizon
        """
        results = []

        for snapshot_date, curve in historical_curves.items():
            for horizon in horizon_days:
                target_date = snapshot_date + timedelta(days=horizon)

                # Get predicted price from the curve
                if target_date in curve.index:
                    predicted = curve[target_date]
                elif pd.Timestamp(target_date) in curve.index:
                    predicted = curve[pd.Timestamp(target_date)]
                else:
                    continue

                # Get realized price
                if target_date in realized_prices.index:
                    actual = realized_prices[target_date]
                elif pd.Timestamp(target_date) in realized_prices.index:
                    actual = realized_prices[pd.Timestamp(target_date)]
                else:
                    continue

                error = predicted - actual
                error_pct = (error / actual) * 100

                results.append({
                    'snapshot_date': snapshot_date,
                    'target_date': target_date,
                    'horizon_days': horizon,
                    'predicted': predicted,
                    'actual': actual,
                    'error': error,
                    'error_pct': error_pct,
                    'abs_error_pct': abs(error_pct)
                })

        return pd.DataFrame(results)

    def generate_accuracy_report(self, backtest_results: pd.DataFrame) -> Dict:
        """
        Generate summary statistics from backtest results.
        """
        if len(backtest_results) == 0:
            return {"error": "No backtest results available"}

        report = {
            "overall": {
                "mape": backtest_results['abs_error_pct'].mean(),
                "rmse": np.sqrt((backtest_results['error'] ** 2).mean()),
                "bias": backtest_results['error'].mean(),
                "n_observations": len(backtest_results)
            },
            "by_horizon": {}
        }

        for horizon in backtest_results['horizon_days'].unique():
            subset = backtest_results[backtest_results['horizon_days'] == horizon]
            report["by_horizon"][f"{horizon}_days"] = {
                "mape": subset['abs_error_pct'].mean(),
                "rmse": np.sqrt((subset['error'] ** 2).mean()),
                "bias": subset['error'].mean(),
                "n_observations": len(subset)
            }

        return report


def calculate_forecast_skill_score(
    model_mape: float,
    naive_mape: float
) -> float:
    """
    Skill score comparing model to naive forecast (no-change forecast).
    > 0 means model is better than naive
    1.0 would mean perfect forecast
    < 0 means model is worse than just predicting no change
    """
    if naive_mape == 0:
        return float('nan')
    return 1 - (model_mape / naive_mape)


def create_naive_forecast(historical_prices: pd.Series, horizon: int) -> pd.Series:
    """
    Creates a naive 'no-change' forecast for comparison.
    Simply predicts that future price = current price.
    """
    return historical_prices.shift(-horizon)
