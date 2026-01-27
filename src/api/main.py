from fastapi import FastAPI, Depends, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from datetime import date, timedelta
from typing import List, Optional
import logging

from src.db.schema import SessionLocal, init_db, MarketSnapshot, ForecastAccuracy
from src.db.access import MarketRepository, ForecastRepository, RealizedPriceRepository
from .excel_export import router as excel_router

logger = logging.getLogger(__name__)

# Initialize DB (In real app, use Alembic)
init_db()

app = FastAPI(title="Pulp Market Intelligence Hub", version="2026.2.0")

# CORS for React Dashboard
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # In prod, restrict to dashboard domain
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include specialized routers
app.include_router(excel_router)


# Dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.get("/")
def health_check():
    return {"status": "ok", "system": "Pulp Market Intelligence Hub", "version": "2026.2.0"}


@app.get("/api/v1/market/curve/latest", response_model=List[dict])
def get_latest_curve(product: str = "NBSK", db: Session = Depends(get_db)):
    """Get the most recent forward curve."""
    repo = MarketRepository(db)
    snapshots = repo.get_latest_curve(product)

    return [
        {
            "date": s.contract_date,
            "price": s.price,
            "is_interpolated": s.is_interpolated
        }
        for s in snapshots
    ]


@app.get("/api/v1/market/curve/history", response_model=List[dict])
def get_historical_curve(
    snapshot_date: date,
    product: str = "NBSK",
    db: Session = Depends(get_db)
):
    """Get a historical forward curve as it was known on a specific date."""
    repo = MarketRepository(db)
    snapshots = repo.get_curve_by_date(snapshot_date, product)
    if not snapshots:
        raise HTTPException(status_code=404, detail="No snapshot found for this date")

    return [
        {
            "date": s.contract_date,
            "price": s.price,
            "is_interpolated": s.is_interpolated
        }
        for s in snapshots
    ]


@app.get("/api/v1/market/curve/dates")
def get_available_dates(product: str = "NBSK", db: Session = Depends(get_db)):
    """Get all available snapshot dates."""
    repo = MarketRepository(db)
    dates = repo.get_all_snapshot_dates(product)
    return {"dates": dates, "count": len(dates)}


# ============ FORECAST ACCURACY ENDPOINTS ============

@app.get("/api/v1/forecast/accuracy/summary")
def get_forecast_accuracy_summary(
    product: str = "NBSK",
    days: int = Query(default=90, description="Lookback period in days"),
    db: Session = Depends(get_db)
):
    """
    Get summary statistics of forecast accuracy.

    Returns MAPE, bias, and breakdown by forecast horizon.
    """
    repo = ForecastRepository(db)
    start_date = date.today() - timedelta(days=days)
    summary = repo.get_accuracy_summary(product, start_date=start_date)
    return summary


@app.get("/api/v1/forecast/accuracy/pending")
def get_pending_forecasts(product: str = "NBSK", db: Session = Depends(get_db)):
    """
    Get forecasts that are awaiting actual price realization.

    These can be validated once PIX publishes the realized price.
    """
    repo = ForecastRepository(db)
    pending = repo.get_pending_forecasts(product)
    return [
        {
            "prediction_date": f.prediction_date,
            "target_date": f.target_date,
            "predicted_price": f.predicted_price,
            "horizon_days": f.forecast_horizon_days,
            "model_version": f.model_version
        }
        for f in pending
    ]


@app.post("/api/v1/forecast/accuracy/update")
def update_forecast_accuracy(
    target_date: date,
    product: str,
    actual_price: float,
    db: Session = Depends(get_db)
):
    """
    Update forecast records with actual realized price.

    Call this when PIX publishes the actual price for a date.
    """
    repo = ForecastRepository(db)
    updated_count = repo.update_with_actual(target_date, product, actual_price)
    return {
        "status": "success",
        "updated_records": updated_count,
        "target_date": target_date,
        "actual_price": actual_price
    }


@app.get("/api/v1/forecast/diagnostics")
def get_forecast_diagnostics(
    product: str = "NBSK",
    db: Session = Depends(get_db)
):
    """
    Get diagnostic information about the forecasting system.

    Useful for debugging data issues.
    """
    market_repo = MarketRepository(db)
    forecast_repo = ForecastRepository(db)
    realized_repo = RealizedPriceRepository(db)

    latest_snapshot = market_repo.get_latest_snapshot_date()
    latest_curve = market_repo.get_latest_curve(product) if latest_snapshot else []
    latest_realized = realized_repo.get_latest_price(product)
    pending = forecast_repo.get_pending_forecasts(product)

    curve_stats = {}
    if latest_curve:
        prices = [s.price for s in latest_curve]
        curve_stats = {
            "min": min(prices),
            "max": max(prices),
            "mean": sum(prices) / len(prices),
            "count": len(prices),
            "start_date": latest_curve[0].contract_date if latest_curve else None,
            "end_date": latest_curve[-1].contract_date if latest_curve else None
        }

    return {
        "product": product,
        "latest_snapshot_date": latest_snapshot,
        "curve_stats": curve_stats,
        "latest_realized_price": {
            "date": latest_realized.price_date if latest_realized else None,
            "price": latest_realized.price if latest_realized else None
        },
        "pending_validations": len(pending),
        "health_checks": {
            "has_curve_data": len(latest_curve) > 0,
            "has_realized_data": latest_realized is not None,
            "price_in_range": (
                800 <= curve_stats.get("mean", 0) <= 2500
                if curve_stats.get("mean") else False
            )
        }
    }


# ============ REALIZED PRICES ENDPOINTS ============

@app.post("/api/v1/realized/price")
def save_realized_price(
    price_date: date,
    product: str,
    price: float,
    source: str = "Fastmarkets PIX",
    db: Session = Depends(get_db)
):
    """
    Save an actual realized PIX price.

    This data is used for backtesting and accuracy calculations.
    """
    # Validate price range
    if product == "NBSK" and not (800 <= price <= 2500):
        raise HTTPException(
            status_code=400,
            detail=f"NBSK price {price} outside expected range (800-2500)"
        )

    repo = RealizedPriceRepository(db)
    repo.save_realized_price(price_date, product, price, source)

    # Also update any pending forecasts for this date
    forecast_repo = ForecastRepository(db)
    updated = forecast_repo.update_with_actual(price_date, product, price)

    return {
        "status": "success",
        "price_date": price_date,
        "product": product,
        "price": price,
        "forecasts_updated": updated
    }


@app.get("/api/v1/realized/prices")
def get_realized_prices(
    product: str = "NBSK",
    days: int = Query(default=365, description="Lookback period in days"),
    db: Session = Depends(get_db)
):
    """
    Get historical realized PIX prices.
    """
    repo = RealizedPriceRepository(db)
    start_date = date.today() - timedelta(days=days)
    prices = repo.get_realized_prices(product, start_date=start_date)

    return {
        "product": product,
        "prices": [
            {"date": str(d), "price": p}
            for d, p in prices.items()
        ],
        "count": len(prices)
    }
