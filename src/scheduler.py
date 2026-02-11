"""
Automated Scheduler for Pulp Market Intelligence Hub

Runs the full data pipeline on a schedule:
- Daily 18:00 UTC: Scrape Norexco -> Generate Curves -> Generate Forecasts
- Weekly Tuesday 08:00 UTC: Check for new PIX prices

Uses APScheduler running inside the FastAPI process (no extra worker needed).
"""
import asyncio
import logging
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional
from calendar import monthrange

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from src.db.schema import init_db, SessionLocal, MarketSnapshot
from src.db.access import MarketRepository, RealizedPriceRepository, ForecastRepository
from src.etl.scraper import HybridScraper
from src.math.spline import MaximumSmoothnessSpline, ContractBlock, SplineBounds, create_blocks_from_market_contracts
from sqlalchemy import delete

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Job status tracking (in-memory, survives across runs within one process)
# ---------------------------------------------------------------------------
_job_history: List[Dict] = []
_MAX_HISTORY = 50


def _record(job_name: str, status: str, detail: str = ""):
    entry = {
        "job": job_name,
        "status": status,
        "detail": detail,
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }
    _job_history.append(entry)
    if len(_job_history) > _MAX_HISTORY:
        _job_history.pop(0)
    return entry


def get_job_history() -> List[Dict]:
    return list(_job_history)


def get_last_run(job_name: str) -> Optional[Dict]:
    for entry in reversed(_job_history):
        if entry["job"] == job_name:
            return entry
    return None


# ---------------------------------------------------------------------------
# Step 1: Scrape Norexco contracts
# ---------------------------------------------------------------------------
async def scrape_norexco() -> List:
    """Scrape latest contracts from Norexco market-view."""
    logger.info("SCHEDULER: Starting Norexco scrape...")
    _record("scrape_norexco", "running")

    try:
        scraper = HybridScraper()
        contracts = await scraper.run()

        if not contracts:
            msg = "Scrape returned 0 contracts"
            logger.warning(f"SCHEDULER: {msg}")
            _record("scrape_norexco", "warning", msg)
            return []

        logger.info(f"SCHEDULER: Scraped {len(contracts)} contracts")
        _record("scrape_norexco", "success", f"{len(contracts)} contracts")
        return contracts

    except Exception as e:
        logger.error(f"SCHEDULER: Norexco scrape failed: {e}", exc_info=True)
        _record("scrape_norexco", "error", str(e))
        return []


# ---------------------------------------------------------------------------
# Step 2: Generate forward curves from scraped contracts (or DB fallback)
# ---------------------------------------------------------------------------
def generate_curves_from_contracts(contracts: list) -> bool:
    """
    Build smooth forward curves for NBSK and BEK from scraped contracts
    and save to the database.  Falls back to the latest DB curve if
    the scrape returned nothing.
    """
    logger.info("SCHEDULER: Generating forward curves...")
    _record("generate_curves", "running")

    init_db()
    session = SessionLocal()
    success = True

    try:
        market_repo = MarketRepository(session)
        realized_repo = RealizedPriceRepository(session)
        snapshot_date = date.today()

        for product in ["NBSK", "BEK"]:
            product_contracts = [c for c in contracts if c.product_type == product]

            if product_contracts:
                # --- live scraped data path ---
                blocks = create_blocks_from_market_contracts(product_contracts)
                spot_price = product_contracts[0].price  # nearest contract as anchor
            else:
                # --- fallback: keep existing curve instead of overwriting with flat data ---
                existing_curve = market_repo.get_latest_curve(product)
                if existing_curve:
                    logger.info(f"SCHEDULER: No scraped {product} contracts, keeping existing curve ({len(existing_curve)} points)")
                    continue
                logger.warning(f"SCHEDULER: No scraped {product} contracts and no existing curve, skipping")
                continue

            if not blocks:
                logger.warning(f"SCHEDULER: No contract blocks for {product}")
                continue

            # Determine bounds per product
            # Norexco spot-based futures (Dec 2025+): lower price levels than old PIX/DAP contracts
            if product == "NBSK":
                bounds = SplineBounds(min_price=500, max_price=1200)
            else:
                bounds = SplineBounds(min_price=400, max_price=1000)

            try:
                spline = MaximumSmoothnessSpline(snapshot_date, spot_price, bounds)
                curve = spline.build_curve(blocks)
            except Exception as e:
                logger.error(f"SCHEDULER: Spline failed for {product}: {e} — keeping existing curve")
                success = False
                continue

            # Sanity check: reject flat curves (all prices identical)
            if len(curve) > 1 and curve.max() - curve.min() < 0.01:
                logger.warning(f"SCHEDULER: Curve for {product} is flat (${curve.iloc[0]:.2f}), keeping existing curve")
                continue

            # Clear today's existing snapshots for this product
            session.execute(
                delete(MarketSnapshot).where(
                    MarketSnapshot.snapshot_date == snapshot_date,
                    MarketSnapshot.product_type == product,
                )
            )
            session.commit()

            # Save new curve
            snapshots = []
            for contract_date, price in curve.items():
                dt = contract_date.date() if hasattr(contract_date, "date") else contract_date
                snapshots.append(
                    MarketSnapshot(
                        snapshot_date=snapshot_date,
                        contract_date=dt,
                        product_type=product,
                        price=float(price),
                        is_interpolated=True,
                    )
                )
            market_repo.save_snapshot(snapshots)
            logger.info(f"SCHEDULER: Saved {len(snapshots)} {product} curve points")

        _record("generate_curves", "success" if success else "partial",
                f"snapshot_date={snapshot_date}")
        return success

    except Exception as e:
        logger.error(f"SCHEDULER: Curve generation failed: {e}", exc_info=True)
        _record("generate_curves", "error", str(e))
        return False
    finally:
        session.close()


# ---------------------------------------------------------------------------
# Step 3: Generate ensemble forecast and save predictions
# ---------------------------------------------------------------------------
def generate_forecast() -> bool:
    """
    Run the ensemble forecaster and store predictions in the
    fact_forecast_accuracy table for later backtesting.
    """
    logger.info("SCHEDULER: Generating ensemble forecast...")
    _record("generate_forecast", "running")

    init_db()
    session = SessionLocal()

    try:
        market_repo = MarketRepository(session)
        realized_repo = RealizedPriceRepository(session)
        forecast_repo = ForecastRepository(session)

        today = date.today()

        for product in ["NBSK", "BEK"]:
            # Get the curve we just built
            curve_snapshots = market_repo.get_latest_curve(product)
            if not curve_snapshots:
                logger.warning(f"SCHEDULER: No curve for {product}, skipping forecast")
                continue

            # Get historical realized prices for statistical models
            realized = realized_repo.get_realized_prices(product)

            # Current spot = first point on the curve
            spot_price = curve_snapshots[0].price

            # Build contract blocks from the curve snapshots
            # Group by month for the ensemble
            from src.math.ensemble import EnsembleForecaster
            import pandas as pd

            # Prepare historical series
            if len(realized) < 3:
                logger.warning(f"SCHEDULER: Not enough history for {product} ({len(realized)} points), using SMA-only")

            # Determine long-term mean per product (spot-level pricing, Dec 2025+)
            long_term_mean = 800.0 if product == "NBSK" else 620.0

            ensemble = EnsembleForecaster(long_term_mean=long_term_mean)

            # Build contract blocks from curve snapshots for spline input
            blocks = _curve_to_blocks(curve_snapshots)

            try:
                result = ensemble.forecast(
                    spot_price=spot_price,
                    spot_date=today,
                    contracts=blocks,
                    historical_prices=realized if len(realized) >= 3 else pd.Series([spot_price] * 30),
                    horizon_days=365,
                )
            except Exception as e:
                logger.error(f"SCHEDULER: Ensemble forecast failed for {product}: {e}", exc_info=True)
                continue

            # Save predictions at key horizons (monthly)
            from src.db.schema import ForecastAccuracy

            forecasts_to_save = []
            horizons = [30, 60, 90, 120, 180, 270, 365]

            for h in horizons:
                if h >= len(result.point_forecast):
                    continue
                target_date = today + timedelta(days=h)
                predicted = float(result.point_forecast.iloc[h - 1])

                forecasts_to_save.append(
                    ForecastAccuracy(
                        prediction_date=today,
                        target_date=target_date,
                        product_type=product,
                        predicted_price=predicted,
                        model_version="ensemble_v1",
                        forecast_horizon_days=h,
                        futures_weight=result.component_weights.get("futures_curve"),
                        statistical_weight=result.component_weights.get("statistical"),
                        mean_reversion_weight=result.component_weights.get("mean_reversion"),
                    )
                )

            if forecasts_to_save:
                forecast_repo.save_forecasts_bulk(forecasts_to_save)
                logger.info(f"SCHEDULER: Saved {len(forecasts_to_save)} {product} forecast points")

        _record("generate_forecast", "success", f"prediction_date={today}")
        return True

    except Exception as e:
        logger.error(f"SCHEDULER: Forecast generation failed: {e}", exc_info=True)
        _record("generate_forecast", "error", str(e))
        return False
    finally:
        session.close()


def _curve_to_blocks(curve_snapshots: list) -> List[ContractBlock]:
    """Group daily curve snapshots into monthly ContractBlocks."""
    from collections import defaultdict

    monthly: Dict[tuple, list] = defaultdict(list)
    for s in curve_snapshots:
        key = (s.contract_date.year, s.contract_date.month)
        monthly[key].append(s.price)

    blocks = []
    for (year, month), prices in sorted(monthly.items()):
        _, last_day = monthrange(year, month)
        avg_price = sum(prices) / len(prices)
        blocks.append(
            ContractBlock(
                start_date=date(year, month, 1),
                end_date=date(year, month, last_day),
                price=avg_price,
            )
        )
    return blocks


# ---------------------------------------------------------------------------
# Step 4: Validate forecasts against newly available realized prices
# ---------------------------------------------------------------------------
def validate_forecasts_against_actuals() -> int:
    """
    Check if any pending forecasts can now be validated against
    realized PIX prices, and update accuracy metrics.
    """
    logger.info("SCHEDULER: Validating forecasts against actuals...")
    _record("validate_forecasts", "running")

    init_db()
    session = SessionLocal()
    total_updated = 0

    try:
        forecast_repo = ForecastRepository(session)
        realized_repo = RealizedPriceRepository(session)

        for product in ["NBSK", "BEK"]:
            pending = forecast_repo.get_pending_forecasts(product)
            if not pending:
                continue

            realized = realized_repo.get_realized_prices(product)

            for forecast in pending:
                # Check if we have a realized price for this target date
                # (PIX is monthly, so match to nearest month-15)
                target = forecast.target_date
                if target in realized.index:
                    actual = float(realized[target])
                    forecast.actual_price = actual
                    forecast.error = forecast.predicted_price - actual
                    forecast.error_pct = (forecast.error / actual) * 100
                    total_updated += 1

            session.commit()

        msg = f"Updated {total_updated} forecasts with actuals"
        logger.info(f"SCHEDULER: {msg}")
        _record("validate_forecasts", "success", msg)
        return total_updated

    except Exception as e:
        logger.error(f"SCHEDULER: Validation failed: {e}", exc_info=True)
        _record("validate_forecasts", "error", str(e))
        return 0
    finally:
        session.close()


# ---------------------------------------------------------------------------
# Orchestration: the daily pipeline chains all steps
# ---------------------------------------------------------------------------
async def daily_pipeline():
    """
    Full daily pipeline:
    1. Scrape Norexco contracts
    2. Generate forward curves (from scrape or DB fallback)
    3. Generate ensemble forecast
    4. Validate any pending forecasts against new actuals
    """
    logger.info("=" * 60)
    logger.info("SCHEDULER: Daily pipeline started")
    logger.info("=" * 60)
    _record("daily_pipeline", "running")

    try:
        # Step 1: Scrape
        contracts = await scrape_norexco()

        # Step 2: Curves (works even if scrape returned nothing)
        generate_curves_from_contracts(contracts)

        # Step 3: Forecast
        generate_forecast()

        # Step 4: Validate
        validate_forecasts_against_actuals()

        _record("daily_pipeline", "success")
        logger.info("SCHEDULER: Daily pipeline completed successfully")

    except Exception as e:
        logger.error(f"SCHEDULER: Daily pipeline failed: {e}", exc_info=True)
        _record("daily_pipeline", "error", str(e))


async def weekly_pix_check():
    """
    Weekly check: validate forecasts against any newly loaded PIX prices.
    (PIX prices are still loaded externally via CSV/script, but this
    ensures any new data triggers accuracy updates automatically.)
    """
    logger.info("SCHEDULER: Weekly PIX check started")
    _record("weekly_pix_check", "running")

    try:
        updated = validate_forecasts_against_actuals()
        _record("weekly_pix_check", "success", f"validated {updated} forecasts")
    except Exception as e:
        logger.error(f"SCHEDULER: Weekly PIX check failed: {e}", exc_info=True)
        _record("weekly_pix_check", "error", str(e))


# ---------------------------------------------------------------------------
# Scheduler setup
# ---------------------------------------------------------------------------
scheduler: Optional[AsyncIOScheduler] = None


def create_scheduler() -> AsyncIOScheduler:
    """Create and configure the APScheduler instance."""
    global scheduler
    scheduler = AsyncIOScheduler(timezone="UTC")

    # Daily at 15:00 UTC (16:00 CET, during trading hours 13:00-17:00 CET)
    scheduler.add_job(
        daily_pipeline,
        trigger=CronTrigger(hour=15, minute=0, timezone="UTC"),
        id="daily_pipeline",
        name="Daily: Scrape + Curve + Forecast",
        replace_existing=True,
        misfire_grace_time=3600,  # allow up to 1h late
    )

    # Weekly Tuesday at 08:00 UTC (PIX validation)
    scheduler.add_job(
        weekly_pix_check,
        trigger=CronTrigger(day_of_week="tue", hour=8, minute=0, timezone="UTC"),
        id="weekly_pix_check",
        name="Weekly: PIX Validation",
        replace_existing=True,
        misfire_grace_time=3600,
    )

    logger.info("SCHEDULER: Configured - Daily 15:00 UTC, Weekly Tue 08:00 UTC")
    return scheduler


def get_scheduler_status() -> Dict:
    """Return current scheduler state and job info."""
    if scheduler is None:
        return {"status": "not_initialized"}

    jobs = []
    for job in scheduler.get_jobs():
        next_run = job.next_run_time
        jobs.append({
            "id": job.id,
            "name": job.name,
            "next_run": next_run.isoformat() if next_run else None,
            "trigger": str(job.trigger),
        })

    return {
        "status": "running" if scheduler.running else "stopped",
        "jobs": jobs,
        "recent_history": _job_history[-10:],
        "last_daily": get_last_run("daily_pipeline"),
        "last_weekly": get_last_run("weekly_pix_check"),
    }
