"""
Scheduler API Routes

Provides endpoints to:
- View scheduler status and job history
- Manually trigger the daily pipeline or individual steps
"""
import asyncio
import logging
from fastapi import APIRouter, BackgroundTasks, Body

from src.scheduler import (
    get_scheduler_status,
    get_job_history,
    daily_pipeline,
    scrape_norexco,
    generate_curves_from_contracts,
    generate_forecast,
    validate_forecasts_against_actuals,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/scheduler", tags=["Scheduler"])


@router.get("/status")
def scheduler_status():
    """
    Get current scheduler status, upcoming jobs, and recent run history.
    """
    return get_scheduler_status()


@router.get("/history")
def scheduler_history():
    """
    Get full job run history (last 50 entries).
    """
    return {"history": get_job_history()}


@router.post("/trigger/pipeline")
async def trigger_pipeline(background_tasks: BackgroundTasks):
    """
    Manually trigger the full daily pipeline:
    Scrape -> Curves -> Forecast -> Validate
    """
    logger.info("SCHEDULER: Manual pipeline trigger via API")
    background_tasks.add_task(_run_pipeline)
    return {"status": "triggered", "message": "Daily pipeline started in background"}


@router.post("/trigger/scrape")
async def trigger_scrape(background_tasks: BackgroundTasks):
    """Manually trigger Norexco scrape only."""
    logger.info("SCHEDULER: Manual scrape trigger via API")
    background_tasks.add_task(_run_scrape)
    return {"status": "triggered", "message": "Scrape started in background"}


@router.post("/trigger/curves")
async def trigger_curves(background_tasks: BackgroundTasks):
    """Manually trigger curve generation (uses DB data, no scrape)."""
    logger.info("SCHEDULER: Manual curve generation trigger via API")
    background_tasks.add_task(_run_curves)
    return {"status": "triggered", "message": "Curve generation started in background"}


@router.post("/trigger/forecast")
async def trigger_forecast(background_tasks: BackgroundTasks):
    """Manually trigger ensemble forecast generation."""
    logger.info("SCHEDULER: Manual forecast trigger via API")
    background_tasks.add_task(generate_forecast)
    return {"status": "triggered", "message": "Forecast generation started in background"}


@router.post("/trigger/validate")
async def trigger_validate():
    """Manually trigger forecast validation against realized prices."""
    logger.info("SCHEDULER: Manual validation trigger via API")
    updated = validate_forecasts_against_actuals()
    return {"status": "complete", "forecasts_updated": updated}


# Background task wrappers (needed because some are async)
async def _run_pipeline():
    await daily_pipeline()


async def _run_scrape():
    contracts = await scrape_norexco()
    return contracts


def _run_curves():
    generate_curves_from_contracts([])  # empty = use DB fallback


@router.post("/inject/contracts")
async def inject_contracts(background_tasks: BackgroundTasks, contracts: list = Body(...)):
    """
    Inject contract data manually and generate curves + forecast.
    Useful when scraper can't run (outside trading hours).

    Body: list of {product_type, contract_date, period_type, price}
    """
    from src.etl.models import MarketContract
    from datetime import date as date_type

    logger.info(f"SCHEDULER: Manual contract injection: {len(contracts)} contracts")

    parsed = []
    for c in contracts:
        try:
            cd = c["contract_date"]
            if isinstance(cd, str):
                cd = date_type.fromisoformat(cd)
            mc = MarketContract(
                ticker=f"{c['product_type']}-MANUAL",
                product_type=c["product_type"],
                contract_date=cd,
                period_type=c.get("period_type", "Monthly"),
                price=float(c["price"]),
            )
            parsed.append(mc)
        except Exception as e:
            logger.warning(f"Skipping invalid contract: {c} — {e}")

    if not parsed:
        return {"status": "error", "message": "No valid contracts parsed"}

    # Run curve generation + forecast in background
    def _run():
        generate_curves_from_contracts(parsed)
        generate_forecast()

    background_tasks.add_task(_run)
    return {"status": "triggered", "message": f"Injected {len(parsed)} contracts, generating curves + forecast"}
