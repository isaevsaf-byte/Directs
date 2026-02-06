"""
Excel Export API - Phase 2: Power Query Integration

Provides CSV streaming endpoints optimized for Excel Power Query.
Uses token-based authentication via query parameter (Power Query friendly).
"""

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session
from src.db.schema import SessionLocal
from src.db.access import MarketRepository, RealizedPriceRepository
from datetime import date
import csv
import io
import os
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/export", tags=["Excel Export"])

# Token from environment variable
EXCEL_EXPORT_TOKEN = os.environ.get("EXCEL_EXPORT_TOKEN", "finance-readonly-2026")


def get_db():
    """Database session dependency."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def verify_token(token: str) -> bool:
    """
    Verify the export token.
    Uses query parameter instead of header for Power Query compatibility.
    """
    if not token or token != EXCEL_EXPORT_TOKEN:
        raise HTTPException(
            status_code=401,
            detail="Invalid or missing token. Contact admin for access."
        )
    return True


def build_flat_table(db: Session) -> list:
    """
    Build a flat table combining Historical (Actuals) and Forecast data
    for both NBSK and BEK products.

    Returns a list of dicts optimized for Excel Pivot Tables:
    - Date (YYYY-MM-DD)
    - Ticker (NBSK, BEK)
    - Price (float, 2 decimals)
    - Type (Actual, Forecast)
    """
    market_repo = MarketRepository(db)
    realized_repo = RealizedPriceRepository(db)

    rows = []

    for product in ["NBSK", "BEK"]:
        # 1. Get Historical Actuals (Realized PIX prices)
        realized_prices = realized_repo.get_realized_prices(product)

        for price_date, price in realized_prices.items():
            rows.append({
                "Date": price_date.strftime("%Y-%m-%d"),
                "Ticker": product,
                "Price": round(float(price), 2),
                "Type": "Actual"
            })

        # 2. Get Forward Curve (Forecast)
        curve = market_repo.get_latest_curve(product)

        # Only include future dates (from today onwards) as forecast
        today = date.today()
        for snapshot in curve:
            if snapshot.contract_date >= today:
                rows.append({
                    "Date": snapshot.contract_date.strftime("%Y-%m-%d"),
                    "Ticker": product,
                    "Price": round(float(snapshot.price), 2),
                    "Type": "Forecast"
                })

    # Sort by Date, then Ticker
    rows.sort(key=lambda x: (x["Date"], x["Ticker"]))

    return rows


def generate_csv_stream(rows: list):
    """
    Generator function for streaming CSV output.
    Memory efficient - doesn't hold entire file in memory.
    """
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=["Date", "Ticker", "Price", "Type"])

    # Write header
    writer.writeheader()
    yield output.getvalue()
    output.seek(0)
    output.truncate(0)

    # Write rows one at a time
    for row in rows:
        writer.writerow(row)
        yield output.getvalue()
        output.seek(0)
        output.truncate(0)


@router.get("/excel/forecast")
def export_forecast_csv(
    token: str = Query(..., description="API token for Power Query access"),
    db: Session = Depends(get_db)
):
    """
    Export combined Historical + Forecast data as CSV for Excel/Power Query.

    **Authentication:** Pass token as query parameter (Power Query compatible).

    **Columns:**
    - Date: YYYY-MM-DD format
    - Ticker: NBSK or BEK
    - Price: Float with 2 decimal places
    - Type: "Actual" (historical PIX) or "Forecast" (forward curve)

    **Usage in Excel Power Query:**
    ```
    = Csv.Document(Web.Contents("https://your-api.com/api/v1/export/excel/forecast?token=YOUR_TOKEN"))
    ```
    """
    verify_token(token)

    logger.info("Excel export requested - building flat table")
    rows = build_flat_table(db)
    logger.info(f"Excel export: {len(rows)} rows prepared for streaming")

    return StreamingResponse(
        generate_csv_stream(rows),
        media_type="text/csv",
        headers={
            "Content-Disposition": "attachment; filename=pulp_forecast_data.csv",
            "Cache-Control": "no-cache"
        }
    )


@router.get("/excel/historical")
def export_historical_csv(
    token: str = Query(..., description="API token for Power Query access"),
    product: str = Query(default="NBSK", description="Product: NBSK or BEK"),
    db: Session = Depends(get_db)
):
    """
    Export historical PIX prices only as CSV.

    Useful for historical analysis without forecast data.
    """
    verify_token(token)

    realized_repo = RealizedPriceRepository(db)
    prices = realized_repo.get_realized_prices(product)

    rows = [
        {
            "Date": price_date.strftime("%Y-%m-%d"),
            "Ticker": product,
            "Price": round(float(price), 2),
            "Type": "Actual"
        }
        for price_date, price in prices.items()
    ]
    rows.sort(key=lambda x: x["Date"])

    return StreamingResponse(
        generate_csv_stream(rows),
        media_type="text/csv",
        headers={
            "Content-Disposition": f"attachment; filename={product}_historical.csv",
            "Cache-Control": "no-cache"
        }
    )


@router.get("/excel/curve")
def export_curve_csv(
    token: str = Query(..., description="API token for Power Query access"),
    product: str = Query(default="NBSK", description="Product: NBSK or BEK"),
    db: Session = Depends(get_db)
):
    """
    Export the latest forward curve only as CSV.

    Includes daily interpolated prices from the spline.
    """
    verify_token(token)

    market_repo = MarketRepository(db)
    curve = market_repo.get_latest_curve(product)

    rows = [
        {
            "Date": s.contract_date.strftime("%Y-%m-%d"),
            "Ticker": product,
            "Price": round(float(s.price), 2),
            "Type": "Forecast"
        }
        for s in curve
    ]

    return StreamingResponse(
        generate_csv_stream(rows),
        media_type="text/csv",
        headers={
            "Content-Disposition": f"attachment; filename={product}_curve.csv",
            "Cache-Control": "no-cache"
        }
    )


# Keep the legacy endpoint for backward compatibility
@router.get("/forecast/excel", deprecated=True)
def export_excel_csv_legacy(
    token: str = Query(..., description="API Key for Power Query Access"),
    product: str = "NBSK",
    db: Session = Depends(get_db)
):
    """
    [DEPRECATED] Use /api/v1/export/excel/forecast instead.

    Legacy endpoint - streams curve data for a single product.
    """
    verify_token(token)

    repo = MarketRepository(db)
    snapshots = repo.get_latest_curve(product)

    def iter_csv():
        output = io.StringIO()
        writer = csv.writer(output)

        writer.writerow(["Snapshot Date", "Contract Date", "Product", "Price", "Type"])
        yield output.getvalue()
        output.seek(0)
        output.truncate(0)

        for row in snapshots:
            row_type = "Spline" if row.is_interpolated else "Raw Block"
            writer.writerow([
                row.snapshot_date,
                row.contract_date,
                row.product_type,
                row.price,
                row_type
            ])
            yield output.getvalue()
            output.seek(0)
            output.truncate(0)

    return StreamingResponse(
        iter_csv(),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={product}_forecast.csv"}
    )
