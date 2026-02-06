"""
Excel Export API - Phase 2: Power Query Integration

Provides XLSX streaming endpoints optimized for Excel.
Uses token-based authentication via query parameter (Power Query friendly).
"""

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session
from src.db.schema import SessionLocal
from src.db.access import MarketRepository, RealizedPriceRepository
from datetime import date
import io
import os
import logging
import xlsxwriter

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
                "Date": price_date,
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
                    "Date": snapshot.contract_date,
                    "Ticker": product,
                    "Price": round(float(snapshot.price), 2),
                    "Type": "Forecast"
                })

    # Sort by Date, then Ticker
    rows.sort(key=lambda x: (x["Date"], x["Ticker"]))

    return rows


def create_xlsx_file(rows: list, sheet_name: str = "Data") -> io.BytesIO:
    """
    Create an XLSX file in memory from row data.
    Returns a BytesIO buffer containing the Excel file.
    """
    output = io.BytesIO()

    workbook = xlsxwriter.Workbook(output, {'in_memory': True})
    worksheet = workbook.add_worksheet(sheet_name)

    # Define formats
    header_format = workbook.add_format({
        'bold': True,
        'bg_color': '#4472C4',
        'font_color': 'white',
        'border': 1,
        'align': 'center'
    })

    date_format = workbook.add_format({
        'num_format': 'yyyy-mm-dd',
        'border': 1
    })

    price_format = workbook.add_format({
        'num_format': '#,##0.00',
        'border': 1
    })

    text_format = workbook.add_format({
        'border': 1
    })

    actual_format = workbook.add_format({
        'border': 1,
        'bg_color': '#E2EFDA',  # Light green for actuals
    })

    forecast_format = workbook.add_format({
        'border': 1,
        'bg_color': '#DDEBF7',  # Light blue for forecasts
    })

    # Write headers
    headers = ["Date", "Ticker", "Price", "Type"]
    for col, header in enumerate(headers):
        worksheet.write(0, col, header, header_format)

    # Write data rows
    for row_idx, row_data in enumerate(rows, start=1):
        # Choose row color based on type
        type_val = row_data["Type"]
        bg_format = actual_format if type_val == "Actual" else forecast_format

        # Date column
        worksheet.write_datetime(row_idx, 0, row_data["Date"], date_format)
        # Ticker column
        worksheet.write(row_idx, 1, row_data["Ticker"], bg_format)
        # Price column
        worksheet.write_number(row_idx, 2, row_data["Price"], price_format)
        # Type column
        worksheet.write(row_idx, 3, row_data["Type"], bg_format)

    # Set column widths
    worksheet.set_column('A:A', 12)  # Date
    worksheet.set_column('B:B', 8)   # Ticker
    worksheet.set_column('C:C', 12)  # Price
    worksheet.set_column('D:D', 10)  # Type

    # Add autofilter
    worksheet.autofilter(0, 0, len(rows), 3)

    # Freeze header row
    worksheet.freeze_panes(1, 0)

    workbook.close()
    output.seek(0)

    return output


@router.get("/excel/forecast")
def export_forecast_xlsx(
    token: str = Query(..., description="API token for Excel export access"),
    db: Session = Depends(get_db)
):
    """
    Export combined Historical + Forecast data as XLSX for Excel.

    **Authentication:** Pass token as query parameter.

    **Columns:**
    - Date: Excel date format
    - Ticker: NBSK or BEK
    - Price: Float with 2 decimal places
    - Type: "Actual" (historical PIX) or "Forecast" (forward curve)

    **Features:**
    - Formatted headers with filters
    - Color-coded rows (green=Actual, blue=Forecast)
    - Frozen header row
    - Ready for Pivot Tables
    """
    verify_token(token)

    logger.info("Excel XLSX export requested - building flat table")
    rows = build_flat_table(db)
    logger.info(f"Excel export: {len(rows)} rows prepared")

    xlsx_buffer = create_xlsx_file(rows, "Pulp Forecast Data")

    return StreamingResponse(
        xlsx_buffer,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": "attachment; filename=pulp_forecast_data.xlsx",
            "Cache-Control": "no-cache"
        }
    )


@router.get("/excel/historical")
def export_historical_xlsx(
    token: str = Query(..., description="API token for Excel export access"),
    product: str = Query(default="NBSK", description="Product: NBSK or BEK"),
    db: Session = Depends(get_db)
):
    """
    Export historical PIX prices only as XLSX.

    Useful for historical analysis without forecast data.
    """
    verify_token(token)

    realized_repo = RealizedPriceRepository(db)
    prices = realized_repo.get_realized_prices(product)

    rows = [
        {
            "Date": price_date,
            "Ticker": product,
            "Price": round(float(price), 2),
            "Type": "Actual"
        }
        for price_date, price in prices.items()
    ]
    rows.sort(key=lambda x: x["Date"])

    xlsx_buffer = create_xlsx_file(rows, f"{product} Historical")

    return StreamingResponse(
        xlsx_buffer,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f"attachment; filename={product}_historical.xlsx",
            "Cache-Control": "no-cache"
        }
    )


@router.get("/excel/curve")
def export_curve_xlsx(
    token: str = Query(..., description="API token for Excel export access"),
    product: str = Query(default="NBSK", description="Product: NBSK or BEK"),
    db: Session = Depends(get_db)
):
    """
    Export the latest forward curve only as XLSX.

    Includes daily interpolated prices from the spline.
    """
    verify_token(token)

    market_repo = MarketRepository(db)
    curve = market_repo.get_latest_curve(product)

    rows = [
        {
            "Date": s.contract_date,
            "Ticker": product,
            "Price": round(float(s.price), 2),
            "Type": "Forecast"
        }
        for s in curve
    ]

    xlsx_buffer = create_xlsx_file(rows, f"{product} Forward Curve")

    return StreamingResponse(
        xlsx_buffer,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f"attachment; filename={product}_curve.xlsx",
            "Cache-Control": "no-cache"
        }
    )


# CSV endpoint for backward compatibility
@router.get("/csv/forecast")
def export_forecast_csv(
    token: str = Query(..., description="API token for export access"),
    db: Session = Depends(get_db)
):
    """
    Export combined Historical + Forecast data as CSV (legacy format).
    """
    import csv

    verify_token(token)
    rows = build_flat_table(db)

    def generate_csv():
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=["Date", "Ticker", "Price", "Type"])
        writer.writeheader()
        yield output.getvalue()
        output.seek(0)
        output.truncate(0)

        for row in rows:
            row_copy = row.copy()
            row_copy["Date"] = row_copy["Date"].strftime("%Y-%m-%d")
            writer.writerow(row_copy)
            yield output.getvalue()
            output.seek(0)
            output.truncate(0)

    return StreamingResponse(
        generate_csv(),
        media_type="text/csv",
        headers={
            "Content-Disposition": "attachment; filename=pulp_forecast_data.csv",
            "Cache-Control": "no-cache"
        }
    )
