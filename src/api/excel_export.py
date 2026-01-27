from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session
from src.db.schema import SessionLocal
from src.db.access import MarketRepository
import csv
import io

router = APIRouter(prefix="/api/v1/forecast/export")

# Dependency for DB
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def verify_token(token: str):
    # In real app, check against a valid token store or JWT
    if token != "finance-secret-2026":
        raise HTTPException(status_code=401, detail="Invalid token")
    return True

@router.get("/excel")
def export_excel_csv(
    token: str = Query(..., description="API Key for Power Query Access"),
    product: str = "NBSK",
    db: Session = Depends(get_db)
):
    """
    Stream a CSV optimized for Excel/Power Query.
    """
    verify_token(token)
    
    repo = MarketRepository(db)
    # Default to latest curve for Excel report
    snapshots = repo.get_latest_curve(product)
    
    # Create a generator for streaming
    def iter_csv():
        output = io.StringIO()
        writer = csv.writer(output)
        
        # Write Header
        writer.writerow(["Snapshot Date", "Contract Date", "Product", "Price", "Type"])
        
        # Yield Header
        yield output.getvalue()
        output.seek(0)
        output.truncate(0)
        
        # Write Rows
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
