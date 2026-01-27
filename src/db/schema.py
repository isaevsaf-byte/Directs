from sqlalchemy import Column, Integer, String, Date, Float, Boolean, create_engine, ForeignKey, Index
from sqlalchemy.orm import DeclarativeBase, sessionmaker
from datetime import date

class Base(DeclarativeBase):
    pass

class MarketSnapshot(Base):
    """
    Fact table storing daily curve values and raw contract data.
    Enables 'Time Machine' analysis by querying on snapshot_date.
    """
    __tablename__ = 'fact_market_snapshot'

    id = Column(Integer, primary_key=True)

    # Temporal Dimensions
    snapshot_date = Column(Date, nullable=False, index=True, doc="The date this data was known/scraped")
    contract_date = Column(Date, nullable=False, index=True, doc="The future date the price refers to")

    # Product Dimensions
    product_type = Column(String(10), nullable=False, index=True) # NBSK, BEK

    # Metrcs
    price = Column(Float, nullable=False)

    # Metadata
    is_interpolated = Column(Boolean, default=False, doc="True if this point is from the spline, False if raw contract")
    source_ticker = Column(String(50), nullable=True, doc="Original ticker if raw data")

    # Composite Index for fast retrieval of a curve for a specific snapshot
    __table_args__ = (
        Index('idx_snapshot_product', 'snapshot_date', 'product_type'),
    )


class ForecastAccuracy(Base):
    """
    Tracks forecast accuracy over time for model improvement.
    Stores predictions and allows comparison against realized prices.
    """
    __tablename__ = 'fact_forecast_accuracy'

    id = Column(Integer, primary_key=True)

    # When the prediction was made
    prediction_date = Column(Date, nullable=False, index=True)

    # What date was being predicted
    target_date = Column(Date, nullable=False, index=True)

    # Product
    product_type = Column(String(10), nullable=False, index=True)

    # The prediction
    predicted_price = Column(Float, nullable=False)

    # The actual price (filled in when realized)
    actual_price = Column(Float, nullable=True)

    # Error metrics (calculated after realization)
    error = Column(Float, nullable=True, doc="predicted - actual")
    error_pct = Column(Float, nullable=True, doc="(predicted - actual) / actual * 100")

    # Model metadata
    model_version = Column(String(50), nullable=True, doc="Which model generated this forecast")
    forecast_horizon_days = Column(Integer, nullable=True, doc="Days between prediction and target")

    # Ensemble component info
    futures_weight = Column(Float, nullable=True)
    statistical_weight = Column(Float, nullable=True)
    mean_reversion_weight = Column(Float, nullable=True)

    __table_args__ = (
        Index('idx_forecast_prediction', 'prediction_date', 'product_type'),
        Index('idx_forecast_target', 'target_date', 'product_type'),
    )


class RealizedPrice(Base):
    """
    Stores actual realized PIX prices for backtesting and accuracy calculation.
    """
    __tablename__ = 'dim_realized_price'

    id = Column(Integer, primary_key=True)

    price_date = Column(Date, nullable=False, index=True)
    product_type = Column(String(10), nullable=False, index=True)

    # Actual PIX values
    price = Column(Float, nullable=False)
    source = Column(String(50), default="Fastmarkets PIX")

    __table_args__ = (
        Index('idx_realized_date_product', 'price_date', 'product_type', unique=True),
    )

    def __repr__(self):
        return f"<RealizedPrice(date={self.price_date}, {self.product_type}=${self.price})>"

# Database Connection Helper
# In a real app, URL comes from env vars
DB_URL = "sqlite:///./pulp_market.db" # Defaulting to SQLite for local dev ease, easily swapped for Postgres
engine = create_engine(DB_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def init_db():
    Base.metadata.create_all(bind=engine)
