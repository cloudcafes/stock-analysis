import uuid
from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, String, Float, Boolean, DateTime, ForeignKey, UniqueConstraint
from sqlalchemy.orm import declarative_base, sessionmaker

Base = declarative_base()

class SystemConfig(Base):
    __tablename__ = 'system_config'
    id = Column(Integer, primary_key=True, autoincrement=True)
    active_feature_version = Column(Integer, nullable=False, default=1)
    is_live_trading_enabled = Column(Boolean, default=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class Asset(Base):
    __tablename__ = 'assets'
    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    symbol = Column(String(50), unique=True)
    asset_type = Column(String(20)) # 'INDEX', 'SECTOR', 'STOCK'
    sector_name = Column(String(50))

class DailyOHLCV(Base):
    __tablename__ = 'daily_ohlcv'
    id = Column(Integer, primary_key=True, autoincrement=True)
    time = Column(DateTime, nullable=False)
    asset_id = Column(String(36), ForeignKey('assets.id'))
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(Float)
    __table_args__ = (UniqueConstraint('time', 'asset_id', name='uix_daily_ohlcv'),)

class StockFeature(Base):
    __tablename__ = 'stock_features'
    id = Column(Integer, primary_key=True, autoincrement=True)
    time = Column(DateTime, nullable=False)
    asset_id = Column(String(36), ForeignKey('assets.id'))
    feature_version = Column(Integer, nullable=False)
    ema_20 = Column(Float)
    rs_10 = Column(Float)
    compression_ratio = Column(Float)
    phase_state = Column(String(50))
    __table_args__ = (UniqueConstraint('time', 'asset_id', 'feature_version', name='uix_stock_features'),)

class SectorFeature(Base):
    __tablename__ = 'sector_features'
    id = Column(Integer, primary_key=True, autoincrement=True)
    time = Column(DateTime, nullable=False)
    sector_id = Column(String(36), ForeignKey('assets.id'))
    feature_version = Column(Integer, nullable=False)
    breadth_percentile = Column(Float)
    dispersion_score = Column(Float)
    rs_vs_market = Column(Float)
    sector_score = Column(Float)
    __table_args__ = (UniqueConstraint('time', 'sector_id', 'feature_version', name='uix_sector_features'),)

class ClusterHistory(Base):
    __tablename__ = 'cluster_history'
    id = Column(Integer, primary_key=True, autoincrement=True)
    time = Column(DateTime, nullable=False)
    sector_id = Column(String(36), ForeignKey('assets.id'))
    feature_version = Column(Integer, nullable=False)
    cluster_id = Column(Integer, nullable=False)
    __table_args__ = (UniqueConstraint('time', 'sector_id', 'feature_version', name='uix_cluster_history'),)

class ExecutionState(Base):
    __tablename__ = 'execution_state'
    trade_id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    client_order_id = Column(String(36), unique=True)
    asset_id = Column(String(36), ForeignKey('assets.id'))
    status = Column(String(30)) 
    ordered_qty = Column(Integer)
    filled_qty = Column(Integer, default=0)
    average_fill_price = Column(Float)
    entry_cluster_id = Column(Integer)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

engine = create_engine('sqlite:///trading_engine.db', echo=False)
Base.metadata.create_all(engine)
SessionLocal = sessionmaker(bind=engine)