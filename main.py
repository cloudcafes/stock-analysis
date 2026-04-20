import time
from models import SessionLocal, SystemConfig, Asset
from data_engine import (
    parse_and_seed_assets, 
    ingest_market_data, 
    compute_stock_features, 
    compute_sector_features, 
    compute_clusters, 
    validate_features,
    commit_staging_to_production,
    ValidationError
)
from execution import (
    PIPELINE_LATENCY, 
    REGIME_GAUGE, 
    MockBrokerAPI, 
    run_broker_reconciliation
)
from prometheus_client import start_http_server

@PIPELINE_LATENCY.time()
def run_pipeline():
    """Strict execution DAG with validation blocks."""
    db = SessionLocal()
    try:
        # 1. Global State Retrieval
        config = db.query(SystemConfig).first()
        if not config:
            config = SystemConfig(active_feature_version=1, is_live_trading_enabled=True)
            db.add(config)
            db.commit()
            
        active_version = config.active_feature_version
        print(f"\n--- Starting Execution DAG (Feature Version: {active_version}) ---")
        
        # 2. Ingest Immutable Data
        raw_data = ingest_market_data()
        if raw_data.empty:
            print("No data fetched. Aborting pipeline.")
            return

        # 3. Stage 1: Stock Features
        stock_features = compute_stock_features(raw_data, active_version)
        validate_features(stock_features, "Stock Features") 
        
        # 4. Stage 2: Sector Features & Dispersion
        sector_features = compute_sector_features(stock_features, active_version)
        validate_features(sector_features, "Sector Features")
        
        # 5. Stage 3: Clustering Mapping
        cluster_map = compute_clusters(sector_features)
        validate_features(cluster_map, "Matrix Clustering")
        
        # 6. Commit Staging to Prod
        commit_staging_to_production()
        
        # Update Telemetry (Mocking a healthy 85% regime)
        REGIME_GAUGE.set(0.85)

    except ValidationError as e:
        print(f"\n[CRITICAL HALT] {str(e)}")
        print("Rolling back staging data. Execution engine shielded.")
    except Exception as e:
        print(f"\n[SYSTEM ERROR] {str(e)}")
    finally:
        db.close()

def run_execution_loop():
    """Simulates the 15-minute execution and reconciliation loop."""
    db = SessionLocal()
    broker = MockBrokerAPI()
    try:
        print("\n--- Running Intraday Execution & Reconciliation ---")
        run_broker_reconciliation(db, broker)
        print("Broker states reconciled.")
    finally:
        db.close()

if __name__ == "__main__":
    # 1. Start Observability
    print("Starting Prometheus Metrics Server on Port 8000...")
    start_http_server(8000)
    
    # 2. Setup Data
    print("Parsing Stock List and Seeding SQLite...")
    parse_and_seed_assets("sector-stock-list.txt")
    
    # 3. Execute Engine
    run_pipeline()
    run_execution_loop()
    
    print("\nSystem Online & Waiting. View metrics at http://localhost:8000")
    while True:
        time.sleep(60)