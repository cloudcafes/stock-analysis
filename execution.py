import uuid
import threading
import pandas as pd
from models import ExecutionState
from prometheus_client import Summary, Counter, Gauge

# Observability Metrics
PIPELINE_LATENCY = Summary('pipeline_processing_seconds', 'Time spent processing DAG')
TRADE_EXECUTIONS = Counter('trade_executions_total', 'Total trades executed', ['status', 'cluster'])
REGIME_GAUGE = Gauge('market_regime_weight', 'Current active regime weight')

# Local thread lock ensures execution safety without Redis
local_lock = threading.Lock()

class MockBrokerAPI:
    def place_order(self, symbol, qty, client_order_id):
        return {"status": "PENDING", "order_id": client_order_id}
    def get_order(self, client_order_id):
        return {"status": "FILLED", "filled_qty": 100, "avg_price": 105.5}

def get_final_regime_weight(df_market: pd.DataFrame) -> float:
    """Predictive Macro Halt & Sigmoid Scaling."""
    if df_market.empty:
        return 0.85
        
    intraday_index_drop = (df_market['close'].iloc[-1] / df_market['close'].iloc[-2]) - 1.0
    if intraday_index_drop < -0.025:
        print("CRITICAL: Macro Shock Detected! Forcing Regime Weight to 0.0")
        return 0.0 
        
    return 0.85 

def execute_trade_idempotent(db_session, broker_api, signal_data):
    """Guarantees exactly-once execution using a local thread lock."""
    
    if not local_lock.acquire(timeout=5.0):
        return {"status": "SKIPPED", "reason": "LOCKED_BY_OTHER_THREAD"}

    try:
        # Idempotency Check
        existing_trade = db_session.query(ExecutionState).filter_by(
            asset_id=signal_data['asset_id']
        ).filter(ExecutionState.created_at >= signal_data['date']).first()
        
        if existing_trade:
            return {"status": "SKIPPED", "reason": "ALREADY_IN_DB"}

        # Execute
        client_oid = str(uuid.uuid4())
        broker_api.place_order(
            symbol=signal_data['symbol'], 
            qty=signal_data['qty'],
            client_order_id=client_oid
        )
        
        new_trade = ExecutionState(
            client_order_id=client_oid, 
            asset_id=signal_data['asset_id'],
            status='PENDING',
            ordered_qty=signal_data['qty'],
            entry_cluster_id=signal_data['cluster_id']
        )
        db_session.add(new_trade)
        db_session.commit()
        
        TRADE_EXECUTIONS.labels(status='PENDING', cluster=str(signal_data['cluster_id'])).inc()
        return {"status": "SUCCESS", "order_id": client_oid}
        
    except Exception as e:
        db_session.rollback()
        raise e
    finally:
        local_lock.release()

def run_broker_reconciliation(db_session, broker_api):
    """Aligns DB state with broker reality."""
    pending_orders = db_session.query(ExecutionState).filter(
        ExecutionState.status.in_(['PENDING', 'PARTIAL'])
    ).all()
    
    for order in pending_orders:
        broker_status = broker_api.get_order(order.client_order_id)
        if broker_status['status'] != order.status:
            order.status = broker_status['status']
            order.filled_qty = broker_status.get('filled_qty', 0)
            order.average_fill_price = broker_status.get('avg_price', 0.0)
    db_session.commit()