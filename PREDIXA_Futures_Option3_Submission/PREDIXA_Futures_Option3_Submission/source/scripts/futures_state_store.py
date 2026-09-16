"""Dedicated tables on the existing SQLAlchemy engine; no legacy schema edits.
Call initialize ONLY as an explicit migration/bootstrap step. Never at reconnect.
"""
import json
from contextlib import contextmanager
from sqlalchemy import MetaData, Table, Column, Integer, Text, select, insert, update

metadata = MetaData()
state = Table('graduated_live_state', metadata,
              Column('id', Integer, primary_key=True), Column('body', Text, nullable=False))
audit = Table('graduated_live_audit', metadata,
              Column('id', Integer, primary_key=True, autoincrement=True),
              Column('body', Text, nullable=False))

class StateStore:
    def __init__(self, engine):
        self.engine = engine

    def initialize(self):
        """Create the dedicated tables as an explicit migration/bootstrap step.

        This method is never called by an order path.  If the migration has not
        been run, reads and reservations fail closed instead of silently creating
        a fresh zeroed state.
        """
        metadata.create_all(self.engine)
        with self.engine.begin() as conn:
            if conn.execute(select(state.c.id)).first() is None:
                conn.execute(insert(state).values(id=1, body=json.dumps(dict(
                    version=1, mode='OPTION3_SERIAL', state='IDLE',
                    dd='0', cash_loss='0', futures_loss='0', tap=False, halt=False,
                    reconciled=False, positions={}, reservations={}, consumed=[],
                    approved={}, gates={}, gate_evidence={}, events=[], completed={}, reservation_history={},
                    serial_reservation=None, active_winner=None, broker_orders={},
                    cancel_requested=False, last_reconciliation=None,
                    global_halt_reason=None, errors=[]))))

    @contextmanager
    def transaction(self):
        with self.engine.connect() as conn:
            if self.engine.dialect.name == 'sqlite':
                conn.exec_driver_sql('BEGIN IMMEDIATE')
            else:
                conn.begin()
            try:
                raw = conn.execute(
                    select(state.c.body).where(state.c.id == 1).with_for_update()
                ).scalar_one()
                data = json.loads(raw)
                self._normalize(data)
                yield data, conn
                conn.execute(update(state).where(state.c.id == 1).values(body=json.dumps(data, allow_nan=False)))
                conn.commit()
            except BaseException:
                conn.rollback()
                raise

    def snapshot(self):
        with self.transaction() as (data, _):
            return data

    def sync_owner_approved_gates(self):
        """Synchronize the configured owner approval into durable state.

        Gate validation is diagnostic only; approval readiness reads this
        durable map.  No other futures state is changed here.
        """
        from scripts.graduated_live_config import OWNER_APPROVED_GATES

        with self.transaction() as (data, _):
            data.setdefault('gates', {}).update(OWNER_APPROVED_GATES)
            return dict(data['gates'])

    @staticmethod
    def _normalize(data):
        """Upgrade first-pass state documents without resetting any state."""
        defaults = {
            'version': 1, 'mode': 'OPTION3_SERIAL', 'state': 'IDLE',
            'serial_reservation': None, 'active_winner': None, 'broker_orders': {},
            'reservation_history': {},
            'gate_evidence': {},
            'winner_exit_confirmed': False, 'winner_exit_signal_id': None,
            'winner_exit_event_id': None,
            'recovery_possible': False, 'broker_state_verified': False,
            'reconciliation_reports': [],
            'cancel_requested': False, 'last_reconciliation': None,
            'global_halt_reason': None, 'errors': [],
        }
        for key, value in defaults.items():
            data.setdefault(key, value)
        return data

    def require_initialized(self):
        """Raise a clear error if the explicit migration has not run."""
        with self.engine.connect() as conn:
            if conn.execute(select(state.c.id).where(state.c.id == 1)).first() is None:
                raise RuntimeError('graduated_live_state migration is not initialized')

    def mark_error(self, reason):
        with self.transaction() as (data, _):
            data['state'] = 'ERROR_LOCKED'
            data.setdefault('errors', []).append(str(reason))
            data['reconciled'] = False

    @staticmethod
    def log(conn, record):
        conn.execute(insert(audit).values(body=json.dumps(record, default=str, allow_nan=False)))
