"""
Database connection and session factory for the async lead management system.
Supports PostgreSQL (via asyncpg/psycopg in Docker/prod) and SQLite (aiosqlite in dev).
"""

import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from leads.models import Base


def get_database_url() -> str:
    url = os.getenv("DATABASE_URL")
    if not url:
        db_dir = Path(os.getenv("DATA_DIR", "outputs")).resolve()
        db_dir.mkdir(parents=True, exist_ok=True)
        return f"sqlite+aiosqlite:///{(db_dir / 'leads.db').as_posix()}"

    # Normalize postgres URL schemes
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql+asyncpg://", 1)
    elif url.startswith("postgresql://") and not url.startswith("postgresql+asyncpg://") and not url.startswith("postgresql+psycopg://"):
        url = url.replace("postgresql://", "postgresql+asyncpg://", 1)
    return url


_async_engine: AsyncEngine | None = None
_async_session_factory: async_sessionmaker[AsyncSession] | None = None


def get_engine() -> AsyncEngine:
    global _async_engine
    if _async_engine is None:
        db_url = get_database_url()
        is_sqlite = db_url.startswith("sqlite")
        engine_kwargs = {"echo": False}
        if not is_sqlite:
            engine_kwargs.update({
                "pool_size": 30,
                "max_overflow": 20,
                "pool_pre_ping": True,
            })
        _async_engine = create_async_engine(db_url, **engine_kwargs)
    return _async_engine


def get_session_factory() -> async_sessionmaker[AsyncSession]:
    global _async_session_factory
    if _async_session_factory is None:
        engine = get_engine()
        _async_session_factory = async_sessionmaker(
            engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )
    return _async_session_factory


@asynccontextmanager
async def get_session() -> AsyncGenerator[AsyncSession, None]:
    factory = get_session_factory()
    async with factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


async def init_models() -> None:
    """Helper to create all tables and migrate new columns."""
    engine = get_engine()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        
        # Lightweight schema migration for SQLite / Postgres if tables already exist
        def _migrate(sync_conn):
            from sqlalchemy import inspect, text
            from loguru import logger
            inspector = inspect(sync_conn)
            table_names = inspector.get_table_names()
            is_postgres = sync_conn.dialect.name == "postgresql"
            timestamp_type = "TIMESTAMP WITH TIME ZONE" if is_postgres else "TIMESTAMP"

            if "site_visits" in table_names:
                existing_cols = {c["name"] for c in inspector.get_columns("site_visits")}
                new_cols = {
                    "call_id": "VARCHAR(255)",
                    "visit_date_iso": "VARCHAR(32)",
                    "visit_date_original": "VARCHAR(64)",
                    "time_slot": "VARCHAR(32)",
                    "configuration": "VARCHAR(64)",
                    "whatsapp_opt_in": "BOOLEAN DEFAULT 0",
                    "whatsapp_opt_in_at": timestamp_type,
                    "whatsapp_status": "VARCHAR(32)",
                    "whatsapp_message_id": "VARCHAR(255)",
                }
                for col_name, col_type in new_cols.items():
                    if col_name not in existing_cols:
                        try:
                            sync_conn.execute(text(f"ALTER TABLE site_visits ADD COLUMN {col_name} {col_type}"))
                        except Exception as exc:
                            logger.warning(f"Schema migration warning for site_visits.{col_name}: {exc}")

            if "outbox" in table_names:
                existing_cols = {c["name"] for c in inspector.get_columns("outbox")}
                new_cols = {
                    "claimed_by": "VARCHAR(255)",
                    "claimed_at": timestamp_type,
                }
                for col_name, col_type in new_cols.items():
                    if col_name not in existing_cols:
                        try:
                            sync_conn.execute(text(f"ALTER TABLE outbox ADD COLUMN {col_name} {col_type}"))
                            logger.info(f"Schema migration: added column {col_name} ({col_type}) to outbox")
                        except Exception as exc:
                            logger.warning(f"Schema migration warning for outbox.{col_name}: {exc}")

        await conn.run_sync(_migrate)


async def ensure_outbox_schema() -> None:
    """Non-destructive, safe to rerun; ensures atomic claiming columns exist on outbox table."""
    from sqlalchemy import inspect, text
    from loguru import logger
    async with get_engine().begin() as conn:
        def _migrate_outbox(sync_conn):
            inspector = inspect(sync_conn)
            if "outbox" in inspector.get_table_names():
                existing_cols = {c["name"] for c in inspector.get_columns("outbox")}
                is_postgres = sync_conn.dialect.name == "postgresql"
                timestamp_type = "TIMESTAMP WITH TIME ZONE" if is_postgres else "TIMESTAMP"
                new_cols = {
                    "claimed_by": "VARCHAR(255)",
                    "claimed_at": timestamp_type,
                }
                for col_name, col_type in new_cols.items():
                    if col_name not in existing_cols:
                        try:
                            sync_conn.execute(text(f"ALTER TABLE outbox ADD COLUMN {col_name} {col_type}"))
                            logger.info(f"Schema migration: added column {col_name} ({col_type}) to outbox")
                        except Exception as exc:
                            logger.warning(f"Schema migration warning for outbox.{col_name}: {exc}")
        await conn.run_sync(_migrate_outbox)


async def ensure_call_sheet_schema() -> None:
    """Non-destructive, safe to rerun; bind matches the queue/session engine."""
    from leads.models import CallSheetExport
    from sqlalchemy import inspect
    async with get_engine().begin() as conn:
        await conn.run_sync(lambda c: CallSheetExport.__table__.create(c, checkfirst=True))
        exists = await conn.run_sync(lambda c: inspect(c).has_table("call_sheet_exports"))
        if not exists:
            raise RuntimeError("call_sheet_exports was not created on the active Leads engine")

