from __future__ import annotations

import sys
import importlib
import pkgutil
from logging.config import fileConfig
from pathlib import Path

from alembic import context
from sqlalchemy import engine_from_config, pool, text

# Ensure project root is importable so we can import Backend.* as a package/namespace package.
_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_ROOT))

from Backend.database import DATABASE_URL, Base  # noqa: E402

def _import_all_model_modules() -> None:
    """
    Ensure all SQLAlchemy models are imported so they register with Base.metadata.

    We avoid maintaining a manual import list and we don't require __init__.py files
    (namespace packages are fine).
    """
    models_pkg = importlib.import_module("Backend.models")
    for modinfo in pkgutil.walk_packages(models_pkg.__path__, prefix="Backend.models."):  # type: ignore[attr-defined]
        importlib.import_module(modinfo.name)


_import_all_model_modules()

config = context.config

# Interpret the config file for Python logging.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def _quote_ident(value: str) -> str:
    # Defensive quoting for identifiers (table names) to avoid SQL injection issues.
    return '"' + value.replace('"', '""') + '"'


def _auto_enable_rls_on_public_tables(connection) -> None:
    """
    Supabase marks tables as "UNRESTRICTED" when RLS is disabled.
    Since the iOS app ships with an anon key, we default to enabling RLS on *all* public tables.

    This does NOT break the backend: the DB role used by the backend (typically the table owner / postgres)
    bypasses RLS unless FORCE ROW LEVEL SECURITY is enabled.
    """
    rows = connection.execute(
        text(
            """
            SELECT c.relname
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'public'
              AND c.relkind = 'r'
              AND c.relrowsecurity IS FALSE
            """
        )
    ).fetchall()

    for (table_name,) in rows:
        connection.execute(
            text(f'ALTER TABLE "public".{_quote_ident(table_name)} ENABLE ROW LEVEL SECURITY;')
        )


def run_migrations_offline() -> None:
    context.configure(
        url=DATABASE_URL,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    configuration = config.get_section(config.config_ini_section) or {}
    configuration["sqlalchemy.url"] = DATABASE_URL

    connectable = engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
        )

        with context.begin_transaction():
            context.run_migrations()
            _auto_enable_rls_on_public_tables(connection)


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()


