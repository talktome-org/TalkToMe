"""Enable RLS on tables that should not be publicly readable via Supabase APIs.

Supabase shows "UNRESTRICTED" when Row Level Security is disabled. Since the iOS
app ships with the Supabase anon key, any table with RLS disabled can be read/written
via Supabase REST unless you lock it down.

We keep this minimal: ENABLE RLS (no FORCE) so it blocks anon/authenticated access,
while not interfering with the backend's privileged DB role.
"""

from __future__ import annotations

from alembic import op
from sqlalchemy import inspect


revision = "20260111_0002"
down_revision = "20260111_0001"
branch_labels = None
depends_on = None


def _enable_rls_if_exists(schema: str, table: str) -> None:
    bind = op.get_bind()
    insp = inspect(bind)
    tables = set(insp.get_table_names(schema=schema))
    if table not in tables:
        return

    op.execute(f'ALTER TABLE "{schema}"."{table}" ENABLE ROW LEVEL SECURITY;')


def upgrade() -> None:
    _enable_rls_if_exists("public", "uploads")
    _enable_rls_if_exists("public", "alembic_version")


def downgrade() -> None:
    bind = op.get_bind()
    insp = inspect(bind)
    public_tables = set(insp.get_table_names(schema="public"))

    if "uploads" in public_tables:
        op.execute('ALTER TABLE "public"."uploads" DISABLE ROW LEVEL SECURITY;')
    if "alembic_version" in public_tables:
        op.execute('ALTER TABLE "public"."alembic_version" DISABLE ROW LEVEL SECURITY;')

