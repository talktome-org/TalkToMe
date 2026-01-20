"""Drop partner/link tables and add friends tables (testing reset).

This migration removes the single-partner linking system and introduces a minimal
"friends via 4-digit code" schema.
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect
from sqlalchemy.dialects import postgresql


revision = "20260119_0004"
down_revision = "20260113_0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    insp = inspect(bind)

    public_tables = set(insp.get_table_names(schema="public"))

    # Remove FK column that points to linked_sessions.
    if "user_chat_sessions" in public_tables:
        cols = {c["name"] for c in insp.get_columns("user_chat_sessions", schema="public")}
        if "linked_session_id" in cols:
            # Drop FK constraint if it exists (name may vary across environments).
            fks = insp.get_foreign_keys("user_chat_sessions", schema="public")
            fk_names = [fk.get("name") for fk in fks if fk.get("constrained_columns") == ["linked_session_id"]]
            with op.batch_alter_table("user_chat_sessions", schema="public") as batch:
                for name in fk_names:
                    if name:
                        batch.drop_constraint(name, type_="foreignkey")
                batch.drop_column("linked_session_id")

    # Drop old partner/link tables (order matters because of foreign keys).
    for table in ["partner_requests", "linked_sessions", "link_invites", "paired_accounts"]:
        if table in public_tables:
            op.drop_table(table, schema="public")

    # New friends tables.
    if "friend_codes" not in public_tables:
        op.create_table(
            "friend_codes",
            sa.Column("user_id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
            sa.Column("code", sa.Text(), nullable=False),
            sa.Column("expires_at", sa.DateTime(timezone=False), nullable=False),
            sa.Column("updated_at", sa.DateTime(timezone=False), nullable=True),
            sa.ForeignKeyConstraint(["user_id"], ["auth.users.id"], name="friend_codes_user_id_fkey"),
            schema="public",
        )
        op.create_index("friend_codes_code_key", "friend_codes", ["code"], unique=True, schema="public")
        op.create_index("friend_codes_expires_at_idx", "friend_codes", ["expires_at"], schema="public")

    if "friendships" not in public_tables:
        op.create_table(
            "friendships",
            sa.Column(
                "id",
                postgresql.UUID(as_uuid=True),
                primary_key=True,
                nullable=False,
                server_default=sa.text("gen_random_uuid()"),
            ),
            sa.Column("user_low_id", postgresql.UUID(as_uuid=True), nullable=False),
            sa.Column("user_high_id", postgresql.UUID(as_uuid=True), nullable=False),
            sa.Column("status", sa.Text(), nullable=False, server_default=sa.text("'accepted'::text")),
            sa.Column("created_at", sa.DateTime(timezone=False), nullable=True, server_default=sa.text("now()")),
            sa.ForeignKeyConstraint(["user_low_id"], ["auth.users.id"], name="friendships_user_low_id_fkey"),
            sa.ForeignKeyConstraint(["user_high_id"], ["auth.users.id"], name="friendships_user_high_id_fkey"),
            schema="public",
        )
        op.create_index("friendships_user_low_id_idx", "friendships", ["user_low_id"], schema="public")
        op.create_index("friendships_user_high_id_idx", "friendships", ["user_high_id"], schema="public")
        op.create_unique_constraint(
            "friendships_user_low_id_user_high_id_key",
            "friendships",
            ["user_low_id", "user_high_id"],
            schema="public",
        )


def downgrade() -> None:
    # Testing reset: no downgrade.
    pass

