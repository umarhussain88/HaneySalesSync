"""create drive_metadata table

Revision ID: 8ff4b7c5536d
Revises: 051258111647
Create Date: 2023-11-05 12:49:23.892117

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "8ff4b7c5536d"
down_revision: Union[str, None] = "051258111647"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "drive_metadata",
        sa.Column(
            "uuid",
            sa.dialects.postgresql.UUID(),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("fileextension", sa.String(512)),
        sa.Column("parents", sa.String(512)),
        sa.Column("owners", sa.String(512)),
        sa.Column("id", sa.String(512)),
        sa.Column("name", sa.String(512)),
        sa.Column("createdtime", sa.String(512)),
        sa.Column("modifiedtime", sa.String(512)),
        sa.Column("lastmodifyinguser_displayname", sa.String(512)),
        sa.Column("lastmodifyinguser_kind", sa.String(512)),
        sa.Column("lastmodifyinguser_me", sa.String(512)),
        sa.Column("lastmodifyinguser_permissionid", sa.String(512)),
        sa.Column("lastmodifyinguser_emailaddress", sa.String(512)),
        sa.Column("lastmodifyinguser_photolink", sa.String(512)),
        sa.Column("created_at", sa.DateTime),
        sa.Index("created_at_idx", "created_at"),
        schema="sales_leads",
    )


def downgrade() -> None:
    op.drop_table("drive_metadata", schema="sales_leads")


