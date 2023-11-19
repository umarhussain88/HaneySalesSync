"""create tracking table for leads

Revision ID: 679ec5cf223a
Revises: e00d0f812076
Create Date: 2023-11-05 21:48:00.309932

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
 

# revision identifiers, used by Alembic.
revision: str = "679ec5cf223a"
down_revision: Union[str, None] = "e00d0f812076"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    status_enum = sa.dialects.postgresql.ENUM(
        "posted",
        "emailed",
        "shopify_customer",
        "duplicate_entry",
        name="status_enum",
        create_type=False,
    )
    status_enum.create(op.get_bind())

    op.create_table(
        "tracking",
        sa.Column(
            "uuid",
            sa.dialects.postgresql.UUID(),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "lead_uuid", sa.dialects.postgresql.UUID(), sa.ForeignKey("sales_leads.leads.uuid")
        ),
        sa.Column("status", status_enum, nullable=False),
        sa.Column("email_address", sa.String(255), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        schema='sales_leads'
        
    )


def downgrade():
    op.drop_table("tracking", schema="sales_leads")
    status_enum = sa.dialects.postgresql.ENUM(
        "posted",
        "emailed",
        "shopify_customer",
        "duplicate_entry",
        name="status_enum",
        create_type=False,
    )
    status_enum.drop(op.get_bind()) 
