"""altering tracking table to make lead_uuid nullable and adding city_search_lead_uuid

Revision ID: 62634693b64a
Revises: c29406c78c93
Create Date: 2024-01-10 13:42:52.210317

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '62634693b64a'
down_revision: Union[str, None] = 'c29406c78c93'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.alter_column(
        table_name="tracking",
        column_name="lead_uuid",
        nullable=True,
        schema='sales_leads'
    )
    op.add_column(
        "tracking",
        sa.Column("city_search_lead_uuid", sa.dialects.postgresql.UUID(),sa.ForeignKey("sales_leads.city_search_enriched.uuid") , nullable=True),
        schema='sales_leads'
    ) 
    


def downgrade() -> None:
    op.drop_column(table_name="tracking", column_name="city_search_lead_uuid", schema='sales_leads')
