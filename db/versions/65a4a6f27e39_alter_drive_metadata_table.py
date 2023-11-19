"""alter drive_metadata_table

Revision ID: 65a4a6f27e39
Revises: 886bb5d494a6
Create Date: 2023-11-11 20:07:36.737353

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "65a4a6f27e39"
down_revision: Union[str, None] = "886bb5d494a6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        table_name="drive_metadata",
        schema="sales_leads",
        column=sa.Column(
            "config_file_uuid", sa.dialects.postgresql.UUID(), nullable=True
        ),
    )

    op.add_column(
        table_name="drive_metadata",
        schema="sales_leads",
        column=sa.Column("hubspot_owner", sa.String(55), nullable=True),
    )
    op.add_column(
        table_name="drive_metadata",
        schema="sales_leads",
        column=sa.Column("zi_search", sa.String(55), nullable=True),
    )

    op.add_column(
        table_name="drive_metadata",
        schema="sales_leads",
        column=sa.Column("updated_at", sa.DateTime, nullable=True),
    )
    
    op.add_column(
        table_name="drive_metadata",
        schema="sales_leads",
        column=sa.Column("has_posted_on_slack", sa.Boolean, nullable=True, default=False),
    )


def downgrade() -> None:
    op.drop_column(
        table_name="drive_metadata",
        schema="sales_leads",
        column_name="config_file_uuid",
    )
    op.drop_column(
        table_name="drive_metadata", schema="sales_leads", column_name="hubspot_owner"
    )
    op.drop_column(
        table_name="drive_metadata", schema="sales_leads", column_name="zi_search"
    )
    
    op.drop_column(
        table_name="drive_metadata", schema="sales_leads", column_name="updated_at"
    )
    
    op.drop_column(
        table_name="drive_metadata", schema="sales_leads", column_name="has_posted_on_slack")
    

