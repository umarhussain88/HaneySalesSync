"""adding has_been_processed flag on drive_metadata table

Revision ID: 0c0b51d9ddd4
Revises: f39dffce89a8
Create Date: 2024-01-07 20:04:26.105040

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '0c0b51d9ddd4'
down_revision: Union[str, None] = 'f39dffce89a8'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "drive_metadata",
        sa.Column("has_been_processed", sa.Boolean(), nullable=True),
        schema="sales_leads",
    )


def downgrade() -> None:
    op.drop_column("drive_metadata", "has_been_processed", schema="sales_leads")
