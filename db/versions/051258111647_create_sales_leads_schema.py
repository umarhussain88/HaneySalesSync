"""create sales_leads schema

Revision ID: 051258111647
Revises: 
Create Date: 2023-11-05 12:45:49.336871

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "051258111647"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("CREATE SCHEMA IF NOT EXISTS sales_leads;")


def downgrade() -> None:
    op.execute("DROP SCHEMA IF EXISTS sales_leads;")
