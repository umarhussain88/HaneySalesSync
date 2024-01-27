"""adding config file type

Revision ID: 6c5caf8a27a7
Revises: 65a4a6f27e39
Create Date: 2023-12-13 14:23:07.661985

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '6c5caf8a27a7'
down_revision: Union[str, None] = '65a4a6f27e39'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    
    file_type_enum = sa.dialects.postgresql.ENUM(
        "city_search",
        "zi_search",
        name="file_type_enum",
        create_type=False,
    )
    file_type_enum.create(op.get_bind())
    op.add_column(
        table_name="drive_metadata",
        schema="sales_leads",
        column=sa.Column("file_type", file_type_enum, nullable=True)
    )


def downgrade() -> None:
    op.drop_column(
        table_name="drive_metadata",
        schema="sales_leads",
        column_name="file_type")
    
    file_type_enum = sa.dialects.postgresql.ENUM(
        "city_search",
        "zi_search",
        name="file_type_enum",
        create_type=False,
    )
    file_type_enum.drop(op.get_bind())
    
