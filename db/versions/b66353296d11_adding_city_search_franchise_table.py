"""adding city search franchise table

Revision ID: b66353296d11
Revises: 0afec5b993b3
Create Date: 2024-01-02 23:52:58.938651

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'b66353296d11'
down_revision: Union[str, None] = '0afec5b993b3'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
     op.create_table(
        'city_search_franchises',
        sa.Column(
            "uuid",
            sa.dialects.postgresql.UUID(),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column('franchise_name', sa.String(255)),
        sa.Column('domain_name', sa.String(255)),
        sa.Column('created_at', sa.DateTime),
        sa.Column('updated_at', sa.DateTime, nullable=True),
        schema='sales_leads'
    )


def downgrade() -> None:
    op.drop_table('city_search_franchises', schema='sales_leads') 
