"""adding city_search table to db

Revision ID: 0afec5b993b3
Revises: 6c5caf8a27a7
Create Date: 2024-01-01 03:34:25.906683

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '0afec5b993b3'
down_revision: Union[str, None] = '6c5caf8a27a7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'city_search',
        sa.Column(
            "uuid",
            sa.dialects.postgresql.UUID(),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("drive_metadata_uuid", sa.dialects.postgresql.UUID(), nullable=False),
        sa.ForeignKeyConstraint(
            ["drive_metadata_uuid"], ["sales_leads.drive_metadata.uuid"]
        ),
        sa.Column('type', sa.String(255)),
        sa.Column('phone', sa.String(255)),
        sa.Column('title', sa.String(255)),
        sa.Column('dataid', sa.String(255)),
        sa.Column('rating', sa.String(255)),
        sa.Column('placeid', sa.String(255)),
        sa.Column('reviews', sa.String(255)),
        sa.Column('website', sa.String(255)),
        sa.Column('position', sa.String(255)),
        sa.Column('thumbnail', sa.String(255)),
        sa.Column('address', sa.String(255)),
        sa.Column('keyword_ll', sa.String(255)),
        sa.Column('keyword_keyword', sa.String(255)),
        sa.Column('gpscoordinates_latitude', sa.String(255)),
        sa.Column('gpscoordinates_longitude', sa.String(255)),
        sa.Column('serviceoptions_0', sa.String(255)),
        sa.Column('emails_0', sa.String(255)),
        sa.Column('serviceoptions_1', sa.String(255)),
        sa.Column('emails_1', sa.String(255)),
        sa.Column('emails_2', sa.String(255)),
        sa.Column('serviceoptions_2', sa.String(255)),
        sa.Column('emails_3', sa.String(255)),
        sa.Column('serviceoptions_3', sa.String(255)),
        sa.Column('serviceoptions_4', sa.String(255)),
        sa.Column('created_at', sa.DateTime),
        sa.Column('updated_at', sa.DateTime, nullable=True),
        schema='sales_leads'
        )


def downgrade() -> None:
    op.drop_table('city_search', schema='sales_leads')
