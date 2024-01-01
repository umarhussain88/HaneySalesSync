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
        sa.Column('type', sa.String(27)),
        sa.Column('phone', sa.String(15)),
        sa.Column('title', sa.String(116)),
        sa.Column('dataid', sa.String(37)),
        sa.Column('rating', sa.String(3)),
        sa.Column('placeid', sa.String(27)),
        sa.Column('reviews', sa.String(3)),
        sa.Column('website', sa.String(71)),
        sa.Column('position', sa.String(3)),
        sa.Column('thumbnail', sa.String(159)),
        sa.Column('address', sa.String(57)),
        sa.Column('keyword_ll', sa.String(27)),
        sa.Column('keyword_keyword', sa.String(45)),
        sa.Column('gpscoordinates_latitude', sa.String(18)),
        sa.Column('gpscoordinates_longitude', sa.String(19)),
        sa.Column('serviceoptions_0', sa.String(33)),
        sa.Column('emails_0', sa.String(40)),
        sa.Column('serviceoptions_1', sa.String(33)),
        sa.Column('emails_1', sa.String(41)),
        sa.Column('emails_2', sa.String(28)),
        sa.Column('serviceoptions_2', sa.String(26)),
        sa.Column('emails_3', sa.String(24)),
        sa.Column('serviceoptions_3', sa.String(27)),
        sa.Column('serviceoptions_4', sa.String(25)),
        sa.Column('created_at', sa.DateTime),
        schema='sales_leads'
        )


def downgrade() -> None:
    op.drop_table('city_search')
