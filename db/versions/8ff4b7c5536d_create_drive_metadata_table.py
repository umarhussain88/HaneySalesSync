"""create drive_metadata table

Revision ID: 8ff4b7c5536d
Revises: 051258111647
Create Date: 2023-11-05 12:49:23.892117

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '8ff4b7c5536d'
down_revision: Union[str, None] = '051258111647'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'drive_metadata',
        sa.Column('uuid', sa.dialects.postgresql.UUID(), primary_key=True),
        sa.Column('fileExtension', sa.String(3)),
        sa.Column('parents', sa.String(37)),
        sa.Column('owners', sa.String(222)),
        sa.Column('id', sa.String(33)),
        sa.Column('name', sa.String(46)),
        sa.Column('createdTime', sa.String(24)),
        sa.Column('modifiedTime', sa.String(24)),
        sa.Column('lastModifyingUser.displayName', sa.String(12)),
        sa.Column('lastModifyingUser.kind', sa.String(10)),
        sa.Column('lastModifyingUser.me', sa.String(5)),
        sa.Column('lastModifyingUser.permissionId', sa.String(20)),
        sa.Column('lastModifyingUser.emailAddress', sa.String(25)),
        sa.Column('lastModifyingUser.photoLink', sa.String(52)),
        sa.Column('created_at', sa.DateTime),
        sa.Index('created_at_idx', 'created_at'),
        schema='sales_leads'
    )

def downgrade() -> None:
    op.drop_table('drive_metadata', schema='sales_leads')
