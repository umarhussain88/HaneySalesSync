"""adding new file_type enumeration

Revision ID: f39dffce89a8
Revises: b66353296d11
Create Date: 2024-01-07 18:09:53.772535

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'f39dffce89a8'
down_revision: Union[str, None] = 'b66353296d11'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

def upgrade() -> None:
    pass # need to find a safe way to remove enum types, then add them back in.
    # for now will ignore this migration.
    # op.execute("ALTER TYPE file_type_enum ADD VALUE 'city_search_enriched';")

def downgrade() -> None:
    pass
