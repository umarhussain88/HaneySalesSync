"""create view and schema for shopify

Revision ID: 886bb5d494a6
Revises: 679ec5cf223a
Create Date: 2023-11-05 22:33:16.550442

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from dotenv import load_dotenv
import os

load_dotenv()


# revision identifiers, used by Alembic.
revision: str = '886bb5d494a6'
down_revision: Union[str, None] = '679ec5cf223a'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    if os.environ.get('FUNCTIONS_ENVIRONMENT') == 'preview':
        op.execute('CREATE SCHEMA IF NOT EXISTS dm_shopify;')
        op.execute("CREATE OR REPLACE VIEW dm_shopify.sales_customer_view AS SELECT 'test' AS EMAIL;")


def downgrade() -> None:
    if os.environ.get('FUNCTIONS_ENVIRONMENT') == 'preview':
        op.execute('DROP VIEW dm_shopify.sales_customer_view;')
        op.execute('DROP SCHEMA IF EXISTS dm_shopify CASCADE;')
        
    
