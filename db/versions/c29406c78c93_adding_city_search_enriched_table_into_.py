"""adding city search enriched table into db

Revision ID: c29406c78c93
Revises: 0c0b51d9ddd4
Create Date: 2024-01-07 21:38:15.586598

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "c29406c78c93"
down_revision: Union[str, None] = "0c0b51d9ddd4"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "city_search_enriched",
        sa.Column(
            "uuid",
            sa.dialects.postgresql.UUID(),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("type", sa.VARCHAR(512)),
        sa.Column("main_point_of_contact", sa.VARCHAR(512)),
        sa.Column("main_point_of_contact_email", sa.VARCHAR(512)),
        sa.Column("main_contact_linkedin", sa.VARCHAR(512)),
        sa.Column("generic_contact_email", sa.VARCHAR(512)),
        sa.Column("company_name", sa.VARCHAR(512)),
        sa.Column("website", sa.VARCHAR(512)),
        sa.Column("phone", sa.VARCHAR(512)),
        sa.Column("position", sa.VARCHAR(512)),
        sa.Column("reviews", sa.VARCHAR(512)),
        sa.Column("address", sa.VARCHAR(512)),
        sa.Column("rating", sa.VARCHAR(512)),
        sa.Column("dataid", sa.VARCHAR(512)),
        sa.Column("placeid", sa.VARCHAR(512)),
        sa.Column("thumbnail", sa.VARCHAR(512)),
        sa.Column("keyword_ll", sa.VARCHAR(512)),
        sa.Column("keyword_keyword", sa.VARCHAR(512)),
        sa.Column("gpscoordinates_latitude", sa.VARCHAR(512)),
        sa.Column("gpscoordinates_longitude", sa.VARCHAR(512)),
        sa.Column("serviceoptions_0", sa.VARCHAR(512)),
        sa.Column("serviceoptions_1", sa.VARCHAR(512)),
        sa.Column("serviceoptions_2", sa.VARCHAR(512)),
        sa.Column("serviceoptions_3", sa.VARCHAR(512)),
        sa.Column("emails_0", sa.VARCHAR(512)),
        sa.Column("emails_1", sa.VARCHAR(512)),
        sa.Column("emails_2", sa.VARCHAR(512)),
        sa.Column("emails_3", sa.VARCHAR(512)),
        sa.Column("emails_4", sa.VARCHAR(512)),
        sa.Column("created_at", sa.DateTime),
        schema="sales_leads",
    )


def downgrade() -> None:
    op.drop_table("city_search_enriched", schema="sales_leads")
