"""create leads table

Revision ID: e00d0f812076
Revises: 8ff4b7c5536d
Create Date: 2023-11-05 14:05:16.812561

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'e00d0f812076'
down_revision: Union[str, None] = '8ff4b7c5536d'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'leads',
        sa.Column('uuid', sa.dialects.postgresql.UUID(), primary_key=True, nullable=False),
        sa.Column('drive_metadata_uuid', sa.dialects.postgresql.UUID(), nullable=False),
        sa.ForeignKeyConstraint(['drive_metadata_uuid'], ['sales_leads.drive_metadata.uuid']),
        sa.Column('zoominfo_contact_id', sa.String(11)),
        sa.Column('last_name', sa.String(17)),
        sa.Column('first_name', sa.String(11)),
        sa.Column('middle_name', sa.String(8)),
        sa.Column('salutation', sa.String(6)),
        sa.Column('suffix', sa.String(4)),
        sa.Column('job_title', sa.String(101)),
        sa.Column('job_function', sa.String(21)),
        sa.Column('management_level', sa.String(11)),
        sa.Column('company_division_name', sa.String(3)),
        sa.Column('direct_phone_number', sa.String(24)),
        sa.Column('email_address', sa.String(39)),
        sa.Column('email_domain', sa.String(21)),
        sa.Column('department', sa.String(10)),
        sa.Column('mobile_phone', sa.String(14)),
        sa.Column('contact_accuracy_score', sa.String(4)),
        sa.Column('contact_accuracy_grade', sa.String(2)),
        sa.Column('zoominfo_contact_profile_url', sa.String(58)),
        sa.Column('linkedin_contact_profile_url', sa.String(61)),
        sa.Column('notice_provided_date', sa.String(18)),
        sa.Column('person_street', sa.String(41)),
        sa.Column('person_city', sa.String(16)),
        sa.Column('person_state', sa.String(14)),
        sa.Column('person_zip_code', sa.String(10)),
        sa.Column('country', sa.String(21)),
        sa.Column('zoominfo_company_id', sa.String(10)),
        sa.Column('company_name', sa.String(44)),
        sa.Column('website', sa.String(26)),
        sa.Column('founded_year', sa.String(6)),
        sa.Column('company_hq_phone', sa.String(14)),
        sa.Column('fax', sa.String(14)),
        sa.Column('ticker', sa.String(3)),
        sa.Column('revenue_in_000s_usd', sa.String(8)),
        sa.Column('revenue_range_in_usd', sa.String(21)),
        sa.Column('employees', sa.String(5)),
        sa.Column('employee_range', sa.String(20)),
        sa.Column('sic_code_1', sa.String(4)),
        sa.Column('sic_code_2', sa.String(4)),
        sa.Column('sic_codes', sa.String(80)),
        sa.Column('naics_code_1', sa.String(6)),
        sa.Column('naics_code_2', sa.String(6)),
        sa.Column('naics_codes', sa.String(153)),
        sa.Column('primary_industry', sa.String(13)),
        sa.Column('primary_sub_industry', sa.String(37)),
        sa.Column('all_industries', sa.String(40)),
        sa.Column('all_sub_industries', sa.String(85)),
        sa.Column('industry_hierarchical_category', sa.String(10)),
        sa.Column('secondary_industry_hierarchical_category', sa.String(20)),
        sa.Column('alexa_rank', sa.String(10)),
        sa.Column('zoominfo_company_profile_url', sa.String(58)),
        sa.Column('linkedin_company_profile_url', sa.String(90)),
        sa.Column('facebook_company_profile_url', sa.String(71)),
        sa.Column('twitter_company_profile_url', sa.String(38)),
        sa.Column('ownership_type', sa.String(7)),
        sa.Column('business_model', sa.String(3)),
        sa.Column('certified_active_company', sa.String(3)),
        sa.Column('certification_date', sa.String(18)),
        sa.Column('total_funding_amount_in_000s_usd', sa.String(7)),
        sa.Column('recent_funding_amount_in_000s_usd', sa.String(5)),
        sa.Column('recent_funding_round', sa.String(11)),
        sa.Column('recent_funding_date', sa.String(17)),
        sa.Column('recent_investors', sa.String(31)),
        sa.Column('all_investors', sa.String(62)),
        sa.Column('company_street_address', sa.String(48)),
        sa.Column('company_city', sa.String(16)),
        sa.Column('company_state', sa.String(14)),
        sa.Column('company_zip_code', sa.String(5)),
        sa.Column('company_country', sa.String(13)),
        sa.Column('full_address', sa.String(91)),
        sa.Column('number_of_locations', sa.String(4)),
        sa.Column('query_name', sa.String(31)),
        sa.Column('created_at', sa.DateTime, nullable=False),
        schema='sales_leads'
    )


def downgrade() -> None:
    op.drop_table('leads', schema='sales_leads')
