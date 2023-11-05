# 	Order and remove columns.
# 	Paste into google sheet.
# 	Filter out records without email addresses.
# 	Match with Shopify to ensure they aren't pre-existing customers.
# These are filtered out.
# 	Brett tells Hannah what the ZI search is.
# @brett
#  is this to be a human fillable field? there's no way to automate this I assume.
# 	Group contacts by company.
# This is to find the decision maker amongst a group of contacts.
# 	Check that the phone number is within the US.
# If any phone is not united states then exclude.
# This is to stop any sample orders going out to foreign countries.

from dataclasses import dataclass
import pandas as pd

target_schema = {
    "first_name": "first_name",
    "last_name": "last_name",
    "title": "job_title",
    "role": "job_function",
    "email": "email_address",
    "linkedin": "linkedin_contact_profile_url",
    "company": "company_name",
    "phone": "direct_phone_number",
    "phone_1": "mobile_phone",
}


@dataclass
class SalesTransformations:
    target_schema = {
        "first_name": "first_name",
        "last_name": "last_name",
        "title": "job_title",
        "role": "job_function",
        "email": "email_address",
        "linkedin": "linkedin_contact_profile_url",
        "company": "company_name",
        "phone": "direct_phone_number",
        "phone_1": "direct_phone_number",
    }

    def get_new_lead_data(self, dataframe, engine):
        df = pd.read_sql(
            """
                SELECT *
                FROM sales_leads.leads                 s
                LEFT JOIN dm_shopify.sales_customer_view   c
                    ON s.email_address = c.email
                LEFT JOIN sales_leads.tracking t
                    ON t.lead_uuid = s.uuid
                WHERE
                    c.email IS NULL -- not seen this customer before
                AND t.uuid IS NULL -- and not sent this record previously.
            """
        )
        
        return df

    def remove_non_us_numbers(self):
        # business rule or regex?
        pass

    def order_duplicate_domains(self):
        """With duplicate domains, we don't want to exclude them and not email
        them in the same group.

        Still need to figure out how we group them, maybe an index and sort the duplicate email
        and the start of each index mod % 10?
        """
        pass

    def create_google_lead_sheet(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        dataframe = dataframe[self.target_schema.values()]

        return dataframe
