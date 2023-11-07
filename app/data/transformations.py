from dataclasses import dataclass
import pandas as pd


@dataclass
class SalesTransformations:
    target_schema = {
        "First Name": "first_name",
        "Last Name": "last_name",
        "Title": "job_title",
        "Role": "job_function",
        "Email": "email_address",
        "Linkedin": "linkedin_contact_profile_url",
        "Company": "company_name",
        "Phone": "direct_phone_number",
        "Mobile": "mobile_phone",
    }

    def get_new_lead_data(self, dataframe, engine):
        df = pd.read_sql(
            """
                WITH cte_new_latest_leads AS
                    (
                    SELECT s.*
                        , ROW_NUMBER()
                            OVER (PARTITION BY s.email_address ORDER BY s.created_at DESC) AS row_number
                    FROM sales_leads.leads                     s
                    LEFT JOIN dm_shopify.sales_customer_view c
                        ON s.email_address = c.email
                    LEFT JOIN sales_leads.tracking           t
                        ON t.lead_uuid = s.uuid
                    WHERE
                        c.email IS NULL -- not seen this customer before
                    AND t.uuid IS NULL -- and not sent this record previously.
                    AND s.email_address IS NOT NULL -- filter out blank emails.
                    )

                SELECT *
                FROM cte_new_latest_leads
                WHERE
                row_number = 1
            """, engine
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
        
        phone_df = dataframe[self.target_schema.values()].copy()
        
        phone_cols = phone_df.filter(like='phone').columns 
        
        phone_df[phone_cols] = phone_df[phone_cols].apply(lambda x : '="' + x + '"',axis=1)
        
        phone_df = phone_df.rename(columns={v : k for k,v in self.target_schema.items()})

        return phone_df 
