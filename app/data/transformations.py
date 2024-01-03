from dataclasses import dataclass
import pandas as pd
from . import GoogleDrive 


@dataclass
class SalesTransformations:
    
    engine: str = None
    google_api : GoogleDrive = None
    target_schema = {
        "First Name": "first_name",
        "Last Name": "last_name",
        "Title": "job_title",
        "Role": "job_function",
        "Email": "email_address",
        "Linkedin": "linkedin_contact_profile_url",
        "Company": "company_name",
        "Phone": "phone_number",
        "Hubspot Owner": "hubspot_owner",
        "ZI Search": "zi_search",
    }

    def get_new_zi_search_lead_data(self) -> pd.DataFrame:
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
                        LEFT JOIN sales_leads.tracking           t1
                            ON t1.email_address = s.email_address
                        WHERE
                            c.email IS NULL -- not seen this customer before
                        AND t.uuid IS NULL -- and not sent this record previously.
                        AND s.email_address IS NOT NULL -- filter out blank emails.
                        AND t1.email_address IS NULL
                        AND s.company_country = 'United States'
                        )

                SELECT first_name, last_name, job_title, job_function, email_address, linkedin_contact_profile_url, company_name
                    , COALESCE(mobile_phone, direct_phone_number) as phone_number, d.zi_search, d.hubspot_owner, d.name as file_name, l.drive_metadata_uuid
                FROM cte_new_latest_leads              l
                LEFT JOIN sales_leads.drive_metadata d
                    ON d.uuid = l.drive_metadata_uuid
                WHERE row_number = 1
                AND d.config_file_uuid IS NOT NULL
                """, self.engine
                )
        
        return df

    def order_duplicate_domains(self):
        """With duplicate domains, we don't want to exclude them and not email
        them in the same group.

        Still need to figure out how we group them, maybe an index and sort the duplicate email
        and the start of each index mod % 10?
        """
        pass

    def create_google_lead_data_frame(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        
        target_columns = list(self.target_schema.values())
        
        phone_df = dataframe[target_columns].copy()
        
        phone_df['phone_number'] = phone_df['phone_number'].fillna('').apply(lambda x : '="' + x + '"')
        
        phone_df = phone_df.rename(columns={v : k for k,v in self.target_schema.items()})

        return phone_df 

    def create_google_sheet_output_for_city_search_data(self) -> pd.DataFrame:
        
        query = f"""
        SELECT city.uuid
            ,  city.type
            , '' AS "Main Point Of Contact"
            , '' AS "Main Point of Contact Email"
            , '' AS "Main Contact Linkedin"
            , '' AS "Generic Contact Email"
            , '' AS company_name
            , city.website
            , city.phone
            , city.position
            , city.rating
            , city.address
            , city.reviews
            , city.emails_0
            , city.emails_1
            , city.emails_2
            , city.emails_3
            , d.name AS file_name
        FROM sales_leads.city_search city
        INNER JOIN sales_leads.drive_metadata d
          ON d.uuid = city.drive_metadata_uuid
        """
        return pd.read_sql(query, self.engine)
    
    def post_city_search_data_to_google_sheet(self, spreadsheet_name : str, folder_id : str):
        
        city_search_df = self.create_google_sheet_output_for_city_search_data()
        
        target_sheet = city_search_df['file_name'].unique()[0]
        
        city_search_df = city_search_df.drop('file_name', axis=1)
        
        self.google_api.write_to_google_sheet(
            dataframe=city_search_df,
            spreadsheet_name=spreadsheet_name,
            target_sheet=target_sheet,
            folder_id=folder_id,
        )
    
    
        


