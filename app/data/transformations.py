from dataclasses import dataclass
import pandas as pd
from typing import TYPE_CHECKING, Optional, Union, Dict
import logging

if TYPE_CHECKING:
    from ..google_drive.drive import GoogleDrive

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [%(levelname)s] %(message)s (at line %(lineno)d in %(funcName)s)'
                    )

@dataclass
class SalesTransformations:
    engine: str = None
    google_api: Optional["GoogleDrive"] = None
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

    def get_new_zi_search_lead_data(self, file_name: str) -> pd.DataFrame:
        df = pd.read_sql(
            f"""
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
                AND d.name = '{file_name}'
                """,
            self.engine,
        )

        return df
    

    def get_new_city_search_lead_data(self,file_id : str) -> pd.DataFrame:
        
        query = f"""WITH cte_new_latest_leads AS (
         SELECT s.*
              , ROW_NUMBER()
                OVER (PARTITION BY COALESCE(s.main_point_of_contact_email, s.generic_contact_email) ORDER BY s.created_at DESC) AS row_number
         FROM sales_leads.city_search_enriched      s
           LEFT JOIN dm_shopify.sales_customer_view c
             ON COALESCE(s.main_point_of_contact_email, s.generic_contact_email) = c.email
           LEFT JOIN sales_leads.tracking           t
             ON t.city_search_lead_uuid = s.uuid
           LEFT JOIN sales_leads.tracking           t1
             ON t1.email_address = COALESCE(s.main_point_of_contact_email, s.generic_contact_email)
         WHERE
             c.email IS NULL -- not seen this customer before
         AND t.uuid IS NULL -- and not sent this record previously.
         AND COALESCE(s.main_point_of_contact_email, s.generic_contact_email) IS NOT NULL -- filter out blank emails.
         AND t1.email_address IS NULL
             )

            SELECT first_name
                , last_name
                , ''                                                           AS job_title
                , ''                                                           AS job_function
                , COALESCE(main_point_of_contact_email, generic_contact_email) AS email_address
                , main_contact_linkedin
                , company_name
                , phone                                                        AS phone_number
                , d.zi_search
                , d.hubspot_owner
                , d.name                                                       AS file_name
                , l.drive_metadata_uuid
            FROM cte_new_latest_leads              l
            LEFT JOIN sales_leads.drive_metadata d
                ON d.uuid = l.drive_metadata_uuid
            WHERE
                row_number = 1
            AND d.config_file_uuid IS NOT NULL
            AND d.id =  '{file_id}' """
            
        return pd.read_sql(query, self.engine)


    def check_if_columns_exist_if_not_create(self, dataframe : pd.DataFrame, target_columns : list):
        
        for column in target_columns:
            if column not in dataframe.columns:
                dataframe[column] = pd.NA
        return dataframe
        

    def create_google_lead_data_frame(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        target_columns = list(self.target_schema.values())
        
        dataframe =  dataframe.rename(columns={'main_contact_linkedin' : 'linkedin_contact_profile_url'})
        quick_mail_df = self.check_if_columns_exist_if_not_create(dataframe, target_columns)

        quick_mail_df = dataframe[target_columns].copy()

        quick_mail_df["phone_number"] = (
            quick_mail_df["phone_number"].fillna("").apply(lambda x: '="' + x + '"')
        )

        quick_mail_df = quick_mail_df.rename(
            columns={v: k for k, v in self.target_schema.items()}
        )

        return quick_mail_df

    def create_google_sheet_output_for_city_search_data(
        self, file_id: str
    ) -> pd.DataFrame:
        query = f"""
        WITH all_file_uuids AS (
                         SELECT uuid
                         FROM sales_leads.drive_metadata
                         WHERE id = '{file_id}')
        SELECT city.uuid
            , COALESCE(f.franchise_name)  AS franchise_name
            , COALESCE(f.domain_name)     AS domain_name
            ,  city.type
            , '' AS "first_name"
            , '' AS "last_name"
            , '' AS "Main Point of Contact Email"
            , '' AS "Main Contact Linkedin"
            , '' AS "Generic Contact Email"
            , city.title AS company_name
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
        LEFT JOIN sales_leads.city_search_franchises f
          ON replace(COALESCE(substring(f.domain_name from 'https?://([^/]*)'), f.domain_name), 'www.', '') = replace(COALESCE(substring(city.website from 'https?://([^/]*)'), city.website), 'www.', '')
        WHERE d.id = '{file_id}'
        AND city.dataid NOT IN (
                         SELECT dataid
                         FROM sales_leads.city_search
                         WHERE
                             drive_metadata_uuid NOT IN (
                                                          SELECT uuid
                                                          FROM all_file_uuids))
        """
        return pd.read_sql(query, self.engine)

    def get_google_sheet_link_by_name(self, spreadsheet_name: str):
        return self.google_api.get_google_sheet_link_by_name(
            spreadsheet_name=spreadsheet_name
        )

    def post_city_search_data_to_google_sheet(
        self, spreadsheet_name: str, folder_id: str,
        file_id : str
        
    ) -> Dict[str, str]:
        city_search_df = self.create_google_sheet_output_for_city_search_data(
            file_id=file_id
        )
        
        file_name = pd.read_sql(f"SELECT name FROM sales_leads.drive_metadata WHERE id = '{file_id}'", self.engine)['name'].values[0]

        sheet_url_dict = {}
        city_search_df["phone"] = (
            city_search_df["phone"].fillna("").apply(lambda x: '="' + x + '"')
        )

        city_search_df = city_search_df.drop("file_name", axis=1)

        city_search_df_franchise = city_search_df[
            city_search_df["franchise_name"].notnull()
        ]
        city_search_df_non_franchise = city_search_df[
            ~city_search_df["franchise_name"].notnull()
        ]
        city_search_df_non_franchise = city_search_df_non_franchise.drop(
            columns=["franchise_name", "domain_name"]
        )
        logging.info('Writing non franchise data to google sheet')
        url = self.google_api.write_to_google_sheet(
            dataframe=city_search_df_non_franchise,
            spreadsheet_name=spreadsheet_name,
            target_sheet=f"{file_name[:30]}_non_franchise",
            folder_id=folder_id,
        )

        sheet_url_dict[file_name] = url
        logging.info('Writing franchise data to google sheet')
        self.google_api.write_to_google_sheet(
            dataframe=city_search_df_franchise,
            spreadsheet_name=spreadsheet_name,
            target_sheet=f"{file_name[:30]}_franchise",
            folder_id=folder_id,
        )

        return sheet_url_dict
