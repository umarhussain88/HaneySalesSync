import os
from dataclasses import dataclass
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine, types, text
from sqlalchemy.engine import URL
from notifiers import get_notifier
import textwrap
import logging
from app.google_drive.drive import GoogleDrive
from azure.storage.blob import BlobServiceClient, BlobType


@dataclass
class AzureExporter:
    sql_server: str = None
    sql_user: str = None
    sql_password: str = None
    sql_db: str = None

    def __post_init__(self):
        self.cxn = URL.create(
            drivername="mssql+pyodbc",
            username=self.sql_user,
            password=self.sql_password,
            host=self.sql_server,
            database=self.sql_db,
            query={
                "driver": "ODBC Driver 18 for SQL Server",
                "Encrypt": "yes",
                "TrustServerCertificate": "yes",
                "Connection Timeout": "30",
            },
        )

        self.engine = create_engine(self.cxn, fast_executemany=True)

    def check_if_schema_exists(self, schema: str) -> bool:
        """Checks if schema exists in database.
        if it does not exist it will be created.

        Args:
            schema (str): schema name

        Returns:
            bool: True if schema exists, False if not.
        """

        sql_str = f"""
          IF schema_id ('{schema}') IS NULL
                BEGIN
                EXEC ('CREATE SCHEMA {schema} AUTHORIZATION dbo;')
                END
        """
        return self.engine.execute(sql_str)

    def insert_to_raw(
        self, dataset: pd.DataFrame, table_name: str, schema: Optional[str] = "stg"
    ) -> None:
        """inserts dataset into a raw table.
           As this is a raw table, it will be replaced if it already exists.

        Args:
            dataset (pd.DataFrame): Pandas dataframe that holds raw API data.
            table_name (str): target table name
            schema (Optional[str], optional): target schema in database. Defaults to 'raw'.
        Returns:
            None
        """

        dataset["_inserted_at"] = pd.to_datetime("now").utcnow()

        # create sqlalchemy types for each column
        col_types = {
            col: types.VARCHAR(dataset[col].astype(str).str.len().max())
            for col in dataset.columns
            if col not in ["_inserted_at"]
        }

        col_types["_inserted_at"] = types.DateTime()

        self.check_if_schema_exists(schema)

        dataset.to_sql(
            name=table_name,
            schema=schema,
            con=self.engine,
            if_exists="replace",
            index=False,
            dtype=col_types,
        )

        return None

    def scale_database(self, scale_sku: str):
        if (
            pd.read_sql("SELECT @@SERVERNAME as servername", self.engine)["servername"][
                0
            ]
            == "data-metrics-server"
        ):
            self.engine.execute(
                f"ALTER DATABASE {self.sql_db} MODIFY (EDITION = 'Standard', SERVICE_OBJECTIVE = '{scale_sku}')"
            )
        else:
            pass

    def execute_stored_procedure(self, sp_name: str, schema: Optional[str] = "stg"):
        with self.engine.begin() as conn:
            conn.execute(f"EXEC {schema}.{sp_name}")


@dataclass
class PostgresExporter:
    username: str = None
    password: str = None
    host: str = None
    port: str = None
    database: str = None

    def __post_init__(self):
        self.connection_string = f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"
        self.engine = create_engine(self.connection_string)

    def check_if_schema_exists(self, schema):
        with self.engine.connect() as connection:
            connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))

    def _clean_column_names(self, dataset: pd.DataFrame) -> pd.DataFrame:
        dataset.columns = (
            dataset.columns.str.strip()
            .str.lower()
            .str.replace("\s|-|\|/|\.|\(|\)", "_", regex=True)
        )
        # remove doulbe underscores and trailing underscores
        dataset.columns = dataset.columns.str.replace("__", "_", regex=True).str.rstrip(
            "_"
        )
        return dataset

    def insert_raw_data(
        self,
        dataset: pd.DataFrame,
        table_name: str,
        schema: str,
        created_at_column: Optional[str] = "created_at",
    ) -> None:
        
        if dataset.empty:
            pass

        else:
            dataset = self._clean_column_names(dataset)

            dataset = dataset.astype(str)
            dataset[created_at_column] = pd.to_datetime("now").utcnow()

            col_types = {
                col: types.VARCHAR(dataset[col].astype(str).str.len().max())
                for col in dataset.columns
                if col not in [created_at_column]
            }

            col_types[created_at_column] = types.DateTime()
            dataset = dataset.replace("nan", None)

            dataset.to_sql(
                name=table_name,
                schema=schema,
                con=self.engine,
                if_exists="append",
                index=False,
                dtype=col_types,
            )

    def check_if_record_exists(
        self,
        table_name: str,
        schema: str,
        source_dataframe: pd.DataFrame,
        look_up_column: str,
    ) -> pd.DataFrame:
        """
        Checks to see if we've already imported the file.

        returns a dataframe of the records that do not exist in the database.
        (Left anti join on id)
        """

        if source_dataframe.empty:
            logging.info('No new files to process')
        else:
            look_up_vals = source_dataframe[look_up_column].unique().tolist()

            query = f"""
            
            SELECT id FROM {schema}.{table_name} WHERE {look_up_column} IN  ({', '.join(f"'{item}'" for item in look_up_vals)})
            """

            current_files = pd.read_sql(query, self.engine)

            return source_dataframe.loc[
                ~source_dataframe[look_up_column].isin(current_files[look_up_column])
            ]

    def get_uuid_from_table(  # TODO: rename this function
        self, table_name: str, schema: str, look_up_val: str, look_up_column
    ) -> pd.DataFrame:
        with self.engine.connect() as connection:
            query = f"""
            SELECT uuid FROM {schema}.{table_name} WHERE {look_up_column} = '{look_up_val}'
            """
            return pd.read_sql(query, connection)

    def update_tracking_table(self, drive_metadata_uuid: str) -> None:
        """
        Get the assoicated UUID and write an update statement
        to the tracking table.
        """

        qry = f"""
        WITH new_data AS (
            SELECT l.email_address
            , l.uuid as lead_uuid
            , 'posted' as status
            FROM sales_leads.leads l
            WHERE l.drive_metadata_uuid = '{drive_metadata_uuid}'
            and l.email_address is not null
        )
        INSERT INTO sales_leads.tracking
        (lead_uuid, status, email_address, created_at)
        SELECT lead_uuid, status::status_enum, email_address, CURRENT_TIMESTAMP
        FROM new_data
        WHERE lead_uuid NOT IN (SELECT lead_uuid FROM sales_leads.tracking);
        """

        with self.engine.begin() as connection:
            connection.execute(text(qry))

    def update_city_search_tracking_table(self, drive_metadata_uuid: str) -> None:
        """
        Get the assoicated UUID and write an update statement
        to the tracking table.
        """

        qry = f"""
        WITH new_data AS (
            SELECT COALESCE(l.main_point_of_contact_email, l.generic_contact_email) AS email_address
            , l.uuid as city_search_lead_uuid
            , 'posted' as status
            FROM sales_leads.city_search_enriched l
            WHERE l.drive_metadata_uuid = '{drive_metadata_uuid}'
            AND COALESCE(l.main_point_of_contact_email, l.generic_contact_email) IS NOT NULL
        )
        INSERT INTO sales_leads.tracking
        (city_search_lead_uuid, status, email_address, created_at)
        SELECT city_search_lead_uuid, status::status_enum, email_address, CURRENT_TIMESTAMP
        FROM new_data
        WHERE city_search_lead_uuid NOT IN (SELECT city_search_lead_uuid FROM sales_leads.tracking);
        """

        with self.engine.begin() as connection:
            connection.execute(text(qry))
    def update_tracking_table_shopify_customer(self, drive_metadata_uuid: str) -> None:
        
        
        with self.engine.begin() as connection:
            qry = f"""WITH new_data AS (
                SELECT  tracking.uuid 
                , l.email_address
                , l.uuid as lead_uuid
                , 'shopify_customer' as status
                FROM sales_leads.leads l
                INNER JOIN dm_shopify.sales_customer_view scv
                ON l.email_address = scv.email
                LEFT JOIN sales_leads.tracking
                  ON l.uuid = tracking.lead_uuid
                WHERE drive_metadata_uuid = '{drive_metadata_uuid}'
                AND (tracking.lead_uuid IS NULL
                    OR tracking.status = 'posted') 
                    -- we check for shopify customers AFTER they have been posted
                    -- so we want to update these records
                )
    
                INSERT INTO sales_leads.tracking
                (uuid, lead_uuid, status, email_address, created_at)
                SELECT uuid, lead_uuid, status::status_enum, email_address, CURRENT_TIMESTAMP
                FROM new_data
                ON CONFLICT (uuid)
                DO UPDATE
                SET status = EXCLUDED.status
                  , created_at = CURRENT_TIMESTAMP
                ;
            """

            connection.execute(text(qry))
            
    def update_city_search_tracking_table_shopify_customer(self, drive_metadata_uuid: str) -> None:
        
        
        with self.engine.begin() as connection:
            qry = f"""WITH new_data AS (
                SELECT  tracking.uuid 
                , COALESCE(l.main_point_of_contact_email, l.generic_contact_email) AS email_address
                , l.uuid as city_search_lead_uuid
                , 'shopify_customer' as status
                FROM sales_leads.city_search_enriched l
                INNER JOIN dm_shopify.sales_customer_view scv
                ON COALESCE(l.main_point_of_contact_email, l.generic_contact_email) = scv.email
                LEFT JOIN sales_leads.tracking
                  ON l.uuid = tracking.city_search_lead_uuid
                WHERE drive_metadata_uuid = '{drive_metadata_uuid}'
                AND (tracking.city_search_lead_uuid IS NULL
                    OR tracking.status = 'posted') 
                    -- we check for shopify customers AFTER they have been posted
                    -- so we want to update these records
                )
    
                INSERT INTO sales_leads.tracking
                (uuid, city_search_lead_uuid, status, email_address, created_at)
                SELECT uuid, city_search_lead_uuid, status::status_enum, email_address, CURRENT_TIMESTAMP
                FROM new_data
                ON CONFLICT (uuid)
                DO UPDATE
                SET status = EXCLUDED.status
                  , created_at = CURRENT_TIMESTAMP
                ;
            """

            connection.execute(text(qry))

    def get_slack_channel_metrics_zi_search(self, drive_metadata_uuid : str) -> pd.DataFrame:
        return pd.read_sql(
            f""" 
                     SELECT d.name
                          , SUM(CASE WHEN tracking.status = 'shopify_customer' THEN 1 ELSE 0 END) AS number_of_shopify_customers
                          , SUM(CASE WHEN tracking.status = 'posted' THEN 1 ELSE 0 END)           AS number_of_posted_leads
                          , d.created_at
                     FROM sales_leads.tracking
                       LEFT JOIN sales_leads.leads          l
                         ON l.uuid = tracking.lead_uuid
                       LEFT JOIN sales_leads.drive_metadata d
                         ON d.uuid = l.drive_metadata_uuid
                     WHERE l.drive_metadata_uuid IN ('{drive_metadata_uuid}')
                     GROUP BY d.name, d.created_at
                     
                           """,
            self.engine,
        )
    def get_slack_channel_metrics_city_search(self, drive_metadata_uuid : str) -> pd.DataFrame:
        return pd.read_sql(
            f""" 
                     SELECT d.name
                          , SUM(CASE WHEN tracking.status = 'shopify_customer' THEN 1 ELSE 0 END) AS number_of_shopify_customers
                          , SUM(CASE WHEN tracking.status = 'posted' THEN 1 ELSE 0 END)           AS number_of_posted_leads
                          , d.created_at
                     FROM sales_leads.tracking
                       LEFT JOIN sales_leads.city_search_enriched          l
                         ON l.uuid = tracking.city_search_lead_uuid
                       LEFT JOIN sales_leads.drive_metadata d
                         ON d.uuid = l.drive_metadata_uuid
                     WHERE l.drive_metadata_uuid IN ('{drive_metadata_uuid}')
                     GROUP BY d.name, d.created_at
                           """,
            self.engine,
        )

    def create_config_temp_table(self, temp_table_name: str) -> None:
        qry = f"""
        CREATE TEMPORARY TABLE {temp_table_name} (
            
            filename varchar(255),
            hubspot_owner varchar(255),
            zi_search varchar(255), 
            file_type varchar(255)
            )           
        """

        with self.engine.begin() as connection:
            connection.execute(text(qry))

    def update_config_metadata(self, dataframe: pd.DataFrame) -> None:
        if dataframe.empty:
            pass
        else:
            self.create_config_temp_table(temp_table_name="temp_config")

            dataframe.columns = ["filename", "hubspot_owner", "zi_search", "file_type"]
            

            dataframe.to_sql(
                name="temp_config", con=self.engine, if_exists="append", index=False
            )

            qry = """
            WITH drive_metadata AS (
                SELECT d.uuid
                , f.hubspot_owner
                , f.zi_search
                , f.file_type::file_type_enum
                , current_timestamp as updated_at
                FROM sales_leads.drive_metadata d
                INNER JOIN temp_config f
                    ON f.filename = d.name
                WHERE d.config_file_uuid IS NULL -- only update records that have not been updated before.
            )
            INSERT INTO sales_leads.drive_metadata
            (uuid, hubspot_owner, zi_search, file_type, updated_at)
            SELECT uuid, hubspot_owner, zi_search, file_type, updated_at
            FROM drive_metadata
            ON CONFLICT (uuid) DO UPDATE
            SET config_file_uuid = gen_random_uuid()
            , hubspot_owner = EXCLUDED.hubspot_owner
            , zi_search = EXCLUDED.zi_search
            , file_type = EXCLUDED.file_type
            , updated_at = EXCLUDED.updated_at;
            DROP TABLE temp_config;
            """

        with self.engine.begin() as connection:
            connection.execute(text(qry))

    def get_and_post_missing_config(self, slack_webhook) -> None:
        missing_config = pd.read_sql(
            """ 
            SELECT name, created_at, lastmodifyinguser_displayname
            FROM sales_leads.drive_metadata
            WHERE
            config_file_uuid IS NULL
            AND has_posted_on_slack IS NULL
            AND file_type NOT IN ('city_search')
            """,
            self.engine,
        )

        if not missing_config.empty:
            for index, row in missing_config.iterrows():
                notifier = get_notifier("slack")
                missing_config_msg = textwrap.dedent(
                    f"""
                Missing Config File: {row['name']}
                Created At: {row['created_at']}
                Last Modified By: {row['lastmodifyinguser_displayname']}
                Please update this file so an output file can be created.
                https://docs.google.com/spreadsheets/d/1_wPctIjTdSXDvJIRmXw9S5MqYe3zE8dfCYGLYWg4BPg/edit#gid=0
                """
                )

                notifier.notify(
                    message=missing_config_msg,
                    webhook_url=slack_webhook,
                )
                
                self.update_drive_table_slack_posted()

    def send_update_slack_metrics(
        self, slack_webhook: str, slack_df: pd.DataFrame, sheet_name : Optional[str] = '', sheet_url : Optional[str] = ''
    ) -> None:
        for group, data in slack_df.groupby("name"):
            message = textwrap.dedent(
                f"""
            {group}:
            Number of Leads: {data['number_of_posted_leads'].values[0]}
            Number of Shopify Customers: {data['number_of_shopify_customers'].values[0]}
            """
            )
            
            if sheet_name != '':
                message = message + f"\nSpreadsheet: <{sheet_url}|{sheet_name}>"

            notifier = get_notifier("slack")
            logging.info(message)
            notifier.notify(
                message=message, webhook_url=os.environ.get("SLACK_WEBHOOK")
            )
            
    def update_drive_table_slack_posted(self) -> None:
        
        qry = f"""
        UPDATE sales_leads.drive_metadata
        SET has_posted_on_slack = True
        WHERE config_file_uuid IS NULL
        AND has_posted_on_slack IS NULL
        """
        
        with self.engine.begin() as connection:
            connection.execute(text(qry))
            
    def get_files_to_process(self, ids: list) -> pd.DataFrame:
        with self.engine.connect() as connection:
            query = f"""
            SELECT id, name, file_type
            FROM sales_leads.drive_metadata
            WHERE id IN ({', '.join(f"'{item}'" for item in ids)})
            """
            return pd.read_sql(query, connection)

    def process_file(self,file_id: str, gdrive: GoogleDrive) -> pd.DataFrame:
        

        
        uuid = self.get_uuid_from_table(
            table_name="drive_metadata",
            schema="sales_leads",
            look_up_val=file_id,
            look_up_column="id",
        )

        stream = gdrive.get_stream_object(file_id)
        
        file_ext = gdrive.get_file_type(file_id)
        
        if file_ext == "csv":
            stream.seek(0)
            df = pd.read_csv(stream)
        elif file_ext == "xlsx":
            df = pd.read_excel(stream)
        
        if "drive_metadata_uuid" in df.columns:
            logging.info("drive_metadata_uuid column found for some reason, sometimes the sales teams adds this from other lead generated files..")
            df = df.drop(columns=["drive_metadata_uuid"])
        
        df["drive_metadata_uuid"] = uuid["uuid"].values[0]
        
        return df        
            

    def filter_file_dataframe_with_file_type(self, file_dataframe: pd.DataFrame) -> pd.DataFrame:
        
        file_dataframe_with_file_type = file_dataframe[file_dataframe['file_type'].isnull() == False]
        
        return file_dataframe_with_file_type


    def upsert_franchise_data(self, dataframe : pd.DataFrame, temp_table_name : Optional[str] = 'city_franchises') -> None:
        
        temp_table_query = f"""CREATE TEMPORARY TABLE {temp_table_name} (
              franchise_name varchar(255)
            ,  domain_name varchar(255)
            )"""
        
        with self.engine.begin() as connection:
            connection.execute(text(temp_table_query))
            
        
        dataframe.columns = ['franchise_name', 'domain_name']
        
        dataframe.to_sql(
            name=temp_table_name, con=self.engine, if_exists="append", index=False
        )
        
        qry = f"""
        WITH new_data AS (
        SELECT tgt.uuid
        , src.franchise_name
        , src.domain_name
        , CURRENT_TIMESTAMP as created_at
        FROM {temp_table_name} src
        LEFT JOIN sales_leads.city_search_franchises tgt
        ON src.franchise_name = tgt.franchise_name
        AND src.domain_name = tgt.domain_name
        )
        INSERT INTO sales_leads.city_search_franchises (uuid,franchise_name, domain_name, created_at)
        SELECT COALESCE(uuid,gen_random_uuid()),franchise_name, domain_name, created_at
        FROM new_data
        ON CONFLICT (uuid) DO UPDATE
        SET   domain_name = EXCLUDED.domain_name
            , updated_at = CURRENT_TIMESTAMP;
        DROP TABLE {temp_table_name};
        """
        
        with self.engine.begin() as connection:
            connection.execute(text(qry))
            
        
    def post_city_search_slack_message(self,link: str, spread_sheet_name : str, owner: Optional[str] = 'U03K3H773RB'):
        

        
        message = f"""Hi <@{owner}>, <{link}|City Search> - {spread_sheet_name} is ready to be disperesed."""
          
    
        notifier = get_notifier("slack")
        
        notifier.notify(
            message=message, webhook_url=os.environ.get("SLACK_WEBHOOK")
        )
        
    def get_missing_file_types(self, gdrive: GoogleDrive) -> pd.DataFrame:
        
        missing_files = pd.read_sql(""" SELECT uuid
                                        , replace(trim(both '[]' from parents), '''','') AS parent
                                        , name 
                                    FROM sales_leads.drive_metadata WHERE file_type IS NULL """, self.engine)
    
        if not missing_files.empty:
            d = {}
            for file in missing_files.itertuples():
                parent_folder = gdrive.get_parent_folder_name(file.parent)
                d[file.uuid] = parent_folder
        
            missing_files['file_type'] = missing_files['uuid'].map(d)
            missing_files['file_type'] = missing_files['file_type'].str.strip().str.lower().str.replace(' ', '_')
            missing_files['file_type'] = missing_files['file_type'].replace('data drop', 'zi_search')

        return pd.DataFrame() if missing_files.empty else missing_files
        
         
    def update_file_types(self, file_dataframe: pd.DataFrame) -> None:

        temp_table_query = f"""CREATE TEMPORARY TABLE missing_file_types (
              uuid uuid
            ,  file_type varchar(255)
            )"""
        
        with self.engine.begin() as connection:
            connection.execute(text(temp_table_query))
            
        file_dataframe.columns = ['uuid', 'file_type']
        
        file_dataframe.to_sql(
            name='missing_file_types', con=self.engine, if_exists="append", index=False
        )
        
        update_query = f"""
        WITH new_data AS (
            SELECT d.uuid
            , f.file_type::file_type_enum
            , current_timestamp as updated_at
            FROM sales_leads.drive_metadata d
            INNER JOIN missing_file_types f
                ON f.uuid = d.uuid
            WHERE d.file_type IS NULL
        )
        INSERT INTO sales_leads.drive_metadata (uuid, file_type, updated_at)
        SELECT uuid, file_type::file_type_enum, updated_at
        FROM new_data
        ON CONFLICT (uuid) DO UPDATE
        SET file_type = EXCLUDED.file_type
            , updated_at = EXCLUDED.updated_at;
        DROP TABLE missing_file_types;"""
        
        with self.engine.begin() as connection:
            connection.execute(text(update_query))
            
        
    def update_file_has_been_processed(self, file_id : str) -> None:
            
        query = f"""
                UPDATE sales_leads.drive_metadata
                SET has_been_processed = True
                WHERE id = '{file_id}'
                """ 
        with self.engine.begin() as connection:
            connection.execute(text(query))
            
    
    def check_if_file_has_been_processed(self,file_id : str) -> bool:
        
        with self.engine.connect() as connection:
            query = f"""
            SELECT id 
            FROM sales_leads.drive_metadata 
            WHERE id = '{file_id}'
            AND has_been_processed = True
            """
            return connection.execute(text(query)).fetchone() is not None
        
    
    def change_file_ext_name_to_csv(self, file_name : str) -> None:
        query = f"UPDATE sales_leads.drive_metadata SET name = REPLACE(name, \'xlsx\', \'csv\') WHERE name = '{file_name}'"
        with self.engine.connect() as conn:
            conn.execute(text(query))
            
            
            
@dataclass
class AzureBlobStorage:
    connection_string: str
    blob_service_client: BlobServiceClient = None

    def __post_init__(self):
        self.blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)

    
    def upload_dataframe(self, dataframe, container_name, blob_name, file_id : Optional[str] = None):
        blob_client = self.blob_service_client.get_blob_client(container_name, blob_name)
        blob_client.upload_blob(dataframe.to_csv(index=False),  blob_type=BlobType.BlockBlob, overwrite=True)
        logging.info(f"Uploaded {blob_name} to {container_name}")
        
        if file_id:
            metadata = {'file_id': file_id}
            blob_client.set_blob_metadata(metadata)
        
    
    def get_blob_metadata(self, container_name, blob_name):
        blob_client = self.blob_service_client.get_blob_client(container_name, blob_name)
        return blob_client.get_blob_properties()
    
    def split_and_return_blob_name(self, blob_name : str) -> str:
        return blob_name.split('/')[-1]
    
    def read_blob_to_dataframe(self, container_name, blob_name):
        blob_client = self.blob_service_client.get_blob_client(container_name, blob_name)
        stream = blob_client.download_blob()
        return pd.read_csv(stream.content_as_text())
    
    def get_blob_from_container(self, container_name, blob_name):
        blob_client = self.blob_service_client.get_blob_client(container_name, blob_name)
        return blob_client.download_blob()
    
    def get_blob_metadata(self, container_name, blob_name):
        blob_client = self.blob_service_client.get_blob_client(container_name, blob_name)
        return blob_client.get_blob_properties()
    
    def list_blobs(self, container_name):
        container_client = self.blob_service_client.get_container_client(container_name)
        return container_client.list_blobs()