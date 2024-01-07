import logging
import azure.functions as func
from app import GoogleDrive, PostgresExporter, SalesTransformations, create_gdrive_service, AzureBlobStorage
from dotenv import load_dotenv
import json
import os
import pandas as pd
import sentry_sdk
from sentry_sdk.integrations.serverless import serverless_function
from time import sleep
from pathlib import Path

load_dotenv()

logging.basicConfig(level=logging.INFO)

def load_local_settings_as_env_vars(file_path: Path):
    with open(file_path) as f:
        data = json.load(f)

    for key, value in data["Values"].items():
        os.environ[key] = value






if __name__ == '__main__':

    
    if os.environ.get("FUNCTIONS_ENVIRONMENT") == "preview":
        logging.info('Running in preview mode')
        logging.info('Loading local settings')
        local_settings_path = Path(__file__).parent.joinpath("local.settings.dev.json")
        load_local_settings_as_env_vars(local_settings_path)
    elif os.environ.get("FUNCTIONS_ENVIRONMENT") == "prod":
        logging.info('Running in preview mode')
        logging.info('Loading local settings')
        local_settings_path = Path(__file__).parent.joinpath("local.settings.json")
        load_local_settings_as_env_vars(local_settings_path)
        
    sheet_week = f"Week {pd.Timestamp('today').isocalendar().week}"
        
    gdrive = create_gdrive_service(service_account_b64_encoded=os.environ.get("SERVICE_ACCOUNT"))

    psql = PostgresExporter(
        username=os.environ.get("PSQL_USERNAME"),
        password=os.environ.get("PSQL_PASSWORD"),
        host=os.environ.get("PSQL_SERVER"),
        port=os.environ.get("PSQL_PORT"),
        database=os.environ.get("PSQL_DATABASE"),
    )
    
    az = AzureBlobStorage(connection_string=os.environ.get("SalesSyncBlogTrigger")) 
    
    st = SalesTransformations(engine=psql.engine, google_api=gdrive)
    
    df = pd.read_csv('/mnt/c/Users/umarh/OneDrive/Downloads/122123 - Jackson County MO - CS Search.csv - Scrape-it Cloud Data.csv')
    
    if 'drive_metadata_uuid' in df.columns:
        logging.info('drive_metadata_uuid column found')
        df = df.drop(columns=['drive_metadata_uuid'])
    
    df.to_sql('city_search_enriched', psql.engine, schema='sales_leads', if_exists='append', index=False)
    
    
        
    
    
