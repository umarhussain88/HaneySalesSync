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
    
    all_child_modified_files = gdrive.get_modified_files_in_folder(
        folder_id=os.environ.get("PARENT_FOLDER"), delta_days=1
    )
    
    uuids = ['e89680ae-ecb2-401d-819e-1793aaf44858', 'd078589f-87ba-4068-bbbf-4f23840c8f3e', '5088af5b-152f-4f10-b527-f5e2a72aff38']
    
    for uuid in uuids:
        logging.info(f'Processing {uuid}')
        slack_df = psql.get_slack_channel_metrics(drive_metadata_uuid=uuid)
        psql.send_update_slack_metrics(
                slack_webhook=os.environ.get("SLACK_WEBHOOK"), slack_df=slack_df
            )
            
        
    
