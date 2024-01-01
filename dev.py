import logging
import azure.functions as func
from app import GoogleDrive, PostgresExporter, SalesTransformations, create_gdrive_service
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
        
        
    st = SalesTransformations()
    gdrive = create_gdrive_service(service_account_b64_encoded=os.environ.get("SERVICE_ACCOUNT"))

    psql = PostgresExporter(
        username=os.environ.get("PSQL_USERNAME"),
        password=os.environ.get("PSQL_PASSWORD"),
        host=os.environ.get("PSQL_SERVER"),
        port=os.environ.get("PSQL_PORT"),
        database=os.environ.get("PSQL_DATABASE"),
    )

    #testing all folders from a parent folder.
    all_child_modified_files = gdrive.get_modified_files_in_folder(folder_id=os.environ.get("PARENT_FOLDER"), delta_days=30)
    
    file_dataframe_all = gdrive.create_file_list_dataframe(all_child_modified_files, record_path=None)
    
    logging.info(f"Number of files edited: {file_dataframe_all.shape[0]}")
        
    file_config = gdrive.get_file_config(
            os.environ.get("QUICK_MAIL_CONFIG_NAME"),
            folder_id=os.environ.get("QUICK_MAIL_CONFIG_FOLDER_ID"),
        )
    
    file_dataframe_new = psql.check_if_record_exists(
            table_name="drive_metadata",
            schema="sales_leads",
            source_dataframe=file_dataframe_all,
            look_up_column="id",
        )
    
    if not file_dataframe_new.empty:
        logging.info('New files to process')
        psql.insert_raw_data(
            dataset=file_dataframe_all.drop(columns=['mimeType']),
            table_name="drive_metadata",
            schema="sales_leads",
        )
            
    psql.update_config_metadata(file_config)
    
    if not file_dataframe_new.empty:
        logging.info('Processing new files')
        files_to_process = psql.get_files_to_process(file_dataframe_new['id'].tolist())
        
        for file in files_to_process.itertuples():
            logging.info(f"Processing file: {file.name}")
            psql.process_file(file.id, gdrive, file.file_type)
    
    
    
    
