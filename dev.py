import logging
import azure.functions as func
from app import (
    GoogleDrive,
    PostgresExporter,
    SalesTransformations,
    create_gdrive_service,
    AzureBlobStorage,
)
from dotenv import load_dotenv
import json
import os
import pandas as pd
import sentry_sdk
from sentry_sdk.integrations.serverless import serverless_function
from time import sleep
from pathlib import Path
import io

load_dotenv()

logging.basicConfig(level=logging.INFO)


def load_local_settings_as_env_vars(file_path: Path):
    with open(file_path) as f:
        data = json.load(f)

    for key, value in data["Values"].items():
        os.environ[key] = value
        

target_file_schema = [
        'first_name',
        'last_name',
        'main_point_of_contact_email',
        'main_contact_linkedin',
        'generic_contact_email',
        'company_name',
        'website',
        'phone'
    ]        


if __name__ == "__main__":
    if os.environ.get("FUNCTIONS_ENVIRONMENT") == "preview":
        logging.info("Running in preview mode")
        logging.info("Loading local settings")
        local_settings_path = Path(__file__).parent.joinpath("local.settings.dev.json")
        load_local_settings_as_env_vars(local_settings_path)
    elif os.environ.get("FUNCTIONS_ENVIRONMENT") == "pre_prod":
        logging.info("Running in pre production mode")
        logging.info("Loading pre-prod settings")
        local_settings_path = Path(__file__).parent.joinpath("local.settings.pre_prod.json")
        load_local_settings_as_env_vars(local_settings_path)
    elif os.environ.get("FUNCTIONS_ENVIRONMENT") == "prod":
        logging.info("Running in preview mode")
        logging.info("Loading local settings")
        local_settings_path = Path(__file__).parent.joinpath("local.settings.json")
        load_local_settings_as_env_vars(local_settings_path)

    sheet_week = f"Week {pd.Timestamp('today').isocalendar().week}"

    gdrive = create_gdrive_service(
        service_account_b64_encoded=os.environ.get("SERVICE_ACCOUNT")
    )

    psql = PostgresExporter(
        username=os.environ.get("PSQL_USERNAME"),
        password=os.environ.get("PSQL_PASSWORD"),
        host=os.environ.get("PSQL_SERVER"),
        port=os.environ.get("PSQL_PORT"),
        database=os.environ.get("PSQL_DATABASE"),
    )

    az = AzureBlobStorage(connection_string=os.environ.get("SalesSyncBlogTrigger"))

    st = SalesTransformations(engine=psql.engine, google_api=gdrive)
    
    
    
    
    myblob = az.get_blob_from_container(
        container_name="salesfiles",
        blob_name="city_search/WI - Brown County - Cleaning Service - 1-23-24.csv",
    )
    # all_child_modified_files = gdrive.get_modified_files_in_folder(
    #         folder_id=os.environ.get("PARENT_FOLDER"), delta_days=3
    #     )

    # if all_child_modified_files:
    #     file_dataframe_all = gdrive.create_file_list_dataframe(
    #         all_child_modified_files, record_path=None
    #     )
    #     logging.info(f"Number of files edited: {file_dataframe_all.shape[0]}")
    #     file_config = gdrive.get_file_config(
    #         os.environ.get("QUICK_MAIL_CONFIG_NAME"),
    #         folder_id=os.environ.get("QUICK_MAIL_CONFIG_FOLDER_ID"),
    #     )
        
        # logging.info('Checking if files exist in database')
        
        # file_dataframe_all = file_dataframe_all.reset_index(drop=True)
        # file_dataframe_all = file_dataframe_all[file_dataframe_all['name'].str.contains('Larimer County CO -')]
        
    blob_name_without_container = myblob.name.replace("salesfiles/", "")
    blob_metadata = az.get_blob_metadata(container_name='salesfiles', blob_name=blob_name_without_container)
    
    file_id = blob_metadata["metadata"]["file_id"]

    has_file_been_processed = psql.check_if_file_has_been_processed(file_id=file_id)

    logging.info("Getting franchise data")
    franchise_df = gdrive.get_franchise_data(
        file_id=os.environ.get("FRANCHISE_MASTER_LIST_FILE_ID")

    )
    logging.info("Upserting franchise data")
    psql.upsert_franchise_data(
        dataframe=franchise_df, temp_table_name="temp_franchise_data"
    )
    if not has_file_been_processed:
        blob_bytes = myblob.read()
        
        columns = psql.get_columns_from_table("city_search", "sales_leads")
        
        df = pd.read_csv(io.BytesIO(blob_bytes))
        psql.insert_raw_data(dataset=df, table_name="city_search", schema="sales_leads", column_names=columns)

    logging.info("Creating city search output")
    sheet_url_dict = st.post_city_search_data_to_google_sheet(
        spreadsheet_name=f"City Search Output - {sheet_week}",
        folder_id=os.environ.get("CITY_SEARCH_OUTPUT_PARENT_FOLDER_ID"),
        file_id=file_id
    )
    for name, sheet_url in sheet_url_dict.items():
        logging.info(f'{name}: {sheet_url}')
        psql.post_city_search_slack_message(
        link=sheet_url_dict[name], spread_sheet_name=name
        )

    psql.update_file_has_been_processed(file_id=file_id)
