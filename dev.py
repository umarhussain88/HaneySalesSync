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
        blob_name=f"city_search_enriched/121223 - Larimer County CO - CS Search.csv",
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
        
        
        
    blob_metadata = az.get_blob_metadata(myblob.container, myblob.name)
    file_id = blob_metadata["metadata"]["file_id"]

    file_name = az.split_and_return_blob_name(myblob.name)

    has_file_been_processed = psql.check_if_file_has_been_processed(file_id=file_id)
    
    if not has_file_been_processed:
        logging.info(f"Processing city search enriched data for {file_name}")
        blob_bytes = myblob.read()
        df = pd.read_csv(io.BytesIO(blob_bytes))
        target_df = psql._clean_column_names(df)
        target_df = target_df[target_file_schema + ["drive_metadata_uuid"]]
        psql.insert_raw_data(target_df, "city_search_enriched", "sales_leads")

    new_lead_data = st.get_new_city_search_lead_data(file_id=file_id)

    if not new_lead_data.empty:
        quick_mail_df = st.create_google_lead_data_frame(new_lead_data)
        logging.info(f"Writing {file_name} to google sheet")
        google_sheet_url = gdrive.write_to_google_sheet(
            dataframe=quick_mail_df,
            spreadsheet_name=f"Quick Mail Output - {sheet_week}",
            target_sheet=file_name,
            folder_id=os.environ.get("QUICK_MAIL_OUTPUT_PARENT_FOLDER_ID"),
        )
        logging.info("Wrote data to google sheet")
        drive_metadata_uuid = new_lead_data["drive_metadata_uuid"].values[0]

        psql.update_city_search_tracking_table(drive_metadata_uuid)
        logging.info("Updated tracking table")

        psql.update_city_search_tracking_table_shopify_customer(
            drive_metadata_uuid=drive_metadata_uuid
        )
        logging.info("Updated tracking table for shopify customer")

        slack_metrics_city_search = psql.get_slack_channel_metrics_city_search(
            drive_metadata_uuid=drive_metadata_uuid
        )

        sheet_name = new_lead_data["file_name"].values[0]
        spread_sheet_name = gdrive.get_spread_name_by_url(google_sheet_url)

        sheet_and_file_name = f"{spread_sheet_name} - {sheet_name}"

        psql.send_update_slack_metrics(
            slack_webhook=os.environ.get("SLACK_WEBHOOK"),
            slack_df=slack_metrics_city_search,
            sheet_name=sheet_and_file_name,
            sheet_url=google_sheet_url,
        )

        
        psql.update_file_has_been_processed(file_id=file_id)