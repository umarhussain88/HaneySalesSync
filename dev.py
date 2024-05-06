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
    
    files = ['xMarga-043024-Hotel-Person-91.csv', 'xMarga-043024-University-Person-65.csv']
    
    for file_n in files:
        
        myblob = az.get_blob_from_container(
            container_name="salesfiles",
            blob_name=f"zi_search/{file_n}",
        )
            
        blob_name_without_container = myblob.name.replace("salesfiles/", "")
        blob_metadata = az.get_blob_metadata(container_name='salesfiles', blob_name=blob_name_without_container)
            
        file_id = blob_metadata["metadata"]["file_id"]

        file_name = az.split_and_return_blob_name(myblob.name)

        has_file_been_processed = psql.check_if_file_has_been_processed(file_id=file_id)

        if not has_file_been_processed:
            
            
            blob_str = myblob.read().decode()
            logging.info(f"Processing file: {file_name}")
            df = pd.read_csv(io.StringIO(blob_str))
            psql.insert_raw_data(dataset=df, table_name="leads", schema="sales_leads")
            psql.update_file_has_been_processed(file_id=file_id)
            
            
            new_lead_data_zi = st.get_new_zi_search_lead_data(file_name=file_name)

            if not new_lead_data_zi.empty:
                sheet_data = st.create_google_lead_data_frame(new_lead_data_zi)
                uuid = new_lead_data_zi["drive_metadata_uuid"].values[0]
                logging.info(f"uuid: {uuid} for {file_name}")

                logging.info(f"Writing {file_name} to google sheet")
                gdrive.write_to_google_sheet(
                    dataframe=sheet_data,
                    spreadsheet_name=f"Quick Mail Output - {sheet_week}",
                    target_sheet=file_name,
                    folder_id=os.environ.get("QUICK_MAIL_OUTPUT_PARENT_FOLDER_ID"),
                )
                logging.info("Wrote data to google sheet")

                psql.update_tracking_table(uuid)
                logging.info("Updated tracking table")

                psql.update_tracking_table_shopify_customer(drive_metadata_uuid=uuid)
                logging.info("Updated tracking table for shopify customer")

                slack_df = psql.get_slack_channel_metrics_zi_search(drive_metadata_uuid=uuid)
                psql.send_update_slack_metrics(
                    slack_webhook=os.environ.get("SLACK_WEBHOOK"), slack_df=slack_df
                )
        
        st.create_zi_list(quick_mail_folder_id=os.environ.get("QUICK_MAIL_CONFIG_FOLDER_ID"))
