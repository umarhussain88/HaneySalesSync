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


if __name__ == "__main__":
    if os.environ.get("FUNCTIONS_ENVIRONMENT") == "preview":
        logging.info("Running in preview mode")
        logging.info("Loading local settings")
        local_settings_path = Path(__file__).parent.joinpath("local.settings.dev.json")
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
        blob_name=f"city_search_enriched/122123 - Jackson County MO - CS Search.csv - Scrape-it Cloud Data.csv",
    )

    blob_metadata = az.get_blob_metadata(myblob.container, myblob.name)
    file_id = blob_metadata["metadata"]["file_id"]

    file_name = az.split_and_return_blob_name(myblob.name)

    has_file_been_processed = psql.check_if_file_has_been_processed(file_id=file_id)

    if not has_file_been_processed:
        logging.info(f"Processing city search enriched data for {file_name}")
        blob_bytes = myblob.read()
        df = pd.read_csv(io.BytesIO(blob_bytes))
        df.columns.str.strip()
        psql.insert_raw_data(df, "city_search_enriched", "sales_leads")

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

        sheet_and_file_name = f"{file_name} - {sheet_name}"

        psql.send_update_slack_metrics(
            slack_webhook=os.environ.get("SLACK_WEBHOOK"),
            slack_df=slack_metrics_city_search,
            sheet_name=sheet_and_file_name,
            sheet_url=google_sheet_url,
        )
