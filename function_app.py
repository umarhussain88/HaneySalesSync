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
logging.info(os.environ["SENTRY_DSN"])

sentry_sdk.init(
    dsn=os.environ["SENTRY_DSN"],
    # Set traces_sample_rate to 1.0 to capture 100%
    # of transactions for performance monitoring.
    traces_sample_rate=1.0,
    # Set profiles_sample_rate to 1.0 to profile 100%
    # of sampled transactions.
    # We recommend adjusting this value in production.
    profiles_sample_rate=1.0,
)

app = func.FunctionApp()


def load_local_settings_as_env_vars(file_path: Path):
    with open(file_path) as f:
        data = json.load(f)

    for key, value in data["Values"].items():
        os.environ[key] = value


if os.environ.get("FUNCTIONS_ENVIRONMENT") == "preview":
    logging.info("Running in preview mode")
    logging.info("Loading local settings")
    local_settings_path = Path(__file__).parent.joinpath("local.settings.dev.json")
    load_local_settings_as_env_vars(local_settings_path)


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

sheet_week = f"Week {pd.Timestamp('today').isocalendar().week}"


@serverless_function
@app.schedule(
    schedule="*/10 * * * 1-5",
    arg_name="GoogleSalesSync",
    run_on_startup=True,
    use_monitor=False,
)
def sales_sync(GoogleSalesSync: func.TimerRequest) -> None:
    if GoogleSalesSync.past_due:
        logging.info("The timer is past due!")

    if os.environ.get("FUNCTIONS_ENVIRONMENT") == "preview":
        logging.info("Running in preview mode")
        logging.info("Loading local settings")
        local_settings_path = Path(__file__).parent.joinpath("local.settings.dev.json")
        load_local_settings_as_env_vars(local_settings_path)

    # testing all folders from a parent folder.
    all_child_modified_files = gdrive.get_modified_files_in_folder(
        folder_id=os.environ.get("PARENT_FOLDER"), delta_days=60
    )

    file_dataframe_all = gdrive.create_file_list_dataframe(
        all_child_modified_files, record_path=None
    )
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
        logging.info("New files to process")
        psql.insert_raw_data(
            dataset=file_dataframe_all.drop(columns=["mimeType"]),
            table_name="drive_metadata",
            schema="sales_leads",
        )

    missing_file_types = psql.get_missing_file_types(gdrive=gdrive)
    if not missing_file_types.empty:
        psql.update_file_types(missing_file_types[["uuid", "file_type"]])

    psql.get_and_post_missing_config(slack_webhook=os.environ.get("SLACK_WEBHOOK"))
    psql.update_config_metadata(file_config)

    if not file_dataframe_new.empty:
        logging.info("Processing new files")
        files_to_process = psql.get_files_to_process(file_dataframe_new["id"].tolist())

        for file in files_to_process.itertuples():
            logging.info(f"Processing file: {file.name}")
            dataframe = psql.process_file(file.id, gdrive, file.file_type)
            parent_folder = gdrive.get_parent_folder(file.id)
            parent_name = gdrive.get_parent_folder_name(parent_folder[0])
            parent_name = parent_name.replace(" ", "_").lower().strip()
            az.upload_dataframe(
                dataframe=dataframe,
                container_name=f"salesfiles/{parent_name}",
                blob_name=file.name.replace("xlsx", "csv"),
            )



@app.blob_trigger(
    arg_name="myblob",
    path="salesfiles/zi_search/{name}.csv",
    connection="SalesSyncBlogTrigger",
)
def ZiSearchBlobTrigger(myblob: func.InputStream):
    logging.info(
        f"Python blob trigger function processed blob"
        f"Name: {myblob.name}"
        f"Blob Size: {myblob.length} bytes"
    )

    file_name = az.split_and_return_blob_name(myblob.name)
    has_file_been_processed = psql.check_if_file_has_been_processed(file_name=file_name)

    if not has_file_been_processed:
        blob_str = myblob.read().decode()
        logging.info(f"Processing file: {file_name}")
        df = pd.read_csv(io.StringIO(blob_str))
        psql.insert_raw_data(dataset=df, table_name="zi_search", schema="sales_leads")
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

        slack_df = psql.get_slack_channel_metrics(drive_metadata_uuid=uuid)
        psql.send_update_slack_metrics(
            slack_webhook=os.environ.get("SLACK_WEBHOOK"), slack_df=slack_df
        )


@app.blob_trigger(
    arg_name="myblob",
    path="salesfiles/city_search/{name}.csv",
    connection="SalesSyncBlogTrigger",
)
def CitySearchBlogTrigger(myblob: func.InputStream):
    logging.info(
        f"Python blob trigger function processed blob"
        f"Name: {myblob.name}"
        f"Blob Size: {myblob.length} bytes"
    )

    file_name = az.split_and_return_blob_name(myblob.name)
    file_name = file_name.replace('csv', 'xlsx') # TODO: remove this once I add in blob metadata and use that to filter for data.
    has_file_been_processed = psql.check_if_file_has_been_processed(file_name=file_name)

    logging.info('Getting franchise data')
    franchise_df = gdrive.get_franchise_data(
        file_id=os.environ.get("FRANCHISE_MASTER_LIST_FILE_ID")
    )
    logging.info('Upserting franchise data')
    psql.upsert_franchise_data(
        dataframe=franchise_df, temp_table_name="temp_franchise_data"
    )

    if not has_file_been_processed:
        blob_bytes = myblob.read()
        df = pd.read_csv(io.BytesIO(blob_bytes))
        psql.insert_raw_data(df, "city_search", "sales_leads")

        logging.info("Creating city search output")
        sheet_url_dict = st.post_city_search_data_to_google_sheet(
            spreadsheet_name=f"City Search Output - {sheet_week}",
            folder_id=os.environ.get("CITY_SEARCH_OUTPUT_PARENT_FOLDER_ID"),
            file_name=file_name, 
        )
        psql.post_city_search_slack_message(
            link=sheet_url_dict[file_name], spread_sheet_name=file_name
        )
        
        psql.update_file_has_been_processed(file_name=file_name)


@app.blob_trigger(arg_name="myblob", path="salesfiles/city_search_enriched/{name}.csv", connection="SalesSyncBlogTrigger")
def CitySearchEnrichedBlogTrigger(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob"
                f"Name: {myblob.name}"
                f"Blob Size: {myblob.length} bytes")
