import logging
import azure.functions as func
from app import GoogleDrive, PostgresExporter, SalesTransformations
from dotenv import load_dotenv
import json
import os
import base64
import pandas as pd
import sentry_sdk
from sentry_sdk.integrations.serverless import serverless_function
from time import sleep
import sys

load_dotenv()


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

# TODO: if new config_file then need to update sheet.
# TODO: convert this scripts into readable functions - then refactor.


app = func.FunctionApp()


st = SalesTransformations()


def create_gdrive_service():
    logging.info("Creating gdrive service")
    encoded_json_string = os.environ.get("SERVICE_ACCOUNT")
    decoded_json_string = base64.b64decode(encoded_json_string).decode()
    decoded_data = json.loads(decoded_json_string)
    return GoogleDrive(
        creds=decoded_data,
    )


def create_psql_service():
    logging.info("Creating psql service")
    return PostgresExporter(
        username=os.environ.get("PSQL_USERNAME"),
        host=os.environ.get("PSQL_SERVER"),
        port=os.environ.get("PSQL_PORT"),
        password=os.environ.get("PSQL_PASSWORD"),
        database=os.environ.get("PSQL_DATABASE"),
    )


def process_file(file: str, gdrive: GoogleDrive, psql: PostgresExporter):
    uuid = psql.get_uuid_from_table(
        table_name="drive_metadata",
        schema="sales_leads",
        look_up_val=file,
        look_up_column="id",
    )

    df = gdrive.get_stream_object(file)
    df["drive_metadata_uuid"] = uuid["uuid"].values[0]
    psql.insert_raw_data(df, "leads", "sales_leads")
    logging.info("Inserted data into leads table")


def load_local_settings_as_env_vars(file_path: str):
    with open(file_path) as f:
        data = json.load(f)

    for key, value in data["Values"].items():
        os.environ[key] = value


if os.environ.get("FUNCTIONS_ENVIRONMENT") == "preview":
    load_local_settings_as_env_vars("local.settings.dev.json")


@serverless_function
@app.schedule(
    schedule="*/10 * * * 1-5",
    arg_name="GoogleSalesSync",
    run_on_startup=False,
    use_monitor=False,
)
def sales_sync(GoogleSalesSync: func.TimerRequest) -> None:
    if GoogleSalesSync.past_due:
        logging.info("The timer is past due!")

    logging.info("Creating gdrive service")
    gdrive = create_gdrive_service()

    psql = create_psql_service()

    latest_files = gdrive.get_recent_or_modified_files(delta_days=1)

    file_dataframe = gdrive.create_file_list_dataframe(
        [latest_files], parent_folder=os.environ.get("PARENT_FOLDER")
    )

    if file_dataframe.empty:
        logging.info("No new files to process")
    else:
            
        logging.info(f"Number of files edited: {file_dataframe.shape[0]}")

        logging.info("Getting file config")

        file_config = gdrive.get_file_config(
            os.environ.get("QUICK_MAIL_CONFIG_NAME"),
            folder_id=os.environ.get("QUICK_MAIL_CONFIG_FOLDER_ID"),
        )
        logging.info(f"Number of config files: {file_config.shape[0]}")

        logging.info("Updating config metadata")

        file_dataframe_new = psql.check_if_record_exists(
            table_name="drive_metadata",
            schema="sales_leads",
            source_dataframe=file_dataframe,
            look_up_column="id",
        )

        logging.info(f"Number of new files: {file_dataframe_new.shape[0]}")

        if not file_dataframe_new.empty:
            psql.insert_raw_data(
                dataset=file_dataframe_new,
                table_name="drive_metadata",
                schema="sales_leads",
            )

        psql.update_config_metadata(file_config)

        ## get missing config file records and post notification to slack.
        psql.get_and_post_missing_config(slack_webhook=os.environ.get("SLACK_WEBHOOK"))
        psql.update_drive_table_slack_posted()

        for file in file_dataframe_new["id"].tolist():
            process_file(file, gdrive, psql)

        new_lead_data = st.get_new_lead_data(psql.engine)

        sheet_week = f"Week {pd.Timestamp('today').isocalendar().week}"
        if not new_lead_data.empty:
            for filename, data in new_lead_data.groupby("file_name"):
                sheet_data = st.create_google_lead_data_frame(data)
                uuid = data["drive_metadata_uuid"].values[0]
                logging.info(f"uuid: {uuid} for {filename}")

                logging.info(f"Writing {filename} to google sheet")
                gdrive.write_to_google_sheet(
                    dataframe=sheet_data,
                    spreadsheet_name=f"Quick Mail Output - {sheet_week}",
                    target_sheet=filename,
                    folder_id=os.environ.get("QUICK_MAIL_OUTPUT_PARENT_FOLDER_ID"),
                )
                logging.info("Wrote data to google sheet")

                psql.update_tracking_table(uuid)
                logging.info("Updated tracking table")
                sleep(1)
                psql.update_tracking_table_shopify_customer(drive_metadata_uuid=uuid)
                logging.info("Updated tracking table for shopify customer")

                slack_df = psql.get_slack_channel_metrics(drive_metadata_uuid=uuid)
                psql.send_update_slack_metrics(
                    slack_webhook=os.environ.get("SLACK_WEBHOOK"), slack_df=slack_df
                )

            else:
                logging.info("No new leads to process")
    logging.info("Python timer trigger function executed.")
