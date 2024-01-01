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



def load_local_settings_as_env_vars(file_path: Path):
    with open(file_path) as f:
        data = json.load(f)

    for key, value in data["Values"].items():
        os.environ[key] = value


if os.environ.get("FUNCTIONS_ENVIRONMENT") == "preview":
    logging.info('Running in preview mode')
    logging.info('Loading local settings')
    local_settings_path = Path(__file__).parent.joinpath("local.settings.dev.json")
    load_local_settings_as_env_vars(local_settings_path)


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

    
    
    gdrive = create_gdrive_service(service_account_b64_encoded=os.environ.get("SERVICE_ACCOUNT"))

    psql = PostgresExporter(
        username=os.environ.get("PSQL_USERNAME"),
        password=os.environ.get("PSQL_PASSWORD"),
        host=os.environ.get("PSQL_SERVER"),
        port=os.environ.get("PSQL_PORT"),
        database=os.environ.get("PSQL_DATABASE"),
    )

    st = SalesTransformations(engine=psql.engine)
    
    latest_files = gdrive.get_recent_or_modified_files(delta_days=30)

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

        for file in file_dataframe_new.itertuples():
            psql.process_file(file.id, gdrive, file.fileExtension)

        new_lead_data = st.get_new_zi_search_lead_data(psql.engine)

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


