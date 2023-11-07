import logging
import azure.functions as func
from app import GoogleDrive, PostgresExporter, SalesTransformations
from dotenv import load_dotenv
import json
import os
import base64
import pandas as pd

load_dotenv()

# TODO: write output to tracking table
# TODO: write query to insert into table if lead is/was shopify.


app = func.FunctionApp()


st = SalesTransformations()


def load_local_settings_as_env_vars(file_path: str):
    with open(file_path) as f:
        data = json.load(f)

    for key, value in data["Values"].items():
        os.environ[key] = value


if os.environ.get("FUNCTIONS_ENVIRONMENT") == "preview":
    load_local_settings_as_env_vars("local.settings.dev.json")


@app.schedule(
    schedule="*/2 * * * *", arg_name="GoogleSalesSync", run_on_startup=True, use_monitor=False
)
def sales_sync(GoogleSalesSync: func.TimerRequest) -> None:
    if GoogleSalesSync.past_due:
        logging.info("The timer is past due!")

    logging.info("Creating gdrive service")
    encoded_json_string = os.environ.get("SERVICE_ACCOUNT")
    decoded_json_string = base64.b64decode(encoded_json_string).decode()
    decoded_data = json.loads(decoded_json_string)

    gdrive = GoogleDrive(creds=decoded_data)
    psql = PostgresExporter(
        username=os.environ.get("PSQL_USERNAME"),
        host=os.environ.get("PSQL_SERVER"),
        port=os.environ.get("PSQL_PORT"),
        password=os.environ.get("PSQL_PASSWORD"),
        database=os.environ.get("PSQL_DATABASE"),
        
    )

    latest_files = gdrive.get_recent_or_modified_files(delta_days=1)

    file_dataframe = gdrive.create_file_list_dataframe(
        [latest_files], parent_folder=os.environ.get("PARENT_FOLDER")
    )

    logging.info(f"Number of files edited: {file_dataframe.shape[0]}")

    if not file_dataframe.empty:
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

        for file in file_dataframe_new["id"].tolist():
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

        new_lead_data = st.get_new_lead_data(file_dataframe_new, psql.engine)

        if not new_lead_data.empty:
            sheet_data = st.create_google_lead_sheet(new_lead_data)
            
            worksheet_name = pd.Timestamp('today').strftime('%Y%m%d')

            gdrive.write_to_google_sheet(
                sheet_data, "Sales Output", target_sheet=worksheet_name
            )
            
            logging.info('Wrote data to google sheet')
            
            psql.update_tracking_table(
                'posted', new_lead_data
            )
            logging.info('Updated tracking table')

    else:
        logging.info("No new files to process")

    logging.info("Python timer trigger function executed.")
