import logging
import azure.functions as func
from app import GoogleDrive, PostgresExporter
from dotenv import load_dotenv
import json
import os
import base64

load_dotenv()

# TODO: extend the Gdrive sheet to write to a Google Sheet.


app = func.FunctionApp()

def load_local_settings_as_env_vars(file_path: str):
    with open(file_path) as f:
        data = json.load(f)

    for key, value in data['Values'].items():
        os.environ[key] = value
        

if os.environ.get('FUNCTIONS_ENVIRONMENT') == 'preview':
    load_local_settings_as_env_vars('local.settings.dev.json')
    


@app.schedule(
    schedule="0 */1 * * *", arg_name="myTimer", run_on_startup=True, use_monitor=False
)
def sales_sync(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info("The timer is past due!")

    logging.info("Creating gdrive service")
    encoded_json_string = os.environ.get("service_account")
    decoded_json_string = base64.b64decode(encoded_json_string).decode()
    decoded_data = json.loads(decoded_json_string)

    gdrive = GoogleDrive(creds=decoded_data)
    psql = PostgresExporter(
        username=os.environ.get("psql_username"),
        host=os.environ.get("psql_server"),
        port=os.environ.get("psql_port"),
        password=os.environ.get("psql_password"),
        database=os.environ.get("psql_database"),
    )

    latest_files = gdrive.get_recent_or_modified_files()
    logging.info(f"Number of files edited: {len(latest_files['files'])}")

    file_dataframe = gdrive.create_file_list_dataframe([latest_files])

    psql.insert_raw_data(
        dataset=file_dataframe, table_name="drive_metadata", schema="sales_leads"
    )

    for file in latest_files["files"]:

        uuid = psql.get_uuid_from_table(
            table_name="drive_metadata",
            schema="sales_leads",
            look_up_val=file["id"],
            look_up_column="id",
        )
        df = gdrive.get_stream_object(file["id"])
        df['drive_metadata_uuid'] = uuid['uuid'].values[0]
        psql.insert_raw_data(df, "leads", "sales_leads")

    logging.info("Python timer trigger function executed.")
