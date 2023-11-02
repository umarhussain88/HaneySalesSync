import logging
import azure.functions as func
from app import GoogleDrive, PostgresExporter
from dotenv import load_dotenv
import json
import os

load_dotenv()

# TODO: cleans the objects & stores it for audit purposes.
# TODO: extend the Gdrive sheet to write to a Google Sheet.


app = func.FunctionApp()


@app.schedule(
    schedule="0 */1 * * *", arg_name="myTimer", run_on_startup=True, use_monitor=False
)
def sales_sync(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info("The timer is past due!")

    logging.info("Creating gdrive service")
    creds = os.environ.get("service_account")
    creds_json = json.loads(creds)
    gdrive = GoogleDrive(creds=creds_json)
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
    
    psql.insert_raw_data(file_dataframe,'drive_metadata','sales_leads' )
    
    for file in latest_files['files']:
        df = gdrive.get_stream_object(file['id'])
        psql.insert_raw_data(df, 'leads', 'sales_leads')
        
    logging.info("Python timer trigger function executed.")

