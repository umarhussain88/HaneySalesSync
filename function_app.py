import logging
import azure.functions as func
from app import GoogleDrive, PostgresExporter
from dotenv import load_dotenv
import json
import os
import base64

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
    
    psql.insert_raw_data(file_dataframe,'drive_metadata','sales_leads' )
    
    for file in latest_files['files']:
        # need a psql function to get the primary key of the file and 
        # then add that key to the file
        # then run dbt models. 
        
        # will then need to output a view into a google sheet. 
        df = gdrive.get_stream_object(file['id'])
        psql.insert_raw_data(df, 'leads', 'sales_leads')
        
    logging.info("Python timer trigger function executed.")

