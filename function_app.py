import logging
import azure.functions as func


from dotenv import load_dotenv
import json
import os
import pandas as pd
import sentry_sdk
from sentry_sdk.integrations.serverless import serverless_function
from time import sleep
from pathlib import Path
import io

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)

logger = logging.getLogger(__name__)

# Add this line back!
app = func.FunctionApp()

sentry_sdk.init(
    dsn=os.environ["SENTRY_DSN"],
    traces_sample_rate=1.0,
    profiles_sample_rate=1.0,
)
def initialize_services():
    """Initialize all required services"""
    from app import (
        GoogleDrive,
        PostgresExporter, 
        SalesTransformations,
        create_gdrive_service,
        AzureBlobStorage,
    )
    
    services = {}
    
    try:
        services['gdrive'] = create_gdrive_service(
            service_account_b64_encoded=os.environ.get("SERVICE_ACCOUNT")
        )
        logger.info("Google Drive service initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize Google Drive service: {e}")
        raise
    
    try:
        services['psql'] = PostgresExporter(
            username=os.environ.get("PSQL_USERNAME"),
            password=os.environ.get("PSQL_PASSWORD"),
            host=os.environ.get("PSQL_SERVER"),
            port=os.environ.get("PSQL_PORT"),
            database=os.environ.get("PSQL_DATABASE"),
        )
        logger.info("PostgreSQL service initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize PostgreSQL service: {e}")
        raise
    
    try:
        services['az'] = AzureBlobStorage(connection_string=os.environ.get("SalesSyncBlogTrigger"))
        services['st'] = SalesTransformations(engine=services['psql'].engine, google_api=services['gdrive'])
        services['sheet_week'] = f"Week {pd.Timestamp('today').isocalendar().week}"
        logger.info("All services initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize services: {e}")
        raise
    
    return services

@app.schedule(
    schedule="*/10 * * * 1-5",
    arg_name="GoogleSalesSync",
    run_on_startup=False,
    use_monitor=True,
)
def sales_sync(GoogleSalesSync: func.TimerRequest) -> None:
    from app import (
    GoogleDrive,
    PostgresExporter,
    SalesTransformations,
    create_gdrive_service,
    AzureBlobStorage,
)
    services = initialize_services()
    gdrive = services['gdrive']
    psql = services['psql']
    az = services['az']
     
    if GoogleSalesSync.past_due:
        logger.info("The timer is past due!")
            
    all_child_modified_files = gdrive.get_modified_files_in_folder(
        folder_id=os.environ.get("PARENT_FOLDER"), delta_days=1
    )

    if all_child_modified_files:
        file_dataframe_all = gdrive.create_file_list_dataframe(
            all_child_modified_files, record_path=None
        )
        logger.info(f"Number of files edited: {file_dataframe_all.shape[0]}")
        file_config = gdrive.get_file_config(
            os.environ.get("QUICK_MAIL_CONFIG_NAME"),
            folder_id=os.environ.get("QUICK_MAIL_CONFIG_FOLDER_ID"),
        )
        
        logger.info('Checking if files exist in database')
        
        file_dataframe_all = file_dataframe_all.reset_index(drop=True)
        # file_dataframe_all = file_dataframe_all[file_dataframe_all['name'].str.contains('Larimer County CO -')]
        file_dataframe_new = psql.check_if_record_exists(
            table_name="drive_metadata",
            schema="sales_leads",
            source_dataframe=file_dataframe_all,
            look_up_column="id",
        )
        
        logger.info(f"Number of new files: {file_dataframe_new.shape[0]}")
        logger.info(f"Name of new files: {file_dataframe_new['name'].tolist()}")

        if not file_dataframe_new.empty:
            logger.info("New files to process")
            # TODO: change this into an upsert to avoid duplicates
            psql.insert_raw_data(
                dataset=file_dataframe_new.drop(columns=["mimeType"]),
                table_name="drive_metadata",
                schema="sales_leads",
            )
        

        missing_file_types = psql.get_missing_file_types(gdrive=gdrive)
        if not missing_file_types.empty:
            psql.update_file_types(missing_file_types[["uuid", "file_type"]])

        psql.update_config_metadata(file_config)
        psql.get_and_post_missing_config(slack_webhook=os.environ.get("SLACK_WEBHOOK"))

        if not file_dataframe_new.empty:
            logger.info("Processing new files")
            files_to_process = psql.get_files_to_process(
                file_dataframe_new["id"].tolist()
            )

            for file in files_to_process.itertuples():
                logger.info(f"Processing file: {file.name}")
                dataframe = psql.process_file(file.id, gdrive)
                parent_folder = gdrive.get_parent_folder(file.id)
                parent_name = gdrive.get_parent_folder_name(parent_folder[0])
                parent_name = parent_name.replace(" ", "_").lower().strip()
                sleep(5)
                az.upload_dataframe(
                    dataframe=dataframe,
                    container_name=f"salesfiles/{parent_name}",
                    blob_name=file.name.replace("xlsx", "csv"),
                    file_id=file.id,
                )
    else:
        logger.info("No files to process")

@app.blob_trigger(
    arg_name="myblob",
    path="salesfiles/zi_search/{name}.csv",
    connection="SalesSyncBlogTrigger",
    use_monitor=True,
    
)
def ZiSearchBlobTrigger(myblob: func.InputStream):
    logger.info(
        f"Python blob trigger function processed blob"
        f"Name: {myblob.name}"
        f"Blob Size: {myblob.length} bytes"
    )
    from app import (
    GoogleDrive,
    PostgresExporter,
    SalesTransformations,
    create_gdrive_service,
    AzureBlobStorage,
)

    services = initialize_services()
    gdrive = services['gdrive']
    psql = services['psql']
    az = services['az']
    st = services['st']
    sheet_week = services['sheet_week']

    blob_name_without_container = myblob.name.replace("salesfiles/", "")
    blob_metadata = az.get_blob_metadata(container_name='salesfiles', blob_name=blob_name_without_container)
    
    file_id = blob_metadata["metadata"]["file_id"]

    file_name = az.split_and_return_blob_name(myblob.name)

    has_file_been_processed = psql.check_if_file_has_been_processed(file_id=file_id)

    if not has_file_been_processed:
        
         
        blob_str = myblob.read().decode()
        logger.info(f"Processing file: {file_name}")
        df = pd.read_csv(io.StringIO(blob_str))
        psql.insert_raw_data(dataset=df, table_name="leads", schema="sales_leads")
        psql.update_file_has_been_processed(file_id=file_id)
        logger.info(f'Processing file_id {file_id}')
        
        
        new_lead_data_zi = st.get_new_zi_search_lead_data(file_name=file_name)

        if new_lead_data_zi.shape[0] == 0:
            logger.info(f'{file_name} emails have all been tracked and sent previously')

        if not new_lead_data_zi.empty:
            sheet_data = st.create_google_lead_data_frame(new_lead_data_zi)
            uuid = new_lead_data_zi["drive_metadata_uuid"].values[0]
            logger.info(f"uuid: {uuid} for {file_name}")

            logger.info(f"Writing {file_name} to google sheet")
            gdrive.write_to_google_sheet(
                dataframe=sheet_data,
                spreadsheet_name=f"Quick Mail Output - {sheet_week}",
                target_sheet=file_name,
                folder_id=os.environ.get("QUICK_MAIL_OUTPUT_PARENT_FOLDER_ID"),
            )
            logger.info("Wrote data to google sheet")

            psql.update_tracking_table(uuid)
            logger.info("Updated tracking table")

            psql.update_tracking_table_shopify_customer(drive_metadata_uuid=uuid)
            logger.info("Updated tracking table for shopify customer")

            slack_df = psql.get_slack_channel_metrics_zi_search(drive_metadata_uuid=uuid)
            psql.send_update_slack_metrics(
                slack_webhook=os.environ.get("SLACK_WEBHOOK"), slack_df=slack_df
            )

