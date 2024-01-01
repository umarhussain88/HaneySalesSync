from .google_drive.drive import GoogleDrive
from .data import PostgresExporter, AzureExporter, SalesTransformations
import logging 
import base64
import json
import os
from pathlib import Path





def create_gdrive_service(service_account_b64_encoded: str = None):
    logging.info("Creating gdrive service")
    encoded_json_string = service_account_b64_encoded
    decoded_json_string = base64.b64decode(encoded_json_string).decode()
    decoded_data = json.loads(decoded_json_string)
    return GoogleDrive(
        creds=decoded_data,
    )


def load_local_settings_as_env_vars(file_path: Path):
    with open(file_path) as f:
        data = json.load(f)

    for key, value in data["Values"].items():
        os.environ[key] = value
