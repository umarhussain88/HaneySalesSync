from dataclasses import dataclass
from googleapiclient.discovery import build, Resource
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2 import service_account
from typing import Optional
from io import BytesIO
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from gspread.exceptions import WorksheetNotFound, SpreadsheetNotFound
import logging
import sys


@dataclass
class GoogleDrive:
    creds: Optional[service_account.Credentials] = None
    drive_service: Optional[Resource] = None

    def __post_init__(self):
        self.creds = service_account.Credentials.from_service_account_info(self.creds)
        self.creds = self.creds.with_scopes(["https://www.googleapis.com/auth/drive"])
        self.drive_service = build("drive", "v3", credentials=self.creds)
        self.client = gspread.authorize(self.creds)

    def get_shared_with_me(self) -> list:
        files = (
            self.drive_service.files()
            .list(
                q="sharedWithMe=true and trashed=false and mimeType='application/vnd.google-apps.folder'",
                fields="files(id, name, parents, mimeType, createdTime, modifiedTime)",
            )
            .execute()
        )
        return files

    def get_child_folders_ids(self, parent_folder_id: str) -> dict:
        folders = (
            self.drive_service.files()
            .list(
                q=f"'{parent_folder_id}' in parents and trashed=false and mimeType='application/vnd.google-apps.folder'",
                fields="files(id, name, parents, createdTime, modifiedTime)",
            )
            .execute()
        )
        return folders

    # # get folder name by id
    def get_folder_dataframe(self) -> pd.DataFrame:
        folders = (
            self.drive_service.files()
            .list(
                q="trashed=false and mimeType='application/vnd.google-apps.folder'",
                fields=("files(name, id, parents)"),
            )
            .execute()
        )

        df = pd.json_normalize(folders["files"], max_level=0)

        df["parents"] = df["parents"].explode()

        parent_df = (
            pd.merge(
                df[["parents"]], df[["id", "name"]], left_on="parents", right_on="id"
            )
            .rename(columns={"name": "parent_name"})
            .drop("id", axis=1)
        )

        return pd.merge(
            df, parent_df, left_on="parents", right_on="parents"
        ).drop_duplicates()

    def get_recent_or_modified_files(
        self,
        delta_days: int = 7,
        sensor: Optional[bool] = False,
    ) -> list:
        if sensor:
            delta = (pd.Timestamp("now") - pd.DateOffset(seconds=60)).strftime(
                "%Y-%m-%dT%H:%M:%S"
            )
        else:
            delta = (pd.Timestamp.today() - pd.Timedelta(days=delta_days)).strftime(
                "%Y-%m-%dT00:00:00"
            )

        files = (
            self.drive_service.files()
            .list(
                q=f"trashed=false and (modifiedTime > '{delta}' or createdTime > '{delta}') and mimeType!='application/vnd.google-apps.folder'",
                fields="files(id, name, parents, createdTime, modifiedTime,owners,lastModifyingUser, fileExtension)",
                pageSize=1000,
            )
            .execute()
        )
        return files
    
    def get_modified_files_in_folder(self, folder_id: str, delta_days: int = 7) -> list:
        delta = (pd.Timestamp.today() - pd.Timedelta(days=delta_days)).strftime(
        "%Y-%m-%dT00:00:00"
    )

        files = (
            self.drive_service.files()
            .list(
                q=f"'{folder_id}' in parents and trashed=false and (modifiedTime > '{delta}' or createdTime > '{delta}') and mimeType!='application/vnd.google-apps.folder'",
                fields="files(id, name, parents, createdTime, modifiedTime,owners,lastModifyingUser, fileExtension)",
                pageSize=1000,
            )
            .execute()
        )
        return files
        

    def create_file_list_dataframe(
        self, folder_list: list, parent_folder: Optional[str] = None
    ) -> pd.DataFrame:
        file_list_df = pd.concat(
            [pd.json_normalize(folder["files"], max_level=1) for folder in folder_list]
        )
        
        if parent_folder and not file_list_df.empty:
            return file_list_df.loc[file_list_df["parents"].explode().eq(parent_folder)]
        else:
            return file_list_df

    def download_file(self, file_id: str, request_type: str = "media") -> BytesIO:
        request = self.drive_service.files().get_media(fileId=file_id)
        downloaded = BytesIO()
        downloader = MediaIoBaseDownload(downloaded, request)
        done = False
        while done is False:
            _, done = downloader.next_chunk()
        return downloaded


    def get_stream_object(self, file_id: str) -> pd.DataFrame:
        downloaded = self.download_file(file_id)
        downloaded.seek(0)
        return pd.read_csv(downloaded)

    # def get_stream_object(self, file_id: str) -> BytesIO:
    #     downloaded = self.download_file(file_id)
    #     return downloaded

    def get_spreadsheet(
        self,
        spreadsheet_name: str,
        worksheet_name: str,
        folder_id: Optional[str] = None,
    ) -> gspread.Worksheet:
        try:
            spreadsheet = self.client.open(spreadsheet_name, folder_id=folder_id)
        except SpreadsheetNotFound:
            self.create_new_quickmail_output_sheet(spreadsheet_name=spreadsheet_name, folder_id=folder_id)
            spreadsheet = self.client.open(spreadsheet_name)

        try:
            worksheet = spreadsheet.worksheet(worksheet_name)
            logging.info("Worksheet Found Returning worksheet object")
            return worksheet
        except WorksheetNotFound:
            new_worksheet = self.create_worksheet(
                sheet_name=worksheet_name, spreadsheet=spreadsheet
            )
            return new_worksheet

    def create_worksheet(
        self, sheet_name: str, spreadsheet: gspread.Spreadsheet
    ) -> gspread.Worksheet:
        """creates worksheet if not found

        Args:
            sheet_name (str): Sheetname of a given spreadsheet.
        """
        logging.info("Worksheet not found, creating new worksheet")
        worksheet = spreadsheet.add_worksheet(title=sheet_name, rows="100", cols="20")
        return worksheet

    def get_row_number_to_append_to(self, worksheet: gspread.Worksheet) -> int:
        current_row_count_values = len(worksheet.get_all_values())

        if current_row_count_values == 0:
            return 1
        else:
            return current_row_count_values + 1

    def write_to_google_sheet(
        self,
        dataframe: pd.DataFrame,
        spreadsheet_name: str,
        target_sheet: str,
        folder_id: str,
        replacement_strategy: Optional[str] = "replace",
    ) -> None:
        worksheet = self.get_spreadsheet(
            spreadsheet_name=spreadsheet_name, worksheet_name=target_sheet, folder_id=folder_id
        )

        if replacement_strategy == "replace":
            rc = 1
            worksheet.clear()
        else:
            rc = self.get_row_number_to_append_to(worksheet=worksheet)

        include_headers = True if rc <= 1 else False

        set_with_dataframe(
            dataframe=dataframe,
            worksheet=worksheet,
            row=rc,
            include_column_header=include_headers,
        )

    def get_file_config(
        self, config_name_spreadsheet_name: str, folder_id: str
    ) -> pd.DataFrame:
        """Get the config file from the google drive folder.

        if a file does not have an assoicated config file then:

        post the file to the google sheet

        don't post the file to the google sheet
        """

        config = self.get_spreadsheet(
            spreadsheet_name=config_name_spreadsheet_name,
            worksheet_name="Sheet1",
            folder_id=folder_id,
        )

        if len(config.get_all_values()) == 1:
            logging.info("No data in config file")
            return pd.DataFrame(data=[], columns=config.get_all_values()[0])
        else:
            return pd.DataFrame(config.get_all_records())

    def create_new_quickmail_output_sheet(self, spreadsheet_name: str, folder_id: str) -> None:
        file_metadata = {
            "name": spreadsheet_name,
            "mimeType": "application/vnd.google-apps.spreadsheet",
            "parents": [folder_id],
        }

        self.drive_service.files().create(body=file_metadata).execute()

    def get_parent_folder_name(self, parent_id: str) -> str:
        parent_folder = (
            self.drive_service.files()
            .get(fileId=parent_id, fields="name")
            .execute()["name"]
        )
        return parent_folder
    
    
    def get_parent_folder(self, file_id: str) -> list:
        file = self.drive_service.files().get(fileId=file_id, fields="parents").execute()
        parents = file.get("parents", [])
        return parents
    
    def get_all_parent_folders(self, folder_id: str, parent_folders=None) -> list:
        if parent_folders is None:
            parent_folders = []

        parents = self.get_parent_folder(folder_id)
        if not parents:
            return parent_folders

        for parent_id in parents:
            parent_folder_name = self.get_parent_folder_name(parent_id)
            parent_folders.append(parent_folder_name)
            self.get_all_parent_folders(parent_id, parent_folders)

        return parent_folders
    
    def get_file_type(self, file_id: str) -> str:
        file = self.drive_service.files().get(fileId=file_id, fields="fileExtension").execute()
        return file.get("fileExtension", [])
        
    #TODO add the following into the above class to recursively trawl child folders for modified files.
    
    # def get_all_files_in_folder(self, folder_id: str) -> list:
    #     query = f"'{folder_id}' in parents and trashed=false"
    #     results = self.drive_service.files().list(q=query, 
    #                                             fields='files(id, name, mimeType)').execute()
    #     items = results.get('files', [])
    #     return items

    # def get_modified_files_in_folder(self, folder_id: str, delta_days: int = 7) -> list:
    #     delta = (pd.Timestamp.today() - pd.Timedelta(days=delta_days)).strftime(
    #         "%Y-%m-%dT00:00:00"
    #     )

    #     all_files = []
    #     folders_to_check = [folder_id]

    #     while folders_to_check:
    #         current_folder_id = folders_to_check.pop(0)
    #         files = self.get_all_files_in_folder(current_folder_id)

    #         for file in files:
    #             if file['mimeType'] == 'application/vnd.google-apps.folder':
    #                 folders_to_check.append(file['id'])
    #             elif file['mimeType'] != 'application/vnd.google-apps.folder' and (file['modifiedTime'] > delta or file['createdTime'] > delta):
    #                 all_files.append(file)

    #     return all_files
        