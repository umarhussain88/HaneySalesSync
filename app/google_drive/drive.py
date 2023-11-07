from dataclasses import dataclass
from googleapiclient.discovery import build, Resource
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2 import service_account
from typing import Optional
from io import BytesIO
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from gspread.exceptions import WorksheetNotFound
import logging


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
        self, delta_days: int = 7, sensor: Optional[bool] = False, 
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

    def create_file_list_dataframe(self, folder_list: list, parent_folder: Optional[str] = None) -> pd.DataFrame:
        file_list_df = pd.concat(
            [pd.json_normalize(folder["files"], max_level=1) for folder in folder_list]
        )

        return file_list_df.loc[file_list_df['parents'].explode().eq(parent_folder)]

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

    def update_tracking_table(self, dataframe) -> None:
        """Get the records posted to google sheet and mark them as sent
        in the tracking table.
        """
        pass

    def write_to_google_sheet(
        self, dataframe: pd.DataFrame, spreadsheet_name: str, target_sheet: str
    ) -> None:
        spreadsheet = self.client.open(spreadsheet_name)
        
        try:
            worksheet = spreadsheet.worksheet(target_sheet)
            logging.info('Worksheet found, updating worksheet')
            start_row = worksheet.row_count + 1 
            
            set_with_dataframe(worksheet, dataframe, row=start_row, include_column_header=False)
        except WorksheetNotFound:
            logging.info('Worksheet not found, creating new worksheet')
            worksheet = spreadsheet.add_worksheet(title=target_sheet, rows='100', cols='20')
            worksheet.clear()
            set_with_dataframe(worksheet, dataframe)
