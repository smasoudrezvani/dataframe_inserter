import pandas as pd
from googleapiclient.discovery import build
from google.oauth2 import service_account


class BaseGoogleSheetsHandler:
    """
    Base class for handling Google Sheets API interactions.
    """

    def __init__(self, service_account_file: str):
        """
        Initializes the BaseGoogleSheetsHandler by loading Google Sheets API credentials.

        Args:
            service_account_file (str): Path to the Google Cloud service account file (JSON format).
        """
        self.service = self.build_service(service_account_file)

    def build_service(self, service_account_file: str):
        """
        Builds the Google Sheets API service.

        Args:
            service_account_file (str): Path to the Google Cloud service account file (JSON format).

        Returns:
            service: Google Sheets API service object.
        """
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
        creds = service_account.Credentials.from_service_account_file(service_account_file, scopes=SCOPES)
        return build('sheets', 'v4', credentials=creds)

    def execute_query(self, spreadsheet_id: str, range_name: str):
        """
        Executes a query to retrieve data from a Google Sheet.

        Args:
            spreadsheet_id (str): The ID of the Google Spreadsheet.
            range_name (str): The range in the spreadsheet to retrieve data from.

        Returns:
            dict: The data retrieved from the Google Sheet.
        """
        sheet = self.service.spreadsheets()
        result = sheet.values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()
        return result.get('values', [])


class GoogleSheetsInserter(BaseGoogleSheetsHandler):
    """
    Class for inserting a Pandas DataFrame into a Google Sheet.
    """

    def __init__(self, service_account_file: str):
        """
        Initializes the GoogleSheetsInserter with Google Sheets API credentials.

        Args:
            service_account_file (str): Path to the Google Cloud service account file (JSON format).
        """
        super().__init__(service_account_file)

    def insert_df_to_google_sheet(self, df: pd.DataFrame, spreadsheet_id: str, sheetname: str,
                                  start_column: str, end_column: str) -> None:
        """
        Inserts a Pandas DataFrame into a Google Sheet at the specified range.

        Args:
            df (pd.DataFrame): The DataFrame to be inserted into the Google Sheet.
            spreadsheet_id (str): The ID of the target Google Spreadsheet.
            sheetname (str): The name of the sheet within the spreadsheet to insert data into.
            start_column (str): The starting column in the range where data will be inserted.
            end_column (str): The ending column in the range where data will be inserted.

        Returns:
            None: Inserts the DataFrame into the specified Google Sheet.

        Raises:
            Exception: If the Google Sheets API call fails or the data insertion fails.
        """
        try:
            # Fetch the current values from the specified sheet and range
            values = self.execute_query(spreadsheet_id, sheetname + "!" + start_column + ":" + end_column)

            # Append the DataFrame's values to the Google Sheet
            sheet = self.service.spreadsheets()
            sheet.values().update(
                spreadsheetId=spreadsheet_id,
                range=sheetname + "!A" + str(len(values) + 1),
                valueInputOption="RAW",
                body={"values": df.values.tolist()}
            ).execute()

            print("Data successfully inserted into Google Sheet")
        except Exception as e:
            print(f"Failed to insert data: {e}")


# Usage Example (to be removed when packaging):
# inserter = GoogleSheetsInserter(service_account_file='path_to_service_account.json')
# inserter.insert_df_to_google_sheet(df, 'spreadsheet_id', 'Sheet1', 'A', 'S')
