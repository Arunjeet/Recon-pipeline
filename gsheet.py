import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
import pandas as pd

# Authenticate using service account
creds = Credentials.from_service_account_file(
    "credentials_gsheet.json",
    scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
)

client = gspread.authorize(creds)

# Open spreadsheet by URL
sheet = client.open_by_url(
    "https://docs.google.com/spreadsheets/d/1mFLRmeCVrJPEfnwFQJcE3bXGb7FOg989wy2_-ZvnX6A/edit?gid=833080280#gid=833080280"
)

# Select tab
#worksheet = sheet.worksheet("Sheet1")

def sheetdump(df: pd.DataFrame,datatype: str):
    if datatype=="pnl":
        try:
            ws = sheet.worksheet("PnL comp chart")
        except gspread.exceptions.WorksheetNotFound:
            ws = sheet.add_worksheet(title=title, rows="1000", cols="1000")

        # Clear old content
        ws.clear()

        # (Optional) resize to DataFrame size for nicer sheet bounds
        nrows = max(len(df) + 1, 100)  # +1 for header
        ncols = max(len(df.columns), 26)
        ws.resize(rows=nrows, cols=ncols)

        # Write DataFrame (header in A1)
        set_with_dataframe(ws, df, include_index=False, include_column_header=True)

        # (Optional) freeze header row
        try:
            ws.freeze(rows=1)
        except Exception:
            pass
    elif datatype=="tb":
        try:
            ws = sheet.worksheet("Trial Balance")
        except gspread.exceptions.WorksheetNotFound:
            ws = sheet.add_worksheet(title="Trial Balance", rows="1000", cols="1000")

        # Clear old content
        ws.clear()

        # (Optional) resize to DataFrame size for nicer sheet bounds
        nrows = max(len(df) + 1, 100)  # +1 for header
        ncols = max(len(df.columns), 26)
        ws.resize(rows=nrows, cols=ncols)

        # Write DataFrame (header in A1)
        set_with_dataframe(ws, df, include_index=False, include_column_header=True)

        # (Optional) freeze header row
        try:
            ws.freeze(rows=1)
        except Exception:
            pass

    elif datatype=="accounttrans":
        try:
            ws = sheet.worksheet("Account_transactions_test")
        except gspread.exceptions.WorksheetNotFound:
            ws = sheet.add_worksheet(title="Account_transactions_test", rows="10000", cols="100")

        # Clear old content
        ws.clear()

        # (Optional) resize to DataFrame size for nicer sheet bounds
        nrows = max(len(df) + 1, 100)  # +1 for header
        ncols = max(len(df.columns), 26)
        ws.resize(rows=nrows, cols=ncols)

        # Write DataFrame (header in A1)
        set_with_dataframe(ws, df, include_index=False, include_column_header=True)

        # (Optional) freeze header row
        try:
            ws.freeze(rows=1)
        except Exception:
            pass

