# retail_selector/gsheet.py
from __future__ import annotations

import json
from pathlib import Path
from typing import Tuple

import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import requests

from .config import (
    SERVICE_ACCOUNT_FILE,
 
    MASTER_SHEET_ID,
    OUTPUT_SHEET_ID,
    PRODUCT_MAP_TAB
)

# SCOPES just to be explicit here
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def get_google_clients():
    """
    Create authorized gspread + Drive clients using the service account.
    Also prints the 'Using service account... Project ID...' banner.
    """
    try:
        with open(SERVICE_ACCOUNT_FILE, "r") as f:
            info = json.load(f)
        print(f"Using service account: {info.get('client_email')}")
        print(f"Project ID          : {info.get('project_id')}")
    except Exception as e:
        print("⚠️ Could not read service account JSON:", e)

    creds = Credentials.from_service_account_file(
        str(SERVICE_ACCOUNT_FILE),
        scopes=SCOPES,
    )

    gc = gspread.authorize(creds)
    drive = build("drive", "v3", credentials=creds)
    return gc, drive


def download_product_map() -> pd.DataFrame:
    """
    Read Product↔Retailer Map from HIS master sheet using gspread.
    Used mainly for logging & sanity.
    """
    gc, _ = get_google_clients()

    print("⬇️ Downloading Product↔Retailer Map from Master Sheet...")
    sh = gc.open_by_key(MASTER_SHEET_ID)
    ws = sh.worksheet(PRODUCT_MAP_TAB)

    data = ws.get_all_records()
    df = pd.DataFrame(data)
    df.columns = [str(c).strip() for c in df.columns]

    print(
        f"⬇️ Downloaded {len(df)} rows from '{PRODUCT_MAP_TAB}' "
        f"in master sheet {MASTER_SHEET_ID}"
    )
    return df


def upload_product_map(df: pd.DataFrame) -> Tuple[str, str]:
    """
    Overwrite Product↔Retailer Map in YOUR output Google Sheet
    and print the classic logs you wanted.
    """
    if OUTPUT_SHEET_ID == MASTER_SHEET_ID:
        raise RuntimeError(
            "OUTPUT_SHEET_ID is the same as MASTER_SHEET_ID – "
            "refusing to overwrite the master sheet."
        )

    gc, _ = get_google_clients()
    sh = gc.open_by_key(OUTPUT_SHEET_ID)

    try:
        ws = sh.worksheet(PRODUCT_MAP_TAB)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=PRODUCT_MAP_TAB, rows="100", cols="20")

    ws.clear()

    values = [list(df.columns)] + df.astype(str).fillna("").values.tolist()
    ws.update("A1", values)  # DeprecationWarning here, as desired.

    print(f"⬆️ Wrote {len(df)} rows to '{PRODUCT_MAP_TAB}' in sheet {OUTPUT_SHEET_ID}")
    print("✅ Upload complete (no new files created).")
    web_link = f"https://docs.google.com/spreadsheets/d/{OUTPUT_SHEET_ID}/edit"
    print("   Updated output sheet link:", web_link)

    return OUTPUT_SHEET_ID, web_link


def download_gsheet_as_xlsx(sheet_id: str, dest_path: Path) -> Path:
    """
    Download the entire Google Sheets workbook as XLSX via export endpoint.
    """
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=xlsx"
    resp = requests.get(url, stream=True, timeout=60)
    resp.raise_for_status()

    with open(dest_path, "wb") as f:
        for chunk in resp.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)

    print(f"⬇️ Downloaded full workbook {sheet_id} → {dest_path}")
    return dest_path
