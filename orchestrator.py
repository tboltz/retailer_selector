# retail_selector/orchestrator.py
from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Dict, Any

from .config import (
    load_secrets,
    DEFAULT_WORKBOOK_PATH,
    DEFAULT_SECRETS_PATH,
    MASTER_SHEET_ID,
)
from .gsheet import download_product_map, upload_product_map, download_gsheet_as_xlsx
from .workbook import scan_workbook_async
from .emailer import send_email_with_attachment_async


async def run_scan_from_gsheet_and_email(
    workbook_path: Path,
    secrets_path: Path,
    limit: Optional[int] = None,
    concurrency: int = 20,
) -> Dict[str, Any]:
    """
    1) Load secrets (ScrapingBee, OpenAI, SMTP, emails).
    2) gspread: download Product↔Retailer Map (for logging).
    3) Export full Google Sheet → XLSX.
    4) Scan Excel workbook.
    5) Push updated Product↔Retailer Map back to your Google sheet.
    6) Email XLSX snapshot.
    """
    print("🔐 Loading secrets...")
    secrets = load_secrets(secrets_path)

    scrapingbee_api_key = secrets["SCRAPINGBEE_API_KEY"]
    smtp_server = secrets["SMTP_SERVER"]
    smtp_port   = int(secrets["SMTP_PORT"])
    smtp_user   = secrets["SMTP_USERNAME"]
    smtp_pass   = secrets["SMTP_PASSWORD"]
    email_from  = secrets["EMAIL_FROM"]
    email_to    = secrets["EMAIL_TO"]

    # STEP 1: gspread download → logs
    await asyncio.to_thread(download_product_map)

    # STEP 2: full workbook export
    print("\n=== STEP 2: Download full Google Sheet → XLSX ===")
    await asyncio.to_thread(download_gsheet_as_xlsx, MASTER_SHEET_ID, workbook_path)

    # STEP 3: async scan
    print("\n=== STEP 3: Async scan & update Excel workbook ===")
    workbook_path, updated_product_df = await scan_workbook_async(
        workbook_path=workbook_path,
        scrapingbee_api_key=scrapingbee_api_key,
        limit=limit,
        concurrency=concurrency,
    )

    # STEP 4: sync updated Product↔Retailer Map back to Google
    sheet_id, web_link = await asyncio.to_thread(upload_product_map, updated_product_df)

    print("\n✅ Async pipeline complete.")
    print(f"   Updated sheet link: {web_link}")
    print("Preview of updated sheet (first 10 rows):")
    try:
        from IPython.display import display
        display(updated_product_df.head(10))
    except Exception:
        print(updated_product_df.head(10))

    # STEP 5: email workbook
    print("\n=== STEP 5: Email updated workbook ===")
    subject = "Retail Selector: Updated Retail Arbitrage Targeting List"
    body = (
        "Attached is the latest updated copy of your Retail Arbitrage Targeting List.\n\n"
        f"Generated at {datetime.now(timezone.utc).isoformat()}"
    )

    await send_email_with_attachment_async(
        smtp_server=smtp_server,
        smtp_port=smtp_port,
        username=smtp_user,
        password=smtp_pass,
        email_from=email_from,
        email_to=email_to,
        subject=subject,
        body=body,
        attachment_path=workbook_path,
    )

    print("\n✅ All done: download → update → sync to Google → email.")

    return {
        "workbook_path": str(workbook_path),
        "email_to": email_to,
        "rows_scanned_limit": limit,
        "google_sheet_link": web_link,
    }


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Retail Selector: async retail arbitrage scanner."
    )
    parser.add_argument(
        "--workbook-path",
        type=Path,
        default=DEFAULT_WORKBOOK_PATH,
        help="Path to local XLSX workbook to overwrite.",
    )
    parser.add_argument(
        "--secrets-path",
        type=Path,
        default=DEFAULT_SECRETS_PATH,
        help="Path to secrets.json (ScrapingBee, OpenAI, SMTP, etc.).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional max number of Product↔Retailer rows to scan.",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=20,
        help="Max concurrent ScrapingBee requests.",
    )
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    asyncio.run(
        run_scan_from_gsheet_and_email(
            workbook_path=args.workbook_path,
            secrets_path=args.secrets_path,
            limit=args.limit,
            concurrency=args.concurrency,
        )
    )


if __name__ == "__main__":
    main()
