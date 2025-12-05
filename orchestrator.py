# retail_selector/orchestrator.py
from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Iterable, Dict, Any, List

import pandas as pd

from .config import (
    load_secrets,
    DEFAULT_WORKBOOK_PATH,
    DEFAULT_SECRETS_PATH,
    MASTER_SHEET_ID,
)
from .gsheet import (
    download_product_map,
    upload_product_map,
    download_gsheet_as_xlsx,
)
from .workbook import scan_workbook_async
from .emailer import send_email_with_attachment_async
from .scraping import scrapingbee_fetch_many
from .parsing import hybrid_lookup_from_bee_result


# -------------------------------------------------------------------
# MODE 1: Direct Product↔Retailer Map scanner (debug / --rows mode)
# -------------------------------------------------------------------


async def run_hybrid_pricer_async(
    scrapingbee_api_key: str,
    limit: Optional[int] = None,
    upload: bool = False,
    concurrency: int = 20,
    row_indices: Optional[Iterable[int]] = None,
) -> pd.DataFrame:
    """
    Direct scanner that works only on the Product↔Retailer Map sheet.

    Steps:
      1) Download Product↔Retailer Map via gspread.
      2) Optional: restrict to specific 0-based row indices (for debugging).
      3) ScrapingBee fetch for each URL.
      4) Hybrid parse (pattern + optional OpenAI HTML).
      5) Optionally upload updated map back to Google Sheet.
    """
    # 1) Download the live Product↔Retailer Map
    df = download_product_map()
    df.columns = [str(c).strip() for c in df.columns]

    if df.empty:
        print("[orchestrator] Product↔Retailer Map is empty. Nothing to scan.")
        return df

    # 2) Restrict rows if requested
    if row_indices is not None:
        idx_list = sorted(set(int(i) for i in row_indices))
        df = df.iloc[idx_list].copy()
        print(f"[orchestrator] Debug mode: restricting to rows {idx_list}")
    elif limit is not None:
        df = df.head(limit).copy()
        print(f"[orchestrator] Limiting scan to first {limit} rows")

    if df.empty:
        print("[orchestrator] After filtering, no rows remain to scan.")
        return df

    # 3) Ensure KPI / output columns
    output_cols: Dict[str, str] = {
        "In Stock (Y/N)": "object",
        "Price ($USD)": "float64",
        "Last Scan (UTC)": "object",
        "HTTP Status": "object",
        "Parse Method": "object",
        "Response ms": "float64",
        "Last Error": "object",
        "URL Status": "object",
        "Validation Issues": "object",
    }

    for col, dtype in output_cols.items():
        if col not in df.columns:
            df[col] = pd.Series([None] * len(df), dtype="object")
        try:
            if dtype.startswith("float"):
                df[col] = pd.to_numeric(df[col], errors="coerce")
        except Exception:
            # The sheet has Opinions about dtypes; we don't argue.
            pass

    # 4) Build URL list
    if "search_url" not in df.columns:
        raise KeyError("Expected column 'search_url' in Product↔Retailer Map.")

    urls: List[str] = []
    row_indices_in_df: List[int] = []

    for idx, row in df.iterrows():
        url = str(row.get("search_url") or "").strip()
        if not url:
            df.at[idx, "URL Status"] = "missing_url"
            df.at[idx, "Last Error"] = "No search_url provided"
            continue
        urls.append(url)
        row_indices_in_df.append(idx)

    if not urls:
        print("[orchestrator] No URLs to scan (all search_url empty).")
        return df

    print(f"[orchestrator] Fetching {len(urls)} URLs with concurrency={concurrency}")

    # 5) ScrapingBee calls
    bee_results = await scrapingbee_fetch_many(
        urls=urls,
        api_key=scrapingbee_api_key,
        concurrency=concurrency,
    )

    if len(bee_results) != len(row_indices_in_df):
        raise RuntimeError(
            f"Internal mismatch: got {len(bee_results)} results "
            f"for {len(row_indices_in_df)} rows."
        )

    # 6) Parse + fill
    now_iso = datetime.now(timezone.utc).isoformat()

    for url, idx_in_df, bee in zip(urls, row_indices_in_df, bee_results):
        row = df.loc[idx_in_df]

        product_id = str(
            row.get("product_id")
            or row.get("Product ID")
            or row.get("product_code")
            or ""
        ).strip()

        description = str(
            row.get("DESCRIPTION")
            or row.get("product_name")
            or row.get("Product Name")
            or row.get("description")
            or ""
        ).strip()

        retailer_key = str(
            row.get("retailer_key")
            or row.get("Retailer Key")
            or row.get("retailer")
            or row.get("Retailer")
            or ""
        ).strip()

        try:
            parsed = hybrid_lookup_from_bee_result(
                product_id=product_id,
                description=description,
                retailer_key=retailer_key,
                original_url=url,
                bee=bee,
                debug=False,
            )
        except Exception as e:
            df.at[idx_in_df, "In Stock (Y/N)"] = ""
            df.at[idx_in_df, "Price ($USD)"] = float("nan")
            df.at[idx_in_df, "Last Scan (UTC)"] = now_iso
            df.at[idx_in_df, "HTTP Status"] = str(
                bee.get("status_code") or bee.get("status") or ""
            )
            df.at[idx_in_df, "Parse Method"] = "error"
            df.at[idx_in_df, "Response ms"] = float(bee.get("response_ms") or 0.0)
            df.at[idx_in_df, "Last Error"] = f"parse_error: {e!r}"
            df.at[idx_in_df, "URL Status"] = "error"
            df.at[idx_in_df, "Validation Issues"] = "exception_in_parser"
            continue

        in_stock = parsed.get("stock")
        price = parsed.get("price")
        parse_method = parsed.get("method") or parsed.get("parse_method") or ""
        status = parsed.get("status") or ""
        http_status = (
            parsed.get("http_status")
            or bee.get("status_code")
            or bee.get("status")
        )
        elapsed_ms = (
            parsed.get("response_ms")
            or parsed.get("elapsed_ms")
            or bee.get("response_ms")
        )
        error_msg = parsed.get("error")
        val_issues = parsed.get("validation_issues") or ""

        # Normalise in_stock to Y/N/""
        if in_stock in (True, "Y", "y", "yes"):
            stock_flag = "Y"
        elif in_stock in (False, "N", "n", "no"):
            stock_flag = "N"
        else:
            stock_flag = str(in_stock or "")

        df.at[idx_in_df, "In Stock (Y/N)"] = stock_flag
        df.at[idx_in_df, "Price ($USD)"] = float(price) if price is not None else float("nan")
        df.at[idx_in_df, "Last Scan (UTC)"] = now_iso
        df.at[idx_in_df, "HTTP Status"] = str(http_status or "")
        df.at[idx_in_df, "Parse Method"] = parse_method
        df.at[idx_in_df, "Response ms"] = float(elapsed_ms or 0.0)
        df.at[idx_in_df, "Last Error"] = str(error_msg or "")
        df.at[idx_in_df, "URL Status"] = status
        df.at[idx_in_df, "Validation Issues"] = str(val_issues or "")

    # 7) Optional upload
    if upload:
        print("[orchestrator] Uploading updated Product↔Retailer Map to sheet...")
        upload_product_map(df)
    else:
        print("[orchestrator] Upload disabled for debug run.")

    return df


# -------------------------------------------------------------------
# MODE 2: Full pipeline (XLSX workflow + email)
# -------------------------------------------------------------------


async def run_scan_from_gsheet_and_email(
    workbook_path: Path,
    secrets_path: Path,
    limit: Optional[int] = None,
    concurrency: int = 20,
    upload: bool = True,
) -> Dict[str, Any]:
    """
    1) Load secrets (ScrapingBee, OpenAI, SMTP, emails).
    2) Download Product↔Retailer Map (for log / sanity).
    3) Export full Google Sheet → XLSX.
    4) Scan Excel workbook (all tabs logic in workbook.py).
    5) Push updated Product↔Retailer Map back to your Google sheet.
    6) Email XLSX snapshot.
    """
    print("🔐 Loading secrets...")
    secrets = load_secrets(secrets_path)

    scrapingbee_api_key = secrets["SCRAPINGBEE_API_KEY"]
    smtp_server = secrets["SMTP_SERVER"]
    smtp_port = int(secrets["SMTP_PORT"])
    smtp_user = secrets["SMTP_USERNAME"]
    smtp_pass = secrets["SMTP_PASSWORD"]
    email_from = secrets["EMAIL_FROM"]
    email_to = secrets["EMAIL_TO"]

    # STEP 1: gspread download → logs
    await asyncio.to_thread(download_product_map)

    # STEP 2: full workbook export
    print("\n=== STEP 2: Download full Google Sheet → XLSX ===")
    workbook_path = Path(workbook_path)
    workbook_path.parent.mkdir(parents=True, exist_ok=True)

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
    if upload:
        sheet_id, web_link = await asyncio.to_thread(
            upload_product_map, updated_product_df
        )
    else:
        sheet_id, web_link = None, None
        print("\n[orchestrator] Upload back to Google disabled (--no-upload).")

    print("\n✅ Async pipeline complete.")
    if web_link:
        print(f"   Updated sheet link: {web_link}")

    print("Preview of updated Product↔Retailer Map (first 10 rows):")
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
        "sheet_id": sheet_id,
    }


# -------------------------------------------------------------------
# CLI plumbing
# -------------------------------------------------------------------


def _parse_row_indices(arg: Optional[str]) -> Optional[List[int]]:
    """Convert '1,5,88' → [1,5,88] or return None."""
    if not arg:
        return None
    items = [x.strip() for x in arg.split(",") if x.strip()]
    nums: List[int] = []
    for it in items:
        try:
            nums.append(int(it))
        except Exception:
            # If the user types garbage, we quietly ignore that element.
            continue
    return nums or None


def build_cli_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=(
            "Retail selector orchestrator.\n"
            "Default: full pipeline (download → scrape → upload → email).\n"
            "With --rows: debug specific Product↔Retailer Map rows only."
        )
    )

    p.add_argument(
        "--rows",
        type=str,
        default=None,
        help=(
            "Comma-separated 0-based row indices from Product↔Retailer Map to scan, "
            "e.g., '7,19,65'. Skips XLSX/email pipeline and prints a DataFrame."
        ),
    )
    p.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit to first N rows when NOT using --rows (full pipeline).",
    )
    p.add_argument(
        "--concurrency",
        type=int,
        default=5,
        help="Max concurrent ScrapingBee requests.",
    )
    p.add_argument(
        "--workbook-path",
        type=str,
        default=str(DEFAULT_WORKBOOK_PATH),
        help="Path to local XLSX file for full pipeline.",
    )
    p.add_argument(
        "--secrets-path",
        type=str,
        default=str(DEFAULT_SECRETS_PATH),
        help="Path to secrets.json",
    )
    p.add_argument(
        "--no-upload",
        action="store_true",
        help="Disable upload back to Google Sheets (both modes).",
    )
    return p


def main() -> None:
    parser = build_cli_parser()
    args = parser.parse_args()

    row_indices = _parse_row_indices(args.rows)
    workbook_path = Path(args.workbook_path)
    secrets_path = Path(args.secrets_path)

    # Load secrets once so OpenAI client is initialized, etc.
    print("🔐 Loading secrets...")
    secrets = load_secrets(secrets_path)
    scrapingbee_api_key = secrets["SCRAPINGBEE_API_KEY"]

    if row_indices is not None:
        # Debug mode: Product↔Retailer Map only
        print(f"🔍 Debug mode: scanning only rows {row_indices}")
        df = asyncio.run(
            run_hybrid_pricer_async(
                scrapingbee_api_key=scrapingbee_api_key,
                limit=None,
                upload=not args.no_upload,
                concurrency=args.concurrency,
                row_indices=row_indices,
            )
        )

        print("\n=== Debug Results ===")
        with pd.option_context("display.max_columns", None, "display.width", 220):
            print(df)
        return

    # Full XLSX pipeline
    meta = asyncio.run(
        run_scan_from_gsheet_and_email(
            workbook_path=workbook_path,
            secrets_path=secrets_path,
            limit=args.limit,
            concurrency=args.concurrency,
            upload=not args.no_upload,
        )
    )

    print("\n=== Pipeline metadata ===")
    for k, v in meta.items():
        print(f"{k}: {v}")


if __name__ == "__main__":
    main()
