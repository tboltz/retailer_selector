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
from .logger import log, set_run_mode, export_logs_as_jsonl, export_logs_as_text


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
    """

    log("Downloading Product↔Retailer Map...", context="orchestrator")
    df = download_product_map()
    df.columns = [str(c).strip() for c in df.columns]

    if df.empty:
        log("Product↔Retailer Map is empty. Nothing to scan.", context="orchestrator")
        return df

    # Mode detection was done by main(), but log here too
    log(f"Filtered to {len(df)} valid URLs", context="orchestrator")

    # Row filtering
    if row_indices is not None:
        idx_list = sorted(set(int(i) for i in row_indices))
        df = df.iloc[idx_list].copy()
        log(f"Debug mode: filtered rows {idx_list}", context="orchestrator")
    elif limit is not None:
        df = df.head(limit).copy()
        log(f"Test mode: limit={limit}", context="orchestrator")

    if df.empty:
        log("After filtering, no rows remain to scan.", context="orchestrator")
        return df

    # KPI / output columns
    output_cols = {
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
        if dtype.startswith("float"):
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "search_url" not in df.columns:
        raise KeyError("Expected column 'search_url'")

    urls = []
    row_lookup = []

    for idx, row in df.iterrows():
        url = str(row.get("search_url") or "").strip()
        if not url:
            df.at[idx, "URL Status"] = "missing_url"
            df.at[idx, "Last Error"] = "No search_url provided"
            log(f"row={idx} missing search_url", context="orchestrator")
            continue

        urls.append(url)
        row_lookup.append(idx)

    if not urls:
        log("No valid URLs to scan (all blank).", context="orchestrator")
        return df

    log(f"Starting fetch for {len(urls)} urls, concurrency={concurrency}", context="orchestrator")

    # Scrape
    bee_results = await scrapingbee_fetch_many(
        urls=urls,
        api_key=scrapingbee_api_key,
        concurrency=concurrency,
    )

    now_iso = datetime.now(timezone.utc).isoformat()

    # Parse each row
    for url, df_idx, bee in zip(urls, row_lookup, bee_results):
        row = df.loc[df_idx]
        product_id = str(row.get("product_id") or row.get("Product ID") or "").strip()
        description = str(row.get("DESCRIPTION") or row.get("product_name") or "").strip()
        retailer_key = str(row.get("retailer_key") or row.get("Retailer") or "").strip()

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
            http_status = bee.get("status_code") or bee.get("status") or ""
            df.at[df_idx, "In Stock (Y/N)"] = ""
            df.at[df_idx, "Price ($USD)"] = float("nan")
            df.at[df_idx, "Last Scan (UTC)"] = now_iso
            df.at[df_idx, "HTTP Status"] = str(http_status)
            df.at[df_idx, "Parse Method"] = "error"
            df.at[df_idx, "Response ms"] = float(bee.get("response_ms") or 0)
            df.at[df_idx, "Last Error"] = f"parse_error: {e!r}"
            df.at[df_idx, "URL Status"] = "error"
            df.at[df_idx, "Validation Issues"] = "exception_in_parser"

            log(
                f"row={df_idx} pid={product_id} retailer={retailer_key} EXCEPTION={e!r}",
                context="orchestrator"
            )
            continue

        # Normal fill
        in_stock = parsed.get("stock")
        price = parsed.get("price")
        parse_method = parsed.get("method") or ""
        status = parsed.get("status") or ""
        http_status = parsed.get("http_status") or bee.get("status_code") or bee.get("status")
        elapsed_ms = parsed.get("response_ms") or bee.get("response_ms") or 0
        error_msg = parsed.get("error") or ""
        val_issues = parsed.get("validation_issues") or ""

        # Normalize stock flag
        if in_stock in (True, "Y", "y", "yes"):
            stock_flag = "Y"
        elif in_stock in (False, "N", "n", "no"):
            stock_flag = "N"
        else:
            stock_flag = str(in_stock or "")

        df.at[df_idx, "In Stock (Y/N)"] = stock_flag
        df.at[df_idx, "Price ($USD)"] = float(price) if price is not None else float("nan")
        df.at[df_idx, "Last Scan (UTC)"] = now_iso
        df.at[df_idx, "HTTP Status"] = str(http_status)
        df.at[df_idx, "Parse Method"] = parse_method
        df.at[df_idx, "Response ms"] = float(elapsed_ms)
        df.at[df_idx, "Last Error"] = error_msg
        df.at[df_idx, "URL Status"] = status
        df.at[df_idx, "Validation Issues"] = val_issues

        log(
            f"row_result row={df_idx} pid={product_id} retailer={retailer_key} "
            f"url={url} price={price} stock={stock_flag} method={parse_method} status={status} err={error_msg}",
            context="orchestrator",
        )

    if upload:
        log("Uploading updated Product↔Retailer Map to Google Sheets...", context="orchestrator")
        upload_product_map(df)
    else:
        log("Upload disabled (debug/test mode).", context="orchestrator")

    return df


# -------------------------------------------------------------------
# FULL PIPELINE (XLSX workflow + email)
# -------------------------------------------------------------------

async def run_scan_from_gsheet_and_email(
    workbook_path: Path,
    secrets_path: Path,
    limit: Optional[int] = None,
    concurrency: int = 20,
    upload: bool = True,
) -> Dict[str, Any]:
    log("Loading secrets...", context="orchestrator")
    secrets = load_secrets(secrets_path)

    scrapingbee_api_key = secrets["SCRAPINGBEE_API_KEY"]
    smtp_server = secrets["SMTP_SERVER"]
    smtp_port = int(secrets["SMTP_PORT"])
    smtp_user = secrets["SMTP_USERNAME"]
    smtp_pass = secrets["SMTP_PASSWORD"]
    email_from = secrets["EMAIL_FROM"]
    email_to = secrets["EMAIL_TO"]

    await asyncio.to_thread(download_product_map)
    log("Downloaded Product↔Retailer Map for sanity.", context="orchestrator")

    log("Downloading Google Sheet → XLSX", context="orchestrator")
    workbook_path = Path(workbook_path)
    workbook_path.parent.mkdir(parents=True, exist_ok=True)
    await asyncio.to_thread(download_gsheet_as_xlsx, MASTER_SHEET_ID, workbook_path)

    log("Running workbook scan...", context="orchestrator")
    workbook_path, updated_product_df = await scan_workbook_async(
        workbook_path=workbook_path,
        scrapingbee_api_key=scrapingbee_api_key,
        limit=limit,
        concurrency=concurrency,
    )

    if upload:
        log("Uploading results back to Google Sheets...", context="orchestrator")
        sheet_id, web_link = await asyncio.to_thread(upload_product_map, updated_product_df)
    else:
        sheet_id, web_link = None, None
        log("Upload disabled (--no-upload)", context="orchestrator")

    # Email XLSX
    subject = "Retail Selector: Updated Retail Arbitrage Targeting List"
    body = (
        "Attached is the latest updated copy of your Retail Arbitrage Targeting List.\n\n"
        f"Generated at {datetime.now(timezone.utc).isoformat()}"
    )

    log(f"Emailing workbook to {email_to}", context="orchestrator")
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

    log("Pipeline complete.", context="orchestrator")

    return {
        "workbook_path": str(workbook_path),
        "email_to": email_to,
        "rows_scanned_limit": limit,
        "google_sheet_link": web_link,
        "sheet_id": sheet_id,
    }


# -------------------------------------------------------------------
# CLI
# -------------------------------------------------------------------

def _parse_row_indices(arg: Optional[str]) -> Optional[List[int]]:
    if not arg:
        return None
    items = [x.strip() for x in arg.split(",") if x.strip()]
    out = []
    for it in items:
        try:
            out.append(int(it))
        except:
            pass
    return out or None


def build_cli_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=(
            "Retail selector orchestrator.\n"
            "Default: prod mode. "
            "With --rows: debug mode. "
            "With --limit: test mode."
        )
    )

    p.add_argument("--rows", type=str, default=None)
    p.add_argument("--limit", type=int, default=None)
    p.add_argument("--concurrency", type=int, default=5)
    p.add_argument("--workbook-path", type=str, default=str(DEFAULT_WORKBOOK_PATH))
    p.add_argument("--secrets-path", type=str, default=str(DEFAULT_SECRETS_PATH))
    p.add_argument("--no-upload", action="store_true")
    return p


def main() -> None:
    parser = build_cli_parser()
    args = parser.parse_args()

    # -------- RUN MODE DETECTION --------
    if args.rows:
        set_run_mode("debug")
    elif args.limit is not None:
        set_run_mode("test")
    else:
        set_run_mode("prod")

    log(f"run_mode={args.rows and 'debug' or args.limit and 'test' or 'prod'}", context="orchestrator")

    row_indices = _parse_row_indices(args.rows)
    workbook_path = Path(args.workbook_path)
    secrets_path = Path(args.secrets_path)

    secrets = load_secrets(secrets_path)
    scrapingbee_api_key = secrets["SCRAPINGBEE_API_KEY"]

    # -------- DEBUG MODE (rows only) --------
    if row_indices is not None:
        log(f"Debug mode: scanning rows {row_indices}", context="orchestrator")
        df = asyncio.run(
            run_hybrid_pricer_async(
                scrapingbee_api_key=scrapingbee_api_key,
                limit=None,
                upload=not args.no_upload,
                concurrency=args.concurrency,
                row_indices=row_indices,
            )
        )
        with pd.option_context("display.max_columns", None, "display.width", 220):
            print(df)

        print("\n\n" + export_logs_as_text())
        export_logs_as_jsonl()
        return

    # -------- FULL OR TEST PIPELINE --------
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

    print("\n\n" + export_logs_as_text())
    export_logs_as_jsonl()


# -------------------------------------------------------------------

if __name__ == "__main__":
    try:
        main()
    finally:
        # ALWAYS write logs even on crash
        path = export_logs_as_jsonl()
        print(f"\n[logger] Logs written to: {path}")
