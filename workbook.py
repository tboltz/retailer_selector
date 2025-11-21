# retail_selector/workbook.py
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional

import numpy as np
import pandas as pd
import asyncio

from .config import PRODUCT_MAP_TAB
from .scraping import scrapingbee_fetch_many
from .parsing import hybrid_lookup_from_bee_result


def load_workbook_all_sheets(path: Path) -> Dict[str, pd.DataFrame]:
    if not path.exists():
        raise FileNotFoundError(f"Workbook not found: {path}")
    sheets = pd.read_excel(path, sheet_name=None)
    for k, df in sheets.items():
        df.columns = [str(c).strip() for c in df.columns]
        sheets[k] = df
    return sheets


async def load_workbook_all_sheets_async(path: Path) -> Dict[str, pd.DataFrame]:
    return await asyncio.to_thread(load_workbook_all_sheets, path)


def save_workbook_all_sheets(path: Path, sheets: Dict[str, pd.DataFrame]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        for name, df in sheets.items():
            df.to_excel(writer, sheet_name=name, index=False)
    print(f"💾 Workbook saved: {path}")
    return path


async def save_workbook_all_sheets_async(path: Path, sheets: Dict[str, pd.DataFrame]) -> Path:
    return await asyncio.to_thread(save_workbook_all_sheets, path, sheets)


def ensure_output_columns(df: pd.DataFrame) -> pd.DataFrame:
    output_cols = {
        "In Stock (Y/N)": "object",
        "Price ($USD)": "float64",
        "Last Scan (UTC)": "object",
        "HTTP Status": "object",
        "Parse Method": "object",
        "Response ms": "float64",
        "Last Error": "object",
        "URL Status": "object",
    }

    for col, dtype in output_cols.items():
        if col not in df.columns:
            df[col] = np.nan if dtype == "float64" else ""
        if dtype == "float64":
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


async def scan_product_map_df_async(
    df: pd.DataFrame,
    scrapingbee_api_key: str,
    limit: Optional[int] = None,
    concurrency: int = 20,
) -> pd.DataFrame:
    df = df.copy()
    df = ensure_output_columns(df)

    rows: List[Tuple[int, pd.Series]] = []
    urls: List[str] = []

    for idx, row in df.iterrows():
        url = str(row.get("search_url", "")).strip()
        not_sold = row.get("not_sold_here", 0)

        if not_sold in [1, 1.0, "1", True]:
            continue
        if url.lower().startswith("http"):
            rows.append((idx, row))
            urls.append(url)

    if limit is not None:
        rows = rows[:limit]
        urls = urls[:limit]

    print(
        f"🔎 Async hybrid pricer will process {len(rows)} rows from tab '{PRODUCT_MAP_TAB}' "
        f"with concurrency={concurrency}."
    )

    if not rows:
        print("Nothing to process in Product↔Retailer Map.")
        return df

    bee_results = await scrapingbee_fetch_many(
        urls=urls,
        api_key=scrapingbee_api_key,
        concurrency=concurrency,
    )

    now_utc = datetime.now(timezone.utc).isoformat()

    for (idx, row), bee in zip(rows, bee_results):
        res = hybrid_lookup_from_bee_result(
            product_id=str(row.get("product_id", "")),
            description=str(row.get("DESCRIPTION", "")),
            retailer_key=str(row.get("retailer_key", "")),
            original_url=str(row.get("search_url", "")),
            bee=bee,
            debug=False,
        )

        df.at[idx, "In Stock (Y/N)"]   = res.get("stock") or ""
        df.at[idx, "Price ($USD)"]     = res.get("price") if res.get("price") is not None else np.nan
        df.at[idx, "Last Scan (UTC)"]  = now_utc
        df.at[idx, "HTTP Status"]      = res.get("status")
        df.at[idx, "Parse Method"]     = res.get("method")
        df.at[idx, "Response ms"]      = res.get("response_ms")
        df.at[idx, "Last Error"]       = res.get("error") or ""
        df.at[idx, "URL Status"]       = "good" if res.get("status") == "ai_ok" else "error"

    return df


async def scan_workbook_async(
    workbook_path: Path,
    scrapingbee_api_key: str,
    limit: Optional[int] = None,
    concurrency: int = 20,
) -> tuple[Path, pd.DataFrame]:
    print(f"\n📖 Loading workbook: {workbook_path}")
    sheets = await load_workbook_all_sheets_async(workbook_path)

    product_df = sheets.get(PRODUCT_MAP_TAB)
    if product_df is None:
        raise KeyError(f"Workbook missing required tab '{PRODUCT_MAP_TAB}'")

    updated_product_df = await scan_product_map_df_async(
        df=product_df,
        scrapingbee_api_key=scrapingbee_api_key,
        limit=limit,
        concurrency=concurrency,
    )

    sheets[PRODUCT_MAP_TAB] = updated_product_df

    print("📝 Writing updated workbook (all tabs preserved)...")
    await save_workbook_all_sheets_async(workbook_path, sheets)

    return workbook_path, updated_product_df
