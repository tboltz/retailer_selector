# retail_selector/config.py
from __future__ import annotations

import os
import json
from pathlib import Path
from typing import Dict, Any, Optional
import time
from openai import OpenAI

# -------------------------
# Base project root
# -------------------------

# Compute root dynamically so you can move the folder without re-breaking stuff
# This file is: C:\Users\suzan\Projects\card\retailer_selector\config.py
PROJECT_ROOT = Path(__file__).resolve().parents[1]
current_time_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
# -------------------------
# Default local file paths
# -------------------------

# Local Excel workbook (if/when you revive XLSX mode)
DEFAULT_WORKBOOK_PATH = PROJECT_ROOT / f"SANDBOX Retail Arbitrage Targeting List {current_time_str}.xlsx"

# Default path for secrets.json
DEFAULT_SECRETS_PATH = PROJECT_ROOT / "retailer_selector" / "secrets.json"

# -------------------------
# Google Sheets configuration
# -------------------------

# Master sheet (source of truth)
MASTER_SHEET_ID = "1rSaOaq52CkWeB_1U62zhY0Xd7PQMLvaHbYGOnjAa6QA"

# Output sheet (where updated Product↔Retailer Map is written)
OUTPUT_SHEET_ID = "1PNLdCOzEL43KxvulsGk_L0aGNk5I0HIshsMkkQzwrGg"

PRODUCT_MAP_TAB = "Product↔Retailer Map"
ACTIVE_WATCH_TAB = "Active Watch List"
RETAILERS_TAB    = "Retailers"

# Service account JSON for Google APIs
SERVICE_ACCOUNT_FILE = PROJECT_ROOT / "retailer_selector" / "retail-selector-bot-294ddd38cfa6.json"

# Scraping concurrency
MAX_CONCURRENCY = 20

# -------------------------
# OpenAI global client/model
# -------------------------

client: Optional[OpenAI] = None
OPENAI_MODEL: str = "gpt-4o-mini"  # default; can be overridden via secrets.json

# Toggle for HTML → AI parsing
USE_AI_HTML: bool = True


def load_secrets(secrets_path: Path | str | None = None) -> Dict[str, Any]:
    """
    Load secrets from JSON and initialize the OpenAI client + model name.

    Expected keys in secrets.json:
      - SCRAPINGBEE_API_KEY
      - OPENAI_API_KEY
      - (optional) OPENAI_MODEL
      - SMTP_SERVER
      - SMTP_PORT
      - SMTP_USERNAME
      - SMTP_PASSWORD
      - EMAIL_FROM
      - EMAIL_TO
    """
    global client, OPENAI_MODEL, USE_AI_HTML

    if secrets_path is None:
        secrets_path = DEFAULT_SECRETS_PATH

    secrets_path = Path(secrets_path)
    if not secrets_path.exists():
        raise FileNotFoundError(f"secrets.json not found at: {secrets_path}")

    with open(secrets_path, "r", encoding="utf-8") as f:
        secrets: Dict[str, Any] = json.load(f)

    required = [
        "SCRAPINGBEE_API_KEY",
        "OPENAI_API_KEY",
        "SMTP_SERVER",
        "SMTP_PORT",
        "SMTP_USERNAME",
        "SMTP_PASSWORD",
        "EMAIL_FROM",
        "EMAIL_TO",
    ]
    missing = [k for k in required if k not in secrets]
    if missing:
        raise KeyError(f"secrets.json is missing keys: {missing}")

    # Allow model override from secrets.json (e.g. 'gpt-4.1', 'gpt-4o-mini')
    OPENAI_MODEL = secrets.get("OPENAI_MODEL") or "gpt-4o-mini"

    # Optional toggle for AI html parsing (default True)
    if "USE_AI_HTML" in secrets:
        USE_AI_HTML = bool(secrets["USE_AI_HTML"])

    # Surface keys via environment for any legacy code
    os.environ["SCRAPINGBEE_API_KEY"] = secrets["SCRAPINGBEE_API_KEY"]
    os.environ["OPENAI_API_KEY"] = secrets["OPENAI_API_KEY"]

    # Initialize the shared OpenAI client
    client = OpenAI(api_key=secrets["OPENAI_API_KEY"])

    return secrets
