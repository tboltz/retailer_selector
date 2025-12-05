# retail_selector/logger.py

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional


# ================================================================
# BASE DIRECTORY (WINDOWS-SAFE)
# ================================================================

# Always use forward slashes — avoids \U unicode escape errors.
PROJECT_ROOT = Path("C:/Users/suzan/Projects/card/retailer_selector")

# Root folder for all logs
LOG_ROOT = PROJECT_ROOT / "logs"
LOG_ROOT.mkdir(parents=True, exist_ok=True)


# ================================================================
# RUN MODE (debug, test, prod)
# ================================================================

# Default until orchestrator sets it
CURRENT_RUN_MODE = "prod"   # fallback

def set_run_mode(mode: str) -> None:
    """
    Set global logging mode. Options:
        debug  → when --rows is used
        test   → when --limit is used
        prod   → default full pipeline
    """
    global CURRENT_RUN_MODE
    if mode not in ("debug", "test", "prod"):
        mode = "prod"
    CURRENT_RUN_MODE = mode


# ================================================================
# IN-MEMORY LOG BUFFER
# ================================================================

_LOG_BUFFER: List[Dict[str, Any]] = []


# ================================================================
# LOGGING FUNCTIONS
# ================================================================

def log(message: str, context: str = "general", extra: Optional[Dict[str, Any]] = None) -> None:
    """
    Append a structured log entry into the global buffer.
    """
    event = {
        "timestamp": datetime.utcnow().isoformat(),
        "mode": CURRENT_RUN_MODE,
        "context": context,
        "message": message,
        "extra": extra or {},
    }
    _LOG_BUFFER.append(event)


# ================================================================
# JSONL EXPORT
# ================================================================

def export_logs_as_jsonl() -> str:
    """
    Write buffered logs into Athena-style partition paths:

        logs/<mode>/date=YYYY-MM-DD/hour=HH/retail_selector.jsonl

    Returns the full file path as a string.
    """
    now = datetime.utcnow()

    date = f"{now.year:04d}-{now.month:02d}-{now.day:02d}"
    hour = f"{now.hour:02d}"

    partition = (
        LOG_ROOT
        / CURRENT_RUN_MODE
        / f"date={date}"
        / f"hour={hour}"
    )
    partition.mkdir(parents=True, exist_ok=True)

    out_file = partition / "retail_selector.jsonl"

    with out_file.open("w", encoding="utf-8") as f:
        for entry in _LOG_BUFFER:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")

    return str(out_file)


# ================================================================
# HUMAN-READABLE TEXT EXPORT
# ================================================================

def export_logs_as_text() -> str:
    """
    Convert buffered events into a plain-text block.
    """
    lines = []
    for ev in _LOG_BUFFER:
        ts = ev["timestamp"]
        ctx = ev["context"]
        msg = ev["message"]
        extra = ev.get("extra") or {}
        lines.append(f"[{ts}] [{ev['mode']}] [{ctx}] {msg} extra={extra}")
    return "\n".join(lines)


# ================================================================
# OPTIONAL QUERY FUNCTIONS (future debugging agents)
# ================================================================

def get_logs(context: Optional[str] = None, text: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Search the in-memory buffer.
    """
    out = []
    for ev in _LOG_BUFFER:
        if context and ev["context"] != context:
            continue
        if text and text.lower() not in ev["message"].lower():
            continue
        out.append(ev)
    return out
