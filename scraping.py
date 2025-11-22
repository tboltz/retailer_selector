# retail_selector/scraping.py
from __future__ import annotations

import asyncio
from typing import List, Dict, Any, Optional

import aiohttp


SCRAPINGBEE_BASE = "https://app.scrapingbee.com/api/v1"


async def _scrapingbee_fetch_async(
    session: aiohttp.ClientSession,
    api_key: str,
    url: str,
    use_js: bool = False,
) -> Dict[str, Any]:
    params = {
        "api_key": api_key,
        "url": url,
        "return_page_text": "true",
    }
    if use_js:
        params["render_js"] = "true"

    try:
        async with session.get(SCRAPINGBEE_BASE, params=params, timeout=30) as resp:
            status = resp.status
            text = await resp.text()

            # Sometimes Bee wraps in JSON
            import json as _json
            try:
                data = _json.loads(text)
                if isinstance(data, dict) and "page_text" in data:
                    text = data.get("page_text") or ""
            except Exception:
                pass

            if len(text) > 30000:
                text = text[:30000]

            return {
                "status_code": status,
                "final_url": str(resp.url),
                "page_text": text,
                "error": None if status == 200 else f"HTTP {status}",
            }
    except Exception as e:
        return {
            "status_code": -1,
            "final_url": url,
            "page_text": "",
            "error": str(e),
        }


async def scrapingbee_fetch_many(
    urls: List[str],
    api_key: str,
    use_js_flags: Optional[List[bool]] = None,
    concurrency: int = 20,
) -> List[Dict[str, Any]]:
    """
    Fetch many URLs in parallel using ScrapingBee.
    """
    if use_js_flags is None:
        use_js_flags = [False] * len(urls)

    sem = asyncio.Semaphore(concurrency)

    async with aiohttp.ClientSession() as session:
        async def bound_fetch(i: int, url: str, use_js: bool):
            async with sem:
                res = await _scrapingbee_fetch_async(session, api_key, url, use_js)
                return i, res

        tasks = [
            bound_fetch(i, url, use_js_flags[i])
            for i, url in enumerate(urls)
        ]
        raw_results = await asyncio.gather(*tasks, return_exceptions=True)

    results: List[Optional[Dict[str, Any]]] = [None] * len(urls)
    for item in raw_results:
        if isinstance(item, Exception):
            continue
        idx, res = item
        results[idx] = res

    for i, r in enumerate(results):
        if r is None:
            results[i] = {
                "status_code": -1,
                "final_url": urls[i],
                "page_text": "",
                "error": "unknown_async_error",
            }

    return results
