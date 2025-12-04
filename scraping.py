# retail_selector/scraping.py
from __future__ import annotations

import asyncio
<<<<<<< HEAD
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
=======
import time
from typing import List, Dict, Any, Iterable, Optional

import aiohttp

SCRAPINGBEE_ENDPOINT = "https://app.scrapingbee.com/api/v1/"
DEFAULT_TIMEOUT = 60  # seconds

# HTTP codes we consider transient and worth retrying
TRANSIENT_STATUS_CODES = {429, 500, 502, 503, 504}


async def _fetch_one_with_retries(
    session: aiohttp.ClientSession,
    api_key: str,
    url: str,
    max_retries: int = 3,
    base_backoff: float = 1.5,
    timeout: int = DEFAULT_TIMEOUT,
) -> Dict[str, Any]:
    """
    Fetch a single URL via ScrapingBee with retries on transient HTTP errors
    (429, 500, 502, 503, 504).

    Normalized return shape:
        {
          "status_code": int | None,
          "final_url": str | None,
          "page_text": str | None,
          "error": str | None,
          "response_ms": float | None,
        }

    Behavior:
      - 429 / 5xx → retried up to max_retries; on final failure: error, no HTML.
      - 401 / 402 / 403 → treated as hard ScrapingBee errors, no retry.
      - 404 / 410 / other 4xx → not retried, but HTML is returned and error is "".
      - Network / timeout exceptions → retried; final failure sets error, no HTML.
    """
    params = {
        "api_key": api_key,
        "url": url,
        "render_js": "false",
>>>>>>> 394d8b1 (Refactor scraping functions to improve error handling and retry logic; enhance concurrency management)
    }
    if use_js:
        params["render_js"] = "true"

<<<<<<< HEAD
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
=======
    last_error: Optional[str] = None
    start_time = time.perf_counter()

    for attempt in range(1, max_retries + 1):
        try:
            async with session.get(
                SCRAPINGBEE_ENDPOINT,
                params=params,
                timeout=timeout,
            ) as resp:
                status = resp.status
                try:
                    text = await resp.text()
                except Exception:
                    text = ""
                elapsed_ms = (time.perf_counter() - start_time) * 1000.0
                final_url = str(resp.url)

                # Transient errors → retry
                if status in TRANSIENT_STATUS_CODES:
                    last_error = f"ScrapingBee error: HTTP {status}"
                    if attempt == max_retries:
                        # Give up, no HTML, mark as error
                        return {
                            "status_code": status,
                            "final_url": final_url,
                            "page_text": None,
                            "error": last_error,
                            "response_ms": elapsed_ms,
                        }

                    sleep_for = base_backoff * attempt
                    print(
                        f"[scraping] {last_error} on {url} "
                        f"(attempt {attempt}/{max_retries}); retrying in {sleep_for:.1f}s"
                    )
                    await asyncio.sleep(sleep_for)
                    continue

                # Hard ScrapingBee errors we don't retry
                if status in (401, 402, 403):
                    last_error = f"ScrapingBee error: HTTP {status}"
                    return {
                        "status_code": status,
                        "final_url": final_url,
                        "page_text": None,
                        "error": last_error,
                        "response_ms": elapsed_ms,
                    }

                # Soft 4xx (404/410 etc) or success:
                # keep HTML and do NOT set 'error' so parser/AI can run.
                return {
                    "status_code": status,
                    "final_url": final_url,
                    "page_text": text,
                    "error": None,
                    "response_ms": elapsed_ms,
                }

        except asyncio.TimeoutError as exc:
            last_error = f"ScrapingBee timeout: {exc!r}"
            elapsed_ms = (time.perf_counter() - start_time) * 1000.0
            if attempt == max_retries:
                return {
                    "status_code": None,
                    "final_url": url,
                    "page_text": None,
                    "error": last_error,
                    "response_ms": elapsed_ms,
                }

            sleep_for = base_backoff * attempt
            print(
                f"[scraping] timeout on {url} "
                f"(attempt {attempt}/{max_retries}); retrying in {sleep_for:.1f}s"
            )
            await asyncio.sleep(sleep_for)

        except Exception as exc:
            # DNS/SSL/network explosions
            last_error = f"ScrapingBee exception: {type(exc).__name__}: {exc}"
            elapsed_ms = (time.perf_counter() - start_time) * 1000.0
            if attempt == max_retries:
                return {
                    "status_code": None,
                    "final_url": url,
                    "page_text": None,
                    "error": last_error,
                    "response_ms": elapsed_ms,
                }

            sleep_for = base_backoff * attempt
            print(
                f"[scraping] exception on {url} "
                f"(attempt {attempt}/{max_retries}); retrying in {sleep_for:.1f}s"
            )
            await asyncio.sleep(sleep_for)

    # Should never really get here, but just in case
    elapsed_ms = (time.perf_counter() - start_time) * 1000.0
    return {
        "status_code": None,
        "final_url": url,
        "page_text": None,
        "error": last_error or "unknown_error",
        "response_ms": elapsed_ms,
    }
>>>>>>> 394d8b1 (Refactor scraping functions to improve error handling and retry logic; enhance concurrency management)


async def scrapingbee_fetch_many(
    urls: Iterable[str],
    api_key: str,
<<<<<<< HEAD
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
=======
    concurrency: int = 10,
    max_retries: int = 3,
    timeout: int = DEFAULT_TIMEOUT,
) -> List[Dict[str, Any]]:
    """
    Fetch many URLs via ScrapingBee concurrently with a concurrency limit.

    Returns a list of result dicts in the same order as `urls`, each shaped like:
      {
        "status_code": int | None,
        "final_url": str | None,
        "page_text": str | None,
        "error": str | None,
        "response_ms": float | None,
      }
    """
    url_list = list(urls)
    results: List[Dict[str, Any]] = [None] * len(url_list)  # type: ignore
    semaphore = asyncio.Semaphore(max(1, concurrency))

    async with aiohttp.ClientSession() as session:

        async def worker(idx: int, u: str):
            async with semaphore:
                results[idx] = await _fetch_one_with_retries(
                    session=session,
                    api_key=api_key,
                    url=u,
                    max_retries=max_retries,
                    timeout=timeout,
                )

        tasks = [asyncio.create_task(worker(i, u)) for i, u in enumerate(url_list)]
        await asyncio.gather(*tasks)
>>>>>>> 394d8b1 (Refactor scraping functions to improve error handling and retry logic; enhance concurrency management)

    return results
