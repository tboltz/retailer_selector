# retail_selector/scraping.py
from __future__ import annotations

import asyncio
import time
from typing import List, Dict, Any, Iterable, Optional

import aiohttp

# Base ScrapingBee endpoint
SCRAPINGBEE_ENDPOINT = "https://app.scrapingbee.com/api/v1/"
DEFAULT_TIMEOUT = 60  # seconds

# HTTP codes we consider transient and worth retrying
TRANSIENT_STATUS_CODES = {429, 500, 502, 503, 504}


def _build_params(
    api_key: str,
    url: str,
    extra_params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Build the ScrapingBee query parameters, allowing caller-provided overrides.

    Defaults:
      - render_js = false  (faster & cheaper; caller can override per-call)
    """
    base: Dict[str, Any] = {
        "api_key": api_key,
        "url": url,
        "render_js": "false",
    }
    if extra_params:
        # Caller wins on conflicts
        base.update(extra_params)
    return base


async def _fetch_one_with_retries(
    session: aiohttp.ClientSession,
    api_key: str,
    url: str,
    max_retries: int = 3,
    base_backoff: float = 1.5,
    timeout: int = DEFAULT_TIMEOUT,
    extra_params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
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
          # extra metadata
          "request_url": str,
          "attempts": int,
          "last_exception_type": str | None,
        }

    Behavior:
      - 429 / 5xx → retried up to max_retries; on final failure: error, no HTML.
      - 401 / 402 / 403 → treated as hard ScrapingBee errors, no retry.
      - 404 / 410 / other 4xx → not retried, but HTML is returned and error is None.
      - Network / timeout exceptions → retried; final failure sets error, no HTML.
    """
    params = _build_params(api_key=api_key, url=url, extra_params=extra_params)

    last_error: Optional[str] = None
    last_exception_type: Optional[str] = None
    start_time = time.perf_counter()

    for attempt in range(1, max_retries + 1):
        try:
            async with session.get(
                SCRAPINGBEE_ENDPOINT,
                params=params,
                headers=headers,
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
                            "request_url": url,
                            "attempts": attempt,
                            "last_exception_type": last_exception_type,
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
                        "request_url": url,
                        "attempts": attempt,
                        "last_exception_type": last_exception_type,
                    }

                # Soft 4xx or success:
                # keep HTML and do NOT set 'error' so parser/AI can run.
                return {
                    "status_code": status,
                    "final_url": final_url,
                    "page_text": text,
                    "error": None,
                    "response_ms": elapsed_ms,
                    "request_url": url,
                    "attempts": attempt,
                    "last_exception_type": last_exception_type,
                }

        except asyncio.TimeoutError as exc:
            last_exception_type = type(exc).__name__
            last_error = f"ScrapingBee timeout: {exc!r}"
            elapsed_ms = (time.perf_counter() - start_time) * 1000.0

            if attempt == max_retries:
                return {
                    "status_code": None,
                    "final_url": url,
                    "page_text": None,
                    "error": last_error,
                    "response_ms": elapsed_ms,
                    "request_url": url,
                    "attempts": attempt,
                    "last_exception_type": last_exception_type,
                }

            sleep_for = base_backoff * attempt
            print(
                f"[scraping] timeout on {url} "
                f"(attempt {attempt}/{max_retries}); retrying in {sleep_for:.1f}s"
            )
            await asyncio.sleep(sleep_for)

        except Exception as exc:
            # DNS/SSL/network explosions
            last_exception_type = type(exc).__name__
            last_error = f"ScrapingBee exception: {type(exc).__name__}: {exc}"
            elapsed_ms = (time.perf_counter() - start_time) * 1000.0

            if attempt == max_retries:
                return {
                    "status_code": None,
                    "final_url": url,
                    "page_text": None,
                    "error": last_error,
                    "response_ms": elapsed_ms,
                    "request_url": url,
                    "attempts": attempt,
                    "last_exception_type": last_exception_type,
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
        "request_url": url,
        "attempts": max_retries,
        "last_exception_type": last_exception_type,
    }


# scraping.py

import asyncio
import time
from typing import Iterable, Dict, Any, List, Optional

import aiohttp

SCRAPINGBEE_ENDPOINT = "https://app.scrapingbee.com/api/v1/"
DEFAULT_TIMEOUT = 60  # seconds – keep your old value if different

TRANSIENT_STATUS = {429, 500, 502, 503, 504}


async def _fetch_one_with_retries(
    session: aiohttp.ClientSession,
    api_key: str,
    url: str,
    max_retries: int = 3,
    base_backoff: float = 1.5,
    timeout: int = DEFAULT_TIMEOUT,
    extra_params: Optional[Dict[str, str]] = None,
    headers: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """
    Single-URL ScrapingBee fetch with retry & backoff.
    Returns a normalized result dict that the rest of your pipeline expects.
    """
    params_base: Dict[str, str] = {
        "api_key": api_key,
        "url": url,
        # ScrapingBee timeout is in *seconds* or *ms* depending on plan.
        # Here we just forward your global timeout value.
        "timeout": str(timeout),
    }
    if extra_params:
        params_base.update(extra_params)

    last_exception: Optional[BaseException] = None
    last_exception_type: Optional[str] = None

    for attempt in range(1, max_retries + 1):
        start = time.perf_counter()
        try:
            async with session.get(
                SCRAPINGBEE_ENDPOINT,
                params=params_base,
                headers=headers,
                timeout=timeout,
            ) as resp:
                elapsed_ms = (time.perf_counter() - start) * 1000.0
                text = await resp.text()

                status = resp.status
                error_msg: Optional[str] = None

                if status in TRANSIENT_STATUS and attempt < max_retries:
                    # transient HTTP error – retry
                    error_msg = f"ScrapingBee error: HTTP {status}"
                    await asyncio.sleep(base_backoff * attempt)
                else:
                    # success or non-retryable error (404, 401, etc.)
                    if status >= 400 and status not in TRANSIENT_STATUS:
                        error_msg = f"ScrapingBee error: HTTP {status}"
                    # Normal exit: return result for this attempt
                    return {
                        "status_code": status,
                        "final_url": str(resp.url),
                        "page_text": text if status == 200 else None,
                        "error": error_msg,
                        "response_ms": elapsed_ms,
                        "request_url": url,
                        "attempts": attempt,
                        "last_exception_type": last_exception_type,
                    }

        except asyncio.TimeoutError as exc:
            elapsed_ms = (time.perf_counter() - start) * 1000.0
            last_exception = exc
            last_exception_type = type(exc).__name__
            # ScrapingBee timed out on us
            error_msg = "ScrapingBee timeout: TimeoutError()"

            if attempt < max_retries:
                await asyncio.sleep(base_backoff * attempt)
            else:
                return {
                    "status_code": None,
                    "final_url": url,
                    "page_text": None,
                    "error": error_msg,
                    "response_ms": elapsed_ms,
                    "request_url": url,
                    "attempts": attempt,
                    "last_exception_type": last_exception_type,
                }

        except Exception as exc:
            # Network / DNS / SSL and other disasters
            elapsed_ms = (time.perf_counter() - start) * 1000.0
            last_exception = exc
            last_exception_type = type(exc).__name__
            error_msg = f"ScrapingBee error: {type(exc).__name__}: {exc}"

            if attempt < max_retries:
                await asyncio.sleep(base_backoff * attempt)
            else:
                return {
                    "status_code": None,
                    "final_url": url,
                    "page_text": None,
                    "error": error_msg,
                    "response_ms": elapsed_ms,
                    "request_url": url,
                    "attempts": attempt,
                    "last_exception_type": last_exception_type,
                }

    # Should never hit here, but in case the loop logic changes later:
    return {
        "status_code": None,
        "final_url": url,
        "page_text": None,
        "error": "ScrapingBee error: unknown_failure",
        "response_ms": None,
        "request_url": url,
        "attempts": max_retries,
        "last_exception_type": last_exception_type,
    }


async def scrapingbee_fetch_many(
    urls: Iterable[str],
    api_key: str,
    concurrency: int = 10,
    max_retries: int = 3,
    timeout: int = DEFAULT_TIMEOUT,
    base_backoff: float = 1.5,
    extra_params: Optional[Dict[str, str]] = None,
    headers: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch many URLs via ScrapingBee concurrently with a concurrency limit.

    Result list is in the same order as `urls`, each entry shaped like:

        {
          "status_code": int | None,
          "final_url": str | None,
          "page_text": str | None,
          "error": str | None,
          "response_ms": float | None,
          "request_url": str,
          "attempts": int,
          "last_exception_type": str | None,
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
                    base_backoff=base_backoff,
                    timeout=timeout,
                    extra_params=extra_params,
                    headers=headers,
                )

        tasks = [asyncio.create_task(worker(i, u)) for i, u in enumerate(url_list)]
        await asyncio.gather(*tasks)

    return results
