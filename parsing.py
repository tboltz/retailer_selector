"""
retail_selector/parsing.py

Hybrid HTML price & stock extractor.

Core entry point:
    hybrid_lookup_from_bee_result(product_id, description, retailer_key,
                                  original_url, bee, debug=False) -> Dict[str, Any]

This function consumes a single ScrapingBee result dict (as produced by
scraping.scrapingbee_fetch_many) and returns a normalized dict used by
workbook.scan_product_map_df_async.

Design goals
------------
* Pattern-based parsing first (fast, no API cost).
* Optional LLM ("AI HTML") refinement when enabled in config and when
  the pattern layer is uncertain or clearly failed.
* Always return a well-formed result dict so the caller never explodes
  on missing keys.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

from bs4 import BeautifulSoup  # type: ignore

from . import config


PRICE_RE = re.compile(
    r"""(?<!\d)(\d{1,4}(?:[.,]\d{3})*(?:[.,]\d{2})|\d{1,4}(?:[.,]\d{2}))""",
    re.VERBOSE,
)

IN_STOCK_POSITIVE = [
    "in stock",
    "available now",
    "ready to ship",
    "ships today",
    "add to cart",
    "add to basket",
    "add to bag",
]

IN_STOCK_NEGATIVE = [
    "out of stock",
    "sold out",
    "unavailable",
    "backorder",
    "preorder",
    "pre-order",
    "temporarily unavailable",
]


@dataclass
class ParsedResult:
    price: Optional[float]
    stock: str  # "Y", "N", or ""
    method: str
    notes: str = ""


def _safe_float(text: str) -> Optional[float]:
    try:
        # normalize commas vs dots
        cleaned = text.replace(",", "")
        return float(cleaned)
    except Exception:
        return None


def _extract_price_from_jsonld(soup: BeautifulSoup) -> Optional[float]:
    for tag in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(tag.string or "")
        except Exception:
            continue

        # Flatten to a list of candidate nodes
        nodes = []
        if isinstance(data, list):
            nodes = data
        else:
            nodes = [data]

        for node in nodes:
            if not isinstance(node, dict):
                continue

            # Product / Offer hierarchy
            if node.get("@type") in ("Product", "ProductModel"):
                offers = node.get("offers")
                if isinstance(offers, dict):
                    price = offers.get("price") or offers.get("lowPrice")
                    if isinstance(price, str):
                        p = _safe_float(price)
                        if p is not None:
                            return p
                elif isinstance(offers, list):
                    for offer in offers:
                        if not isinstance(offer, dict):
                            continue
                        price = offer.get("price") or offer.get("lowPrice")
                        if isinstance(price, str):
                            p = _safe_float(price)
                            if p is not None:
                                return p

            # Direct Offer node
            if node.get("@type") in ("Offer", "AggregateOffer"):
                price = node.get("price") or node.get("lowPrice")
                if isinstance(price, str):
                    p = _safe_float(price)
                    if p is not None:
                        return p

    return None


def _extract_price_from_meta(soup: BeautifulSoup) -> Optional[float]:
    # Common HTML microdata / meta patterns
    candidates = []

    for tag in soup.find_all(attrs={"itemprop": "price"}):
        if tag.has_attr("content"):
            candidates.append(tag["content"])
        if tag.string:
            candidates.append(tag.string)

    for tag in soup.find_all("meta", itemprop="price"):
        if tag.has_attr("content"):
            candidates.append(tag["content"])

    for tag in soup.find_all("span", class_=re.compile("price", re.I)):
        if tag.string:
            candidates.append(tag.string)

    for raw in candidates:
        if not raw:
            continue
        m = PRICE_RE.search(raw)
        if not m:
            continue
        p = _safe_float(m.group(1).replace("$", "").strip())
        if p is not None:
            return p

    return None


def _extract_price_from_text(soup: BeautifulSoup) -> Optional[float]:
    # Look for something that looks like a main product price, biased toward "$"
    text = soup.get_text(" ", strip=True)
    # Restrict to first ~2000 characters to avoid footer spam
    text = text[:2000]

    # Prefer "$"-prefixed prices
    dollar_prices = re.findall(r"\$\s*([0-9][0-9.,]*)", text)
    for raw in dollar_prices:
        p = _safe_float(raw)
        if p is not None:
            return p

    # Fallback: any price-shaped number
    for m in PRICE_RE.finditer(text):
        p = _safe_float(m.group(1))
        if p is not None:
            return p

    return None


def _extract_stock_from_text(soup: BeautifulSoup) -> str:
    text = soup.get_text(" ", strip=True).lower()

    neg_hits = [kw for kw in IN_STOCK_NEGATIVE if kw in text]
    pos_hits = [kw for kw in IN_STOCK_POSITIVE if kw in text]

    if neg_hits and not pos_hits:
        return "N"
    if pos_hits and not neg_hits:
        return "Y"
    # Ambiguous or no signal
    return ""


def _pattern_parse(html: str) -> ParsedResult:
    soup = BeautifulSoup(html, "html.parser")

    # 1) JSON-LD
    price = _extract_price_from_jsonld(soup)
    if price is not None:
        stock = _extract_stock_from_text(soup)
        return ParsedResult(price=price, stock=stock, method="pattern_jsonld", notes="jsonld")

    # 2) Meta / microdata
    price = _extract_price_from_meta(soup)
    if price is not None:
        stock = _extract_stock_from_text(soup)
        return ParsedResult(price=price, stock=stock, method="pattern_meta", notes="meta")

    # 3) Text heuristics
    price = _extract_price_from_text(soup)
    stock = _extract_stock_from_text(soup)
    if price is not None or stock:
        return ParsedResult(price=price, stock=stock, method="pattern_text", notes="text")

    return ParsedResult(price=None, stock="", method="pattern_fail", notes="no_match")


def _ai_parse(html: str, description: str, retailer_key: str) -> ParsedResult:
    """
    Ask the LLM to extract price & stock from a truncated HTML snippet.

    Returns ParsedResult; any exception becomes ParsedResult with price=None
    and method='ai_error'.
    """
    if config.client is None:
        return ParsedResult(
            price=None,
            stock="",
            method="ai_error",
            notes="openai_client_not_configured",
        )

    # Trim HTML to something sane
    snippet = html
    if len(snippet) > 16000:
        snippet = snippet[:16000]

    system_prompt = (
        "You read messy HTML product pages and extract a single product price and stock status. "
        "Focus only on the main product shown, not related items or ads."
    )

    user_prompt = f"""
Product description (from spreadsheet): {description!r}
Retailer key: {retailer_key}

HTML snippet (truncated):
<BEGIN_HTML>
{snippet}
<END_HTML>

Return a JSON object with:
  - price: number or null (in USD)
  - in_stock: "Y", "N", or "" if uncertain
"""

    try:
        resp = config.client.responses.create(
            model=config.OPENAI_MODEL,
            input=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            response_format={"type": "json_object"},
            max_output_tokens=256,
            temperature=0,
        )

        content = resp.output[0].content[0].text  # type: ignore[attr-defined]
        data = json.loads(content)

        raw_price = data.get("price")
        in_stock = data.get("in_stock") or ""

        price: Optional[float]
        if raw_price is None:
            price = None
        else:
            try:
                price = float(raw_price)
            except Exception:
                price = None

        if in_stock not in ("Y", "N", ""):
            in_stock = ""

        return ParsedResult(
            price=price,
            stock=in_stock,
            method="ai_html",
            notes="ai_ok",
        )

    except Exception as exc:
        return ParsedResult(
            price=None,
            stock="",
            method="ai_error",
            notes=f"ai_exception:{type(exc).__name__}",
        )


def hybrid_lookup_from_bee_result(
    product_id: str,
    description: str,
    retailer_key: str,
    original_url: str,
    bee: Dict[str, Any],
    debug: bool = False,
) -> Dict[str, Any]:
    """
    Given a single ScrapingBee result dict, perform a hybrid (pattern + AI)
    extraction of price & stock.

    Parameters
    ----------
    product_id : str
        ID from the Product↔Retailer Map sheet.
    description : str
        Human-readable product description.
    retailer_key : str
        Retailer identifier (matches the registry key).
    original_url : str
        URL as stored in the sheet.
    bee : dict
        A single item from scraping.scrapingbee_fetch_many:
            {
              "status_code": int,
              "final_url": str,
              "page_text": str,
              "error": str | None,
              "response_ms": float | None,
            }
    debug : bool
        If True, include extra notes in the result.
    """
    status_code = bee.get("status_code")
    final_url = bee.get("final_url") or original_url
    html = bee.get("page_text") or ""
    bee_error = bee.get("error") or ""
    response_ms = bee.get("response_ms")

    # Base envelope returned to caller
    result: Dict[str, Any] = {
        "product_id": product_id,
        "description": description,
        "retailer_key": retailer_key,
        "original_url": original_url,
        "final_url": final_url,
        "price": None,
        "stock": "",
        "status": "ai_error",      # historical naming; 'ai_ok' on success
        "method": "http_error",
        "response_ms": response_ms,
        "error": "",
        "http_status": status_code,
        "debug_notes": "",
    }

    # 1) HTTP-level guardrails
    if bee_error:
        result["error"] = bee_error
        result["method"] = "http_error"
        result["debug_notes"] = f"bee_error:{bee_error}"
        return result

    if status_code != 200:
        result["error"] = f"http_{status_code}"
        result["method"] = "http_error"
        result["debug_notes"] = f"http_status:{status_code}"
        return result

    if not html.strip():
        result["error"] = "empty_html"
        result["method"] = "empty_html"
        result["debug_notes"] = "no_html"
        return result

    # 2) Pattern-based parse
    pattern_res = _pattern_parse(html)

    best = pattern_res
    used_ai = False

    # Decide whether to invoke AI:
    # * if pattern completely failed (no price, no stock)
    # * or if config.USE_AI_HTML is True and we want refinement
    use_ai = getattr(config, "USE_AI_HTML", False)

    if use_ai and (pattern_res.price is None and not pattern_res.stock):
        ai_res = _ai_parse(html, description, retailer_key)
        used_ai = True

        # Prefer AI result if it gives us anything
        if ai_res.price is not None or ai_res.stock:
            best = ai_res
        else:
            # keep pattern_res (which is basically "fail") but carry AI error notes
            best = ParsedResult(
                price=None,
                stock="",
                method="ai_error",
                notes=ai_res.notes or "ai_no_signal",
            )

    # 3) Populate final result
    result["price"] = best.price
    result["stock"] = best.stock
    result["method"] = best.method

    if best.price is not None or best.stock:
        result["status"] = "ai_ok"  # keep existing workbook logic happy
    else:
        result["status"] = "ai_error"
        if not result["error"]:
            result["error"] = "no_price_or_stock"

    if used_ai and "ai_exception" in best.notes:
        # surface AI exception in error field
        result["error"] = best.notes

    if debug:
        result["debug_notes"] = f"pattern={pattern_res.method}; ai_used={used_ai}; notes={best.notes}"

    return result
