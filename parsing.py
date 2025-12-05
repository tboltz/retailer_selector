# retail_selector/parsing.py
from __future__ import annotations

import json
import re
import time
from typing import Dict, Any, Optional

from bs4 import BeautifulSoup
from urllib.parse import urlparse

from . import config
from .logger import log


def parse_shopify_variant_json(page_text: str) -> Optional[Dict[str, Any]]:
    if "inventory_quantity" not in page_text or '"price"' not in page_text:
        return None
    m = re.search(r'(\[\s*\{.*?"inventory_quantity".*?\}\s*\])', page_text, re.DOTALL)
    if not m:
        return None
    try:
        arr = json.loads(m.group(1))
        if not isinstance(arr, list) or not arr:
            return None
        v = arr[0]
    except Exception:
        return None

    price = None
    cents = v.get("price")
    if isinstance(cents, (int, float)):
        price = round(float(cents) / 100.0, 2)

    qty = v.get("inventory_quantity")
    available = v.get("available")

    stock = None
    if isinstance(available, bool):
        stock = "Y" if available else "N"
    elif isinstance(qty, (int, float)):
        stock = "Y" if qty > 0 else "N"

    log(
        f"shopify_variant price={price} stock={stock}",
        context="parsing",
    )
    return {"price": price, "stock": stock, "raw": v}


def detect_retailer_family(url: str, html: str) -> str:
    host = urlparse(url).netloc.lower()
    if "amazon." in host:
        family = "amazon"
    elif "cdn.shopify.com" in html or "Shopify.theme" in html or "window.Shopify" in html:
        family = "shopify"
    else:
        family = "generic"

    log(f"retailer_family={family} host={host}", context="parsing")
    return family


def parse_jsonld_price_stock(html: str) -> Optional[Dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")

    def _extract_prices_from_offers(offers):
        prices = []

        def _extract(obj):
            if not isinstance(obj, dict):
                return
            p = obj.get("price")
            if isinstance(p, (int, float)):
                prices.append(float(p))
            elif isinstance(p, str):
                m = re.search(r"(\d+(?:\.\d{1,2})?)", p)
                if m:
                    try:
                        prices.append(float(m.group(1)))
                    except Exception:
                        pass

            pspec = obj.get("priceSpecification")
            if isinstance(pspec, dict):
                _extract(pspec)
            elif isinstance(pspec, list):
                for sub in pspec:
                    _extract(sub)

        if isinstance(offers, list):
            for off in offers:
                _extract(off)
        else:
            _extract(offers)
        return prices

    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
        except Exception:
            continue

        nodes = [data] if isinstance(data, dict) else (data if isinstance(data, list) else [])
        for node in nodes:
            if not isinstance(node, dict):
                continue
            kind = str(node.get("@type", "")).lower()
            if kind not in ["product", "productgroup"]:
                continue

            offers = node.get("offers")
            if not offers:
                continue

            prices = _extract_prices_from_offers(offers)
            if not prices:
                continue

            price = min(prices)

            avail = ""
            if isinstance(offers, dict):
                avail = str(offers.get("availability", "") or "")
            elif isinstance(offers, list) and offers:
                first = offers[0]
                if isinstance(first, dict):
                    avail = str(first.get("availability", "") or "")

            avail_lower = avail.lower()
            stock = None
            if "instock" in avail_lower:
                stock = "Y"
            elif any(k in avail_lower for k in ["outofstock", "soldout", "oos", "preorder", "backorder"]):
                stock = "N"

            log(
                f"jsonld price={price} stock={stock} availability={avail}",
                context="parsing",
            )
            return {"price": price, "stock": stock, "raw": node}
    return None


def parse_generic_price_stock(html: str) -> Optional[Dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    full_text = soup.get_text(" ", strip=True)
    lower = full_text.lower()

    stock = None
    if any(s in lower for s in ["out of stock", "sold out", "unavailable", "backorder", "preorder", "coming soon"]):
        stock = "N"
    elif any(s in lower for s in ["in stock", "available now", "ready to ship", "add to cart", "add to basket"]):
        stock = "Y"

    discount_kw = ["you save", "save ", "saving", "% off"]
    original_kw = ["rrp", "r.r.p", "was ", "compare at", "compare-at", "list price", "retail price", "original price"]
    sale_kw     = ["now", "now only", "our price", "sale", "special", "deal", "today", "promo", "offer"]

    original_candidates, sale_candidates, generic_candidates = [], [], []

    for m in re.finditer(r"([£$€]\s*(\d{1,5}(?:\.\d{1,2})?))", full_text):
        num_str = m.group(2)
        try:
            value = float(num_str)
        except Exception:
            continue

        start = max(0, m.start() - 60)
        end   = min(len(lower), m.end() + 60)
        ctx   = lower[start:end]

        if any(kw in ctx for kw in discount_kw):
            continue

        if any(kw in ctx for kw in original_kw):
            original_candidates.append(value)
        elif any(kw in ctx for kw in sale_kw):
            sale_candidates.append(value)
        else:
            generic_candidates.append(value)

    price = None
    if sale_candidates:
        price = min(sale_candidates)
    elif generic_candidates:
        price = min(generic_candidates)
    else:
        if original_candidates:
            price = min(original_candidates)

    if price is None:
        m2 = re.search(r"(\d{1,5}\.\d{2})", full_text)
        if m2:
            try:
                price = float(m2.group(1))
            except Exception:
                price = None

    # default logic: price → Y if no explicit stock
    if price is not None and stock is None:
        stock = "Y"

    if price is None and stock is None:
        log("generic parser found no price and no stock", context="parsing")
        return None

    log(
        f"generic parser price={price} stock={stock} "
        f"candidates_sale={sale_candidates} candidates_generic={generic_candidates}",
        context="parsing",
    )

    return {
        "price": price,
        "stock": stock,
        "source": "generic_sale_price",
        "raw": {
            "original_candidates": original_candidates,
            "sale_candidates": sale_candidates,
            "generic_candidates": generic_candidates,
        },
    }


def parse_html_price_stock(url: str, html: str) -> Optional[Dict[str, Any]]:
    family = detect_retailer_family(url, html)

    if family == "shopify":
        shopify_res = parse_shopify_variant_json(html)
        if shopify_res and (shopify_res["price"] is not None or shopify_res["stock"] is not None):
            log(
                f"parse_html using shopify_variants price={shopify_res['price']} "
                f"stock={shopify_res['stock']}",
                context="parsing",
            )
            return {
                "price": shopify_res["price"],
                "stock": shopify_res["stock"],
                "source": "shopify_variants",
            }

    jsonld_res = parse_jsonld_price_stock(html)
    if jsonld_res:
        log(
            f"parse_html using jsonld_product price={jsonld_res['price']} "
            f"stock={jsonld_res['stock']}",
            context="parsing",
        )
        return {
            "price": jsonld_res["price"],
            "stock": jsonld_res["stock"],
            "source": "jsonld_product",
        }

    generic_res = parse_generic_price_stock(html)
    if generic_res:
        log(
            f"parse_html using generic_text price={generic_res['price']} "
            f"stock={generic_res['stock']}",
            context="parsing",
        )
        return {
            "price": generic_res["price"],
            "stock": generic_res["stock"],
            "source": "generic_text",
        }

    log("parse_html could not extract price/stock; falling back to AI", context="parsing")
    return None


def _clean_json_text(raw_text: str) -> str:
    clean = raw_text.strip()
    if clean.startswith("```"):
        parts = clean.split("```")
        if len(parts) >= 2:
            clean = parts[1].strip()
            if clean.lower().startswith("json"):
                clean = clean[4:].strip()
        if "```" in clean:
            clean = clean.split("```")[0].strip()
    return clean


def hybrid_lookup_from_bee_result(
    product_id: str,
    description: str,
    retailer_key: str,
    original_url: str,
    bee: Dict[str, Any],
    debug: bool = False,
) -> Dict[str, Any]:

    if config.client is None:
        raise RuntimeError("OpenAI client not initialized. Call load_secrets() first.")

    start = time.time()

    html = bee.get("page_text", "") or ""
    final_url = bee.get("final_url", original_url)
    bee_error = bee.get("error")
    http_status = bee.get("status_code") or bee.get("status")

    if bee_error:
        elapsed_ms = int((time.time() - start) * 1000)
        log(
            f"bee_error url={final_url} status={http_status} error={bee_error}",
            context="parsing",
        )
        return {
            "price": None,
            "stock": None,
            "url_used": final_url,
            "notes": "",
            "error": f"ScrapingBee error: {bee_error}",
            "response_ms": elapsed_ms,
            "status": "ai_error",
            "method": "ai_html",
        }

    # pattern / HTML heuristic path
    parsed = parse_html_price_stock(final_url, html)
    if parsed and (parsed["price"] is not None or parsed["stock"] is not None):
        elapsed_ms = int((time.time() - start) * 1000)
        log(
            f"pattern_parse success url={final_url} price={parsed['price']} "
            f"stock={parsed['stock']} source={parsed['source']}",
            context="parsing",
        )
        return {
            "price": parsed["price"],
            "stock": parsed["stock"],
            "url_used": final_url,
            "notes": f"Parsed via {parsed['source']}.",
            "error": None,
            "response_ms": elapsed_ms,
            "status": "ai_ok",
            "method": "ai_html",
        }

    snippet = html[:15000]
    log(
        f"invoking AI fallback url={final_url} len_snippet={len(snippet)}",
        context="parsing",
    )

    system_prompt = (
        "You are a precise retail price and stock extractor. "
        "Given raw HTML/text of a single product page, identify:\n"
        "- The product's main CURRENT SELLING PRICE as shown on the page.\n"
        "- Whether the product is in stock.\n\n"
        "IMPORTANT RULES:\n"
        "- Do NOT convert currencies. Return the numeric price exactly as it appears.\n"
        "- Ignore discount amounts like 'Save £4.74', 'You save £X', or '% off'.\n"
        "- If you cannot find a reliable price, set price=null.\n\n"
        "Return ONLY a JSON object."
    )

    user_prompt = f"""
Product:
- Product ID: {product_id}
- Description: {description}
- Retailer: {retailer_key}
- URL: {final_url}

<page>
{snippet}
</page>

Return JSON:
{{
  "price": <number or null>,
  "in_stock": "Y" or "N" or "unknown",
  "url_used": "{final_url}",
  "notes": "<short explanation>"
}}
"""

    try:
        resp = config.client.responses.create(
            model=config.OPENAI_MODEL,
            input=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        )

        elapsed_ms = int((time.time() - start) * 1000)
        raw_text = (resp.output_text or "").strip()
        clean = _clean_json_text(raw_text)
        data = json.loads(clean)

        price = data.get("price")
        in_stock = data.get("in_stock", "unknown")
        notes = data.get("notes") or ""

        if isinstance(price, str):
            m = re.search(r"(\d+(\.\d{1,2})?)", price)
            price = float(m.group(1)) if m else None

        if in_stock not in ["Y", "N", "unknown"]:
            text = notes.lower()
            if any(s in text for s in ["out of stock", "sold out", "unavailable"]):
                in_stock = "N"
            elif any(s in text for s in ["in stock", "available", "ready to ship"]):
                in_stock = "Y"
            else:
                in_stock = "unknown"

        if in_stock == "unknown" and price is not None:
            in_stock = "Y"

        log(
            f"AI parse url={final_url} price={price} in_stock={in_stock} notes={notes[:120]}",
            context="parsing",
        )

        return {
            "price": price,
            "stock": None if in_stock == "unknown" else in_stock,
            "url_used": final_url,
            "notes": notes,
            "error": None,
            "response_ms": elapsed_ms,
            "status": "ai_ok",
            "method": "ai_html",
        }

    except Exception as e:
        elapsed_ms = int((time.time() - start) * 1000)
        log(
            f"AI HTML parse error url={final_url} exc={e!r}",
            context="parsing",
        )
        return {
            "price": None,
            "stock": None,
            "url_used": final_url,
            "notes": "",
            "error": f"AI HTML parse error: {e}",
            "response_ms": elapsed_ms,
            "status": "ai_error",
            "method": "ai_html",
        }
