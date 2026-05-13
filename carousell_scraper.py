"""
Carousell Scraper — Profile + Category support.

Scrapes product listings (title, price, link) from:
  1. A user's profile page (all available products)
  2. A category page (e.g. Men's Fashion, sorted by newest)

Strategy:
  - Opens the page in Playwright to get a real session (cookies/headers).
  - Intercepts JSON API responses to capture listing data.
  - Detects the pagination endpoint and replays it directly with
    cursor-based OR offset-based pagination — no scrolling needed.
  - Falls back to DOM scraping if API replay doesn't work.

Usage:
    # Scrape a user profile
    python carousell_scraper.py https://id.carousell.com/u/pennipennipenni/

    # Scrape a category (sorted newest first)
    python carousell_scraper.py https://id.carousell.com/categories/mens-fashion-3/

    # Options
    python carousell_scraper.py <url> -o output.csv --headful --overwrite
"""

from __future__ import annotations

import argparse
import csv
import functools
import json
import os
import re
import sys
import time
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

from playwright.sync_api import (
    Page,
    Response,
    TimeoutError as PWTimeout,
    sync_playwright,
)

# Force unbuffered so progress lines show up live in PowerShell.
print = functools.partial(print, flush=True)  # noqa: A001

PRICE_RE = re.compile(
    r"(?:Rp\.?|S\$|HK\$|US\$|A\$|NT\$|RM|IDR|SGD|MYR|PHP|HKD|USD|TWD|AUD|THB|VND|"
    r"\$|₱|₩|¥|€|£|฿|₫)\s?"
    r"[\d.,]+(?:\s?(?:k|K|juta|jt|rb|ribu|million|M))?",
    re.IGNORECASE,
)


@dataclass
class Product:
    title: str = ""
    price: str = ""
    link: str = ""


# ---------------------------------------------------------------------------
# JSON walking — extract listing-shaped objects from any JSON blob
# ---------------------------------------------------------------------------

def _looks_like_listing(obj: Dict[str, Any]) -> bool:
    id_keys = ("id", "listingId", "listing_id", "productId", "product_id")
    title_keys = ("title", "name", "productTitle", "product_title")
    price_keys = (
        "price", "priceFormatted", "price_formatted", "displayPrice",
        "display_price", "currencyPriceFormatted",
    )
    has_id = any(k in obj for k in id_keys)
    has_title = any(k in obj for k in title_keys)
    has_price = any(k in obj for k in price_keys)
    return has_id and (has_title or has_price)


def _extract_listing(obj: Dict[str, Any], host: str) -> Optional[Product]:
    title = ""
    for k in ("title", "productTitle", "product_title", "name"):
        v = obj.get(k)
        if isinstance(v, str) and v.strip():
            title = v.strip()
            break

    price = ""
    for k in ("priceFormatted", "price_formatted", "displayPrice",
              "display_price", "currencyPriceFormatted", "price"):
        v = obj.get(k)
        if isinstance(v, str) and v.strip():
            price = v.strip()
            break
        if isinstance(v, (int, float)) and not price:
            price = str(v)
            break
        if isinstance(v, dict):
            for fk in ("formatted", "display", "amount_formatted"):
                fv = v.get(fk)
                if isinstance(fv, str) and fv.strip():
                    price = fv.strip()
                    break
            if price:
                break

    listing_id = None
    for k in ("id", "listingId", "listing_id", "productId", "product_id"):
        v = obj.get(k)
        if isinstance(v, (str, int)) and str(v).strip():
            listing_id = str(v).strip()
            break

    slug = None
    for k in ("slug", "productSlug", "product_slug", "urlSlug", "url_slug"):
        v = obj.get(k)
        if isinstance(v, str) and v.strip():
            slug = v.strip()
            break

    direct_link = None
    for k in ("url", "href", "permalink", "listing_url", "productUrl"):
        v = obj.get(k)
        if isinstance(v, str) and v.strip():
            direct_link = v.strip()
            break

    if direct_link:
        link = direct_link if direct_link.startswith("http") else f"https://{host}{direct_link if direct_link.startswith('/') else '/' + direct_link}"
    elif listing_id:
        slug_part = f"{slug}-" if slug else ""
        link = f"https://{host}/p/{slug_part}{listing_id}/"
    else:
        return None

    if not title and not price:
        return None

    return Product(title=title, price=price, link=link)


def _walk_for_listings(node: Any, host: str, out: Dict[str, Product]) -> None:
    if isinstance(node, dict):
        if _looks_like_listing(node):
            prod = _extract_listing(node, host)
            if prod and prod.link:
                existing = out.get(prod.link)
                if existing is None:
                    out[prod.link] = prod
                else:
                    if not existing.title and prod.title:
                        existing.title = prod.title
                    if not existing.price and prod.price:
                        existing.price = prod.price
        for v in node.values():
            _walk_for_listings(v, host, out)
    elif isinstance(node, list):
        for v in node:
            _walk_for_listings(v, host, out)


# ---------------------------------------------------------------------------
# Pagination — detect & replay the listings API (cursor OR offset)
# ---------------------------------------------------------------------------

def _find_cursor_in_json(data: Any) -> Optional[str]:
    """
    Recursively search a JSON blob for cursor/token fields that indicate
    there's a next page. Common field names Carousell uses.
    """
    cursor_keys = (
        "nextCursor", "next_cursor", "cursor", "endCursor", "end_cursor",
        "nextPageToken", "next_page_token", "session_id",
    )
    if isinstance(data, dict):
        for k in cursor_keys:
            v = data.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()
        for v in data.values():
            result = _find_cursor_in_json(v)
            if result:
                return result
    elif isinstance(data, list):
        for v in data:
            result = _find_cursor_in_json(v)
            if result:
                return result
    return None


def _bump_pagination_offset(url: str, step: int) -> Optional[str]:
    """Bump offset/start/page in the URL query string."""
    parsed = urlparse(url)
    qs = dict(parse_qsl(parsed.query, keep_blank_values=True))

    if "offset" in qs:
        try:
            qs["offset"] = str(int(qs["offset"]) + step)
        except ValueError:
            return None
    elif "start" in qs:
        try:
            qs["start"] = str(int(qs["start"]) + step)
        except ValueError:
            return None
    elif "page" in qs:
        try:
            qs["page"] = str(int(qs["page"]) + 1)
        except ValueError:
            return None
    else:
        return None

    return urlunparse(parsed._replace(query=urlencode(qs)))


def _set_cursor_in_url(url: str, cursor: str) -> str:
    """Replace or add a cursor/session param in the URL."""
    parsed = urlparse(url)
    qs = dict(parse_qsl(parsed.query, keep_blank_values=True))

    # Try common cursor param names
    for key in ("cursor", "next_cursor", "session_id", "nextCursor", "pageToken"):
        if key in qs:
            qs[key] = cursor
            return urlunparse(parsed._replace(query=urlencode(qs)))

    # If none found, add "cursor" param
    qs["cursor"] = cursor
    return urlunparse(parsed._replace(query=urlencode(qs)))


def _count_listings_in_json(data: Any) -> int:
    count = 0
    def walk(n: Any) -> None:
        nonlocal count
        if isinstance(n, dict):
            if _looks_like_listing(n):
                count += 1
            for v in n.values():
                walk(v)
        elif isinstance(n, list):
            for v in n:
                walk(v)
    walk(data)
    return count


def _replay_pagination(
    page: Page,
    host: str,
    seen_urls: List[str],
    seen_bodies: Dict[str, str],
    collected: Dict[str, Product],
    max_pages: int = 200,
) -> None:
    """
    Detect the pagination endpoint from captured traffic and replay it.
    Supports both offset-based and cursor-based pagination.
    """
    # Find the URL that returned the most listings
    scored: List[Tuple[int, str]] = []
    for url in seen_urls:
        body = seen_bodies.get(url)
        if not body:
            continue
        try:
            data = json.loads(body)
        except Exception:
            continue
        n = _count_listings_in_json(data)
        if n >= 3:
            scored.append((n, url))

    if not scored:
        print("[!] No paginated API endpoint detected in traffic.", file=sys.stderr)
        return

    scored.sort(reverse=True)
    best_url = scored[0][1]
    best_body = seen_bodies[best_url]

    print(
        f"[+] Using API: {best_url.split('?')[0]}",
        file=sys.stderr,
    )

    # Determine pagination type: cursor or offset?
    try:
        best_data = json.loads(best_body)
    except Exception:
        return

    cursor = _find_cursor_in_json(best_data)
    can_offset = _bump_pagination_offset(best_url, 20) is not None

    ctx = page.context
    empty_count = 0
    current_url = best_url

    for i in range(max_pages):
        # Build next URL
        if cursor:
            next_url = _set_cursor_in_url(current_url, cursor)
        elif can_offset:
            next_url = _bump_pagination_offset(current_url, 20)
            if not next_url:
                break
        else:
            # No pagination mechanism found
            print("[!] No pagination mechanism (cursor/offset) found.", file=sys.stderr)
            break

        try:
            resp = ctx.request.get(next_url, timeout=20_000)
        except Exception as e:
            print(f"[!] Request failed: {e}", file=sys.stderr)
            break

        if resp.status != 200:
            print(f"[!] HTTP {resp.status}, stopping.", file=sys.stderr)
            break

        try:
            data = resp.json()
        except Exception:
            break

        before = len(collected)
        _walk_for_listings(data, host, collected)
        gained = len(collected) - before

        print(
            f"[api] page {i + 2}: +{gained} new (total: {len(collected)})",
            file=sys.stderr,
        )

        if gained == 0:
            empty_count += 1
            if empty_count >= 2:
                break
        else:
            empty_count = 0

        # Update for next iteration
        if cursor:
            new_cursor = _find_cursor_in_json(data)
            if new_cursor and new_cursor != cursor:
                cursor = new_cursor
            else:
                # Cursor didn't change — try offset fallback
                if can_offset:
                    current_url = next_url
                    cursor = None
                else:
                    break
        else:
            current_url = next_url


# ---------------------------------------------------------------------------
# DOM fallback
# ---------------------------------------------------------------------------

def _dom_listings(page: Page, host: str) -> List[Product]:
    raw = page.evaluate(
        """
        () => {
            const seen = new Map();
            const anchors = document.querySelectorAll('a[href*="/p/"]');
            anchors.forEach(a => {
                const href = a.getAttribute('href') || '';
                if (!/\\/p\\/[^/]+-\\d+/.test(href)) return;
                const text = (a.innerText || '').trim();
                let card = a;
                for (let i = 0; i < 5 && card.parentElement; i++) {
                    card = card.parentElement;
                }
                const cardText = (card.innerText || '').trim();
                const prev = seen.get(href);
                if (!prev || cardText.length > prev.cardText.length) {
                    seen.set(href, { text, cardText });
                }
            });
            return Array.from(seen, ([href, v]) => ({
                href, text: v.text, cardText: v.cardText,
            }));
        }
        """
    )

    BOILERPLATE = {
        "protection", "carousell protection", "mailing", "meetup",
        "bumped", "boosted", "promoted", "featured",
    }

    products: List[Product] = []
    for item in raw:
        href: str = item["href"]
        text: str = item["text"] or ""
        card_text: str = item.get("cardText") or text

        link = href if href.startswith("http") else f"https://{host}{href}"

        price_match = PRICE_RE.search(card_text)
        price = price_match.group(0).strip() if price_match else ""

        title = ""
        for ln in (l.strip() for l in text.splitlines() if l.strip()):
            if PRICE_RE.search(ln) or ln.lower() in BOILERPLATE:
                continue
            title = ln
            break

        if title or price:
            products.append(Product(title=title, price=price, link=link))
    return products


# ---------------------------------------------------------------------------
# Scrolling fallback (only if API replay fails)
# ---------------------------------------------------------------------------

def _scroll_and_capture(page: Page, rounds: int = 40, pause_ms: int = 1500) -> None:
    """Light scroll as last resort — scrollIntoView on last product card."""
    last = -1
    stable = 0
    for i in range(rounds):
        page.evaluate(
            """
            () => {
                const anchors = document.querySelectorAll('a[href*="/p/"]');
                let last = null;
                for (const a of anchors) {
                    const h = a.getAttribute('href') || '';
                    if (/\\/p\\/[^/]+-\\d+/.test(h)) last = a;
                }
                if (last) last.scrollIntoView({ block: 'end', behavior: 'instant' });
                window.scrollTo(0, document.documentElement.scrollHeight);
            }
            """
        )
        try:
            page.mouse.wheel(0, 3000)
        except Exception:
            pass
        page.wait_for_timeout(pause_ms)

        count = page.evaluate(
            """
            () => {
                const set = new Set();
                document.querySelectorAll('a[href*="/p/"]').forEach(a => {
                    const h = a.getAttribute('href') || '';
                    if (/\\/p\\/[^/]+-\\d+/.test(h)) set.add(h);
                });
                return set.size;
            }
            """
        )
        if count == last:
            stable += 1
            if stable >= 8:
                break
        else:
            stable = 0
            last = count
            print(f"[scroll] {count} listings on page...", file=sys.stderr)


# ---------------------------------------------------------------------------
# Main scrape logic
# ---------------------------------------------------------------------------

def scrape(target_url: str, headless: bool = True, sort_newest: bool = False) -> List[Product]:
    """
    Scrape listings from a Carousell profile URL or category URL.
    If sort_newest=True and it's a category page, sort by newest.
    """
    host = urlparse(target_url).hostname or "id.carousell.com"
    collected: Dict[str, Product] = {}
    seen_urls: List[str] = []
    seen_bodies: Dict[str, str] = {}

    # If it's a category URL and sort_newest, append sort param
    if "/categories/" in target_url and sort_newest:
        parsed = urlparse(target_url)
        qs = dict(parse_qsl(parsed.query))
        if "sort_by" not in qs:
            qs["sort_by"] = "3"  # 3 = newest on Carousell
            target_url = urlunparse(parsed._replace(query=urlencode(qs)))

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=headless)
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1366, "height": 900},
            locale="en-US",
        )
        page = context.new_page()

        # Intercept JSON responses
        def on_response(resp: Response) -> None:
            try:
                ct = (resp.headers.get("content-type") or "").lower()
                if "json" not in ct:
                    return
                url = resp.url
                if "carousell" not in url:
                    return
                body_text = resp.text()
                if not body_text or len(body_text) > 5_000_000:
                    return
                data = json.loads(body_text)
            except Exception:
                return
            if url not in seen_bodies:
                seen_urls.append(url)
                seen_bodies[url] = body_text
            before = len(collected)
            _walk_for_listings(data, host, collected)
            gained = len(collected) - before
            if gained:
                print(
                    f"[net] +{gained} from {url.split('?')[0]} (total: {len(collected)})",
                    file=sys.stderr,
                )

        page.on("response", on_response)

        print(f"[+] Opening {target_url}", file=sys.stderr)
        page.goto(target_url, wait_until="domcontentloaded", timeout=60_000)

        try:
            page.wait_for_selector('a[href*="/p/"]', timeout=20_000)
        except PWTimeout:
            print(
                "[!] No listings appeared within 20s.",
                file=sys.stderr,
            )

        # Parse __NEXT_DATA__ if present
        try:
            nd = page.evaluate(
                "() => { const el = document.getElementById('__NEXT_DATA__');"
                " return el ? el.textContent : null; }"
            )
            if nd:
                data = json.loads(nd)
                before = len(collected)
                _walk_for_listings(data, host, collected)
                gained = len(collected) - before
                if gained:
                    print(f"[+] __NEXT_DATA__: +{gained} (total: {len(collected)})", file=sys.stderr)
        except Exception:
            pass

        # Wait for initial API calls to fire
        page.wait_for_timeout(3000)

        # PRIMARY: replay pagination API
        print("[+] Replaying pagination API (no scroll needed)...", file=sys.stderr)
        _replay_pagination(page, host, seen_urls, seen_bodies, collected)

        # FALLBACK: if we got very few, try scrolling
        if len(collected) < 30:
            print("[+] Few results from API; trying scroll fallback...", file=sys.stderr)
            _scroll_and_capture(page)
            # Collect DOM listings
            dom = _dom_listings(page, host)
            for p in dom:
                if p.link not in collected:
                    collected[p.link] = p

        browser.close()

    return list(collected.values())


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def write_csv(products: List[Product], path: str) -> None:
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(
            f, fieldnames=["title", "price", "link"], delimiter=";"
        )
        writer.writeheader()
        for p in products:
            writer.writerow(asdict(p))


def write_xlsx(products: List[Product], path: str) -> bool:
    try:
        from openpyxl import Workbook
        from openpyxl.styles import Font
    except ImportError:
        print("[i] pip install openpyxl for .xlsx output", file=sys.stderr)
        return False

    wb = Workbook()
    ws = wb.active
    ws.title = "Products"
    ws.append(["title", "price", "link"])
    for cell in ws[1]:
        cell.font = Font(bold=True)
    for p in products:
        ws.append([p.title, p.price, p.link])
    ws.column_dimensions["A"].width = 60
    ws.column_dimensions["B"].width = 18
    ws.column_dimensions["C"].width = 80
    wb.save(path)
    return True


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Scrape Carousell listings from a user profile or category page.\n"
            "Supports both profile URLs (/u/username/) and category URLs (/categories/...)."
        ),
    )
    parser.add_argument(
        "url",
        help=(
            "Profile URL (https://id.carousell.com/u/username/) "
            "or Category URL (https://id.carousell.com/categories/mens-fashion-3/)"
        ),
    )
    parser.add_argument("-o", "--output", default=None)
    parser.add_argument("--overwrite", action="store_true",
                        help="Use plain filename without timestamp (overwrites previous)")
    parser.add_argument("--headful", action="store_true",
                        help="Show browser window")
    parser.add_argument("--newest", action="store_true",
                        help="Sort by newest (for category pages)")
    args = parser.parse_args()

    target_url = args.url.strip()

    # Determine a name for the output file
    parsed = urlparse(target_url)
    path_parts = [p for p in parsed.path.strip("/").split("/") if p]
    if path_parts:
        name = path_parts[-1]
    else:
        name = "carousell"

    if args.output:
        csv_path = args.output
    elif args.overwrite:
        csv_path = f"{name}_products.csv"
    else:
        stamp = time.strftime("%Y%m%d_%H%M%S")
        csv_path = f"{name}_products_{stamp}.csv"

    stem, _ = os.path.splitext(csv_path)
    xlsx_path = f"{stem}.xlsx"

    start = time.time()
    products = scrape(target_url, headless=not args.headful, sort_newest=args.newest)
    elapsed = time.time() - start

    write_csv(products, csv_path)
    wrote_xlsx = write_xlsx(products, xlsx_path)

    outputs = csv_path + (f" + {xlsx_path}" if wrote_xlsx else "")
    print(
        f"[+] Done. {len(products)} product(s) in {elapsed:.1f}s -> {outputs}",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
