"""
Carousell user-profile scraper.

Scrapes every product listing from a Carousell user's profile page
(title, price, link) and writes the results to CSV and XLSX.

Strategy:
  1. Open the profile in a real browser (Playwright).
  2. Intercept ALL JSON responses from the page and extract anything that
     looks like a listing. This is the primary source of data - it catches
     every listing the page itself sees, including ones loaded via infinite
     scroll.
  3. Also parse any __NEXT_DATA__ / JSON-LD embedded in the HTML as a backup.
  4. Scroll aggressively to trigger the API calls that fetch more listings.
  5. Fall back to DOM scraping as a last resort.

Usage:
    python carousell_scraper.py <username-or-profile-url> [-o output.csv] [--headful]

Examples:
    python carousell_scraper.py johndoe
    python carousell_scraper.py https://id.carousell.com/u/johndoe/
    python carousell_scraper.py johndoe -o john.csv --headful
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
from dataclasses import dataclass, asdict, field
from typing import Any, Dict, List, Optional, Set
from urllib.parse import urlparse

from playwright.sync_api import (
    Page,
    Response,
    TimeoutError as PWTimeout,
    sync_playwright,
)

# Force unbuffered so progress lines show up live in PowerShell.
print = functools.partial(print, flush=True)  # noqa: A001


BASE = "https://www.carousell.com"

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
# URL helpers
# ---------------------------------------------------------------------------

def normalise_profile_url(arg: str) -> str:
    """Accept either a bare username or a full profile URL; return full URL."""
    arg = arg.strip()
    if arg.startswith("http://") or arg.startswith("https://"):
        parsed = urlparse(arg)
        if "/u/" not in parsed.path:
            raise ValueError(
                f"URL does not look like a Carousell user profile: {arg}"
            )
        return arg.rstrip("/") + "/"
    return f"{BASE}/u/{arg.strip('/')}/"


def _build_listing_url(host: str, listing_id: str, slug: Optional[str] = None) -> str:
    slug_part = f"{slug}-" if slug else ""
    return f"https://{host}/p/{slug_part}{listing_id}/"


# ---------------------------------------------------------------------------
# JSON walking - pull any object that looks like a listing out of a big blob
# ---------------------------------------------------------------------------

def _looks_like_listing(obj: Dict[str, Any]) -> bool:
    """
    Heuristic: the object has an id-like field AND some combination of
    title/price/slug. Carousell's API uses a handful of shapes across
    endpoints so we match on any of them.
    """
    id_keys = ("id", "listingId", "listing_id", "productId", "product_id")
    title_keys = ("title", "name", "productTitle", "product_title")
    price_keys = (
        "price",
        "priceFormatted",
        "price_formatted",
        "priceHtml",
        "displayPrice",
        "display_price",
        "currencyPriceFormatted",
    )

    has_id = any(k in obj for k in id_keys)
    has_title = any(k in obj for k in title_keys)
    has_price = any(k in obj for k in price_keys)

    return has_id and has_title and has_price


def _extract_listing(obj: Dict[str, Any], host: str) -> Optional[Product]:
    """Pull title / price / link out of a JSON object matching a listing."""
    # Title
    title = ""
    for k in ("title", "productTitle", "product_title", "name"):
        v = obj.get(k)
        if isinstance(v, str) and v.strip():
            title = v.strip()
            break

    # Price (prefer pre-formatted strings).
    price = ""
    for k in (
        "priceFormatted",
        "price_formatted",
        "displayPrice",
        "display_price",
        "currencyPriceFormatted",
        "price",
    ):
        v = obj.get(k)
        if isinstance(v, str) and v.strip():
            price = v.strip()
            break
        if isinstance(v, (int, float)) and price == "":
            # No currency - leave raw for now; better than nothing.
            price = str(v)
            break
        if isinstance(v, dict):
            # Some APIs: { amount: 100000, currency: "IDR", formatted: "Rp 100.000" }
            for fk in ("formatted", "display", "amount_formatted"):
                fv = v.get(fk)
                if isinstance(fv, str) and fv.strip():
                    price = fv.strip()
                    break
            if price:
                break

    # ID + slug -> build the link.
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

    # Some endpoints include a ready-made URL path/href.
    direct_link = None
    for k in ("url", "href", "permalink", "listing_url", "productUrl"):
        v = obj.get(k)
        if isinstance(v, str) and v.strip():
            direct_link = v.strip()
            break

    if direct_link:
        link = direct_link if direct_link.startswith("http") else f"https://{host}{direct_link if direct_link.startswith('/') else '/' + direct_link}"
    elif listing_id:
        link = _build_listing_url(host, listing_id, slug)
    else:
        return None

    if not title and not price:
        return None

    return Product(title=title, price=price, link=link)


def _walk_for_listings(
    node: Any,
    host: str,
    out: Dict[str, Product],
) -> None:
    """Recursively walk a JSON structure, collecting every listing-like object."""
    if isinstance(node, dict):
        if _looks_like_listing(node):
            prod = _extract_listing(node, host)
            if prod and prod.link:
                # Dedupe by link; prefer entries that have both title and price.
                existing = out.get(prod.link)
                if existing is None:
                    out[prod.link] = prod
                else:
                    # Merge: keep richer fields.
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
# DOM fallback
# ---------------------------------------------------------------------------

def _dom_listings(page: Page, host: str) -> List[Product]:
    """Last-resort DOM scrape. Gets whatever cards are currently rendered."""
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
        if not title:
            for ln in (l.strip() for l in card_text.splitlines() if l.strip()):
                if PRICE_RE.search(ln) or ln.lower() in BOILERPLATE:
                    continue
                title = ln
                break

        if title or price:
            products.append(Product(title=title, price=price, link=link))
    return products


# ---------------------------------------------------------------------------
# Scrolling - just keep nudging the page to trigger API fetches.
# ---------------------------------------------------------------------------

def _count_listing_anchors(page: Page) -> int:
    return page.evaluate(
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


def _scroll_hard(page: Page, rounds: int = 60, pause_ms: int = 1500) -> None:
    """
    Scroll aggressively to trigger every lazy-loader the page might have.
    Progress is printed each round so the user can see what's happening.
    """
    last = -1
    stable = 0
    for i in range(rounds):
        # 1. nudge up
        page.evaluate("() => window.scrollBy(0, -400)")
        page.wait_for_timeout(120)

        # 2. scroll window + every scrollable descendant to the bottom
        page.evaluate(
            """
            () => {
                window.scrollTo(0, document.documentElement.scrollHeight);
                for (const el of document.querySelectorAll('*')) {
                    if (!(el instanceof HTMLElement)) continue;
                    const s = getComputedStyle(el);
                    if (/(auto|scroll|overlay)/.test(s.overflowY)
                        && el.scrollHeight > el.clientHeight + 5) {
                        el.scrollTop = el.scrollHeight;
                    }
                }
            }
            """
        )

        # 3. real mouse wheel near the bottom of the viewport
        try:
            vw = page.viewport_size or {"width": 1366, "height": 900}
            page.mouse.move(vw["width"] // 2, vw["height"] - 80)
            for _ in range(6):
                page.mouse.wheel(0, 3000)
                page.wait_for_timeout(120)
        except Exception:
            pass

        # 4. keyboard fallbacks
        try:
            page.keyboard.press("End")
            page.wait_for_timeout(80)
            page.keyboard.press("PageDown")
        except Exception:
            pass

        # 5. click any "Show more" / "Lihat lainnya" button
        try:
            page.evaluate(
                """
                () => {
                    const needles = [
                        'show more', 'load more', 'see more',
                        'lihat lainnya', 'lihat lebih banyak',
                        'muat lebih banyak', 'tampilkan lebih',
                    ];
                    for (const el of document.querySelectorAll(
                        'button, a, div[role="button"]'
                    )) {
                        const t = (el.innerText || '').trim().toLowerCase();
                        if (!t) continue;
                        if (needles.some(n => t.includes(n))) {
                            el.scrollIntoView({ block: 'center' });
                            el.click();
                            return;
                        }
                    }
                }
                """
            )
        except Exception:
            pass

        page.wait_for_timeout(pause_ms)

        count = _count_listing_anchors(page)
        print(
            f"[+] Round {i + 1}: {count} listing anchors on page "
            f"(stable {stable}/10)",
            file=sys.stderr,
        )

        if count == last:
            stable += 1
            if stable >= 10:
                break
        else:
            stable = 0
            last = count


# ---------------------------------------------------------------------------
# Main scrape
# ---------------------------------------------------------------------------

def scrape_profile(profile_url: str, headless: bool = True) -> List[Product]:
    host = urlparse(profile_url).hostname or "www.carousell.com"
    collected: Dict[str, Product] = {}  # link -> Product

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

        # Intercept every JSON-ish response and mine it for listings.
        def on_response(resp: Response) -> None:
            try:
                ct = (resp.headers.get("content-type") or "").lower()
                if "json" not in ct:
                    return
                url = resp.url
                # Only listen to Carousell's own endpoints.
                if "carousell" not in url:
                    return
                body_text = resp.text()
                if not body_text or len(body_text) > 4_000_000:
                    return
                data = json.loads(body_text)
            except Exception:
                return
            before = len(collected)
            _walk_for_listings(data, host, collected)
            gained = len(collected) - before
            if gained:
                print(
                    f"[net] +{gained} listings from {url.split('?')[0]} "
                    f"(total: {len(collected)})",
                    file=sys.stderr,
                )

        page.on("response", on_response)

        print(f"[+] Opening {profile_url}", file=sys.stderr)
        page.goto(profile_url, wait_until="domcontentloaded", timeout=60_000)

        try:
            page.wait_for_selector('a[href*="/p/"]', timeout=20_000)
        except PWTimeout:
            print(
                "[!] No product links appeared within 20s. "
                "The profile may be empty, private, or blocking access.",
                file=sys.stderr,
            )

        # Harvest any listings embedded in __NEXT_DATA__ / the initial HTML.
        try:
            next_data_raw = page.evaluate(
                "() => { const el = document.getElementById('__NEXT_DATA__');"
                " return el ? el.textContent : null; }"
            )
            if next_data_raw:
                try:
                    data = json.loads(next_data_raw)
                    before = len(collected)
                    _walk_for_listings(data, host, collected)
                    print(
                        f"[+] __NEXT_DATA__ yielded {len(collected) - before} "
                        f"listings (total: {len(collected)})",
                        file=sys.stderr,
                    )
                except Exception:
                    pass
        except Exception:
            pass

        print("[+] Scrolling to trigger API pagination...", file=sys.stderr)
        _scroll_hard(page)

        # Final DOM scrape as a backup / enrichment pass.
        dom_products = _dom_listings(page, host)
        for p in dom_products:
            existing = collected.get(p.link)
            if existing is None:
                collected[p.link] = p
            else:
                if not existing.title and p.title:
                    existing.title = p.title
                if not existing.price and p.price:
                    existing.price = p.price

        browser.close()

    # Filter out sold / reserved from whatever the DOM told us (the API
    # results may not carry status; we keep them unless they came from a
    # card with a sold badge).
    # Keep it simple: just return everything we've got, deduped.
    return list(collected.values())


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def write_csv(products: List[Product], path: str, delimiter: str = ";") -> None:
    """
    Write a UTF-8 (with BOM) CSV. The BOM makes Excel pick up UTF-8 reliably,
    and the semicolon delimiter is what Excel expects on Indonesian /
    European locales - it will parse into proper columns.
    """
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(
            f, fieldnames=["title", "price", "link"], delimiter=delimiter
        )
        writer.writeheader()
        for p in products:
            writer.writerow(asdict(p))


def write_xlsx(products: List[Product], path: str) -> bool:
    try:
        from openpyxl import Workbook
        from openpyxl.styles import Font
    except ImportError:
        print(
            "[i] Skipping .xlsx output. Install openpyxl to enable:\n"
            "    pip install openpyxl",
            file=sys.stderr,
        )
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
        description="Scrape a Carousell user's profile and export listings.",
    )
    parser.add_argument("profile")
    parser.add_argument("-o", "--output", default=None)
    parser.add_argument("--headful", action="store_true")
    args = parser.parse_args()

    try:
        url = normalise_profile_url(args.profile)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 2

    username = url.rstrip("/").split("/")[-1]
    csv_path = args.output or f"{username}_products.csv"
    stem, _ = os.path.splitext(csv_path)
    xlsx_path = f"{stem}.xlsx"

    start = time.time()
    products = scrape_profile(url, headless=not args.headful)
    elapsed = time.time() - start

    write_csv(products, csv_path)
    wrote_xlsx = write_xlsx(products, xlsx_path)

    outputs = csv_path + (f" + {xlsx_path}" if wrote_xlsx else "")
    print(
        f"[+] Done. Scraped {len(products)} product(s) in {elapsed:.1f}s -> {outputs}",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
