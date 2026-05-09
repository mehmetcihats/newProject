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
# API pagination replay - the reliable path.
# ---------------------------------------------------------------------------

def _bump_pagination(url: str, step: int) -> Optional[str]:
    """
    Given a Carousell listings-API URL, produce the next page URL by
    bumping whichever pagination param is present. Returns None if the
    URL doesn't look like a paginated listings endpoint.
    """
    from urllib.parse import urlparse as _up, parse_qsl, urlencode, urlunparse

    parsed = _up(url)
    qs = dict(parse_qsl(parsed.query, keep_blank_values=True))

    # Common cursor/offset params used by Carousell's various endpoints.
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


def _count_listings_in_json(data: Any) -> int:
    """Count how many listing-like objects are in a JSON blob."""
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
    page_size: int = 30,
    max_pages: int = 500,
) -> None:
    """
    Find the pagination URL from the captured network traffic and replay
    it with bumped offsets until the API returns no more listings.

    This is the PRIMARY path to loading every listing, zero scrolling
    required. We pick the URL that returned the most listings (that's
    the main seller feed endpoint), then page through it directly.
    """
    # Rank captured URLs by how many listings they returned.
    scored = []
    for url in seen_urls:
        body = seen_bodies.get(url)
        if not body:
            continue
        try:
            data = json.loads(body)
        except Exception:
            continue
        n = _count_listings_in_json(data)
        if n >= 3 and _bump_pagination(url, page_size) is not None:
            scored.append((n, url))

    if not scored:
        print(
            "[!] Could not detect a paginated listings API from captured traffic. "
            "Falling back to whatever was already loaded.",
            file=sys.stderr,
        )
        return

    scored.sort(reverse=True)
    best_url = scored[0][1]
    print(
        f"[+] Using API endpoint {best_url.split('?')[0]} for pagination",
        file=sys.stderr,
    )

    # Replay via the browser's request context so cookies/headers carry over.
    ctx = page.context
    url = best_url
    empty_in_a_row = 0

    for i in range(max_pages):
        next_url = _bump_pagination(url, page_size)
        if not next_url:
            break
        try:
            resp = ctx.request.get(next_url, timeout=20_000)
        except Exception as e:
            print(f"[!] Request failed: {e}", file=sys.stderr)
            break
        if resp.status != 200:
            print(
                f"[!] API returned HTTP {resp.status}, stopping pagination.",
                file=sys.stderr,
            )
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
            empty_in_a_row += 1
            # Two empty pages in a row = we've reached the end.
            if empty_in_a_row >= 2:
                break
        else:
            empty_in_a_row = 0

        url = next_url


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


def _scroll_hard(page: Page, rounds: int = 80, pause_ms: int = 1200) -> None:
    """
    Trigger Carousell's lazy-loader fully automatically.

    The reliable technique: grab the LAST product card currently in the DOM
    and call `scrollIntoView()` on it. This makes the element visible no
    matter what kind of virtualized / overflow container holds it, which is
    exactly what Carousell's IntersectionObserver needs to fetch the next
    page of listings. Window / container / wheel scrolls don't always
    reach the right scroll context on the Indonesian profile layout, which
    is why the older version stalled at 20 and required manual scrolling.
    """
    last = -1
    stable = 0
    for i in range(rounds):
        # 1. The main trick: scroll the last visible product card into view.
        #    This forces the browser to realize "this element should be
        #    on screen", which cascades through every scroll container and
        #    triggers the intersection observer that loads the next batch.
        page.evaluate(
            """
            () => {
                const anchors = document.querySelectorAll('a[href*="/p/"]');
                let last = null;
                for (const a of anchors) {
                    const h = a.getAttribute('href') || '';
                    if (/\\/p\\/[^/]+-\\d+/.test(h)) last = a;
                }
                if (last) {
                    last.scrollIntoView({ block: 'end', behavior: 'instant' });
                }
                // Also drop a window scroll + every overflow container scroll,
                // in case the page has a footer-style loader below the grid.
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

        # 2. Real mouse wheel at the bottom of the viewport as extra nudge.
        try:
            vw = page.viewport_size or {"width": 1366, "height": 900}
            page.mouse.move(vw["width"] // 2, vw["height"] - 80)
            for _ in range(4):
                page.mouse.wheel(0, 2500)
                page.wait_for_timeout(100)
        except Exception:
            pass

        # 3. Keyboard fallbacks.
        try:
            page.keyboard.press("End")
            page.wait_for_timeout(60)
            page.keyboard.press("PageDown")
        except Exception:
            pass

        # 4. Click any "Show more" / "Lihat lainnya" button.
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
            f"(stable {stable}/12)",
            file=sys.stderr,
        )

        if count == last:
            stable += 1
            # Need 12 stable rounds before giving up - API responses can
            # be slow and we'd rather wait than truncate a big profile.
            if stable >= 12:
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
    seen_urls: List[str] = []           # order-preserving list of Carousell JSON URLs
    seen_bodies: Dict[str, str] = {}    # url -> response body text

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
        # We also remember the response body so we can later detect the
        # pagination endpoint and replay it directly.
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
            if url not in seen_bodies:
                seen_urls.append(url)
                seen_bodies[url] = body_text
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

        # Give the initial page a moment to fire its first API calls.
        page.wait_for_timeout(2000)

        # PRIMARY PATH: detect Carousell's listings-feed endpoint from the
        # traffic we've seen and page through it directly. Zero scrolling
        # required - this is a plain HTTP fetch loop.
        print(
            "[+] Replaying Carousell's pagination API directly "
            "(no scrolling needed)...",
            file=sys.stderr,
        )
        _replay_pagination(page, host, seen_urls, seen_bodies, collected)

        # BACKUP PATH: if the API replay didn't work on this layout for some
        # reason, fall back to scroll-and-capture.
        if len(collected) < 30:
            print(
                "[+] API replay returned < 30 listings; falling back to "
                "scroll-capture as a backup.",
                file=sys.stderr,
            )
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
