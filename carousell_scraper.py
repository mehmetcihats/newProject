"""
Carousell user-profile scraper.

Scrapes every product listing from a Carousell user's profile page
(title, price, link) and writes the results to both CSV and XLSX.

Usage:
    python carousell_scraper.py <username-or-profile-url> [-o output.csv] [--headful]

Examples:
    python carousell_scraper.py johndoe
    python carousell_scraper.py https://www.carousell.com/u/johndoe/
    python carousell_scraper.py johndoe -o john.csv --headful
"""

from __future__ import annotations

import argparse
import csv
import os
import re
import sys
import time
from dataclasses import dataclass, asdict
from typing import List, Set
from urllib.parse import urlparse

from playwright.sync_api import Page, TimeoutError as PWTimeout, sync_playwright


BASE = "https://www.carousell.com"

# Any string that starts with a currency symbol / code followed by a number.
# Covers S$, $, RM, HK$, US$, A$, NT$, ₱, ₩, ¥, €, £, Rp, IDR, SGD, MYR, PHP, etc.
# Case-insensitive so 'rp', 'Rp', 'RP' all match.
PRICE_RE = re.compile(
    r"(?:Rp\.?|S\$|HK\$|US\$|A\$|NT\$|RM|IDR|SGD|MYR|PHP|HKD|USD|TWD|AUD|THB|VND|"
    r"\$|₱|₩|¥|€|£|฿|₫)\s?"
    r"[\d.,]+(?:\s?(?:k|K|juta|jt|rb|ribu|million|M))?",
    re.IGNORECASE,
)


@dataclass
class Product:
    title: str
    price: str
    link: str


def normalise_profile_url(arg: str) -> str:
    """Accept either a bare username or a full profile URL and return a full URL."""
    arg = arg.strip()
    if arg.startswith("http://") or arg.startswith("https://"):
        parsed = urlparse(arg)
        if "/u/" not in parsed.path:
            raise ValueError(
                f"URL does not look like a Carousell user profile: {arg}"
            )
        return arg.rstrip("/") + "/"
    return f"{BASE}/u/{arg.strip('/')}/"


def _count_listings(page: Page) -> int:
    """Count unique listing anchors currently rendered on the page."""
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


def _click_load_more(page: Page) -> bool:
    """
    Try to click a 'Show more listings' / 'Load more' button if one exists.
    Returns True if a button was clicked.
    """
    clicked = page.evaluate(
        """
        () => {
            const needles = [
                'show more', 'load more', 'see more',
                'lihat lainnya', 'lihat lebih banyak', 'muat lebih banyak',
                'tampilkan lebih', 'mais',
            ];
            const els = Array.from(document.querySelectorAll('button, a, div[role="button"]'));
            for (const el of els) {
                const t = (el.innerText || '').trim().toLowerCase();
                if (!t) continue;
                if (needles.some(n => t.includes(n))) {
                    // Make sure it's visible on screen.
                    el.scrollIntoView({ block: 'center' });
                    el.click();
                    return true;
                }
            }
            return false;
        }
        """
    )
    return bool(clicked)


def auto_scroll(
    page: Page,
    pause_ms: int = 2000,
    max_rounds: int = 400,
    stable_limit: int = 6,
) -> None:
    """
    Keep scrolling (and clicking 'show more' buttons) until the count of
    product listings stops growing for `stable_limit` rounds in a row.

    We key off the number of listing anchors rather than page height,
    because some Carousell layouts render a fixed-height container whose
    scrollHeight barely changes as items stream in.
    """
    last_count = -1
    stable_rounds = 0

    for i in range(max_rounds):
        # Nudge up a bit then jump to the bottom. The small upward scroll
        # seems to help re-trigger Carousell's intersection-observer.
        page.evaluate("() => window.scrollBy(0, -200)")
        page.wait_for_timeout(150)
        page.evaluate("() => window.scrollTo(0, document.body.scrollHeight)")

        # Try pressing End too, in case focus is inside a scrollable child.
        try:
            page.keyboard.press("End")
        except Exception:
            pass

        page.wait_for_timeout(pause_ms)

        # If there's a 'Show more listings' button, click it.
        if _click_load_more(page):
            page.wait_for_timeout(pause_ms)

        count = _count_listings(page)
        if count != last_count:
            print(f"[+] Loaded {count} listings so far...", file=sys.stderr)

        if count == last_count:
            stable_rounds += 1
            if stable_rounds >= stable_limit:
                break
        else:
            stable_rounds = 0
            last_count = count

    page.evaluate("() => window.scrollTo(0, 0)")


def extract_products(page: Page) -> List[Product]:
    """Find every product card and extract title, price, link."""
    raw = page.evaluate(
        """
        () => {
            const seen = new Map();
            const anchors = document.querySelectorAll('a[href*="/p/"]');
            anchors.forEach(a => {
                const href = a.getAttribute('href') || '';
                if (!/\\/p\\/[^/]+-\\d+/.test(href)) return;

                const text = (a.innerText || '').trim();
                if (!text) return;

                // Walk up to find the whole product card so we can also see
                // Sold/Reserved overlays and the price (which may be a sibling
                // of the anchor, not inside it).
                let card = a;
                for (let i = 0; i < 5 && card.parentElement; i++) {
                    card = card.parentElement;
                }
                const cardText = (card.innerText || '').trim();

                const prev = seen.get(href);
                const candidate = { text, cardText };
                if (!prev || cardText.length > prev.cardText.length) {
                    seen.set(href, candidate);
                }
            });
            return Array.from(seen, ([href, v]) => ({
                href, text: v.text, cardText: v.cardText,
            }));
        }
        """
    )

    products: List[Product] = []
    seen_links: Set[str] = set()
    skipped_sold = 0

    # Status words (English + Indonesian) that mean NOT available.
    UNAVAILABLE_STATUSES = (
        "sold", "reserved", "pending",
        "terjual", "dipesan",  # Indonesian
    )
    BOILERPLATE = {
        "protection", "carousell protection", "mailing", "meetup",
        "bumped", "boosted", "promoted", "featured",
    }

    for item in raw:
        href: str = item["href"]
        text: str = item["text"]
        card_text: str = item.get("cardText", "") or text

        link = href if href.startswith("http") else f"{BASE}{href}"
        if link in seen_links:
            continue
        seen_links.add(link)

        lowered = card_text.lower()
        if any(
            re.search(rf"\b{re.escape(status)}\b", lowered)
            for status in UNAVAILABLE_STATUSES
        ):
            skipped_sold += 1
            continue

        # Pull price out of the whole card text (more reliable than anchor text;
        # on many Carousell layouts the price sits outside the anchor).
        price = ""
        price_match = PRICE_RE.search(card_text)
        if price_match:
            price = price_match.group(0).strip()

        # Title: first non-price, non-boilerplate line from the anchor text.
        title = ""
        for ln in (l.strip() for l in text.splitlines() if l.strip()):
            if PRICE_RE.search(ln):
                continue
            if ln.lower() in BOILERPLATE:
                continue
            title = ln
            break

        if not title:
            # Fall back to first non-price line in the full card text.
            for ln in (l.strip() for l in card_text.splitlines() if l.strip()):
                if PRICE_RE.search(ln) or ln.lower() in BOILERPLATE:
                    continue
                title = ln
                break

        if not title and not price:
            continue

        products.append(Product(title=title, price=price, link=link))

    if skipped_sold:
        print(
            f"[i] Skipped {skipped_sold} sold/reserved listing(s).",
            file=sys.stderr,
        )
    return products


def scrape_profile(profile_url: str, headless: bool = True) -> List[Product]:
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=headless)
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1366, "height": 900},
            locale="en-US",
        )
        page = context.new_page()
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

        print("[+] Scrolling to load all listings...", file=sys.stderr)
        auto_scroll(page)

        print("[+] Extracting products...", file=sys.stderr)
        products = extract_products(page)

        browser.close()
        return products


def write_csv(products: List[Product], path: str) -> None:
    """
    Write a UTF-8 CSV with a BOM so that Excel (on any locale) opens it
    with the correct encoding and separates it into real columns.
    """
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=["title", "price", "link"])
        writer.writeheader()
        for p in products:
            writer.writerow(asdict(p))


def write_xlsx(products: List[Product], path: str) -> bool:
    """
    Write a proper .xlsx spreadsheet with each field in its own column.
    Returns False (and prints a hint) if openpyxl isn't installed.
    """
    try:
        from openpyxl import Workbook
        from openpyxl.styles import Font
    except ImportError:
        print(
            "[i] Skipping .xlsx output (install openpyxl to enable: "
            "'pip install openpyxl').",
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

    # Reasonable default column widths.
    ws.column_dimensions["A"].width = 60
    ws.column_dimensions["B"].width = 18
    ws.column_dimensions["C"].width = 80

    wb.save(path)
    return True


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Scrape a Carousell user's profile and export listings to CSV + XLSX.",
    )
    parser.add_argument(
        "profile",
        help="Carousell username or full profile URL (https://www.carousell.com/u/<username>/).",
    )
    parser.add_argument(
        "-o", "--output",
        default=None,
        help="Output file path. Defaults to '<username>_products.csv'. "
             "An .xlsx file with the same stem is also written.",
    )
    parser.add_argument(
        "--headful",
        action="store_true",
        help="Run the browser in a visible window (useful for debugging).",
    )
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
