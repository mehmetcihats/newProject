"""
Carousell user-profile scraper.

Scrapes every product listing from a Carousell user's profile page
(title, price, link) and writes the results to a CSV file.

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
import re
import sys
import time
from dataclasses import dataclass, asdict
from typing import List, Set
from urllib.parse import urlparse

from playwright.sync_api import Page, TimeoutError as PWTimeout, sync_playwright


BASE = "https://www.carousell.com"

# Any string that starts with a currency symbol / code followed by a number.
# Covers S$, $, RM, HK$, US$, A$, NT$, ₱, ₩, ¥, €, £, IDR, SGD, MYR, PHP, etc.
PRICE_RE = re.compile(
    r"(?:S\$|HK\$|US\$|A\$|NT\$|RM|IDR|SGD|MYR|PHP|HKD|USD|TWD|AUD|\$|₱|₩|¥|€|£)\s?"
    r"[\d.,]+(?:\s?(?:k|K|million|M))?"
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
        # Ensure it points at /u/<username>/
        parsed = urlparse(arg)
        if "/u/" not in parsed.path:
            raise ValueError(
                f"URL does not look like a Carousell user profile: {arg}"
            )
        return arg.rstrip("/") + "/"
    # bare username
    return f"{BASE}/u/{arg.strip('/')}/"


def auto_scroll(page: Page, pause_ms: int = 1500, max_rounds: int = 200) -> None:
    """
    Scroll to the bottom of the page repeatedly until the page height
    stops growing (no more listings are being lazily loaded).
    """
    last_height = 0
    stable_rounds = 0
    for i in range(max_rounds):
        height = page.evaluate("() => document.body.scrollHeight")
        page.evaluate("() => window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(pause_ms)

        if height == last_height:
            stable_rounds += 1
            # Two stable rounds in a row = we've reached the end.
            if stable_rounds >= 2:
                break
        else:
            stable_rounds = 0
            last_height = height
    # Scroll back to top so lazy images finish rendering in the DOM (harmless).
    page.evaluate("() => window.scrollTo(0, 0)")


def extract_products(page: Page) -> List[Product]:
    """
    Find every product listing card on the page and pull out title, price, link.

    Strategy: product cards are <a> tags whose href contains '/p/<slug>-<id>'.
    Within each card we look for lines of text; the line containing a currency
    token is the price, and the first meaningful non-price line is the title.
    """
    # Grab a de-duplicated list of (href, innerText) tuples from the DOM.
    # We walk up from each listing anchor to its card container so we can also
    # detect "Sold" / "Reserved" badges, which are sibling elements rather
    # than part of the anchor's own innerText.
    raw = page.evaluate(
        """
        () => {
            const seen = new Map();
            const anchors = document.querySelectorAll('a[href*="/p/"]');
            anchors.forEach(a => {
                const href = a.getAttribute('href') || '';
                // Only listing-detail URLs look like /p/<slug>-<digits>
                if (!/\\/p\\/[^/]+-\\d+/.test(href)) return;

                const text = (a.innerText || '').trim();
                if (!text) return;

                // Walk up a few levels to find the whole product card so we
                // can inspect its full text for Sold/Reserved overlays.
                let card = a;
                for (let i = 0; i < 4 && card.parentElement; i++) {
                    card = card.parentElement;
                }
                const cardText = (card.innerText || '').trim();

                const prev = seen.get(href);
                const candidate = { text, cardText };
                if (!prev || text.length > prev.text.length) {
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

    # Status words that mean a listing is NOT available for sale.
    UNAVAILABLE_STATUSES = ("sold", "reserved", "pending")

    for item in raw:
        href: str = item["href"]
        text: str = item["text"]
        card_text: str = item.get("cardText", "") or text

        link = href if href.startswith("http") else f"{BASE}{href}"
        if link in seen_links:
            continue
        seen_links.add(link)

        # Skip any listing whose card has a Sold / Reserved / Pending badge.
        # We check word-boundaries so we don't filter titles that legitimately
        # contain the substring (e.g. "Solid oak table").
        lowered = card_text.lower()
        if any(
            re.search(rf"\b{status}\b", lowered) for status in UNAVAILABLE_STATUSES
        ):
            skipped_sold += 1
            continue

        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
        price = ""
        title = ""
        for ln in lines:
            m = PRICE_RE.search(ln)
            if m and not price:
                price = m.group(0).strip()
            elif not title and not PRICE_RE.search(ln):
                # First non-price line is the title.
                # Skip obvious non-title boilerplate.
                if ln.lower() in {"protection", "carousell protection", "mailing", "meetup", "bumped"}:
                    continue
                title = ln

        if not title and lines:
            title = lines[0]

        # Skip rows that are clearly not products (e.g. empty text, only price).
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

        # Give the SPA a moment to hydrate and render the first batch.
        try:
            page.wait_for_selector('a[href*="/p/"]', timeout=20_000)
        except PWTimeout:
            print(
                "[!] No product links appeared within 20s. "
                "The profile may be empty, private, or Carousell is blocking access.",
                file=sys.stderr,
            )

        print("[+] Scrolling to load all listings...", file=sys.stderr)
        auto_scroll(page)

        print("[+] Extracting products...", file=sys.stderr)
        products = extract_products(page)

        browser.close()
        return products


def write_csv(products: List[Product], path: str) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["title", "price", "link"])
        writer.writeheader()
        for p in products:
            writer.writerow(asdict(p))


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Scrape a Carousell user's profile and export product listings to CSV.",
    )
    parser.add_argument(
        "profile",
        help="Carousell username or full profile URL (https://www.carousell.com/u/<username>/).",
    )
    parser.add_argument(
        "-o", "--output",
        default=None,
        help="Output CSV file path. Defaults to '<username>_products.csv'.",
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
    output = args.output or f"{username}_products.csv"

    start = time.time()
    products = scrape_profile(url, headless=not args.headful)
    elapsed = time.time() - start

    write_csv(products, output)
    print(
        f"[+] Done. Scraped {len(products)} product(s) in {elapsed:.1f}s -> {output}",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
