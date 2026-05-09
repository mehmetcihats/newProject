# Carousell Profile Scraper

A small Python bot that scrapes every product listing on a Carousell user's profile
(title, price, link) and saves them to a CSV file.

It uses [Playwright](https://playwright.dev/python/) because Carousell renders
listings with JavaScript and loads more items via infinite scroll.

## Install

```bash
# 1. Create a venv (recommended)
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate

# 2. Install deps
pip install -r requirements.txt

# 3. Install the Chromium browser Playwright drives
python -m playwright install chromium
```

## Usage

```bash
# By username
python carousell_scraper.py johndoe

# By full URL
python carousell_scraper.py https://www.carousell.com/u/johndoe/

# Custom output file
python carousell_scraper.py johndoe -o john.csv

# Watch it work in a real browser window
python carousell_scraper.py johndoe --headful
```

Output CSV columns:

| title | price | link |
|-------|-------|------|
| iPhone 13 Pro 256GB | S$850 | https://www.carousell.com/p/iphone-13-pro-... |

## Notes

- If the profile is empty, private, or Carousell blocks the request, the CSV will be empty.
- Carousell occasionally changes its DOM; if extraction stops working, run with
  `--headful` to watch the page and adjust the selector logic in `extract_products`.
- This scraper is for personal/educational use. Respect Carousell's Terms of Service
  and `robots.txt`, and don't hammer their servers.
