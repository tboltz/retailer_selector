# retailer_selector
Retail Selector Scraper

The Retail Selector Scraper is an asynchronous Python system for scanning targeted retailer product pages, extracting pricing and in-stock status, and updating a centralized Excel workbook (Retail Arbitrage Targeting List).
It provides a daily snapshot of market conditions across dozens of online stores and supports both static HTML scraping and JS-rendered pages (for retailers with dynamic pricing displays).

Features

🔄 Asynchronous Scraping using aiohttp

🔁 Retry logic for transient HTTP failures (429, 5xx)

⚡ Fast static scraping (render_js=false) plus
🧩 Optional JS rendering (render_js=true)

📄 Uniform normalized output for every fetch:

{
  "status_code",
  "final_url",
  "page_text",
  "error",
  "response_ms",
  "request_url",
  "attempts",
  "last_exception_type"
}


🧹 Designed for batch operations across dozens or hundreds of URLs

🧬 Clean integration with your orchestrator + Excel pipeline

🛡️ Secrets protected via .gitignore

Repository Structure
retailer_selector/
│
├── scraping.py                <-- ScrapingBee wrapper (async, retries, JS toggle)
├── orchestrator.py            <-- Pipeline: load → scrape → parse → write
├── parser.py                  <-- Regex / HTML / AI price parsing logic
├── workbook.py                <-- Excel I/O utilities
├── retailers/                 <-- Retailer-specific hints / patterns
├── secrets.template.json      <-- Example secrets (safe to commit)
├── requirements.txt           <-- Python dependencies
└── README.md                  <-- This file

Installation
1. Clone the repository
git clone https://github.com/tboltz/retailer_selector.git
cd retailer_selector

2. Install dependencies
pip install -r requirements.txt

Secrets Setup

Create a file called secrets.json in the project root:

{
  "SCRAPINGBEE_API_KEY": "your-key-here",
  "OPENAI_API_KEY": "your-key-here"
}


This file must not be committed.
.gitignore already protects it.

If it isn’t ignored:

echo secrets.json >> .gitignore
git add .gitignore
git commit -m "Ensure secrets.json stays ignored"
git push

Scraping Behavior
Fast Mode (default)
extra_params=None


Scrapes static HTML (fast + cheap).
Use for most retailers.

JS Rendered Mode (for dynamic sites)
extra_params={"render_js": "true"}


Use for retailers whose sale prices only appear after JavaScript executes
(example: AlphaOmegaHobby showing $13 sale price only via JS).

Using scrapingbee_fetch_many

Example:

from retail_selector.scraping import scrapingbee_fetch_many

results = await scrapingbee_fetch_many(
    urls=[
        "https://www.alphaomegahobby.com/products/marvel-champions-lcg-ant-man"
    ],
    api_key=SCRAPINGBEE_API_KEY,
    extra_params={"render_js": "true"},   # enable JS for this retailer
    concurrency=10,
)


Each result entry includes:

{
  "status_code": 200,
  "final_url": "...",
  "page_text": "<html>...</html>",
  "error": None,
  "response_ms": 2345.2,
  "request_url": "...",
  "attempts": 1,
  "last_exception_type": None
}

Integrating With the Orchestrator

Your orchestrator likely does something like:

results = await scrapingbee_fetch_many(
    urls=all_urls,
    api_key=api_key,
    concurrency=20,
    max_retries=3
)


To selectively turn on JS rendering:

if retailer_family == "ALPHAOMEGA":
    extra = {"render_js": "true"}
else:
    extra = None

results = await scrapingbee_fetch_many(
    urls=urls_for_this_retailer,
    api_key=api_key,
    extra_params=extra,
)

Error Handling

401/402/403 → Hard ScrapingBee errors (no retries)

429/500/502/503/504 → Retried up to max_retries

Timeouts → Retried

Network errors → Retried

404/410 → Parsed as normal HTML (not errors)

Daily Automation

To run nightly:

Windows Task Scheduler
python C:\path\to\orchestrator.py

Linux / WSL cron
0 3 * * * /usr/bin/python3 /home/user/retailer_selector/orchestrator.py

COMMANDS YOU WILL DEFINITELY NEED AGAIN

(so you don’t have to fight Git like you did today)

✔ Check repo status
git status

✔ Move secrets back into the repo safely
Move-Item C:\Users\suzan\Projects\card\secrets.json.backup `
          C:\Users\suzan\Projects\card\retailer_selector\secrets.json

✔ Ensure secrets.json is ignored
echo secrets.json >> .gitignore
git add .gitignore
git commit -m "Ignore secrets.json"
git push

✔ Restore old file from a previous commit (VERY important)
git checkout <commit-hash> -- scraping.py


Example:

git checkout 394d8b1 -- scraping.py

✔ Force enable JS rendering when scraping
extra_params={"render_js": "true"}

✔ Run the scraper manually
python orchestrator.py

✔ Run JUST the scraper module (debug)
python -m retailer_selector.scraping

✔ Push your changes after making edits
git add retail_selector/scraping.py
git commit -m "Update scraping logic"
git push

✔ If Git ever traps you in a rebase again (your escape rope)
git rebase --abort


If you want, I can also add a CONTRIBUTING.md, an architecture diagram, or an operator’s manual for the scraping pipeline.
✔ Run the orchestrator for only 1 URL (development mode)

If you added your dev flag:

python orchestrator.py --limit 1


Or:

python orchestrator.py -l 1

✔ Run with all three: concurrency, limit, JS
python orchestrator.py --concurrency 3 --limit 10 --render-js true

✔ If you want concurrency directly in Python (not CLI):
results = await scrapingbee_fetch_many(
    urls=my_urls,
    api_key=api_key,
    concurrency=3,
)


That’s the Python equivalent of:

python orchestrator.py --concurrency 3


If your orchestrator does not currently support flags, tell me and I’ll wire up argparse so you can run:

python orchestrator.py --concurrency 3 --retailer alpha


without hacking the source every time.

Use this command cd C:\Users\suzan\Projects\card
python -m retailer_selector.orchestrator --concurrency 3
