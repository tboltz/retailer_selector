# retail_selector/__init__.py
"""
Retail Selector: async retail arbitrage scanner.

Download Google Sheet → XLSX → scan prices with ScrapingBee + OpenAI →
sync Product↔Retailer Map back to Google Sheets → email XLSX snapshot.
"""

__all__ = [
    "config",
    "gsheet",
    "scraping",
    "parsing",
    "workbook",
    "emailer",
    "orchestrator",
]
