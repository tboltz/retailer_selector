# Retailer Selector

A Python-based retail arbitrage automation tool that monitors product availability and pricing across multiple retailer websites. The system downloads product data from Google Sheets, scrapes retailer websites asynchronously, analyzes pricing and stock data using AI, updates the tracking spreadsheet, and emails the results.

## Features

- **Asynchronous Web Scraping**: Concurrent scraping of multiple retailer websites using ScrapingBee API
- **AI-Powered Parsing**: Intelligent price and stock extraction using OpenAI's GPT models
- **Google Sheets Integration**: Seamlessly syncs with Google Sheets for product tracking
- **Multi-Retailer Support**: Handles various retailer platforms including Shopify, Amazon, Walmart, Target, and more
- **Email Notifications**: Automatically emails updated workbooks with the latest scan results
- **Robust Error Handling**: Comprehensive error handling for network issues, parsing failures, and API limits
- **Customizable Concurrency**: Configurable parallel request limits to optimize performance

## Requirements

- Python 3.8+
- Google Cloud Service Account with Sheets and Drive API access
- ScrapingBee API key
- OpenAI API key
- SMTP server credentials for email notifications

## Installation

1. Clone the repository:
```bash
git clone https://github.com/tboltz/retailer_selector.git
cd retailer_selector
```

2. Install required dependencies:
```bash
pip install pandas numpy openpyxl gspread google-api-python-client google-auth aiohttp beautifulsoup4 openai
```

Required Python packages:
- `pandas` - Data manipulation and Excel file handling
- `numpy` - Numerical operations
- `openpyxl` - Excel file writing
- `gspread` - Google Sheets API
- `google-api-python-client` - Google Drive API
- `google-auth` - Google authentication
- `aiohttp` - Async HTTP client
- `beautifulsoup4` - HTML parsing
- `openai` - OpenAI API client

## Configuration

### 1. Google Sheets Setup

The system requires access to two Google Sheets:
- **Master Sheet**: Source of truth containing product and retailer data
- **Output Sheet**: Destination for updated Product↔Retailer Map

Update `config.py` with your sheet IDs:
```python
MASTER_SHEET_ID = "your-master-sheet-id"
OUTPUT_SHEET_ID = "your-output-sheet-id"
```

Your Google Sheet should have the following tabs:
- `Product↔Retailer Map` - Main tracking sheet with product URLs and retailer info
- `Active Watch List` - Products to actively monitor
- `Retailers` - Retailer configuration data

### 2. Service Account Setup

1. Create a Google Cloud Project
2. Enable Google Sheets API and Google Drive API
3. Create a Service Account and download the JSON key file
4. Update the `SERVICE_ACCOUNT_FILE` path in `config.py`:
```python
SERVICE_ACCOUNT_FILE = Path("path/to/your/service-account.json")
```
5. Share your Google Sheets with the service account email address (found in the JSON file)

### 3. Secrets Configuration

Create a `secrets.json` file with your API keys and credentials:

```json
{
  "SCRAPINGBEE_API_KEY": "your-scrapingbee-api-key",
  "OPENAI_API_KEY": "your-openai-api-key",
  "OPENAI_MODEL": "gpt-4o-mini",
  "SMTP_SERVER": "smtp.gmail.com",
  "SMTP_PORT": "587",
  "SMTP_USERNAME": "your-email@gmail.com",
  "SMTP_PASSWORD": "your-app-password",
  "EMAIL_FROM": "your-email@gmail.com",
  "EMAIL_TO": "recipient@example.com"
}
```

Update the `DEFAULT_SECRETS_PATH` in `config.py` to point to your secrets file.

### 4. Workbook Path

Update the `DEFAULT_WORKBOOK_PATH` in `config.py` to specify where the Excel workbook should be saved:
```python
DEFAULT_WORKBOOK_PATH = Path("path/to/your/workbook.xlsx")
```

## Usage

### Command Line

Run the scanner from the command line:

```bash
python -m orchestrator
```

### Command Line Options

```bash
python -m orchestrator [OPTIONS]

Options:
  --workbook-path PATH    Path to local XLSX workbook to overwrite
                         (default: configured in config.py)
  
  --secrets-path PATH     Path to secrets.json file
                         (default: configured in config.py)
  
  --limit INTEGER        Optional max number of Product↔Retailer rows to scan
                         (default: scan all rows)
  
  --concurrency INTEGER  Max concurrent ScrapingBee requests
                         (default: 20)
```

### Example Commands

Scan all products with default settings:
```bash
python -m orchestrator
```

Scan only the first 10 products:
```bash
python -m orchestrator --limit 10
```

Use custom paths and concurrency:
```bash
python -m orchestrator --workbook-path ./data/products.xlsx --secrets-path ./config/secrets.json --concurrency 30
```

### Programmatic Usage

You can also use the scanner programmatically in your Python code:

```python
import asyncio
from pathlib import Path
from orchestrator import run_scan_from_gsheet_and_email

async def main():
    result = await run_scan_from_gsheet_and_email(
        workbook_path=Path("./products.xlsx"),
        secrets_path=Path("./secrets.json"),
        limit=10,
        concurrency=20
    )
    print(f"Scan complete! Results: {result}")

if __name__ == "__main__":
    asyncio.run(main())
```

## How It Works

The retailer scanner follows this workflow:

1. **Load Configuration**: Loads API keys and configuration from `secrets.json`

2. **Download Product Map**: Fetches the Product↔Retailer Map from Google Sheets for logging

3. **Export Google Sheet**: Downloads the full master Google Sheet as an XLSX file

4. **Async Scanning**: 
   - Reads product URLs from the workbook
   - Scrapes retailer websites concurrently using ScrapingBee
   - Parses HTML/JSON to extract price and stock information
   - Uses OpenAI for intelligent parsing when needed
   - Updates the workbook with scan results

5. **Sync to Google Sheets**: Uploads the updated Product↔Retailer Map back to Google Sheets

6. **Email Results**: Sends the updated workbook via email

## Project Structure

```
retailer_selector/
├── __init__.py           # Package initialization
├── config.py             # Configuration and secrets management
├── orchestrator.py       # Main entry point and workflow orchestration
├── scraping.py          # ScrapingBee API integration for async web scraping
├── parsing.py           # HTML/JSON parsing and AI-powered extraction
├── workbook.py          # Excel workbook operations and data management
├── gsheet.py            # Google Sheets API integration
├── emailer.py           # Email notification functionality
├── secrets.json         # API keys and credentials (not in repo)
└── README.md            # This file
```

### Module Descriptions

- **config.py**: Manages configuration settings, loads secrets, initializes OpenAI client
- **orchestrator.py**: Main workflow coordinator, CLI argument parsing, and async pipeline
- **scraping.py**: Handles async HTTP requests to ScrapingBee API with rate limiting
- **parsing.py**: Extracts price and stock data from scraped HTML using pattern matching and AI
- **workbook.py**: Loads, processes, and saves Excel workbooks with product data
- **gsheet.py**: Interfaces with Google Sheets API for reading/writing data
- **emailer.py**: Sends email notifications with workbook attachments via SMTP

## Parsing Methods

The scanner uses multiple parsing strategies to extract price and stock information:

1. **Shopify JSON**: Direct extraction from Shopify variant JSON
2. **Structured Data**: Parsing JSON-LD and schema.org markup
3. **Meta Tags**: Extraction from OpenGraph and product meta tags
4. **AI Fallback**: GPT-powered extraction when other methods fail

## API Credits

The scanner uses paid APIs:
- **ScrapingBee**: Charged per API request (web scraping)
- **OpenAI**: Charged per token (AI-powered parsing)

Monitor your usage to avoid unexpected costs. Use the `--limit` option for testing.

## Troubleshooting

### Common Issues

**Google Sheets Access Denied**
- Ensure the service account email has been granted access to your Google Sheets
- Verify the service account JSON file path is correct
- Check that Google Sheets API and Drive API are enabled in your Google Cloud project

**ScrapingBee Rate Limiting**
- Reduce the `--concurrency` parameter
- Check your ScrapingBee API credit balance
- Some plans have rate limits (e.g., 100 requests/second)

**Email Sending Fails**
- For Gmail, use an App Password instead of your regular password
- Verify SMTP server and port settings
- Check that "Less secure app access" is enabled (if applicable)

**OpenAI API Errors**
- Verify your API key is valid
- Check your OpenAI account has available credits
- Ensure the model name in secrets.json is correct

### Debug Mode

Enable verbose logging by modifying the print statements in the code or redirect output:
```bash
python -m orchestrator --limit 5 2>&1 | tee scan.log
```

## Security Notes

- **Never commit `secrets.json`** or service account JSON files to version control
- Store sensitive files outside the repository or use environment variables
- Use `.gitignore` to exclude sensitive files:
  ```
  secrets.json
  *-service-account.json
  *.xlsx
  ```
- Rotate API keys periodically
- Use restricted service accounts with minimal required permissions

## Contributing

Contributions are welcome! Please ensure your code:
- Follows existing code style and conventions
- Includes docstrings for functions and classes
- Handles errors gracefully
- Works with async/await patterns where appropriate

## License

This project is provided as-is for personal use. Please review and comply with the terms of service for ScrapingBee, OpenAI, and Google APIs.

## Support

For issues or questions:
1. Check the troubleshooting section above
2. Review API documentation for ScrapingBee and OpenAI
3. Open an issue in the GitHub repository

## Future Enhancements

Potential improvements:
- Database storage for historical tracking
- Web dashboard for monitoring
- Additional retailer support
- Price drop alerts
- Inventory threshold notifications
- Multi-user support