# NEXUS Auto-Update Scripts

Scans test repos for assertions and updates Google Sheet with formatted data.

## Files
- `nexus_scan.py` — Scanner functions (assertion extraction + deep page object tracing)
- `rebuild_sheet.py` — Main entry point (scan + write data+format atomically)
- `github-action-web.yml` — GitHub Action for Web test repo
- `github-action-mobile.yml` — GitHub Action for Mobile test repo
- `requirements.txt` — Python dependencies

## Usage
```bash
# Local run (scans both repos)
python rebuild_sheet.py

# Web only
python rebuild_sheet.py --web-only

# Mobile only
python rebuild_sheet.py --mobile-only
```

## Required Secrets (GitHub Actions)
- `GOOGLE_SHEETS_CREDS_JSON` — Service account JSON key
- `NEXUS_SHEET_ID` — Google Sheet ID

