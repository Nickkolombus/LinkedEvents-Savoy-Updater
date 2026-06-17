# Linked Events Savoy Updater

Fetches Savoy Teatteri events from the City of Helsinki Linked Events API and keeps the workbook `2026 Savoy-Tapahtumat Automation.xlsx` in sync (sheet `Savoy Tapahtumat 2026`).

## Setup

1. Install dependencies: `pip install -r requirements.txt`
2. Make sure `2026 Savoy-Tapahtumat Automation.xlsx` is in the project root.

## Usage

- One-time sync for 2026 (default year): `python main.py`
- Sync a specific year: `python main.py --year 2025`
 - Keep syncing every N minutes: `python main.py --watch --interval-minutes 60`

What the sync does:
- Adds new Savoy events and refreshes existing ones by `event_dt_iso` (or title+date).
- Only writes API-backed fields (title, Finnish date/time string, weekday/date, start time, end time/duration if provided, event type from keywords, description, first image URL, event_dt_iso). Manual columns remain untouched.
- Marks rows as cancelled by applying strikethrough formatting when: (a) the API marks the event as cancelled, or (b) a future-row key (`event_dt_iso`) no longer appears in the API response. Strikethrough is cleared if the event returns.
- Reports added / changed / unchanged / cancelled counts after each run.