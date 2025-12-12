import requests
import openpyxl
from datetime import datetime, timedelta
import schedule
import time

API_BASE = "https://api.hel.fi/linkedevents/v1/"
EXCEL_FILE = "savoy_events.xlsx"

def get_place_id(name):
    response = requests.get(f"{API_BASE}search/", params={"q": name, "type": "place"})
    if response.status_code == 200:
        data = response.json()
        for item in data.get("data", []):
            if item.get("resource_type") == "place" and "Savoy" in str(item.get("name")):
                return item["id"]
    return None

def get_events(location_id):
    events = []
    url = f"{API_BASE}event/"
    params = {
        "location": location_id,
        "start": datetime.now().isoformat(),
        "sort": "start_time"
    }
    while url:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            events.extend(data.get("data", []))
            url = data.get("next")
            params = {}  # params only for first request
        else:
            break
    return events

def update_excel(events):
    try:
        wb = openpyxl.load_workbook(EXCEL_FILE)
        ws = wb.active
    except FileNotFoundError:
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.append(["Event Name", "Start Time", "End Time", "Description"])

    # Clear existing data except header
    for row in ws.iter_rows(min_row=2, max_row=ws.max_row):
        for cell in row:
            cell.value = None

    for event in events:
        name = event.get("name", {}).get("fi", "") or event.get("name", {}).get("en", "")
        start = event.get("start_time")
        end = event.get("end_time")
        desc = event.get("description", {}).get("fi", "") or event.get("description", {}).get("en", "")
        ws.append([name, start, end, desc])

    wb.save(EXCEL_FILE)
    print(f"Updated {EXCEL_FILE} with {len(events)} events")

def main():
    place_id = get_place_id("Savoy Teatteri")
    if not place_id:
        print("Savoy Teatteri place not found")
        return
    events = get_events(place_id)
    update_excel(events)

if __name__ == "__main__":
    main()
    # For real-time, uncomment below
    # schedule.every(1).hours.do(main)
    # while True:
    #     schedule.run_pending()
    #     time.sleep(1)