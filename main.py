import requests
import json
from datetime import datetime, timedelta, timezone
import time
from typing import Optional, Callable

# URLs for the API itself and the authentication endpoint.
# Replace "us" with "eu" if your environment is in EMEA.
BASE_URL = "https://us.monitor.relexsolutions.com/api/v1"
AUTH_URL = "https://identity.prod-us.prod.cc.relexsolutions.com/monitoring_api_prod/connect/token"


def authenticate(client_id: str, client_secret: str):
    payload = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret
    }

    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }
  
    response = requests.request("POST", AUTH_URL, data=payload, headers=headers)

    
    if response.status_code == 200:
        return response.json().get("access_token")
    else:
        response.raise_for_status()


def get_file_events(access_token: str, customer_id: str, env: str, file_name: Optional[str] = None,
                    start_timestamp: Optional[datetime] = None, end_timestamp: Optional[datetime] = None,
                    event_filter: Optional[Callable] = lambda x: True):
    # Construct the URL
    url = f"{BASE_URL}/{customer_id}/events/file"

    # Define query parameters
    params = {"env": env}
    if file_name:
        params["file_name"] = file_name
    if start_timestamp:
        params["start_timestamp"] = start_timestamp.isoformat()
    if end_timestamp:
        params["end_timestamp"] = end_timestamp.isoformat()
    
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    # Make the GET request
    response = requests.get(url, headers=headers, params=params)
    
    # Check for successful response
    if response.status_code == 200:
        return [f for f in response.json().get("data") if event_filter(f)]
    else:
        response.raise_for_status()


if __name__ == "__main__":
    # A function to filter files events based on event type. In this case, 
    # files for which an event with the title "File received by RELEX" exists,
    # indicating a successful upload to RELEX.
    def upload_filter(file):
        return "File received by RELEX" in [e.get("title") for e in file.get("events")]
    
    # Filter for files that have been fully processed.
    def processed_filter(file):
        return "File processing finished by RELEX" in [e.get("title") for e in file.get("events")]

    try:
        access_token = authenticate("some-client-id", "some-client-secret")

        # An example list of files that, as a RELEX user, I know have been uploaded to RELEX.
        files = ["LocationMasterData_2024-10-30.csv", "SalesData_2024-10-30.csv", "InventoryData_2024-10-30.csv"]

        # Examine events for the past 24 hours
        start_timestamp = datetime.now(timezone.utc) - timedelta(days=1)
        end_timestamp = datetime.now(timezone.utc)
     
        # =====================================================================
        # Example 1: comparing expected and actual uploads
        # =====================================================================
        events = get_file_events(
            access_token=access_token,
            customer_id="some-customer",
            env="prod",
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
            event_filter=upload_filter
        )

        print("Expected files:", files)
        print("Files uploaded to RELEX:", json.dumps([f.get("file") for f in events], indent=2))


        # =====================================================================
        # Example 2: 
        # Wait until all files have been fully processed, 
        # with a timeout of 1 hour.
        # =====================================================================

        # 12 iterations of 5 minutes each = 1 hour
        backoff = 12  
        while True:
            events = get_file_events(
                access_token=access_token,
                customer_id="some-customer",
                env="prod",
                start_timestamp=start_timestamp,
                end_timestamp=end_timestamp,
                event_filter=processed_filter
            )
            processed_files = [f.get("file") for f in events]

            print("Processed files:", json.dumps(processed_files, indent=2))

            if all(f in processed_files for f in files):
                print('All files have been processed.')
                break
            else:
                if backoff == 0:
                    print("Timeout reached. Not all files have been processed.")
                    break
                backoff -= 1
                print("Waiting for files to be processed...")
                time.sleep(5*60)  # Wait for five minutes before checking again

    except Exception as e:
        print("An error occurred:", e)
