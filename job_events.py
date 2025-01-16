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


def get_job_events(access_token: str, customer_id: str, env: str, job_id: Optional[str] = None,
                    start_timestamp: Optional[datetime] = None, end_timestamp: Optional[datetime] = None,
                    event_filter: Optional[Callable] = lambda x: True):
    # Construct the URL
    url = f"{BASE_URL}/{customer_id}/events/job"

    # Define query parameters
    params = {"env": env}
    if job_id:
        params["job_id"] = job_id
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
    # A function to filter job events based on event type. In this case, 
    # jobs for which an event with the status "RUNNING" exists,
    # indicating ongoing job execution.
    def running_filter(job):
        return "RUNNING" in [e.get("status") for e in job.get("events")]
    
    # Filter for jobs that have been completed.
    def completed_filter(job):
        return "COMPLETED" in [e.get("status") for e in job.get("events")]

    try:
        access_token = authenticate("some-client-id", "some-client-secret")

        # An example list of jobs that, as a RELEX user, I know have been uploaded to RELEX.
        jobs = ["Scheduled job - Update Product Locations", "Scheduled job - Update Locations", "Scheduled job - Update Products"]

        # Examine events for the past 24 hours
        start_timestamp = datetime.now(timezone.utc) - timedelta(days=1)
        end_timestamp = datetime.now(timezone.utc)
     
        # =====================================================================
        # Example 1: comparing expected and actual jobs
        # =====================================================================
        events = get_job_events(
            access_token=access_token,
            customer_id="some-customer",
            env="prod",
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
            event_filter=running_filter
        )

        print("Expected jobs:", jobs)
        print("Jobs in progress by RELEX:", json.dumps([f.get("name") for f in events], indent=2))


        # =====================================================================
        # Example 2: 
        # Wait until all jobs have been completed, 
        # with a timeout of 1 hour.
        # =====================================================================

        # 12 iterations of 5 minutes each = 1 hour
        backoff = 12  
        while True:
            events = get_job_events(
                access_token=access_token,
                customer_id="some-customer",
                env="prod",
                start_timestamp=start_timestamp,
                end_timestamp=end_timestamp,
                event_filter=completed_filter
            )
            completed_jobs = [f.get("name") for f in events]

            print("Completed jobs:", json.dumps(completed_jobs, indent=2))

            if all(j in completed_jobs for j in jobs):
                print('All jobs have been completed.')
                break
            else:
                if backoff == 0:
                    print("Timeout reached. Not all jobs have been completed.")
                    break
                backoff -= 1
                print("Waiting for jobs to be completed...")
                time.sleep(5*60)  # Wait for five minutes before checking again

    except Exception as e:
        print("An error occurred:", e)
