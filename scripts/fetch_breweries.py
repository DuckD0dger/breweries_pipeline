import os
import requests
import json

def fetch_all_breweries():
    all_breweries = []
    page = 1
    per_page = 50

    while True:
        url = f'https://api.openbrewerydb.org/v1/breweries?page={page}&per_page={per_page}'
        response = requests.get(url)
        response.raise_for_status()

        breweries = response.json()
        if not breweries:
            break

        all_breweries.extend(breweries)
        print(f"Fetched page {page} with {len(breweries)} breweries")
        page += 1

    os.makedirs('/opt/airflow/data/bronze', exist_ok=True)
    with open('/opt/airflow/data/bronze/breweries_raw.json', 'w') as f:
        json.dump(all_breweries, f)
