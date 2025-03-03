import requests
import json
import os
import datetime
import ijson
import gzip
# import your database libraries (e.g., psycopg2) and connection logic here

# ---------------------------
# CONFIGURATION
# ---------------------------
# Set the bulk data type here. Change to "unique_artwork" to use unique artwork data.
BULK_DATA_TYPE = "oracle_cards"  # or "unique_artwork"

# Scryfall bulk data API endpoint
BULK_DATA_URL = "https://api.scryfall.com/bulk-data"

# ---------------------------
# FETCHING BULK DATA LIST
# ---------------------------
def fetch_bulk_data_list():
    response = requests.get(BULK_DATA_URL)
    if response.status_code == 200:
        return response.json()['data']
    else:
        raise Exception(f"Failed to fetch bulk data list: HTTP {response.status_code}")

# ---------------------------
# SELECT THE DESIRED BULK DATA ENTRY
# ---------------------------
def get_bulk_data_entry(bulk_data_list, data_type):
    for entry in bulk_data_list:
        if entry['type'] == data_type:
            return entry
    raise Exception(f"Bulk data of type '{data_type}' not found")

# ---------------------------
# DOWNLOAD THE BULK DATA FILE
# ---------------------------
def download_bulk_data(entry, local_filename):
    download_uri = entry['download_uri']
    response = requests.get(download_uri, stream=True)
    if response.status_code == 200:
        with open(local_filename, 'wb') as f:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
    else:
        raise Exception(f"Failed to download bulk data: HTTP {response.status_code}")
    
    # Update file modification time to match the updated_at timestamp from Scryfall
    updated_at = datetime.datetime.strptime(entry['updated_at'], "%Y-%m-%dT%H:%M:%S.%f%z")
    mod_time = updated_at.timestamp()
    os.utime(local_filename, (mod_time, mod_time))

# ---------------------------
# PROCESS THE BULK DATA FILE AND UPSERT INTO THE DATABASE
# ---------------------------
def process_bulk_data(local_filename):
    # Open and decompress the file
    with gzip.open(local_filename, 'rb') as f:
        # Use ijson to stream parse each card object in the file
        objects = ijson.items(f, 'item')
        for card in objects:
            # Here you would map the card data to your database schema
            # and perform an upsert into PostgreSQL.
            # Example (pseudo-code):
            #
            # upsert_card(card)
            #
            # For instance:
            #   cursor.execute("""
            #       INSERT INTO cards (oracle_id, name, image_uris, ...)
            #       VALUES (%s, %s, %s, ...)
            #       ON CONFLICT (oracle_id) DO UPDATE SET
            #       name = EXCLUDED.name,
            #       image_uris = EXCLUDED.image_uris,
            #       ...
            #   """, (card['oracle_id'], card.get('name'), json.dumps(card.get('image_uris')), ...))
            pass

# ---------------------------
# MAIN FUNCTION
# ---------------------------
def main():
    bulk_data_list = fetch_bulk_data_list()
    entry = get_bulk_data_entry(bulk_data_list, BULK_DATA_TYPE)
    
    local_filename = f"{BULK_DATA_TYPE}.json.gz"
    download_required = True
    
    # Check if a local file exists and whether it is up-to-date
    if os.path.exists(local_filename):
        file_mod_time = datetime.datetime.fromtimestamp(os.path.getmtime(local_filename), tz=datetime.timezone.utc)
        entry_updated_time = datetime.datetime.strptime(entry['updated_at'], "%Y-%m-%dT%H:%M:%S.%f%z")
        if file_mod_time >= entry_updated_time:
            download_required = False
    
    if download_required:
        print(f"Downloading {BULK_DATA_TYPE} bulk data...")
        download_bulk_data(entry, local_filename)
    else:
        print("Local bulk data is up-to-date.")
    
    # Process the downloaded data file (perform upsert into your PostgreSQL database)
    process_bulk_data(local_filename)

if __name__ == "__main__":
    main()
