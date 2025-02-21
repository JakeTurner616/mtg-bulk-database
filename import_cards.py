#!/usr/bin/env python3
import os
import time
import requests
import psycopg2
import psycopg2.extras
import ijson
from datetime import datetime, timezone
from dotenv import load_dotenv

# Load DB configuration from .env
load_dotenv(dotenv_path=os.path.join(os.getcwd(), "mtg-database", ".env"))
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_NAME = os.getenv("POSTGRES_DB")
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")

# Connect to PostgreSQL
conn = psycopg2.connect(
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT
)
cursor = conn.cursor()

# List of columns exactly matching the init.sql table definition.
# oracle_id is the primary key and is not updated.
columns = [
    "oracle_id",  # primary key
    "id",
    "object",
    "multiverse_ids",
    "mtgo_id",
    "tcgplayer_id",
    "cardmarket_id",
    "name",
    "lang",
    "released_at",
    "uri",
    "scryfall_uri",
    "layout",
    "highres_image",
    "image_status",
    "image_uris",
    "mana_cost",
    "cmc",
    "type_line",
    "oracle_text",
    "power",
    "toughness",
    "colors",
    "color_identity",
    "keywords",
    "all_parts",
    "legalities",
    "games",
    "reserved",
    "game_changer",
    "foil",
    "nonfoil",
    "finishes",
    "oversized",
    "promo",
    "reprint",
    "variation",
    "set_id",
    "set",
    "set_name",
    "set_type",
    "set_uri",
    "set_search_uri",
    "scryfall_set_uri",
    "rulings_uri",
    "prints_search_uri",
    "collector_number",
    "digital",
    "rarity",
    "watermark",
    "flavor_text",
    "card_back_id",
    "artist",
    "artist_ids",
    "illustration_id",
    "border_color",
    "frame",
    "frame_effects",
    "security_stamp",
    "full_art",
    "textless",
    "booster",
    "story_spotlight",
    "edhrec_rank",
    "preview",
    "prices",
    "related_uris",
    "purchase_uris"
]

def parse_date(date_str):
    """Convert an ISO date string to a date object; return None if invalid."""
    if not date_str:
        return None
    try:
        return datetime.fromisoformat(date_str).date()
    except Exception:
        return None

def process_card(card):
    """
    Process a card JSON object to prepare it for database insertion.
    Converts the released_at field and wraps dictionaries/lists in psycopg2.extras.Json for JSONB columns.
    """
    processed = {}
    for col in columns:
        val = card.get(col)
        if col == "released_at":
            processed[col] = parse_date(val)
        elif isinstance(val, (dict, list)):
            processed[col] = psycopg2.extras.Json(val)
        else:
            processed[col] = val
    return processed

def download_latest_json(json_file):
    """
    Checks the Scryfall bulk-data API for the Oracle Cards file.
    If the local file is missing or its modification time is older than the server's updated_at,
    downloads the file from the provided download_uri.
    After a download, the file's modification time is set to the server's updated_at.
    """
    bulk_api = "https://api.scryfall.com/bulk-data"
    print("Querying Scryfall bulk-data API for the latest Oracle Cards JSON URL...")
    resp = requests.get(bulk_api)
    if resp.status_code != 200:
        raise Exception(f"Failed to query bulk-data API: {resp.status_code}")
    bulk_data = resp.json().get("data", [])
    oracle_bulk = next((item for item in bulk_data if item.get("type") == "oracle_cards"), None)
    if not oracle_bulk:
        raise Exception("Oracle Cards bulk data not found")
    
    # Parse the server updated_at timestamp.
    server_updated_at = datetime.fromisoformat(oracle_bulk.get("updated_at").replace("Z", "+00:00"))
    download_uri = oracle_bulk.get("download_uri")
    print(f"Server reports updated_at: {server_updated_at.isoformat()}")
    print(f"Download URI: {download_uri}")

    # Check if local file exists and is up-to-date.
    if os.path.exists(json_file):
        local_mtime = datetime.fromtimestamp(os.path.getmtime(json_file), tz=timezone.utc)
        print(f"Local file modification time: {local_mtime.isoformat()}")
        if local_mtime >= server_updated_at:
            print(f"{json_file} is up-to-date; skipping download.")
            return
        else:
            print(f"{json_file} is outdated; downloading new version...")

    # Download the JSON file.
    r = requests.get(download_uri)
    if r.status_code != 200:
        raise Exception(f"Failed to download Oracle Cards JSON: {r.status_code}")
    
    with open(json_file, "wb") as f:
        f.write(r.content)
    # Set the local file's modification time to server_updated_at.
    mod_time = server_updated_at.timestamp()
    os.utime(json_file, (mod_time, mod_time))
    print(f"Downloaded and saved as {json_file} with mtime set to {server_updated_at.isoformat()}")

def main():
    json_file = "scryfall-cards.json"  # Local path to the JSON file
    download_latest_json(json_file)

    data = []
    print("Streaming and processing JSON file...")
    # Stream parse the JSON file for speed and low memory overhead.
    with open(json_file, 'rb') as f:
        # Assumes the file is a JSON array of card objects.
        cards = ijson.items(f, 'item')
        count = 0
        for card in cards:
            processed = process_card(card)
            row = tuple(processed.get(col) for col in columns)
            data.append(row)
            count += 1
            if count % 10000 == 0:
                print(f"Processed {count} cards...")
    print(f"Total cards processed: {count}")

    print("Inserting data into database...")
    # Build SET clause for update: update every column except the primary key.
    update_columns = [col for col in columns if col != "oracle_id"]
    set_clause = ", ".join(f"{col} = EXCLUDED.{col}" for col in update_columns)
    # Qualify target columns with the table name ("cards") in the WHERE clause.
    where_clause = (
        " WHERE (" +
        ", ".join("cards." + col for col in update_columns) +
        ") IS DISTINCT FROM (" +
        ", ".join(f"EXCLUDED.{col}" for col in update_columns) +
        ")"
    )
    
    sql = f"""
    INSERT INTO cards ({', '.join(columns)}) VALUES %s
    ON CONFLICT (oracle_id) DO UPDATE SET {set_clause}{where_clause}
    """
    psycopg2.extras.execute_values(
        cursor, sql, data, template=None, page_size=1000
    )
    conn.commit()
    cursor.close()
    conn.close()
    print("Data import complete.")

if __name__ == "__main__":
    main()
