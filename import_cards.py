#!/usr/bin/env python3
"""
Import Scryfall bulk data into a PostgreSQL database.

This script downloads the specified bulk data (oracle_cards or unique_artwork)
from Scryfall, processes the JSON with low memory overhead using ijson, and
performs a bulk UPSERT into the PostgreSQL database. The schema uses the unique
Scryfall card 'id' as the primary key.
"""

import os
import decimal
from datetime import datetime, timezone

import requests
import psycopg2
import psycopg2.extras
import ijson
from dotenv import load_dotenv

# ---------------------------
# CONFIGURATION
# ---------------------------
# Set the bulk data type here: "oracle_cards" or "unique_artwork"
BULK_DATA_TYPE = "oracle_cards"  # or "unique_artwork"

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

# List of columns exactly matching the updated init.sql table definition.
# IMPORTANT STRUCTURAL CHANGE:
# The unique Scryfall card "id" is now used as the primary key instead of oracle_id.
columns = [
    "oracle_id",  # stored as a regular field
    "id",         # unique Scryfall card id (PRIMARY KEY)
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
    "purchase_uris",
    "card_faces"
]

def parse_date(date_str):
    """Convert an ISO date string to a date object; return None if invalid."""
    if not date_str:
        return None
    try:
        return datetime.fromisoformat(date_str).date()
    except ValueError:
        return None

def convert_decimals(obj):
    """
    Recursively convert Decimal objects to float within dictionaries or lists.

    Code Review Note:
    -----------------
    This helper method ensures that all decimal.Decimal instances in our card data are
    converted to float, preventing JSON serialization errors when wrapping data with
    psycopg2.extras.Json. Since Scryfall's API may include Decimal values (e.g., in price
    fields or mana cost calculations), this recursive conversion handles cases where
    Decimal objects might be deeply nested within dictionaries or lists.
    """
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    elif isinstance(obj, dict):
        return {k: convert_decimals(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_decimals(item) for item in obj]
    else:
        return obj

def process_card(card):
    """
    Process a card JSON object for database insertion.
    
    - Converts released_at to a date.
    - If the card has multiple faces (card_faces) and no top-level image_uris,
      aggregates image URLs from each face into a list and sets it to image_uris.
    - Wraps dictionaries/lists in psycopg2.extras.Json for JSONB columns.
    - Converts Decimal objects to float.
    """
    if "card_faces" in card and not card.get("image_uris"):
        aggregated = []
        for face in card["card_faces"]:
            if "image_uris" in face:
                aggregated.append(face["image_uris"])
        if aggregated:
            card["image_uris"] = aggregated

    processed = {}
    for col in columns:
        val = card.get(col)
        if col == "released_at":
            processed[col] = parse_date(val)
        else:
            if isinstance(val, decimal.Decimal):
                processed[col] = float(val)
            elif isinstance(val, (dict, list)):
                processed[col] = psycopg2.extras.Json(convert_decimals(val))
            else:
                processed[col] = val
    return processed

def download_latest_json(json_file):
    """
    Checks the Scryfall bulk-data API for the desired JSON file (Oracle Cards or Unique Artwork).
    If the local file is missing or its modification time is older than the server's updated_at,
    downloads the file from the provided download_uri. The file's modification time is then updated.
    """
    bulk_api = "https://api.scryfall.com/bulk-data"
    print(f"Querying Scryfall bulk-data API for the latest {BULK_DATA_TYPE} JSON URL...")
    resp = requests.get(bulk_api, timeout=10)
    if resp.status_code != 200:
        raise RuntimeError(f"Failed to query bulk-data API: {resp.status_code}")
    bulk_data = resp.json().get("data", [])
    desired_bulk = next((item for item in bulk_data if item.get("type") == BULK_DATA_TYPE), None)
    if not desired_bulk:
        raise RuntimeError(f"{BULK_DATA_TYPE} bulk data not found")
    
    server_updated_at = datetime.fromisoformat(
        desired_bulk.get("updated_at").replace("Z", "+00:00")
    )
    download_uri = desired_bulk.get("download_uri")
    print(f"Server reports updated_at: {server_updated_at.isoformat()}")
    print(f"Download URI: {download_uri}")

    if os.path.exists(json_file):
        local_mtime = datetime.fromtimestamp(os.path.getmtime(json_file), tz=timezone.utc)
        print(f"Local file modification time: {local_mtime.isoformat()}")
        if local_mtime >= server_updated_at:
            print(f"{json_file} is up-to-date; skipping download.")
            return
        print(f"{json_file} is outdated; downloading new version...")

    r = requests.get(download_uri, timeout=10)
    if r.status_code != 200:
        raise RuntimeError(f"Failed to download {BULK_DATA_TYPE} JSON: {r.status_code}")
    
    with open(json_file, "wb") as f:
        f.write(r.content)
    mod_time = server_updated_at.timestamp()
    os.utime(json_file, (mod_time, mod_time))
    print(f"Downloaded and saved as {json_file} with mtime set to {server_updated_at.isoformat()}")

def main():
    """Main function to download, process, and import card data into the database."""
    json_file = f"scryfall-{BULK_DATA_TYPE}.json"
    download_latest_json(json_file)

    data = []
    print("Streaming and processing JSON file...")
    with open(json_file, 'rb') as f:
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
    update_columns = [col for col in columns if col != "id"]
    set_clause = ", ".join(f"{col} = EXCLUDED.{col}" for col in update_columns)
    where_clause = (
        " WHERE (" +
        ", ".join("cards." + col for col in update_columns) +
        ") IS DISTINCT FROM (" +
        ", ".join(f"EXCLUDED.{col}" for col in update_columns) +
        ")"
    )
    
    sql = f"""
    INSERT INTO cards ({', '.join(columns)}) VALUES %s
    ON CONFLICT (id) DO UPDATE SET {set_clause}{where_clause}
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
