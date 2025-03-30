#!/usr/bin/env python3
"""
Import Scryfall bulk card data into a PostgreSQL database and import set information from Scryfall.

This script downloads the specified bulk card data (oracle_cards, unique_artwork, or all_prints) from Scryfall,
processes the JSON with low memory overhead using ijson, and performs a bulk UPSERT into the PostgreSQL
database using the unique Scryfall card 'id' as the primary key.

It also downloads the sets data from Scryfall and upserts that data into a new "sets" table using the set's "id" as the primary key.
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
# Set the bulk data type here: "oracle_cards", "unique_artwork", or "all_prints"
BULK_DATA_TYPE = "all_prints"  # Change this to the desired option

# Define allowed layout values so we can validate card data.
ALLOWED_LAYOUTS = {
    'normal', 'split', 'flip', 'transform', 'modal_dfc', 'meld', 'leveler',
    'class', 'case', 'saga', 'adventure', 'mutate', 'prototype', 'battle',
    'planar', 'scheme', 'vanguard', 'token', 'double_faced_token', 'emblem',
    'augment', 'host', 'art_series', 'reversible_card'
}

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

# ---------------------------
# TABLE SCHEMAS AND COLUMNS
# ---------------------------
# List of columns for the cards table exactly matching the updated init.sql table definition.
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

# Columns for the new sets table.
set_columns = [
    "id",            # unique Scryfall set id (PRIMARY KEY)
    "code",
    "name",
    "uri",
    "scryfall_uri",
    "search_uri",
    "released_at",
    "set_type",
    "card_count",
    "parent_set_code",
    "digital",
    "nonfoil_only",
    "foil_only",
    "icon_svg_uri"
]

# ---------------------------
# HELPER FUNCTIONS
# ---------------------------
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
    This helper method ensures that all decimal.Decimal instances in our card data are
    converted to float.
    """
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    elif isinstance(obj, dict):
        return {k: convert_decimals(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_decimals(item) for item in obj]
    else:
        return obj

# ---------------------------
# CARD PROCESSING FUNCTIONS
# ---------------------------
def process_card(card):
    """
    Process a card JSON object for database insertion.
    - Converts released_at to a date.
    - Aggregates image_uris from card_faces if missing at top-level.
    - Validates the layout value against ALLOWED_LAYOUTS.
    - Wraps dictionaries/lists in psycopg2.extras.Json for JSONB columns.
    - Converts Decimal objects to float.
    """
    # Validate layout value.
    layout = card.get("layout")
    if layout not in ALLOWED_LAYOUTS:
        print(f"Warning: Unexpected layout '{layout}' encountered for card {card.get('name')}.")
    
    # Aggregate image_uris if missing but present in card_faces.
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

def upsert_batch(data_batch):
    """Execute the UPSERT for a batch of card rows."""
    update_columns = [col for col in columns if col != "id"]
    set_clause = ", ".join(f"{col} = EXCLUDED.{col}" for col in update_columns)
    sql = f"""
    INSERT INTO cards ({', '.join(columns)}) VALUES %s
    ON CONFLICT (id) DO UPDATE SET {set_clause}
    """
    psycopg2.extras.execute_values(
        cursor, sql, data_batch, template=None, page_size=1000
    )
    conn.commit()

# ---------------------------
# SETS PROCESSING FUNCTIONS
# ---------------------------
def process_set(set_data):
    """
    Process a set JSON object for database insertion.
    Converts released_at to a date and returns a dictionary matching the set_columns.
    """
    processed = {}
    for col in set_columns:
        val = set_data.get(col)
        if col == "released_at":
            processed[col] = parse_date(val)
        else:
            processed[col] = val
    return processed

def upsert_sets_batch(data_batch):
    """Execute the UPSERT for a batch of set rows."""
    update_columns = [col for col in set_columns if col != "id"]
    set_clause = ", ".join(f"{col} = EXCLUDED.{col}" for col in update_columns)
    sql = f"""
    INSERT INTO sets ({', '.join(set_columns)}) VALUES %s
    ON CONFLICT (id) DO UPDATE SET {set_clause}
    """
    psycopg2.extras.execute_values(
        cursor, sql, data_batch, template=None, page_size=1000
    )
    conn.commit()

def import_sets():
    """
    Downloads the sets data from Scryfall, processes each set,
    and performs a bulk UPSERT into the 'sets' table.
    """
    sets_url = "https://api.scryfall.com/sets"
    print(f"Downloading sets data from {sets_url} ...")
    resp = requests.get(sets_url, timeout=10)
    if resp.status_code != 200:
        raise RuntimeError(f"Failed to download sets data: {resp.status_code}")
    sets_json = resp.json()
    sets_list = sets_json.get("data", [])
    print(f"Processing {len(sets_list)} sets...")

    batch = []
    for set_item in sets_list:
        processed = process_set(set_item)
        # Ensure we have an 'id'
        if not processed.get("id"):
            print(f"Warning: Set missing 'id', skipping {processed.get('name')}")
            continue
        row = tuple(processed.get(col) for col in set_columns)
        batch.append(row)
    if batch:
        upsert_sets_batch(batch)
        print(f"Upserted {len(batch)} sets into the database.")

# ---------------------------
# DOWNLOAD FUNCTIONS
# ---------------------------
def download_latest_json(json_file):
    """
    Checks the Scryfall bulk-data API for the desired JSON file.
    If the file is missing or outdated, it downloads the file.
    Supports bulk types "oracle_cards", "unique_artwork", and "all_prints".
    
    Note: When BULK_DATA_TYPE is set to "all_prints", the script will search for the bulk data
    object with type "all_cards" from Scryfall.
    """
    bulk_api = "https://api.scryfall.com/bulk-data"
    print(f"Querying Scryfall bulk-data API for the latest {BULK_DATA_TYPE} JSON URL...")

    resp = requests.get(bulk_api, timeout=10)
    if resp.status_code != 200:
        raise RuntimeError(f"Failed to query bulk-data API: {resp.status_code}")
    bulk_data = resp.json().get("data", [])

    # Map "all_prints" to the Scryfall bulk data type "all_cards"
    desired_type = BULK_DATA_TYPE
    if BULK_DATA_TYPE == "all_prints":
        desired_type = "all_cards"

    desired_bulk = next((item for item in bulk_data if item.get("type") == desired_type), None)
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

# ---------------------------
# MAIN FUNCTION
# ---------------------------
def main():
    """Main function to download, process, and import card and set data into the database."""
    # Import cards
    json_file = f"scryfall-{BULK_DATA_TYPE}.json"
    download_latest_json(json_file)

    batch_size = 10000
    batch = []
    total_count = 0
    print("Streaming and processing card JSON file...")
    with open(json_file, 'rb') as f:
        cards = ijson.items(f, 'item')
        for card in cards:
            processed = process_card(card)
            # Skip any card that lacks an 'id'
            if not processed.get("id"):
                print(f"Warning: card missing 'id', skipping {processed.get('name')}")
                continue
            row = tuple(processed.get(col) for col in columns)
            batch.append(row)
            total_count += 1

            if total_count % batch_size == 0:
                print(f"Processing batch of {batch_size} cards (total processed: {total_count})...")
                upsert_batch(batch)
                batch = []
    # Process any remaining rows for cards
    if batch:
        print(f"Processing final batch of {len(batch)} cards...")
        upsert_batch(batch)
    print(f"Card data import complete. Total cards processed: {total_count}")

    # Import sets data
    import_sets()

    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
