# mtg-bulk-database

Simple MTG (Magic: The Gathering) database implementation using Scryfall’s bulk data JSON to PostgreSQL.

## Overview

The `mtg-bulk-database` script is a data science utility that:

- Queries the Scryfall Bulk Data API for the latest Oracle Cards JSON file.
- Checks if a local copy of the file is up-to-date based on the Scryfall server’s `updated_at` timestamp.
- Downloads the JSON file (if necessary) and sets its modification time to match the server.
- Streams and processes the JSON file with low memory overhead using `ijson`.
- Performs a bulk upsert into a PostgreSQL database using an UPSERT (ON CONFLICT) clause that updates only changed fields.
- Supports multifaced cards by:
  - Storing detailed multiface information in a dedicated `card_faces` JSONB column.
  - Aggregating image URLs from individual faces if the top-level image URLs are absent.
- **Important Structural Change:**  
  The database schema now uses the unique Scryfall card **id** as the primary key (instead of **oracle_id**).  
  This ensures that every card object (including reprints and variants) is uniquely identified.

## SQL Schema

Below is the updated 1:1 Scryfall bulk data JSON to an SQL schema that the utility will use.  
[Scryfall Types & Methods docs](https://scryfall.com/docs/api/bulk-data)

```sql
DROP TABLE IF EXISTS cards;
CREATE TABLE cards (
    id UUID PRIMARY KEY,            -- Use the unique Scryfall card `id` as the primary key
    oracle_id UUID,
    object TEXT,
    multiverse_ids JSONB,
    mtgo_id INTEGER,
    tcgplayer_id INTEGER,
    cardmarket_id INTEGER,
    name TEXT,
    lang TEXT,
    released_at DATE,
    uri TEXT,
    scryfall_uri TEXT,
    layout TEXT,
    highres_image BOOLEAN,
    image_status TEXT,
    image_uris JSONB,
    mana_cost TEXT,
    cmc NUMERIC,
    type_line TEXT,
    oracle_text TEXT,
    power TEXT,
    toughness TEXT,
    colors JSONB,
    color_identity JSONB,
    keywords JSONB,
    all_parts JSONB,
    legalities JSONB,
    games JSONB,
    reserved BOOLEAN,
    game_changer BOOLEAN,
    foil BOOLEAN,
    nonfoil BOOLEAN,
    finishes JSONB,
    oversized BOOLEAN,
    promo BOOLEAN,
    reprint BOOLEAN,
    variation BOOLEAN,
    set_id UUID,
    set TEXT,
    set_name TEXT,
    set_type TEXT,
    set_uri TEXT,
    set_search_uri TEXT,
    scryfall_set_uri TEXT,
    rulings_uri TEXT,
    prints_search_uri TEXT,
    collector_number TEXT,
    digital BOOLEAN,
    rarity TEXT,
    watermark TEXT,
    flavor_text TEXT,
    card_back_id UUID,
    artist TEXT,
    artist_ids JSONB,
    illustration_id UUID,
    border_color TEXT,
    frame TEXT,
    frame_effects JSONB,
    security_stamp TEXT,
    full_art BOOLEAN,
    textless BOOLEAN,
    booster BOOLEAN,
    story_spotlight BOOLEAN,
    edhrec_rank INTEGER,
    preview JSONB,
    prices JSONB,
    related_uris JSONB,
    purchase_uris JSONB,
    card_faces JSONB
);
```

## Multifaced Cards

Magic cards can have multiple faces. Scryfall represents these as a single card object with a `card_faces` array. This script:

- Adds the new `card_faces` column (type JSONB) to store multiface details.
- In cases where a multifaced card lacks a top-level `image_uris`, aggregates image URLs from each face into the top-level `image_uris` field.

## Installation

1. **Clone the Repository:**

   ```bash
   git clone https://github.com/JakeTurner616/mtg-bulk-database
   cd mtg-bulk-database
   ```

2. **Create and Activate a Virtual Environment:**

   - For Unix/Linux/Mac:

     ```bash
     python -m venv venv
     source venv/bin/activate
     ```

   - For Windows:

     ```bash
     python -m venv venv
     venv\Scripts\activate
     ```

3. **Install Required Dependencies:**

   ```bash
   pip install -r requirements.txt
   ```

   *(Dependencies include: `psycopg2`, `requests`, `ijson`, `python-dotenv`.)*

4. **Set Up Your PostgreSQL Database Schema:**

   Run the updated SQL schema to create the database table. For example, using `psql`:

   ```bash
   psql -U myuser -d mtgdb -f init.sql
   ```

   *Make sure the table has a primary key on the **id** column.*

5. **(Optional) Run a PostgreSQL Instance using Docker:**

   ```powershell
   docker build -t mtg-postgres .\mtg-database\
   docker run -d --name mtg-postgres --env-file ./mtg-database/.env -p 5432:5432 -v ${PWD}/mtg-database/postgres:/var/lib/postgresql/data mtg-postgres
   ```

## Environment Variables

Create a `.env` file in the project root with your PostgreSQL connection settings:

```env
POSTGRES_USER=myuser
POSTGRES_PASSWORD=mypassword
POSTGRES_DB=mtgdb
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
```

## Usage

Run the importer script:

```bash
python import_cards.py
```

The script will:

- Query the Scryfall Bulk Data API for the latest JSON file (Oracle Cards or Unique Artwork).
- Check if the local copy (e.g., `scryfall-unique_artwork.json`) is up-to-date.
- Download a new copy if necessary.
- Stream through the JSON file and process each card.
- For multifaced cards, aggregate image URLs from `card_faces` if required.
- Perform a bulk UPSERT into the PostgreSQL database using `ON CONFLICT (id)` so that if a card with the same unique **id** already exists, only changed fields are updated.

This allows you to efficiently update your `cards` table with thousands of records per second.

## Author

Developed by [Jakob Turner](https://github.com/JakeTurner616).

## License

This project is licensed under the GNU GPL 3.0 License. See the [LICENSE](./LICENSE) file for details.
