-- Drop the table and ENUM type if they already exist
DROP TABLE IF EXISTS cards;
DROP TYPE IF EXISTS layout_type;

-- Create an ENUM type for the allowed layout values
CREATE TYPE layout_type AS ENUM (
    'normal',
    'split',
    'flip',
    'transform',
    'modal_dfc',
    'meld',
    'leveler',
    'class',
    'case',
    'saga',
    'adventure',
    'mutate',
    'prototype',
    'battle',
    'planar',
    'scheme',
    'vanguard',
    'token',
    'double_faced_token',
    'emblem',
    'augment',
    'host',
    'art_series',
    'reversible_card'
);

-- Create the cards table using the ENUM type for layout
CREATE TABLE cards (
    id UUID PRIMARY KEY,  -- Using the unique Scryfall card id as the primary key
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
    layout layout_type,  -- Now using our ENUM type for layout values
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

-- Drop the sets table if it exists
DROP TABLE IF EXISTS sets;

-- Create the sets table
CREATE TABLE sets (
    id UUID PRIMARY KEY,             -- Using the unique Scryfall set id as the primary key
    code TEXT,
    name TEXT,
    uri TEXT,
    scryfall_uri TEXT,
    search_uri TEXT,
    released_at DATE,
    set_type TEXT,
    card_count INTEGER,
    parent_set_code TEXT,
    digital BOOLEAN,
    nonfoil_only BOOLEAN,
    foil_only BOOLEAN,
    icon_svg_uri TEXT
);

-- Index for quick lookup by oracle_id
CREATE INDEX idx_cards_oracle_id ON cards(oracle_id);

-- JSONB indexes to speed up containment searches for keywords and colors
CREATE INDEX idx_cards_keywords ON cards USING gin(keywords);
CREATE INDEX idx_cards_colors ON cards USING gin(colors);

-- Enable the pg_trgm extension for improved text search performance
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Trigram indexes for faster ILIKE searches on name and oracle_text
CREATE INDEX idx_cards_name_trgm ON cards USING gin(name gin_trgm_ops);
CREATE INDEX idx_cards_oracle_text_trgm ON cards USING gin(oracle_text gin_trgm_ops);

-- B-tree index for numeric comparisons on cmc
CREATE INDEX idx_cards_cmc ON cards(cmc);

-- 
CREATE INDEX cards_text_idx ON cards USING GIN (
  to_tsvector('english', coalesce(name, '') || ' ' || coalesce(oracle_text, ''))
);