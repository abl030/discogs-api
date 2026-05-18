// DDL constants for the Discogs mirror database.
// Full replacement on each import: DROP CASCADE -> CREATE -> import -> index -> VACUUM.

pub const DROP_ALL: &str = "\
DROP TABLE IF EXISTS import_meta CASCADE;
DROP TABLE IF EXISTS release_identifier CASCADE;
DROP TABLE IF EXISTS release_style CASCADE;
DROP TABLE IF EXISTS release_genre CASCADE;
DROP TABLE IF EXISTS release_track_artist CASCADE;
DROP TABLE IF EXISTS release_track CASCADE;
DROP TABLE IF EXISTS release_format CASCADE;
DROP TABLE IF EXISTS release_label CASCADE;
DROP TABLE IF EXISTS release_artist CASCADE;
DROP TABLE IF EXISTS master_artist CASCADE;
DROP TABLE IF EXISTS artist_namevariation CASCADE;
DROP TABLE IF EXISTS artist_alias CASCADE;
DROP TABLE IF EXISTS release CASCADE;
DROP TABLE IF EXISTS master CASCADE;
DROP TABLE IF EXISTS label CASCADE;
DROP TABLE IF EXISTS artist CASCADE;
";

pub const CREATE_TABLES: &str = "\
CREATE TABLE artist (
    id INT PRIMARY KEY,
    name TEXT NOT NULL,
    realname TEXT NOT NULL DEFAULT '',
    profile TEXT NOT NULL DEFAULT '',
    data_quality TEXT NOT NULL DEFAULT ''
);

CREATE TABLE label (
    id INT PRIMARY KEY,
    name TEXT NOT NULL,
    contactinfo TEXT NOT NULL DEFAULT '',
    profile TEXT NOT NULL DEFAULT '',
    parent_label_id INT,
    data_quality TEXT NOT NULL DEFAULT ''
);

CREATE TABLE master (
    id INT PRIMARY KEY,
    title TEXT NOT NULL,
    year INT,
    main_release_id INT,
    data_quality TEXT NOT NULL DEFAULT ''
);

CREATE TABLE release (
    id INT PRIMARY KEY,
    title TEXT NOT NULL,
    country TEXT NOT NULL DEFAULT '',
    released TEXT NOT NULL DEFAULT '',
    notes TEXT NOT NULL DEFAULT '',
    master_id INT,
    status TEXT NOT NULL DEFAULT '',
    data_quality TEXT NOT NULL DEFAULT '',
    search_text TEXT NOT NULL DEFAULT ''
);

CREATE TABLE release_artist (
    release_id INT NOT NULL,
    artist_id INT NOT NULL,
    artist_name TEXT NOT NULL DEFAULT '',
    role TEXT NOT NULL DEFAULT '',
    anv TEXT NOT NULL DEFAULT '',
    join_relation TEXT NOT NULL DEFAULT ''
);

CREATE TABLE release_label (
    release_id INT NOT NULL,
    label_id INT NOT NULL,
    label_name TEXT NOT NULL DEFAULT '',
    catno TEXT NOT NULL DEFAULT ''
);

CREATE TABLE release_format (
    release_id INT NOT NULL,
    name TEXT NOT NULL DEFAULT '',
    qty INT NOT NULL DEFAULT 1,
    descriptions TEXT NOT NULL DEFAULT '',
    free_text TEXT NOT NULL DEFAULT ''
);

CREATE TABLE release_track (
    release_id INT NOT NULL,
    sequence INT NOT NULL,
    position TEXT NOT NULL DEFAULT '',
    title TEXT NOT NULL DEFAULT '',
    duration TEXT NOT NULL DEFAULT ''
);

CREATE TABLE release_track_artist (
    release_id INT NOT NULL,
    sequence INT NOT NULL,
    artist_id INT NOT NULL,
    artist_name TEXT NOT NULL DEFAULT '',
    role TEXT NOT NULL DEFAULT '',
    anv TEXT NOT NULL DEFAULT ''
);

CREATE TABLE release_genre (
    release_id INT NOT NULL,
    genre TEXT NOT NULL
);

CREATE TABLE release_style (
    release_id INT NOT NULL,
    style TEXT NOT NULL
);

CREATE TABLE release_identifier (
    release_id INT NOT NULL,
    type TEXT NOT NULL DEFAULT '',
    value TEXT NOT NULL DEFAULT '',
    description TEXT NOT NULL DEFAULT ''
);

CREATE TABLE artist_alias (
    artist_id INT NOT NULL,
    alias_id INT NOT NULL,
    name TEXT NOT NULL DEFAULT ''
);

CREATE TABLE artist_namevariation (
    artist_id INT NOT NULL,
    name TEXT NOT NULL
);

CREATE TABLE master_artist (
    master_id INT NOT NULL,
    artist_id INT NOT NULL,
    artist_name TEXT NOT NULL DEFAULT '',
    role TEXT NOT NULL DEFAULT '',
    anv TEXT NOT NULL DEFAULT ''
);

CREATE TABLE import_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
";

// Indexes are built after import for speed.
pub const CREATE_INDEXES: &str = "\
CREATE INDEX idx_release_master_id ON release (master_id);
CREATE INDEX idx_release_artist_release_id ON release_artist (release_id);
CREATE INDEX idx_release_artist_artist_id ON release_artist (artist_id);
CREATE INDEX idx_release_label_release_id ON release_label (release_id);
CREATE INDEX idx_release_label_label_id ON release_label (label_id);
CREATE INDEX idx_release_format_release_id ON release_format (release_id);
CREATE INDEX idx_release_track_release_id ON release_track (release_id);
CREATE INDEX idx_release_track_artist_release_id ON release_track_artist (release_id);
CREATE INDEX idx_release_track_artist_artist_id ON release_track_artist (artist_id);
CREATE INDEX idx_release_genre_release_id ON release_genre (release_id);
CREATE INDEX idx_release_style_release_id ON release_style (release_id);
CREATE INDEX idx_release_identifier_release_id ON release_identifier (release_id);
CREATE INDEX idx_artist_alias_artist_id ON artist_alias (artist_id);
CREATE INDEX idx_artist_namevariation_artist_id ON artist_namevariation (artist_id);
CREATE INDEX idx_master_artist_master_id ON master_artist (master_id);
CREATE INDEX idx_label_parent_id ON label (parent_label_id);
CREATE INDEX idx_master_main_release ON master (main_release_id);

CREATE INDEX idx_release_title_fts ON release USING GIN (to_tsvector('english', title));
CREATE INDEX idx_artist_name_fts ON artist USING GIN (to_tsvector('english', name));
CREATE INDEX idx_label_name_fts ON label USING GIN (to_tsvector('english', name));
CREATE INDEX idx_release_search_text_fts ON release USING GIN (to_tsvector('english', search_text));
";

pub const VACUUM_ANALYZE: &str = "\
VACUUM ANALYZE artist;
VACUUM ANALYZE label;
VACUUM ANALYZE master;
VACUUM ANALYZE release;
VACUUM ANALYZE release_artist;
VACUUM ANALYZE release_label;
VACUUM ANALYZE release_format;
VACUUM ANALYZE release_track;
VACUUM ANALYZE release_track_artist;
VACUUM ANALYZE release_genre;
VACUUM ANALYZE release_style;
VACUUM ANALYZE release_identifier;
VACUUM ANALYZE artist_alias;
VACUUM ANALYZE artist_namevariation;
VACUUM ANALYZE master_artist;
VACUUM ANALYZE import_meta;
";
