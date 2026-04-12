# Self-Hosted Discogs Mirror

## Context

We self-host a MusicBrainz mirror (podman-compose, daily replication) and want to add Discogs data for disambiguation in soularr-web. Discogs publishes monthly CC0 XML dumps at data.discogs.com (~11 GB compressed, 19M+ releases). No turnkey self-hosted solution exists — we need to assemble: database, importer, API server, NixOS module.

The end goal is `discogs.ablz.au` serving a JSON API that soularr-web can query alongside the MB mirror for release disambiguation.

## Architecture

```
data.discogs.com (monthly XML dumps)
        |
        v  systemd timer (2nd of month)
+-----------------------------+
| discogs-import (oneshot)    |
| download -> parse -> COPY   |
| into PostgreSQL             |
+-------------+---------------+
              v
+-----------------------------+
| nspawn PG container         |
| hostNum=6                   |
| 192.168.100.12 / .13       |
| data: /mnt/mirrors/discogs  |  <- re-downloadable, no backup
+-------------+---------------+
              v
+-----------------------------+
| discogs-api (systemd)       |
| Python http.server          |
| port 8086                   |
| discogs.ablz.au             |
+-----------------------------+
```

## Repo Structure

```
discogs/
  importer.py    -- Downloads XML dumps, parses them, loads into Postgres via COPY.
                    Takes --dsn and --dump-dir as CLI args.
  server.py      -- HTTP JSON API server. Takes --port and --dsn as CLI args.
  schema.sql     -- DDL for all tables + indexes.
  types.py       -- Typed dataclasses for parsed XML elements.
tests/
  test_importer.py  -- Pure function + XML parsing tests
  test_server.py    -- Contract tests with REQUIRED_FIELDS per endpoint
docs/
  plan.md           -- This file
```

Pure application code -- no infrastructure awareness. These could run anywhere with a Postgres DSN.

The NixOS module lives in nixosconfig (`discogs.nix`) -- same pattern as soularr itself.

## Data Source

- **URL**: https://data.discogs.com/
- **Format**: 4 gzipped XML files per month (artists, labels, masters, releases)
- **Size**: ~11 GB compressed, ~70-80 GB uncompressed XML
- **Schedule**: Published on the 1st of each month
- **License**: CC0 (public domain, no restrictions)
- **Content**: Releases with tracklists, formats, labels, catalogue numbers, countries, years, genres, styles, barcodes, identifiers, artist aliases, label hierarchies, master release groupings
- **Not included**: Images (copyright), marketplace data, user data

## Database Schema

~16 tables. Estimated PostgreSQL size: **80-120 GB** with indexes.

### Core entities

```sql
CREATE TABLE artist (id INT PRIMARY KEY, name TEXT, realname TEXT, profile TEXT, data_quality TEXT);
CREATE TABLE label (id INT PRIMARY KEY, name TEXT, contactinfo TEXT, profile TEXT, parent_label_id INT, data_quality TEXT);
CREATE TABLE master (id INT PRIMARY KEY, title TEXT, year INT, main_release_id INT, data_quality TEXT);
CREATE TABLE release (id INT PRIMARY KEY, title TEXT, country TEXT, released TEXT, notes TEXT, master_id INT, status TEXT, data_quality TEXT);
```

### Relation tables

```sql
CREATE TABLE release_artist (release_id INT, artist_id INT, artist_name TEXT, role TEXT, anv TEXT, join_relation TEXT);
CREATE TABLE release_label (release_id INT, label_id INT, label_name TEXT, catno TEXT);
CREATE TABLE release_format (release_id INT, name TEXT, qty INT, descriptions TEXT, free_text TEXT);
CREATE TABLE release_track (release_id INT, sequence INT, position TEXT, title TEXT, duration TEXT);
CREATE TABLE release_track_artist (release_id INT, sequence INT, artist_id INT, artist_name TEXT, role TEXT, anv TEXT);
CREATE TABLE release_genre (release_id INT, genre TEXT);
CREATE TABLE release_style (release_id INT, style TEXT);
CREATE TABLE release_identifier (release_id INT, type TEXT, value TEXT, description TEXT);
CREATE TABLE artist_alias (artist_id INT, alias_id INT, name TEXT);
CREATE TABLE artist_namevariation (artist_id INT, name TEXT);
CREATE TABLE master_artist (master_id INT, artist_id INT, artist_name TEXT, role TEXT, anv TEXT);
CREATE TABLE import_meta (key TEXT PRIMARY KEY, value TEXT);
```

### Indexes

Full-text search (GIN) on `release.title` and `artist.name`. B-tree on all foreign keys for join performance.

## Importer

### Strategy

Custom Python streaming XML parser. Streams each XML.gz file through `ET.iterparse()`, builds typed dataclass instances per element, flushes in COPY batches (50k rows) to Postgres. No full DOM in memory.

### Import cycle

1. Discover latest dump date from data.discogs.com
2. Download any missing XML.gz files to `--dump-dir`
3. DROP CASCADE + CREATE all tables (full replacement, no incremental)
4. Import artists -> labels -> masters -> releases (with all junction tables)
5. Build indexes (done post-import for speed)
6. VACUUM ANALYZE
7. Record metadata in `import_meta` (timestamp, counts, dump date)

### Performance

Python with COPY: ~1-2 hours for full import. Acceptable given monthly cadence.

### Monthly timer

```
OnCalendar = *-*-02 04:00:00  (2nd of month, after dumps publish on 1st)
TimeoutStartSec = 4h
```

## API Server

Python `http.server`, same pattern as soularr-web. Short-lived per-request DB connections via `psycopg2`.

### Endpoints

```
GET /health
  -> {"status": "ok", "releases": 19000000, "last_import": "2026-04-01T04:00:00Z", "dump_date": "20260401"}

GET /api/search?artist=X&title=Y&page=1&per_page=25
  -> {"results": [{id, title, country, released, master_id, artists, labels, formats}], "page": 1, "per_page": 25}

GET /api/releases/{id}
  -> {id, title, country, released, master_id, artists, labels, formats, tracks, genres, styles, identifiers}

GET /api/masters/{id}
  -> {id, title, year, main_release_id, artists, releases: [{id, title, country, formats, labels}]}

GET /api/artists/{id}
  -> {id, name, realname, profile, aliases, namevariations}
```

### Search implementation

Uses `to_tsvector('english', ...)` / `plainto_tsquery('english', ...)` with GIN indexes. Artist search uses an EXISTS subquery against `release_artist`. Results enriched with artists/labels/formats per release.

## Test Plan

### Importer tests (`tests/test_importer.py`)

- Pure function tests: `_tab_escape`, `_text` (COPY format escaping, XML text extraction)
- XML parsing tests: `parse_artist_element`, `parse_label_element`, `parse_master_element`, `parse_release_element` -- each returns a typed dataclass
- Dump date discovery: `discover_latest_dump_date_from_html` (HTML scraping pure function)

### Server contract tests (`tests/test_server.py`)

- `REQUIRED_FIELDS` per endpoint -- fields the consumer relies on
- Real HTTP server on random port with mocked DB cursor
- 404 handling, error responses, CORS headers
- Pagination fields present

### What's NOT tested in unit tests

- Actual network downloads (integration test / manual)
- Full XML.gz streaming into PG (too slow for CI, tested manually on first import)
- Index build performance

## Implementation Order

1. Write tests (RED) -- importer pure functions + server contract tests
2. Write `discogs/types.py` -- typed dataclasses for parsed elements
3. Write `discogs/importer.py` -- makes importer tests GREEN
4. Write `discogs/server.py` -- makes server tests GREEN
5. Write `discogs/schema.sql` -- DDL
6. Write `nixosconfig/modules/nixos/services/discogs.nix`
7. Enable in doc2 config, deploy, verify PG is up
8. Run first import manually (`sudo discogs-import`)
9. Verify API: `curl https://discogs.ablz.au/health`

## NixOS Module (in nixosconfig)

### Options

```nix
homelab.services.discogs = {
  enable = mkEnableOption "Discogs mirror";
  mirrorDir = mkOption { default = "/mnt/mirrors/discogs"; };
  apiPort = mkOption { default = 8086; };
};
```

### Components

| Systemd unit | Type | Purpose |
|---|---|---|
| `containers.discogs-db` | nspawn | PostgreSQL 16, hostNum=6 |
| `discogs-import.service` | oneshot | Download + parse + load dumps |
| `discogs-import.timer` | timer | Monthly trigger (2nd, 04:00) |
| `discogs-api.service` | simple | JSON API server (port 8086) |

### Storage

- `/mnt/mirrors/discogs/postgres` -- PG data dir (bind-mounted into container)
- `/mnt/mirrors/discogs/dumps` -- cached XML.gz files

All under `/mnt/mirrors/` -- re-downloadable, NOT backed up. Same tier as MB mirror pgdata/solrdata.

### Integration

- `discogs.ablz.au` via `homelab.localProxy.hosts` (auto ACME + Cloudflare DNS)
- Kuma health monitor on `/health`
- `discogs-import` on system PATH for manual runs

## Verification

```bash
# PG container running
ssh doc2 'systemctl is-active container@discogs-db.service'

# Row count after import
ssh doc2 'psql -h 192.168.100.13 -U discogs -d discogs -c "SELECT count(*) FROM release"'

# API health
curl https://discogs.ablz.au/health

# Search works
curl 'https://discogs.ablz.au/api/search?artist=Radiohead&title=OK+Computer'

# Kuma green
# -> check status.ablz.au
```

## Future: Soularr Web Integration (out of scope for this repo)

Once the mirror + API are stable:

- Dual-search in music.ablz.au (MB + Discogs results side by side)
- Cross-reference Discogs releases with MB releases for disambiguation
- Use Discogs format details (pressing weight, color, country) to identify specific pressings
- Resolve Discogs-sourced albums in the pipeline (currently blocked -- numeric IDs, no MB UUID)
