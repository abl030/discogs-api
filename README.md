# Discogs Mirror API

Self-hosted mirror of the [Discogs](https://www.discogs.com/) music database. Downloads monthly CC0 XML dumps from [data.discogs.com](https://data.discogs.com/), imports into PostgreSQL, and serves a JSON API.

**Live instance**: `https://discogs.ablz.au`

## What's in the box

Two binaries from one Rust crate:

| Binary | Purpose |
|--------|---------|
| `discogs-import` | Oneshot: discovers latest dump, downloads XML.gz files, streams into Postgres via binary COPY |
| `discogs-api` | Long-running: axum HTTP server, JSON API with full-text search |

Data source: ~11 GB compressed XML across 4 files (artists, labels, masters, releases), published on the 1st of each month under CC0. The importer does a full replacement each run -- no incremental updates.

## API

### Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Service status, release count, last import timestamp |
| GET | `/api/search?artist=X&title=Y&page=1&per_page=25` | Full-text search across releases, enriched with artists/labels/formats |
| GET | `/api/releases/{id}` | Full release detail: tracks, genres, styles, identifiers |
| GET | `/api/masters/{id}` | Master release with all child releases |
| GET | `/api/artists/{id}` | Artist profile, aliases, name variations |

### Examples

```bash
# Health check
curl https://discogs.ablz.au/health
# {"status":"ok","releases":19035253,"last_import":"2026-04-12T06:03:26...","dump_date":"20260401"}

# Search by artist + title
curl 'https://discogs.ablz.au/api/search?artist=Radiohead&title=OK+Computer'

# Search by title only
curl 'https://discogs.ablz.au/api/search?title=Blue+Train&per_page=5'

# Get a specific release with full tracklist
curl https://discogs.ablz.au/api/releases/83182

# Get all pressings of a master release
curl https://discogs.ablz.au/api/masters/21491

# Get artist profile
curl https://discogs.ablz.au/api/artists/3840
```

### Search

Search uses PostgreSQL full-text search (`to_tsvector`/`plainto_tsquery`) with GIN indexes. Both `artist` and `title` params are optional but at least one is required. Results are paginated (default 25, max 100) and enriched with artists, labels, and formats per release.

## Building

Requires Rust 1.85+ (edition 2024), OpenSSL dev headers, and pkg-config.

```bash
# With nix-shell
nix-shell -p cargo rustc pkg-config openssl --run "cargo build --release"

# Or directly
cargo build --release
```

Produces two binaries in `target/release/`: `discogs-import` and `discogs-api`.

## Running

Both binaries need a PostgreSQL database and take a `--dsn` connection string.

### Importer

```bash
# Create a Postgres database first
createdb discogs

# Run the importer (downloads ~12 GB, imports ~19M releases)
./discogs-import --dsn 'postgresql://user@localhost:5432/discogs' --dump-dir ./dumps
```

The importer will:
1. Discover the latest dump date from data.discogs.com
2. Download any missing `.xml.gz` files to `--dump-dir`
3. Drop and recreate all tables
4. Stream-parse each XML file, COPY in batches of 10,000
5. Build indexes (B-tree on FKs, GIN for full-text search)
6. VACUUM ANALYZE
7. Clean up old dump files

Full import takes ~15-20 minutes on reasonable hardware. Progress is logged every 100K entities.

### API Server

```bash
./discogs-api --dsn 'postgresql://user@localhost:5432/discogs' --port 8086
```

The server starts immediately and serves on the given port. It works before the first import (returns `{"status":"awaiting_import","releases":0,...}`).

## Database Schema

16 tables covering 4 core entities and their relations:

**Core**: `artist`, `label`, `master`, `release`

**Relations**: `release_artist`, `release_label`, `release_format`, `release_track`, `release_track_artist`, `release_genre`, `release_style`, `release_identifier`, `artist_alias`, `artist_namevariation`, `master_artist`, `import_meta`

Full DDL is in `src/schema.rs`. Estimated Postgres size: ~80-120 GB with indexes.

## Deploying with NixOS

The production deployment uses a NixOS module in [nixosconfig](https://github.com/abl030/nixosconfig). The module creates:

- An nspawn PostgreSQL 16 container (`container@discogs-db.service`)
- `discogs-import.service` + `discogs-import.timer` (monthly, 2nd at 04:00)
- `discogs-api.service` (long-running, port 8086)
- Reverse proxy via `localProxy` at `discogs.ablz.au`

```nix
homelab.services.discogs = {
  enable = true;
  mirrorDir = "/mnt/mirrors/discogs";  # dumps + postgres data
  apiPort = 8086;                       # default
};
```

### Manual deploy cycle

```bash
# 1. Push code changes
git push

# 2. Update flake lock in nixosconfig
cd ~/nixosconfig
nix flake update discogs-src
git add flake.lock && git commit -m "discogs: <description>" && git push

# 3. Rebuild on target host
ssh doc2 'sudo nixos-rebuild switch --flake github:abl030/nixosconfig#doc2 --refresh'

# 4. (First time only) Run initial import
ssh doc2 'sudo systemctl start discogs-import'
ssh doc2 'journalctl -u discogs-import -f'  # watch progress
```

## Deploying without NixOS

Any Linux box with PostgreSQL 16+ works:

```bash
# 1. Build
cargo build --release

# 2. Set up Postgres
sudo -u postgres createuser discogs
sudo -u postgres createdb -O discogs discogs

# 3. Import
./target/release/discogs-import \
  --dsn 'postgresql://discogs@localhost:5432/discogs' \
  --dump-dir /var/lib/discogs/dumps

# 4. Run the API
./target/release/discogs-api \
  --dsn 'postgresql://discogs@localhost:5432/discogs' \
  --port 8086

# 5. (Optional) Cron for monthly re-import
# 0 4 2 * * /usr/local/bin/discogs-import --dsn '...' --dump-dir /var/lib/discogs/dumps
```

## Repo Structure

```
src/
  types.rs     -- Structs: import entities + API response types
  schema.rs    -- DDL constants (CREATE TABLE, indexes, VACUUM)
  xml.rs       -- Streaming XML parsers (quick-xml) for all 4 entity types
  db.rs        -- Postgres: connect, COPY, query helpers
  import.rs    -- Binary: download + parse + import orchestration
  server.rs    -- Binary: axum HTTP JSON API
  lib.rs       -- Shared module root
docs/
  plan.md      -- Original architecture and design plan
```

## License

Data is from Discogs under CC0. Code is MIT.
