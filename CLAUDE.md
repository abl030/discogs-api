# **RUN `hostname` AT THE START OF EVERY CHAT.**

# Discogs Mirror API

Self-hosted Discogs data mirror in Rust. Downloads monthly CC0 XML dumps from data.discogs.com, imports into PostgreSQL, serves a JSON API at `discogs.ablz.au`. Live with 19M+ releases.

## Repo Structure

```
src/
  types.rs     -- Structs: import entities (Default) + API responses (serde)
  schema.rs    -- DDL constants (CREATE TABLE, indexes, VACUUM)
  xml.rs       -- Streaming XML parsers (quick-xml, one per entity type)
  db.rs        -- Postgres: connect, binary COPY helpers, query functions
  import.rs    -- Binary: CLI, dump discovery, download, parse+COPY pipeline
  server.rs    -- Binary: axum HTTP JSON API (health, search, CRUD endpoints)
  lib.rs       -- Shared module root
docs/
  plan.md      -- Original architecture plan (reference only)
```

Two binaries from one crate: `discogs-import` (oneshot) and `discogs-api` (long-running axum server).

## Building & Testing

```bash
nix-shell -p cargo rustc pkg-config openssl --run "cargo check"
nix-shell -p cargo rustc pkg-config openssl --run "cargo test"
nix-shell -p cargo rustc pkg-config openssl --run "cargo build --release"
```

Tests cover the XML parsers (4 tests in `src/xml.rs`). No integration tests — DB layer is verified against the live instance.

## Infrastructure

Pure application code — no infrastructure awareness. Takes `--dsn` and `--dump-dir` / `--port` as CLI args.

- **Postgres**: nspawn container `discogs-db` on doc2, hostNum=6 (192.168.100.13:5432)
- **DSN**: `postgresql://discogs@192.168.100.13:5432/discogs`
- **API**: `discogs.ablz.au` (port 8086, behind localProxy with auto ACME)
- **Data**: `/mnt/mirrors/discogs` on doc2 (re-downloadable, not backed up)
- **NixOS module**: `nixosconfig/modules/nixos/services/discogs.nix`
- **Flake input**: `discogs-src` in nixosconfig's flake.nix (non-flake, `github:abl030/discogs-api`)

## Deploying

Deploy from proxmox-vm (where this repo lives):

```bash
git push
cd ~/nixosconfig && nix flake update discogs-src && git add flake.lock && git commit -m "discogs: description" && git push
ssh doc2 'sudo nixos-rebuild switch --flake github:abl030/nixosconfig#doc2 --refresh'
```

The API service restarts automatically. The import does NOT restart (timer-triggered oneshot).

## Debugging on doc2

```bash
ssh doc2 'systemctl status discogs-api.service'
ssh doc2 'journalctl -u discogs-api.service -f'
ssh doc2 'journalctl -u discogs-import.service -f'
ssh doc2 'curl -s http://127.0.0.1:8086/health'
ssh doc2 'psql -h 192.168.100.13 -U discogs -d discogs -c "SELECT count(*) FROM release"'
```

## Key Design Decisions

- **Binary COPY** (not text): `BinaryCopyInWriter` from tokio-postgres. Handles NULLs natively, no escaping needed.
- **Channel pipeline**: XML parsing runs on a blocking thread, sends 10K-entity batches through `mpsc` to async COPY. Backpressure via channel capacity of 4.
- **Atomic downloads**: Files written to `.partial`, renamed on success. Prevents truncated files from being treated as complete.
- **Full replacement**: Every import drops and recreates all tables. No incremental updates. Indexes built post-import for speed.
- **Single PG connection**: The API server uses one `tokio_postgres::Client` (multiplexed). No pool needed for this traffic level.
