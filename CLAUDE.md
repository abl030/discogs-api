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

- **Postgres**: nspawn container `discogs-db` on doc2, reachable at `10.20.0.13:5432` (the live nspawn-bridge address the service connects over; `machinectl list` confirms it). The host-namespace address `192.168.100.13` times out.
- **DSN**: `postgresql://discogs@10.20.0.13:5432/discogs`
- **API**: `discogs.ablz.au` (port 8086, behind localProxy with auto ACME)
- **Data**: `/mnt/mirrors/discogs` on doc2 (re-downloadable, not backed up)
- **NixOS module**: `nixosconfig/modules/nixos/services/discogs.nix`
- **Flake input**: `discogs-src` in nixosconfig's flake.nix (non-flake, `github:abl030/discogs-api`)

## Deploying

Deploy from proxmox-vm/doc1 (where this repo lives, and which holds the Forgejo
token + SSH signing key).

**Since the 2026-06-10 Forgejo cutover, the nixosconfig leg goes through Forgejo
(`git.ablz.au`) + `fleet-update`, NOT `github:abl030/nixosconfig`.** GitHub's
nixosconfig is a frozen, stale fallback. This repo itself still pushes to GitHub;
only the nixosconfig leg changed.

```bash
git push                                          # this repo → GitHub (unchanged)
cd ~/nixosconfig && git pull && nix flake update discogs-src && git add flake.lock \
  && git commit -m "discogs: description"         # commit must be SSH-signed (commit.gpgsign is on)
TOKEN=$(cat /run/secrets/forgejo/nixbot-token) \
  && git -c "http.extraHeader=Authorization: token ${TOKEN}" push origin master   # never echo the token
ssh doc2 'sudo fleet-update'                       # verifies signatures, builds from its own clone
```

`fleet-update` verifies every commit in range is SSH-signed by a key in
hosts.nix, then builds from its own root-owned clone. The `discogs-api` service
restarts automatically on switch. The import does NOT restart (timer-triggered
oneshot). Do NOT deploy with `nixos-rebuild switch --flake github:...` — GitHub
nixosconfig is stale.

## Debugging on doc2

```bash
ssh doc2 'systemctl status discogs-api.service'
ssh doc2 'journalctl -u discogs-api.service -f'
ssh doc2 'journalctl -u discogs-import.service -f'
ssh doc2 'curl -s http://127.0.0.1:8086/health'
# The DB nspawn is reachable at 10.20.0.13 (NOT 192.168.100.13, which times out
# from doc2's host namespace — `machinectl list` shows the live address). The
# password lives in /run/secrets/discogs-pgpass in POSTGRES_PASSWORD=... format.
ssh doc2 'export PGPASSWORD=$(sudo cat /run/secrets/discogs-pgpass | grep -oP "POSTGRES_PASSWORD=\K.*"); psql -h 10.20.0.13 -U discogs -d discogs -c "SELECT count(*) FROM release"'
```

## Key Design Decisions

- **Binary COPY** (not text): `BinaryCopyInWriter` from tokio-postgres. Handles NULLs natively, no escaping needed.
- **Channel pipeline**: XML parsing runs on a blocking thread, sends 10K-entity batches through `mpsc` to async COPY. Backpressure via channel capacity of 4.
- **Atomic downloads**: Files written to `.partial`, renamed on success. Prevents truncated files from being treated as complete.
- **Full replacement**: Every import drops and recreates all tables. No incremental updates. Indexes built post-import for speed.
- **API Postgres pool**: The API server uses a `deadpool-postgres` pool
  (max size 16, bounded wait/create/recycle timeouts), while the importer still uses one dedicated
  `tokio_postgres::Client` for COPY. Do not add transaction-scoped session
  state such as `SET LOCAL statement_timeout` on a shared multiplexed client;
  use a pool-acquired connection so timeout and aborted-transaction state stay
  scoped to one request. See
  `cratedigger/docs/plans/2026-04-29-002-fix-discogs-api-connection-pool-plan.md`.
