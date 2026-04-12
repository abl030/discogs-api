# **RUN `hostname` AT THE START OF EVERY CHAT.**

# Discogs Mirror API

Self-hosted Discogs data mirror in Rust. Downloads monthly CC0 XML dumps from data.discogs.com, imports into PostgreSQL, serves a JSON API at `discogs.ablz.au`.

See `docs/plan.md` for full architecture, schema, and implementation plan.

## Repo Structure

```
src/
  import.rs    -- Binary: XML dump downloader + streaming Postgres COPY loader
  server.rs    -- Binary: axum HTTP JSON API server
  schema.rs    -- DDL constants
  types.rs     -- Typed structs for parsed XML elements + API responses
  xml.rs       -- Streaming XML parser (quick-xml)
  db.rs        -- Postgres helpers (COPY, queries)
  lib.rs       -- Shared library root
docs/
  plan.md      -- Architecture, schema, implementation plan
```

Two binaries: `discogs-import` (oneshot) and `discogs-api` (long-running).

## Infrastructure

Pure application code — no infrastructure awareness. Takes `--dsn` and `--dump-dir` / `--port` as CLI args. The NixOS module (`discogs.nix`) lives in nixosconfig, same pattern as soularr.

- **Postgres**: nspawn container on doc2 (192.168.100.13:5432)
- **API**: `discogs.ablz.au` (port 8086, behind localProxy)
- **Data**: `/mnt/mirrors/discogs` (re-downloadable, not backed up)

## Building & Testing

```bash
nix-shell -p cargo rustc pkg-config openssl --run "cargo build"
nix-shell -p cargo rustc pkg-config openssl --run "cargo test"
```

## Deploying

Same pattern as soularr: push -> flake update on doc1 -> rebuild doc2.

```bash
git push
ssh doc1 'cd ~/nixosconfig && nix flake update discogs-src && git add flake.lock && git commit -m "discogs: description" && git push'
ssh doc2 'sudo nixos-rebuild switch --flake github:abl030/nixosconfig#doc2 --refresh'
```
