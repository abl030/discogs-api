# **RUN `hostname` AT THE START OF EVERY CHAT.**

# Discogs Mirror API

Self-hosted Discogs data mirror. Downloads monthly CC0 XML dumps from data.discogs.com, imports into PostgreSQL, serves a JSON API at `discogs.ablz.au`.

See `docs/plan.md` for full architecture, schema, and implementation plan.

## Repo Structure

```
discogs/
  importer.py    -- XML dump downloader + parser + Postgres COPY loader
  server.py      -- HTTP JSON API server (http.server)
  schema.sql     -- DDL for all tables + indexes
  types.py       -- Typed dataclasses for parsed XML elements
tests/
  test_importer.py  -- Pure function + XML parsing tests
  test_server.py    -- Contract tests with REQUIRED_FIELDS per endpoint
docs/
  plan.md           -- Architecture, schema, implementation plan
```

## Infrastructure

This repo is pure application code -- no infrastructure awareness. Takes `--dsn` and `--dump-dir` / `--port` as CLI args. The NixOS module (`discogs.nix`) lives in nixosconfig, same pattern as soularr.

- **Postgres**: nspawn container on doc2 (192.168.100.13:5432)
- **API**: `discogs.ablz.au` (port 8086, behind localProxy)
- **Data**: `/mnt/mirrors/discogs` (re-downloadable, not backed up)

## Running Tests

```bash
nix-shell --run "python3 -m unittest discover tests -v"
```

## Deploying

Same pattern as soularr: push -> flake update on doc1 -> rebuild doc2.

```bash
# 1. Push code
git push

# 2. Flake update on doc1
ssh doc1 'cd ~/nixosconfig && nix flake update discogs-src && git add flake.lock && git commit -m "discogs: description" && git push'

# 3. Deploy to doc2
ssh doc2 'sudo nixos-rebuild switch --flake github:abl030/nixosconfig#doc2 --refresh'
```

## Critical Rules

- All new code must pass pyright with 0 errors
- Use typed dataclasses, not dicts, for structured data
- Tests first (RED), then implement (GREEN)
- Contract tests with REQUIRED_FIELDS for every API endpoint
- Full table replacement on import (DROP CASCADE + CREATE), no incremental updates
