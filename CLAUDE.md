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
nix-shell -p cargo rustc pkg-config openssl postgresql --run "cargo test"
nix-shell -p cargo rustc pkg-config openssl --run "cargo build --release"
```

Tests include parser/unit coverage plus generated SQL conservation checks that
start an ephemeral Nix-provided PostgreSQL instance. PostgreSQL must therefore
be on `PATH` for every `cargo test`; tests never skip or fall back to a live DB.

## Infrastructure

Pure application code — no infrastructure awareness. Both binaries take a passwordless `--dsn`, a private dotenv-compatible `--credential-file` containing a literal `PGPASSWORD=...` line, and `--dump-dir` / `--port`. Password-bearing DSNs are rejected before network activity; credential values never expand the process environment.

- **Host**: the `discogs` LXC at `192.168.1.44` (moved off doc2). `homelab.services.discogs` in `nixosconfig/hosts/discogs/configuration-lxc.nix` with `databaseMode = "native"`.
- **Postgres**: native `postgresql.service` on the LXC, data in `/var/lib/discogs-postgresql` (a verified ZFS dataset mount).
- **API**: `discogs.ablz.au` (port 8086 on the LXC, behind localProxy with auto ACME); Cratedigger consumers hit `192.168.1.44:8086` directly.
- **Data**: `/var/lib/discogs-mirror` on the LXC (re-downloadable, not backed up)
- **NixOS module**: `nixosconfig/modules/nixos/services/discogs.nix`
- **Flake input**: `discogs-src` in nixosconfig's flake.nix (non-flake, `github:abl030/discogs-api`)

## Deploying

Deploy from proxmox-vm/doc1 (where this repo lives, and which holds the Forgejo
token + SSH signing key).

**The service runs on the `discogs` LXC, which has NO `nixos-upgrade.service`
and password-only sudo — `fleet-deploy discogs` and `ssh discogs sudo
fleet-update` do NOT work.** It deploys via **push-deploy** (forgejo#10,
`nixosconfig/modules/nixos/autoupdate/push-deploy.nix`): doc1 builds the
closure locally and hands the store path to a root forced-command key on the
LXC, which realises it from nixcache.ablz.au (nix-serve over doc1's live
store) and switches. The nixosconfig leg goes through Forgejo (`git.ablz.au`);
GitHub's nixosconfig is a frozen, stale fallback. This repo itself still
pushes to GitHub.

```bash
# 1. Merge to this repo's main on GitHub (merge commit).
# 2. Pin nixosconfig to the exact merged SHA (never a bare branch-tip update):
cd ~/nixosconfig && git pull
nix flake update discogs-src --override-input discogs-src github:abl030/discogs-api/<full-sha>
git add flake.lock && SSH_AUTH_SOCK='' git commit -m "discogs: description"   # SSH-signed
# 3. Push to Forgejo master with the token in ENV config, never argv, never echoed:
#    GIT_CONFIG_COUNT=1 GIT_CONFIG_KEY_0='http.https://git.ablz.au.extraHeader'
#    GIT_CONFIG_VALUE_0="Authorization: token $(cat /run/secrets/forgejo/nixbot-token)"
#    git push origin HEAD:refs/heads/master
# 4. Build the LXC closure on doc1 and trigger activation:
nix build ~/nixosconfig#nixosConfigurations.discogs.config.system.build.toplevel --out-link /tmp/discogs-system
env -u SSH_AUTH_SOCK ssh -i /run/secrets/deploy-trigger/key -o IdentitiesOnly=yes -o BatchMode=yes \
  root@192.168.1.44 "$(readlink -f /tmp/discogs-system)"
# 5. Poll until push-activate.service is inactive/success and the generation matches:
env -u SSH_AUTH_SOCK ssh discogs 'systemctl is-active push-activate.service; readlink -f /nix/var/nix/profiles/system'
```

The `discogs-api` service restarts automatically on the switch. The import does
NOT restart (timer-triggered oneshot). The nightly rolling-flake-update CI also
push-deploys this host, so an unmerged pin is picked up automatically overnight.

## Debugging (on the discogs LXC)

```bash
ssh discogs 'systemctl status discogs-api.service'
ssh discogs 'journalctl -u discogs-api.service -f'
ssh discogs 'journalctl -u discogs-import.service -f'
curl -s http://192.168.1.44:8086/health
# Postgres is native on the LXC (data in /var/lib/discogs-postgresql);
# password auth is required — credentials come from the LXC's provisioned
# secret, not doc2's. The old doc2 nspawn path (10.20.0.13) is gone.
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
