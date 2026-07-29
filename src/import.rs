use std::path::{Path, PathBuf};

use clap::Parser;
use futures_util::StreamExt;
use tokio::io::AsyncWriteExt;

use discogs_api::{db, xml, types::*};

const BATCH_SIZE: usize = 10_000;

#[derive(Parser)]
#[command(name = "discogs-import", about = "Download and import Discogs XML dumps into PostgreSQL")]
struct Args {
    /// PostgreSQL connection string
    #[arg(long)]
    dsn: String,

    /// Root-only file containing a literal PGPASSWORD=... line
    #[arg(long)]
    credential_file: PathBuf,

    /// Directory to store downloaded XML dump files
    #[arg(long)]
    dump_dir: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    std::fs::create_dir_all(&args.dump_dir)?;

    // Discover latest dump
    tracing::info!("discovering latest dump date...");
    let date = discover_latest_dump().await?;
    tracing::info!("latest dump: {date}");

    // Download missing files
    for entity in &["artist", "label", "master", "release"] {
        download_dump(&args.dump_dir, &date, entity).await?;
    }

    // Connect and init schema
    let client = db::connect_with_credential(&args.dsn, &args.credential_file).await?;
    db::init_schema(&client).await?;

    // Import each entity type
    let path = dump_path(&args.dump_dir, &date, "artist");
    let count = import_artists(&client, &path).await?;
    tracing::info!("imported {count} artists");

    let path = dump_path(&args.dump_dir, &date, "label");
    let count = import_labels(&client, &path).await?;
    tracing::info!("imported {count} labels");

    let path = dump_path(&args.dump_dir, &date, "master");
    let count = import_masters(&client, &path).await?;
    tracing::info!("imported {count} masters");

    let path = dump_path(&args.dump_dir, &date, "release");
    let count = import_releases(&client, &path).await?;
    tracing::info!("imported {count} releases");

    // Post-import
    db::build_indexes(&client).await?;
    db::vacuum(&client).await?;

    let now = chrono::Utc::now().to_rfc3339();
    db::insert_meta(&client, "last_import", &now).await?;
    db::insert_meta(&client, "dump_date", &date).await?;

    // Clean up old dump files
    cleanup_old_dumps(&args.dump_dir, &date)?;

    tracing::info!("import complete");
    Ok(())
}

// ---------------------------------------------------------------------------
// Dump discovery and download
// ---------------------------------------------------------------------------

fn dump_path(dump_dir: &Path, date: &str, entity: &str) -> PathBuf {
    dump_dir.join(format!("discogs_{date}_{entity}s.xml.gz"))
}

async fn discover_latest_dump() -> anyhow::Result<String> {
    // Index page lists years; we need to fetch the latest year page to find dumps
    let index = reqwest::get("https://data.discogs.com/").await?.text().await?;

    // Find the latest year listed (e.g. "data%2F2026%2F")
    let mut years: Vec<String> = Vec::new();
    for segment in index.split("prefix=data%2F") {
        if segment.len() >= 4 {
            let maybe_year = &segment[..4];
            if maybe_year.bytes().all(|b| b.is_ascii_digit()) {
                years.push(maybe_year.to_string());
            }
        }
    }
    years.sort();
    let year = years.last().ok_or_else(|| anyhow::anyhow!("no years found on data.discogs.com"))?;

    // Fetch the year page to find dump dates
    let year_url = format!("https://data.discogs.com/?prefix=data%2F{year}%2F");
    let year_page = reqwest::get(&year_url).await?.text().await?;

    let mut dates: Vec<String> = Vec::new();
    for segment in year_page.split("discogs_") {
        if segment.len() >= 8 {
            let maybe_date = &segment[..8];
            if maybe_date.bytes().all(|b| b.is_ascii_digit()) {
                dates.push(maybe_date.to_string());
            }
        }
    }
    dates.sort();
    dates.dedup();
    dates.last().cloned().ok_or_else(|| anyhow::anyhow!("no dumps found for {year} on data.discogs.com"))
}

async fn download_dump(dump_dir: &Path, date: &str, entity: &str) -> anyhow::Result<()> {
    let filename = format!("discogs_{date}_{entity}s.xml.gz");
    let path = dump_dir.join(&filename);
    let partial = dump_dir.join(format!("{filename}.partial"));

    if path.exists() {
        tracing::info!("{filename} exists, skipping download");
        return Ok(());
    }

    // Clean up any leftover partial download
    let _ = tokio::fs::remove_file(&partial).await;

    let year = &date[..4];
    let url = format!("https://data.discogs.com/?download=data/{year}/{filename}");
    tracing::info!("downloading {filename}...");

    let resp = reqwest::get(&url).await?.error_for_status()?;
    let mut stream = resp.bytes_stream();
    let mut file = tokio::fs::File::create(&partial).await?;
    let mut bytes_written = 0u64;

    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        file.write_all(&chunk).await?;
        bytes_written += chunk.len() as u64;
    }
    file.flush().await?;
    drop(file);

    // Atomic rename — only a complete file gets the final name
    tokio::fs::rename(&partial, &path).await?;

    tracing::info!("downloaded {filename} ({} MB)", bytes_written / 1_000_000);
    Ok(())
}

fn cleanup_old_dumps(dump_dir: &Path, current_date: &str) -> anyhow::Result<()> {
    let keep_prefix = format!("discogs_{current_date}_");
    let mut removed = 0;
    for entry in std::fs::read_dir(dump_dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with("discogs_") && !name.starts_with(&keep_prefix) {
            std::fs::remove_file(entry.path())?;
            tracing::info!("removed old dump: {name}");
            removed += 1;
        }
    }
    if removed > 0 {
        tracing::info!("cleaned up {removed} old dump files");
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Import pipelines: parse on blocking thread, COPY batches async
// ---------------------------------------------------------------------------

async fn import_artists(client: &tokio_postgres::Client, path: &Path) -> anyhow::Result<usize> {
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<Artist>>(4);
    let path = path.to_owned();

    let handle = tokio::task::spawn_blocking(move || -> anyhow::Result<usize> {
        let file = std::fs::File::open(&path)?;
        let gz = flate2::read::GzDecoder::new(file);
        let reader = std::io::BufReader::new(gz);
        let mut batch = Vec::with_capacity(BATCH_SIZE);
        let count = xml::parse_artists(reader, |entity| {
            batch.push(entity);
            if batch.len() >= BATCH_SIZE {
                let full = std::mem::replace(&mut batch, Vec::with_capacity(BATCH_SIZE));
                let _ = tx.blocking_send(full);
            }
        })?;
        if !batch.is_empty() {
            let _ = tx.blocking_send(batch);
        }
        Ok(count)
    });

    let mut loaded = 0usize;
    while let Some(batch) = rx.recv().await {
        loaded += batch.len();
        db::copy_artists(client, &batch).await?;
        if loaded % 100_000 < BATCH_SIZE { tracing::info!("  artists: {loaded} loaded..."); }
    }
    handle.await?
}

async fn import_labels(client: &tokio_postgres::Client, path: &Path) -> anyhow::Result<usize> {
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<Label>>(4);
    let path = path.to_owned();

    let handle = tokio::task::spawn_blocking(move || -> anyhow::Result<usize> {
        let file = std::fs::File::open(&path)?;
        let gz = flate2::read::GzDecoder::new(file);
        let reader = std::io::BufReader::new(gz);
        let mut batch = Vec::with_capacity(BATCH_SIZE);
        let count = xml::parse_labels(reader, |entity| {
            batch.push(entity);
            if batch.len() >= BATCH_SIZE {
                let full = std::mem::replace(&mut batch, Vec::with_capacity(BATCH_SIZE));
                let _ = tx.blocking_send(full);
            }
        })?;
        if !batch.is_empty() {
            let _ = tx.blocking_send(batch);
        }
        Ok(count)
    });

    let mut loaded = 0usize;
    while let Some(batch) = rx.recv().await {
        loaded += batch.len();
        db::copy_labels(client, &batch).await?;
        if loaded % 100_000 < BATCH_SIZE { tracing::info!("  labels: {loaded} loaded..."); }
    }
    handle.await?
}

async fn import_masters(client: &tokio_postgres::Client, path: &Path) -> anyhow::Result<usize> {
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<Master>>(4);
    let path = path.to_owned();

    let handle = tokio::task::spawn_blocking(move || -> anyhow::Result<usize> {
        let file = std::fs::File::open(&path)?;
        let gz = flate2::read::GzDecoder::new(file);
        let reader = std::io::BufReader::new(gz);
        let mut batch = Vec::with_capacity(BATCH_SIZE);
        let count = xml::parse_masters(reader, |entity| {
            batch.push(entity);
            if batch.len() >= BATCH_SIZE {
                let full = std::mem::replace(&mut batch, Vec::with_capacity(BATCH_SIZE));
                let _ = tx.blocking_send(full);
            }
        })?;
        if !batch.is_empty() {
            let _ = tx.blocking_send(batch);
        }
        Ok(count)
    });

    let mut loaded = 0usize;
    while let Some(batch) = rx.recv().await {
        loaded += batch.len();
        db::copy_masters(client, &batch).await?;
        if loaded % 100_000 < BATCH_SIZE { tracing::info!("  masters: {loaded} loaded..."); }
    }
    handle.await?
}

async fn import_releases(client: &tokio_postgres::Client, path: &Path) -> anyhow::Result<usize> {
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<Release>>(4);
    let path = path.to_owned();

    let handle = tokio::task::spawn_blocking(move || -> anyhow::Result<usize> {
        let file = std::fs::File::open(&path)?;
        let gz = flate2::read::GzDecoder::new(file);
        let reader = std::io::BufReader::new(gz);
        let mut batch = Vec::with_capacity(BATCH_SIZE);
        let count = xml::parse_releases(reader, |entity| {
            batch.push(entity);
            if batch.len() >= BATCH_SIZE {
                let full = std::mem::replace(&mut batch, Vec::with_capacity(BATCH_SIZE));
                let _ = tx.blocking_send(full);
            }
        })?;
        if !batch.is_empty() {
            let _ = tx.blocking_send(batch);
        }
        Ok(count)
    });

    let mut loaded = 0usize;
    while let Some(batch) = rx.recv().await {
        loaded += batch.len();
        db::copy_releases(client, &batch).await?;
        if loaded % 100_000 < BATCH_SIZE { tracing::info!("  releases: {loaded} loaded..."); }
    }
    handle.await?
}
