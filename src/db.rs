use std::collections::{BTreeSet, HashMap};
use std::fmt;
use std::pin::pin;
use std::str::FromStr;
use std::time::Duration;

use deadpool_postgres::{
    Client as PooledClient, Manager, ManagerConfig, Pool, PoolError, RecyclingMethod, Runtime,
    Transaction as PooledTransaction,
};
use tokio_postgres::binary_copy::BinaryCopyInWriter;
use tokio_postgres::error::SqlState;
use tokio_postgres::types::{ToSql, Type};
use tokio_postgres::{Client, NoTls};

use crate::schema;
use crate::types::*;

/// Sentinel error raised when the recursive label-releases query exceeds the
/// transaction-scoped statement timeout. The HTTP layer maps this to 503 so
/// callers can retry without sub-labels.
#[derive(Debug)]
pub struct LabelReleasesTimeout;

impl fmt::Display for LabelReleasesTimeout {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "label releases query exceeded statement_timeout")
    }
}

impl std::error::Error for LabelReleasesTimeout {}

/// Sentinel error raised when the PostgreSQL pool cannot hand out a connection
/// within its configured timeout. HTTP handlers map this to 503.
#[derive(Debug)]
pub struct PoolUnavailable;

impl fmt::Display for PoolUnavailable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "postgres connection pool unavailable")
    }
}

impl std::error::Error for PoolUnavailable {}

fn pool_get_error(err: PoolError) -> anyhow::Error {
    match err {
        PoolError::Timeout(_) => anyhow::Error::new(PoolUnavailable),
        other => anyhow::Error::new(other),
    }
}

async fn get_client(pool: &Pool) -> anyhow::Result<PooledClient> {
    pool.get().await.map_err(pool_get_error)
}

fn is_query_canceled(err: &tokio_postgres::Error) -> bool {
    err.code() == Some(&SqlState::QUERY_CANCELED)
}

fn label_releases_query_error(err: tokio_postgres::Error) -> anyhow::Error {
    if is_query_canceled(&err) {
        anyhow::Error::new(LabelReleasesTimeout)
    } else {
        anyhow::Error::new(err)
    }
}

// ---------------------------------------------------------------------------
// Connection
// ---------------------------------------------------------------------------

pub async fn connect(dsn: &str) -> anyhow::Result<Client> {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::error!("postgres connection error: {e}");
        }
    });
    Ok(client)
}

pub async fn create_pool(dsn: &str) -> anyhow::Result<Pool> {
    let mut pg_config = tokio_postgres::Config::from_str(dsn)?;
    pg_config.connect_timeout(Duration::from_secs(5));
    let manager = Manager::from_config(
        pg_config,
        NoTls,
        ManagerConfig {
            recycling_method: RecyclingMethod::Verified,
        },
    );
    let pool = Pool::builder(manager)
        .max_size(16)
        .runtime(Runtime::Tokio1)
        .wait_timeout(Some(Duration::from_secs(2)))
        .create_timeout(Some(Duration::from_secs(5)))
        .recycle_timeout(Some(Duration::from_secs(5)))
        .build()?;

    // Deadpool creates connections lazily; acquire once so startup fails fast
    // when the DSN is wrong or Postgres is unreachable.
    let client = get_client(&pool).await?;
    drop(client);

    Ok(pool)
}

// ---------------------------------------------------------------------------
// Schema operations
// ---------------------------------------------------------------------------

pub async fn init_schema(client: &Client) -> anyhow::Result<()> {
    tracing::info!("dropping existing tables...");
    client.batch_execute(schema::DROP_ALL).await?;
    tracing::info!("creating tables...");
    client.batch_execute(schema::CREATE_TABLES).await?;
    Ok(())
}

pub async fn build_indexes(client: &Client) -> anyhow::Result<()> {
    tracing::info!("building indexes...");
    client.batch_execute(schema::CREATE_INDEXES).await?;
    Ok(())
}

pub async fn vacuum(client: &Client) -> anyhow::Result<()> {
    tracing::info!("running VACUUM ANALYZE...");
    // VACUUM cannot run inside a transaction block, so execute each statement
    // individually (batch_execute wraps in an implicit transaction).
    for line in schema::VACUUM_ANALYZE.lines() {
        let stmt = line.trim();
        if stmt.is_empty() {
            continue;
        }
        client.execute(stmt, &[]).await?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// COPY helpers (binary format for speed)
// ---------------------------------------------------------------------------

pub async fn copy_artists(client: &Client, artists: &[Artist]) -> anyhow::Result<()> {
    if artists.is_empty() {
        return Ok(());
    }

    // artist table
    {
        let sink = client.copy_in(
            "COPY artist (id, name, realname, profile, data_quality) FROM STDIN WITH (FORMAT binary)"
        ).await?;
        let mut w = pin!(BinaryCopyInWriter::new(
            sink,
            &[Type::INT4, Type::TEXT, Type::TEXT, Type::TEXT, Type::TEXT]
        ));
        for a in artists {
            w.as_mut()
                .write(&[
                    &a.id as &(dyn ToSql + Sync),
                    &a.name as _,
                    &a.realname as _,
                    &a.profile as _,
                    &a.data_quality as _,
                ])
                .await?;
        }
        w.as_mut().finish().await?;
    }

    // artist_alias
    if artists.iter().any(|a| !a.aliases.is_empty()) {
        let sink = client
            .copy_in(
                "COPY artist_alias (artist_id, alias_id, name) FROM STDIN WITH (FORMAT binary)",
            )
            .await?;
        let mut w = pin!(BinaryCopyInWriter::new(
            sink,
            &[Type::INT4, Type::INT4, Type::TEXT]
        ));
        for a in artists {
            for al in &a.aliases {
                w.as_mut()
                    .write(&[
                        &a.id as &(dyn ToSql + Sync),
                        &al.alias_id as _,
                        &al.name as _,
                    ])
                    .await?;
            }
        }
        w.as_mut().finish().await?;
    }

    // artist_namevariation
    if artists.iter().any(|a| !a.namevariations.is_empty()) {
        let sink = client
            .copy_in("COPY artist_namevariation (artist_id, name) FROM STDIN WITH (FORMAT binary)")
            .await?;
        let mut w = pin!(BinaryCopyInWriter::new(sink, &[Type::INT4, Type::TEXT]));
        for a in artists {
            for nv in &a.namevariations {
                w.as_mut()
                    .write(&[&a.id as &(dyn ToSql + Sync), nv as _])
                    .await?;
            }
        }
        w.as_mut().finish().await?;
    }

    Ok(())
}

pub async fn copy_labels(client: &Client, labels: &[Label]) -> anyhow::Result<()> {
    if labels.is_empty() {
        return Ok(());
    }

    let sink = client.copy_in(
        "COPY label (id, name, contactinfo, profile, parent_label_id, data_quality) FROM STDIN WITH (FORMAT binary)"
    ).await?;
    let mut w = pin!(BinaryCopyInWriter::new(
        sink,
        &[
            Type::INT4,
            Type::TEXT,
            Type::TEXT,
            Type::TEXT,
            Type::INT4,
            Type::TEXT
        ]
    ));
    for l in labels {
        w.as_mut()
            .write(&[
                &l.id as &(dyn ToSql + Sync),
                &l.name as _,
                &l.contactinfo as _,
                &l.profile as _,
                &l.parent_label_id as _,
                &l.data_quality as _,
            ])
            .await?;
    }
    w.as_mut().finish().await?;
    Ok(())
}

pub async fn copy_masters(client: &Client, masters: &[Master]) -> anyhow::Result<()> {
    if masters.is_empty() {
        return Ok(());
    }

    // master table
    {
        let sink = client.copy_in(
            "COPY master (id, title, year, main_release_id, data_quality) FROM STDIN WITH (FORMAT binary)"
        ).await?;
        let mut w = pin!(BinaryCopyInWriter::new(
            sink,
            &[Type::INT4, Type::TEXT, Type::INT4, Type::INT4, Type::TEXT]
        ));
        for m in masters {
            w.as_mut()
                .write(&[
                    &m.id as &(dyn ToSql + Sync),
                    &m.title as _,
                    &m.year as _,
                    &m.main_release_id as _,
                    &m.data_quality as _,
                ])
                .await?;
        }
        w.as_mut().finish().await?;
    }

    // master_artist
    if masters.iter().any(|m| !m.artists.is_empty()) {
        let sink = client.copy_in(
            "COPY master_artist (master_id, artist_id, artist_name, role, anv) FROM STDIN WITH (FORMAT binary)"
        ).await?;
        let mut w = pin!(BinaryCopyInWriter::new(
            sink,
            &[Type::INT4, Type::INT4, Type::TEXT, Type::TEXT, Type::TEXT]
        ));
        for m in masters {
            for a in &m.artists {
                w.as_mut()
                    .write(&[
                        &m.id as &(dyn ToSql + Sync),
                        &a.artist_id as _,
                        &a.artist_name as _,
                        &a.role as _,
                        &a.anv as _,
                    ])
                    .await?;
            }
        }
        w.as_mut().finish().await?;
    }

    Ok(())
}

pub async fn copy_releases(client: &Client, releases: &[Release]) -> anyhow::Result<()> {
    if releases.is_empty() {
        return Ok(());
    }

    // release table
    {
        let sink = client.copy_in(
            "COPY release (id, title, country, released, notes, master_id, status, data_quality, search_text) FROM STDIN WITH (FORMAT binary)"
        ).await?;
        let mut w = pin!(BinaryCopyInWriter::new(
            sink,
            &[
                Type::INT4,
                Type::TEXT,
                Type::TEXT,
                Type::TEXT,
                Type::TEXT,
                Type::INT4,
                Type::TEXT,
                Type::TEXT,
                Type::TEXT,
            ]
        ));
        for r in releases {
            let search_text = build_search_text(&r.title, &r.artists);
            w.as_mut()
                .write(&[
                    &r.id as &(dyn ToSql + Sync),
                    &r.title as _,
                    &r.country as _,
                    &r.released as _,
                    &r.notes as _,
                    &r.master_id as _,
                    &r.status as _,
                    &r.data_quality as _,
                    &search_text as _,
                ])
                .await?;
        }
        w.as_mut().finish().await?;
    }

    // release_artist
    if releases.iter().any(|r| !r.artists.is_empty()) {
        let sink = client.copy_in(
            "COPY release_artist (release_id, artist_id, artist_name, role, anv, join_relation) FROM STDIN WITH (FORMAT binary)"
        ).await?;
        let mut w = pin!(BinaryCopyInWriter::new(
            sink,
            &[
                Type::INT4,
                Type::INT4,
                Type::TEXT,
                Type::TEXT,
                Type::TEXT,
                Type::TEXT,
            ]
        ));
        for r in releases {
            for a in &r.artists {
                w.as_mut()
                    .write(&[
                        &r.id as &(dyn ToSql + Sync),
                        &a.artist_id as _,
                        &a.artist_name as _,
                        &a.role as _,
                        &a.anv as _,
                        &a.join_relation as _,
                    ])
                    .await?;
            }
        }
        w.as_mut().finish().await?;
    }

    // release_label
    if releases.iter().any(|r| !r.labels.is_empty()) {
        let sink = client.copy_in(
            "COPY release_label (release_id, label_id, label_name, catno) FROM STDIN WITH (FORMAT binary)"
        ).await?;
        let mut w = pin!(BinaryCopyInWriter::new(
            sink,
            &[Type::INT4, Type::INT4, Type::TEXT, Type::TEXT]
        ));
        for r in releases {
            for l in &r.labels {
                w.as_mut()
                    .write(&[
                        &r.id as &(dyn ToSql + Sync),
                        &l.label_id as _,
                        &l.label_name as _,
                        &l.catno as _,
                    ])
                    .await?;
            }
        }
        w.as_mut().finish().await?;
    }

    // release_format
    if releases.iter().any(|r| !r.formats.is_empty()) {
        let sink = client.copy_in(
            "COPY release_format (release_id, name, qty, descriptions, free_text) FROM STDIN WITH (FORMAT binary)"
        ).await?;
        let mut w = pin!(BinaryCopyInWriter::new(
            sink,
            &[Type::INT4, Type::TEXT, Type::INT4, Type::TEXT, Type::TEXT]
        ));
        for r in releases {
            for f in &r.formats {
                w.as_mut()
                    .write(&[
                        &r.id as &(dyn ToSql + Sync),
                        &f.name as _,
                        &f.qty as _,
                        &f.descriptions as _,
                        &f.free_text as _,
                    ])
                    .await?;
            }
        }
        w.as_mut().finish().await?;
    }

    // release_track
    if releases.iter().any(|r| !r.tracks.is_empty()) {
        let sink = client.copy_in(
            "COPY release_track (release_id, sequence, position, title, duration) FROM STDIN WITH (FORMAT binary)"
        ).await?;
        let mut w = pin!(BinaryCopyInWriter::new(
            sink,
            &[Type::INT4, Type::INT4, Type::TEXT, Type::TEXT, Type::TEXT]
        ));
        for r in releases {
            for t in &r.tracks {
                w.as_mut()
                    .write(&[
                        &r.id as &(dyn ToSql + Sync),
                        &t.sequence as _,
                        &t.position as _,
                        &t.title as _,
                        &t.duration as _,
                    ])
                    .await?;
            }
        }
        w.as_mut().finish().await?;
    }

    // release_track_artist
    if releases
        .iter()
        .any(|r| r.tracks.iter().any(|t| !t.artists.is_empty()))
    {
        let sink = client.copy_in(
            "COPY release_track_artist (release_id, sequence, artist_id, artist_name, role, anv) FROM STDIN WITH (FORMAT binary)"
        ).await?;
        let mut w = pin!(BinaryCopyInWriter::new(
            sink,
            &[
                Type::INT4,
                Type::INT4,
                Type::INT4,
                Type::TEXT,
                Type::TEXT,
                Type::TEXT,
            ]
        ));
        for r in releases {
            for t in &r.tracks {
                for a in &t.artists {
                    w.as_mut()
                        .write(&[
                            &r.id as &(dyn ToSql + Sync),
                            &t.sequence as _,
                            &a.artist_id as _,
                            &a.artist_name as _,
                            &a.role as _,
                            &a.anv as _,
                        ])
                        .await?;
                }
            }
        }
        w.as_mut().finish().await?;
    }

    // release_genre
    if releases.iter().any(|r| !r.genres.is_empty()) {
        let sink = client
            .copy_in("COPY release_genre (release_id, genre) FROM STDIN WITH (FORMAT binary)")
            .await?;
        let mut w = pin!(BinaryCopyInWriter::new(sink, &[Type::INT4, Type::TEXT]));
        for r in releases {
            for g in &r.genres {
                w.as_mut()
                    .write(&[&r.id as &(dyn ToSql + Sync), g as _])
                    .await?;
            }
        }
        w.as_mut().finish().await?;
    }

    // release_style
    if releases.iter().any(|r| !r.styles.is_empty()) {
        let sink = client
            .copy_in("COPY release_style (release_id, style) FROM STDIN WITH (FORMAT binary)")
            .await?;
        let mut w = pin!(BinaryCopyInWriter::new(sink, &[Type::INT4, Type::TEXT]));
        for r in releases {
            for s in &r.styles {
                w.as_mut()
                    .write(&[&r.id as &(dyn ToSql + Sync), s as _])
                    .await?;
            }
        }
        w.as_mut().finish().await?;
    }

    // release_identifier
    if releases.iter().any(|r| !r.identifiers.is_empty()) {
        let sink = client.copy_in(
            "COPY release_identifier (release_id, type, value, description) FROM STDIN WITH (FORMAT binary)"
        ).await?;
        let mut w = pin!(BinaryCopyInWriter::new(
            sink,
            &[Type::INT4, Type::TEXT, Type::TEXT, Type::TEXT]
        ));
        for r in releases {
            for i in &r.identifiers {
                w.as_mut()
                    .write(&[
                        &r.id as &(dyn ToSql + Sync),
                        &i.type_ as _,
                        &i.value as _,
                        &i.description as _,
                    ])
                    .await?;
            }
        }
        w.as_mut().finish().await?;
    }

    Ok(())
}

pub async fn insert_meta(client: &Client, key: &str, value: &str) -> anyhow::Result<()> {
    client.execute(
        "INSERT INTO import_meta (key, value) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET value = $2",
        &[&key, &value],
    ).await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Query helpers (API server)
// ---------------------------------------------------------------------------

pub async fn query_health(pool: &Pool) -> anyhow::Result<HealthResponse> {
    let client = get_client(pool).await?;
    let client = &**client;

    // Tables may not exist before first import — handle gracefully
    let releases = match client
        .query_one("SELECT count(*) AS cnt FROM release", &[])
        .await
    {
        Ok(row) => row.get::<_, i64>("cnt"),
        Err(_) => 0,
    };

    let last_import = match client
        .query_opt(
            "SELECT value FROM import_meta WHERE key = 'last_import'",
            &[],
        )
        .await
    {
        Ok(Some(r)) => r.get::<_, String>("value"),
        _ => String::new(),
    };

    let dump_date = match client
        .query_opt("SELECT value FROM import_meta WHERE key = 'dump_date'", &[])
        .await
    {
        Ok(Some(r)) => r.get::<_, String>("value"),
        _ => String::new(),
    };

    let status = if releases > 0 {
        "ok"
    } else {
        "awaiting_import"
    };
    Ok(HealthResponse {
        status: status.to_string(),
        releases,
        last_import,
        dump_date,
    })
}

pub async fn query_search(
    pool: &Pool,
    title: Option<&str>,
    artist: Option<&str>,
    artist_id: Option<i32>,
    page: i32,
    per_page: i32,
) -> anyhow::Result<SearchResponse> {
    let client = get_client(pool).await?;
    let client = &**client;

    let per_page = per_page.clamp(1, 100);
    let page = page.max(1);
    let limit = per_page as i64;
    let offset = (page as i64 - 1) * limit;

    if title.is_none() && artist.is_none() && artist_id.is_none() {
        return Ok(SearchResponse {
            results: vec![],
            page,
            per_page,
        });
    }

    // Combine non-null terms into a single scoring query against search_text (title + artists).
    let score_q: String = match (title, artist) {
        (Some(t), Some(a)) => format!("{} {}", t, a),
        (Some(t), None) => t.to_string(),
        (None, Some(a)) => a.to_string(),
        (None, None) => String::new(),
    };

    let rows = client.query(
        "SELECT r.id, r.title, r.country, r.released, r.master_id,
                m.title AS master_title,
                (SELECT MIN(r2.released) FROM release r2
                   WHERE r2.master_id = r.master_id AND r2.released <> '') AS master_first_released,
                ts_rank(to_tsvector('english', r.search_text),
                        plainto_tsquery('english', $5)) AS score
         FROM release r
         LEFT JOIN master m ON m.id = r.master_id
         WHERE ($1::text IS NULL OR to_tsvector('english', r.title) @@ plainto_tsquery('english', $1))
           AND ($2::text IS NULL OR EXISTS (
                SELECT 1 FROM release_artist ra
                JOIN artist a ON ra.artist_id = a.id
                WHERE ra.release_id = r.id
                AND to_tsvector('english', a.name) @@ plainto_tsquery('english', $2)))
           AND ($6::int IS NULL OR EXISTS (
                SELECT 1 FROM release_artist rai
                WHERE rai.release_id = r.id AND rai.artist_id = $6))
         ORDER BY score DESC, r.id LIMIT $3 OFFSET $4",
        &[&title, &artist, &limit, &offset, &score_q, &artist_id],
    ).await?;

    let release_ids: Vec<i32> = rows.iter().map(|r| r.get("id")).collect();
    if release_ids.is_empty() {
        return Ok(SearchResponse {
            results: vec![],
            page,
            per_page,
        });
    }

    let (artist_map, label_map, format_map) =
        fetch_release_enrichments(client, &release_ids).await?;

    let results = rows
        .iter()
        .map(|r| {
            let id: i32 = r.get("id");
            let formats = format_map.get(&id).cloned().unwrap_or_default();
            let descs: Vec<String> = formats.iter().map(|f| f.descriptions.clone()).collect();
            let primary_type = infer_primary_type(&descs);
            SearchResult {
                id,
                title: r.get("title"),
                country: r.get("country"),
                released: r.get("released"),
                master_id: r.get("master_id"),
                master_title: r.get("master_title"),
                master_first_released: r.get("master_first_released"),
                primary_type,
                score: r.get::<_, f32>("score"),
                artists: artist_map.get(&id).cloned().unwrap_or_default(),
                labels: label_map.get(&id).cloned().unwrap_or_default(),
                formats,
            }
        })
        .collect();

    Ok(SearchResponse {
        results,
        page,
        per_page,
    })
}

pub async fn query_release(pool: &Pool, id: i32) -> anyhow::Result<Option<ReleaseDetail>> {
    let client = get_client(pool).await?;
    let client = &**client;

    let row = match client
        .query_opt(
            "SELECT id, title, country, released, master_id FROM release WHERE id = $1",
            &[&id],
        )
        .await?
    {
        Some(r) => r,
        None => return Ok(None),
    };

    let ids = vec![id];
    let (artist_map, label_map, format_map) = fetch_release_enrichments(client, &ids).await?;

    let track_rows = client.query(
        "SELECT sequence, position, title, duration FROM release_track WHERE release_id = $1 ORDER BY sequence",
        &[&id],
    ).await?;

    let track_artist_rows = client.query(
        "SELECT sequence, artist_id, artist_name, role, anv FROM release_track_artist WHERE release_id = $1 ORDER BY sequence",
        &[&id],
    ).await?;
    let mut track_artist_map: HashMap<i32, Vec<ApiArtistCredit>> = HashMap::new();
    for r in &track_artist_rows {
        let seq: i32 = r.get("sequence");
        track_artist_map
            .entry(seq)
            .or_default()
            .push(ApiArtistCredit {
                id: r.get("artist_id"),
                name: r.get("artist_name"),
                role: r.get("role"),
                anv: r.get("anv"),
            });
    }

    let tracks: Vec<ApiTrack> = track_rows
        .iter()
        .map(|r| {
            let seq: i32 = r.get("sequence");
            ApiTrack {
                position: r.get("position"),
                title: r.get("title"),
                duration: r.get("duration"),
                artists: track_artist_map.remove(&seq).unwrap_or_default(),
            }
        })
        .collect();

    let genre_rows = client
        .query(
            "SELECT genre FROM release_genre WHERE release_id = $1",
            &[&id],
        )
        .await?;
    let genres: Vec<String> = genre_rows.iter().map(|r| r.get("genre")).collect();

    let style_rows = client
        .query(
            "SELECT style FROM release_style WHERE release_id = $1",
            &[&id],
        )
        .await?;
    let styles: Vec<String> = style_rows.iter().map(|r| r.get("style")).collect();

    let ident_rows = client
        .query(
            "SELECT type, value, description FROM release_identifier WHERE release_id = $1",
            &[&id],
        )
        .await?;
    let identifiers: Vec<ApiIdentifier> = ident_rows
        .iter()
        .map(|r| ApiIdentifier {
            type_: r.get("type"),
            value: r.get("value"),
            description: r.get("description"),
        })
        .collect();

    Ok(Some(ReleaseDetail {
        id,
        title: row.get("title"),
        country: row.get("country"),
        released: row.get("released"),
        master_id: row.get("master_id"),
        artists: artist_map.get(&id).cloned().unwrap_or_default(),
        labels: label_map.get(&id).cloned().unwrap_or_default(),
        formats: format_map.get(&id).cloned().unwrap_or_default(),
        tracks,
        genres,
        styles,
        identifiers,
    }))
}

pub async fn query_master(pool: &Pool, id: i32) -> anyhow::Result<Option<MasterDetail>> {
    let client = get_client(pool).await?;
    let client = &**client;

    let row = match client
        .query_opt(
            "SELECT id, title, year, main_release_id FROM master WHERE id = $1",
            &[&id],
        )
        .await?
    {
        Some(r) => r,
        None => return Ok(None),
    };
    let main_release_id: Option<i32> = row.get("main_release_id");

    let artist_rows = client
        .query(
            "SELECT artist_id, artist_name, role, anv FROM master_artist WHERE master_id = $1",
            &[&id],
        )
        .await?;
    let artists: Vec<ApiArtistCredit> = artist_rows
        .iter()
        .map(|r| ApiArtistCredit {
            id: r.get("artist_id"),
            name: r.get("artist_name"),
            role: r.get("role"),
            anv: r.get("anv"),
        })
        .collect();
    // master_artist has no join column — use " & " separator.
    let credit_pairs: Vec<(String, String)> = artists
        .iter()
        .map(|a| (a.name.clone(), String::new()))
        .collect();
    let artist_credit = join_artist_credit(&credit_pairs);
    let primary_artist_id = artists.first().map(|a| a.id);

    let release_rows = client
        .query(
            "SELECT id, title, country, released FROM release WHERE master_id = $1 ORDER BY id",
            &[&id],
        )
        .await?;
    let release_ids: Vec<i32> = release_rows.iter().map(|r| r.get("id")).collect();

    // Earliest non-empty released date across child releases.
    let first_release_date: String = release_rows
        .iter()
        .filter_map(|r| {
            let d: String = r.get("released");
            if d.is_empty() { None } else { Some(d) }
        })
        .min()
        .unwrap_or_default();

    let (_, label_map, format_map) = if release_ids.is_empty() {
        (HashMap::new(), HashMap::new(), HashMap::new())
    } else {
        fetch_release_enrichments(client, &release_ids).await?
    };

    let track_count_map: HashMap<i32, i32> = if release_ids.is_empty() {
        HashMap::new()
    } else {
        let track_rows = client.query(
            "SELECT release_id, COUNT(*)::INT AS c FROM release_track WHERE release_id = ANY($1) GROUP BY release_id",
            &[&release_ids],
        ).await?;
        track_rows
            .iter()
            .map(|r| (r.get::<_, i32>("release_id"), r.get::<_, i32>("c")))
            .collect()
    };

    // Pick rep release for primary_type: main_release_id if present, else the
    // earliest-dated release, else the first by id.
    let rep_release_id: Option<i32> = main_release_id
        .filter(|id| release_ids.contains(id))
        .or_else(|| {
            release_rows
                .iter()
                .min_by(|a, b| {
                    let ad: String = a.get("released");
                    let bd: String = b.get("released");
                    let ak = if ad.is_empty() {
                        "9999".to_string()
                    } else {
                        ad
                    };
                    let bk = if bd.is_empty() {
                        "9999".to_string()
                    } else {
                        bd
                    };
                    ak.cmp(&bk)
                })
                .map(|r| r.get("id"))
        });
    let primary_type = match rep_release_id.and_then(|rid| format_map.get(&rid)) {
        Some(formats) => {
            let descs: Vec<String> = formats.iter().map(|f| f.descriptions.clone()).collect();
            infer_primary_type(&descs)
        }
        None => "Other".to_string(),
    };

    let releases: Vec<MasterRelease> = release_rows
        .iter()
        .map(|r| {
            let rid: i32 = r.get("id");
            MasterRelease {
                id: rid,
                title: r.get("title"),
                country: r.get("country"),
                released: r.get("released"),
                track_count: track_count_map.get(&rid).copied().unwrap_or(0),
                formats: format_map.get(&rid).cloned().unwrap_or_default(),
                labels: label_map.get(&rid).cloned().unwrap_or_default(),
            }
        })
        .collect();

    Ok(Some(MasterDetail {
        id,
        title: row.get("title"),
        year: row.get("year"),
        main_release_id,
        primary_type,
        first_release_date,
        artist_credit,
        primary_artist_id,
        artists,
        releases,
    }))
}

pub async fn query_artist(pool: &Pool, id: i32) -> anyhow::Result<Option<ArtistDetail>> {
    let client = get_client(pool).await?;
    let client = &**client;

    let row = match client
        .query_opt(
            "SELECT id, name, realname, profile FROM artist WHERE id = $1",
            &[&id],
        )
        .await?
    {
        Some(r) => r,
        None => return Ok(None),
    };

    let alias_rows = client
        .query(
            "SELECT alias_id, name FROM artist_alias WHERE artist_id = $1",
            &[&id],
        )
        .await?;
    let aliases: Vec<ApiAlias> = alias_rows
        .iter()
        .map(|r| ApiAlias {
            id: r.get("alias_id"),
            name: r.get("name"),
        })
        .collect();

    let nv_rows = client
        .query(
            "SELECT name FROM artist_namevariation WHERE artist_id = $1",
            &[&id],
        )
        .await?;
    let namevariations: Vec<String> = nv_rows.iter().map(|r| r.get("name")).collect();

    Ok(Some(ArtistDetail {
        id,
        name: row.get("name"),
        realname: row.get("realname"),
        profile: row.get("profile"),
        aliases,
        namevariations,
    }))
}

pub async fn query_artist_releases(
    pool: &Pool,
    artist_id: i32,
    page: i32,
    per_page: i32,
) -> anyhow::Result<Option<ArtistReleasesResponse>> {
    let client = get_client(pool).await?;
    let client = &**client;

    // Check artist exists
    let exists = client
        .query_opt("SELECT 1 FROM artist WHERE id = $1", &[&artist_id])
        .await?;
    if exists.is_none() {
        return Ok(None);
    }

    let per_page = per_page.clamp(1, 100);
    let page = page.max(1);
    let limit = per_page as i64;
    let offset = (page as i64 - 1) * limit;

    let count_row = client.query_one(
        "SELECT count(*) AS cnt FROM release r JOIN release_artist ra ON ra.release_id = r.id WHERE ra.artist_id = $1",
        &[&artist_id],
    ).await?;
    let total: i64 = count_row.get("cnt");

    let rows = client
        .query(
            "SELECT r.id, r.title, r.country, r.released, r.master_id,
                m.title AS master_title,
                (SELECT MIN(r2.released) FROM release r2
                   WHERE r2.master_id = r.master_id AND r2.released <> '') AS master_first_released
         FROM release r
         LEFT JOIN master m ON m.id = r.master_id
         JOIN release_artist ra ON ra.release_id = r.id
         WHERE ra.artist_id = $1
         ORDER BY r.released, r.id
         LIMIT $2 OFFSET $3",
            &[&artist_id, &limit, &offset],
        )
        .await?;

    let release_ids: Vec<i32> = rows.iter().map(|r| r.get("id")).collect();
    let (artist_map, label_map, format_map) = if release_ids.is_empty() {
        (HashMap::new(), HashMap::new(), HashMap::new())
    } else {
        fetch_release_enrichments(client, &release_ids).await?
    };

    let results = rows
        .iter()
        .map(|r| {
            let id: i32 = r.get("id");
            let formats = format_map.get(&id).cloned().unwrap_or_default();
            let descs: Vec<String> = formats.iter().map(|f| f.descriptions.clone()).collect();
            let primary_type = infer_primary_type(&descs);
            SearchResult {
                id,
                title: r.get("title"),
                country: r.get("country"),
                released: r.get("released"),
                master_id: r.get("master_id"),
                master_title: r.get("master_title"),
                master_first_released: r.get("master_first_released"),
                primary_type,
                score: 0.0,
                artists: artist_map.get(&id).cloned().unwrap_or_default(),
                labels: label_map.get(&id).cloned().unwrap_or_default(),
                formats,
            }
        })
        .collect();

    let pages = if per_page > 0 {
        ((total as f64) / (per_page as f64)).ceil() as i32
    } else {
        1
    };

    Ok(Some(ArtistReleasesResponse {
        results,
        pagination: Pagination {
            page,
            per_page,
            pages,
            items: total,
        },
    }))
}

pub async fn query_artist_search(
    pool: &Pool,
    name: &str,
    page: i32,
    per_page: i32,
) -> anyhow::Result<ArtistSearchResponse> {
    let client = get_client(pool).await?;
    let client = &**client;

    let per_page = per_page.clamp(1, 100);
    let page = page.max(1);
    let limit = per_page as i64;
    let offset = (page as i64 - 1) * limit;

    let count_row = client
        .query_one(
            "SELECT count(*) AS cnt FROM artist
         WHERE to_tsvector('english', name) @@ plainto_tsquery('english', $1)",
            &[&name],
        )
        .await?;
    let total: i64 = count_row.get("cnt");

    let rows = client
        .query(
            "SELECT id, name, profile,
                ts_rank(to_tsvector('english', name), plainto_tsquery('english', $1)) AS score,
                (lower(name) = lower($1)) AS exact_match
         FROM artist
         WHERE to_tsvector('english', name) @@ plainto_tsquery('english', $1)
         ORDER BY exact_match DESC, score DESC, length(name), id
         LIMIT $2 OFFSET $3",
            &[&name, &limit, &offset],
        )
        .await?;

    let results = rows
        .iter()
        .map(|r| {
            let profile: String = r.get("profile");
            ArtistSearchResult {
                id: r.get("id"),
                name: r.get("name"),
                profile: truncate_profile(&profile, 200),
                score: r.get::<_, f32>("score"),
            }
        })
        .collect();

    Ok(ArtistSearchResponse {
        results,
        total,
        page,
        per_page,
    })
}

pub async fn query_artist_masters(
    pool: &Pool,
    artist_id: i32,
    page: i32,
    per_page: i32,
) -> anyhow::Result<Option<ArtistMastersResponse>> {
    let client = get_client(pool).await?;
    let client = &**client;

    let exists = client
        .query_opt("SELECT 1 FROM artist WHERE id = $1", &[&artist_id])
        .await?;
    if exists.is_none() {
        return Ok(None);
    }

    let per_page = per_page.clamp(1, 100);
    let page = page.max(1);
    let limit = per_page as i64;
    let offset = (page as i64 - 1) * limit;

    // The Discogs dump uses `master_id = 0` as the masterless sentinel
    // (not NULL). Normalise both 0 and NULL to "no master" here so the
    // count agrees with the projection — see the `NULLIF(r.master_id, 0)`
    // alias in the data CTE below. Without this, an artist whose releases
    // are all masterless reports `total > 0` but yields an empty result
    // set (count(DISTINCT 0) = 1, but `master` has no id=0 row).
    let count_row = client
        .query_one(
            "SELECT
           (SELECT count(DISTINCT r.master_id) FROM release r
              JOIN release_artist ra ON ra.release_id = r.id
              WHERE ra.artist_id = $1 AND r.master_id IS NOT NULL AND r.master_id <> 0)
         + (SELECT count(*) FROM release r
              JOIN release_artist ra ON ra.release_id = r.id
              WHERE ra.artist_id = $1 AND (r.master_id IS NULL OR r.master_id = 0))
         AS total",
            &[&artist_id],
        )
        .await?;
    let total: i64 = count_row.get("total");

    let rows = client.query(
        "WITH ar AS (
             SELECT r.id, r.title, NULLIF(r.master_id, 0) AS master_id,
                    NULLIF(r.released, '') AS released
             FROM release r
             JOIN release_artist ra ON ra.release_id = r.id
             WHERE ra.artist_id = $1
         ),
         combined AS (
             SELECT 'master'::text AS kind,
                    m.id AS entry_id,
                    m.title AS title,
                    COALESCE(
                        m.main_release_id,
                        (SELECT ar2.id FROM ar ar2 WHERE ar2.master_id = m.id
                         ORDER BY ar2.released NULLS LAST, ar2.id LIMIT 1)
                    ) AS rep_release_id,
                    (SELECT MIN(ar2.released) FROM ar ar2 WHERE ar2.master_id = m.id) AS first_release_date
             FROM master m
             WHERE EXISTS (SELECT 1 FROM ar WHERE ar.master_id = m.id)
             UNION ALL
             SELECT 'release'::text AS kind,
                    ar.id AS entry_id,
                    ar.title AS title,
                    ar.id AS rep_release_id,
                    ar.released AS first_release_date
             FROM ar
             WHERE ar.master_id IS NULL
         )
         SELECT kind, entry_id, title, rep_release_id,
                COALESCE(first_release_date, '') AS released_date
         FROM combined
         ORDER BY first_release_date NULLS LAST, entry_id
         LIMIT $2 OFFSET $3",
        &[&artist_id, &limit, &offset],
    ).await?;

    // Collect ids we need to enrich.
    let mut rep_release_ids: Vec<i32> = Vec::with_capacity(rows.len());
    let mut master_ids: Vec<i32> = Vec::new();
    let mut masterless_release_ids: Vec<i32> = Vec::new();
    for r in &rows {
        let rep: i32 = r.get("rep_release_id");
        rep_release_ids.push(rep);
        let kind: String = r.get("kind");
        let entry_id: i32 = r.get("entry_id");
        if kind == "master" {
            master_ids.push(entry_id);
        } else {
            masterless_release_ids.push(entry_id);
        }
    }

    // Keep the legacy scalar based on the representative release, while the
    // additive structural set covers every pressing represented by each row.
    let fmt_map = fetch_format_descriptions(client, &rep_release_ids).await?;
    let (master_type_map, release_type_map) =
        fetch_artist_entry_primary_types(client, &master_ids, &masterless_release_ids).await?;
    // Artist-credit pairs.
    let master_credit_map = fetch_master_artist_credits(client, &master_ids).await?;
    let release_credit_map = fetch_release_artist_credits(client, &masterless_release_ids).await?;

    let results: Vec<ArtistMasterEntry> = rows
        .iter()
        .map(|r| {
            let kind: String = r.get("kind");
            let entry_id: i32 = r.get("entry_id");
            let title: String = r.get("title");
            let rep: i32 = r.get("rep_release_id");
            let first_release_date: String = r.get("released_date");

            let descriptions = fmt_map.get(&rep).cloned().unwrap_or_default();
            let primary_type = infer_primary_type(&descriptions);
            let primary_types = if kind == "master" {
                master_type_map.get(&entry_id).cloned().unwrap_or_default()
            } else {
                release_type_map.get(&entry_id).cloned().unwrap_or_default()
            };

            let (credit_str, primary_artist) = if kind == "master" {
                let pairs = master_credit_map
                    .get(&entry_id)
                    .cloned()
                    .unwrap_or_default();
                let primary = pairs.first().map(|&(_, _, id)| id);
                let name_join: Vec<(String, String)> =
                    pairs.into_iter().map(|(n, j, _)| (n, j)).collect();
                (join_artist_credit(&name_join), primary)
            } else {
                let pairs = release_credit_map
                    .get(&entry_id)
                    .cloned()
                    .unwrap_or_default();
                let primary = pairs.first().map(|&(_, _, id)| id);
                let name_join: Vec<(String, String)> =
                    pairs.into_iter().map(|(n, j, _)| (n, j)).collect();
                (join_artist_credit(&name_join), primary)
            };

            let (id_value, is_masterless) = if kind == "master" {
                (MasterEntryId::Master(entry_id), false)
            } else {
                (
                    MasterEntryId::Release(format!("release-{}", entry_id)),
                    true,
                )
            };

            ArtistMasterEntry {
                id: id_value,
                title,
                type_: primary_type,
                primary_types,
                first_release_date,
                artist_credit: credit_str,
                primary_artist_id: primary_artist,
                is_masterless,
            }
        })
        .collect();

    Ok(Some(ArtistMastersResponse {
        results,
        total,
        page,
        per_page,
    }))
}

/// Releases where the artist appears via a track-level credit only —
/// compilation appearances, guest spots, sampler tracks — i.e. rows
/// that ``query_artist_masters`` does NOT return because the artist
/// isn't in the master/release-level credits.
///
/// Same enrichment + response shape as the masters endpoint so the
/// frontend can render both lists identically. No LIMIT/OFFSET; for
/// prolific session musicians callers may need to budget render time
/// rather than expect a paged window.
///
/// SQL strategy: anchor on ``release_track_artist`` (indexed by
/// ``artist_id``), anti-join against ``release_artist`` to drop rows
/// already counted as primary, then collapse by master_id like the
/// masters endpoint does.
pub async fn query_artist_appearances(
    pool: &Pool,
    artist_id: i32,
) -> anyhow::Result<Option<ArtistMastersResponse>> {
    let client = get_client(pool).await?;
    let client = &**client;

    let exists = client
        .query_opt("SELECT 1 FROM artist WHERE id = $1", &[&artist_id])
        .await?;
    if exists.is_none() {
        return Ok(None);
    }

    let rows = client.query(
        "WITH ar AS (
             SELECT DISTINCT r.id, r.title, NULLIF(r.master_id, 0) AS master_id,
                    NULLIF(r.released, '') AS released
             FROM release r
             JOIN release_track_artist rta
               ON rta.release_id = r.id AND rta.artist_id = $1
             WHERE NOT EXISTS (
                 SELECT 1 FROM release_artist ra
                 WHERE ra.release_id = r.id AND ra.artist_id = $1
             )
         ),
         combined AS (
             SELECT 'master'::text AS kind,
                    m.id AS entry_id,
                    m.title AS title,
                    COALESCE(
                        m.main_release_id,
                        (SELECT ar2.id FROM ar ar2 WHERE ar2.master_id = m.id
                         ORDER BY ar2.released NULLS LAST, ar2.id LIMIT 1)
                    ) AS rep_release_id,
                    (SELECT MIN(ar2.released) FROM ar ar2 WHERE ar2.master_id = m.id) AS first_release_date
             FROM master m
             WHERE EXISTS (SELECT 1 FROM ar WHERE ar.master_id = m.id)
             UNION ALL
             SELECT 'release'::text AS kind,
                    ar.id AS entry_id,
                    ar.title AS title,
                    ar.id AS rep_release_id,
                    ar.released AS first_release_date
             FROM ar
             WHERE ar.master_id IS NULL
         )
         SELECT kind, entry_id, title, rep_release_id,
                COALESCE(first_release_date, '') AS released_date
         FROM combined
         ORDER BY first_release_date NULLS LAST, entry_id",
        &[&artist_id],
    ).await?;

    let mut rep_release_ids: Vec<i32> = Vec::with_capacity(rows.len());
    let mut master_ids: Vec<i32> = Vec::new();
    let mut masterless_release_ids: Vec<i32> = Vec::new();
    for r in &rows {
        let rep: i32 = r.get("rep_release_id");
        rep_release_ids.push(rep);
        let kind: String = r.get("kind");
        let entry_id: i32 = r.get("entry_id");
        if kind == "master" {
            master_ids.push(entry_id);
        } else {
            masterless_release_ids.push(entry_id);
        }
    }

    let fmt_map = fetch_format_descriptions(client, &rep_release_ids).await?;
    let (master_type_map, release_type_map) =
        fetch_artist_entry_primary_types(client, &master_ids, &masterless_release_ids).await?;
    let master_credit_map = fetch_master_artist_credits(client, &master_ids).await?;
    let release_credit_map = fetch_release_artist_credits(client, &masterless_release_ids).await?;

    let results: Vec<ArtistMasterEntry> = rows
        .iter()
        .map(|r| {
            let kind: String = r.get("kind");
            let entry_id: i32 = r.get("entry_id");
            let title: String = r.get("title");
            let rep: i32 = r.get("rep_release_id");
            let first_release_date: String = r.get("released_date");

            let descriptions = fmt_map.get(&rep).cloned().unwrap_or_default();
            let primary_type = infer_primary_type(&descriptions);
            let primary_types = if kind == "master" {
                master_type_map.get(&entry_id).cloned().unwrap_or_default()
            } else {
                release_type_map.get(&entry_id).cloned().unwrap_or_default()
            };

            let (credit_str, primary_artist) = if kind == "master" {
                let pairs = master_credit_map
                    .get(&entry_id)
                    .cloned()
                    .unwrap_or_default();
                let primary = pairs.first().map(|&(_, _, id)| id);
                let name_join: Vec<(String, String)> =
                    pairs.into_iter().map(|(n, j, _)| (n, j)).collect();
                (join_artist_credit(&name_join), primary)
            } else {
                let pairs = release_credit_map
                    .get(&entry_id)
                    .cloned()
                    .unwrap_or_default();
                let primary = pairs.first().map(|&(_, _, id)| id);
                let name_join: Vec<(String, String)> =
                    pairs.into_iter().map(|(n, j, _)| (n, j)).collect();
                (join_artist_credit(&name_join), primary)
            };

            let (id_value, is_masterless) = if kind == "master" {
                (MasterEntryId::Master(entry_id), false)
            } else {
                (
                    MasterEntryId::Release(format!("release-{}", entry_id)),
                    true,
                )
            };

            ArtistMasterEntry {
                id: id_value,
                title,
                type_: primary_type,
                primary_types,
                first_release_date,
                artist_credit: credit_str,
                primary_artist_id: primary_artist,
                is_masterless,
            }
        })
        .collect();

    let total = results.len() as i64;
    Ok(Some(ArtistMastersResponse {
        results,
        total,
        page: 1,
        per_page: total.max(1) as i32,
    }))
}

// ---------------------------------------------------------------------------
// Label endpoints
// ---------------------------------------------------------------------------

pub async fn query_label_search(
    pool: &Pool,
    name: &str,
    page: i32,
    per_page: i32,
) -> anyhow::Result<LabelSearchResponse> {
    let client = get_client(pool).await?;
    let client = &**client;

    let per_page = per_page.clamp(1, 100);
    let page = page.max(1);
    let limit = per_page as i64;
    let offset = (page as i64 - 1) * limit;

    let count_row = client
        .query_one(
            "SELECT count(*) AS cnt FROM label
         WHERE to_tsvector('english', name) @@ plainto_tsquery('english', $1)",
            &[&name],
        )
        .await?;
    let total: i64 = count_row.get("cnt");

    // Single query: FTS-match labels, count releases via LEFT JOIN, denormalize parent name.
    let rows = client
        .query(
            "SELECT l.id, l.name, COALESCE(l.profile, '') AS profile,
                l.parent_label_id, p.name AS parent_label_name,
                COUNT(rl.release_id) AS release_count,
                ts_rank(to_tsvector('english', l.name), plainto_tsquery('english', $1)) AS score,
                (lower(l.name) = lower($1)) AS exact_match
         FROM label l
         LEFT JOIN label p ON p.id = l.parent_label_id
         LEFT JOIN release_label rl ON rl.label_id = l.id
         WHERE to_tsvector('english', l.name) @@ plainto_tsquery('english', $1)
         GROUP BY l.id, l.name, l.profile, l.parent_label_id, p.name
         ORDER BY exact_match DESC, score DESC, length(l.name), l.id
         LIMIT $2 OFFSET $3",
            &[&name, &limit, &offset],
        )
        .await?;

    let results = rows
        .iter()
        .map(|r| {
            let profile: String = r.get("profile");
            LabelHit {
                id: r.get("id"),
                name: r.get("name"),
                profile: truncate_profile(&profile, 200),
                parent_label_id: r.get("parent_label_id"),
                parent_label_name: r.get("parent_label_name"),
                release_count: r.get("release_count"),
                score: r.get::<_, f32>("score"),
            }
        })
        .collect();

    Ok(LabelSearchResponse {
        results,
        total,
        page,
        per_page,
    })
}

pub async fn query_label(pool: &Pool, id: i32) -> anyhow::Result<Option<LabelDetail>> {
    let client = get_client(pool).await?;
    let client = &**client;

    let row = match client
        .query_opt(
            "SELECT l.id, l.name, COALESCE(l.profile, '') AS profile,
                COALESCE(l.contactinfo, '') AS contactinfo,
                COALESCE(l.data_quality, '') AS data_quality,
                l.parent_label_id, p.name AS parent_label_name
         FROM label l
         LEFT JOIN label p ON p.id = l.parent_label_id
         WHERE l.id = $1",
            &[&id],
        )
        .await?
    {
        Some(r) => r,
        None => return Ok(None),
    };

    let total_row = client
        .query_one(
            "SELECT count(*) AS cnt FROM release_label WHERE label_id = $1",
            &[&id],
        )
        .await?;
    let total_releases: i64 = total_row.get("cnt");

    let sub_rows = client
        .query(
            "SELECT l.id, l.name, COUNT(rl.release_id) AS release_count
         FROM label l
         LEFT JOIN release_label rl ON rl.label_id = l.id
         WHERE l.parent_label_id = $1
         GROUP BY l.id, l.name
         ORDER BY l.name, l.id",
            &[&id],
        )
        .await?;
    let sub_labels: Vec<SubLabel> = sub_rows
        .iter()
        .map(|r| SubLabel {
            id: r.get("id"),
            name: r.get("name"),
            release_count: r.get("release_count"),
        })
        .collect();

    Ok(Some(LabelDetail {
        id: row.get("id"),
        name: row.get("name"),
        profile: row.get("profile"),
        contactinfo: row.get("contactinfo"),
        data_quality: row.get("data_quality"),
        parent_label_id: row.get("parent_label_id"),
        parent_label_name: row.get("parent_label_name"),
        total_releases,
        sub_labels,
    }))
}

async fn rollback_label_releases_tx(
    tx: PooledTransaction<'_>,
    err: anyhow::Error,
) -> anyhow::Error {
    if let Err(rollback_err) = tx.rollback().await {
        tracing::error!("failed to roll back label releases transaction: {rollback_err}");
    }
    err
}

pub async fn query_label_releases(
    pool: &Pool,
    label_id: i32,
    page: i32,
    per_page: i32,
    include_sublabels: bool,
) -> anyhow::Result<Option<LabelReleasesResponse>> {
    let mut client = get_client(pool).await?;

    // Existence check
    let exists = client
        .query_opt("SELECT 1 FROM label WHERE id = $1", &[&label_id])
        .await?;
    if exists.is_none() {
        return Ok(None);
    }

    let per_page = per_page.clamp(1, 100);
    let page = page.max(1);
    let limit = per_page as i64;
    let offset = (page as i64 - 1) * limit;

    let count_sql = "SELECT count(DISTINCT rl.release_id) AS cnt
             FROM release_label rl
             WHERE rl.label_id = ANY($1::int[])";
    let fetch_sql_by_released = "WITH matched_ids AS (
                 SELECT DISTINCT ON (rl.release_id)
                        rl.release_id AS id,
                        rl.label_id AS via_label_id
                 FROM release_label rl
                 WHERE rl.label_id = ANY($1::int[])
                 ORDER BY rl.release_id, (rl.label_id = $2) DESC, rl.label_id
             ),
             paged AS (
                 SELECT r.id, r.title, r.country, r.released, r.master_id,
                        mi.via_label_id
                 FROM matched_ids mi
                 JOIN release r ON r.id = mi.id
                 ORDER BY r.released, r.id
                 LIMIT $3 OFFSET $4
             )
             SELECT p.id, p.title, p.country, p.released, p.master_id,
                    p.via_label_id,
                    m.title AS master_title,
                    CASE WHEN p.master_id IS NULL OR p.master_id = 0 THEN NULL
                         ELSE (SELECT MIN(r2.released) FROM release r2
                               WHERE r2.master_id = p.master_id AND r2.released <> '')
                    END AS master_first_released,
                    CASE WHEN p.via_label_id = $2 THEN NULL
                         ELSE l_via.name END AS sub_label_name
             FROM paged p
             LEFT JOIN master m ON m.id = NULLIF(p.master_id, 0)
             LEFT JOIN label l_via ON l_via.id = p.via_label_id
             ORDER BY p.released, p.id";
    let fetch_sql_by_id = "WITH matched_ids AS (
                 SELECT DISTINCT ON (rl.release_id)
                        rl.release_id AS id,
                        rl.label_id AS via_label_id
                 FROM release_label rl
                 WHERE rl.label_id = ANY($1::int[])
                 ORDER BY rl.release_id, (rl.label_id = $2) DESC, rl.label_id
             ),
             paged AS (
                 SELECT r.id, r.title, r.country, r.released, r.master_id,
                        mi.via_label_id
                 FROM matched_ids mi
                 JOIN release r ON r.id = mi.id
                 ORDER BY r.id, r.released
                 LIMIT $3 OFFSET $4
             )
             SELECT p.id, p.title, p.country, p.released, p.master_id,
                    p.via_label_id,
                    m.title AS master_title,
                    CASE WHEN p.master_id IS NULL OR p.master_id = 0 THEN NULL
                         ELSE (SELECT MIN(r2.released) FROM release r2
                               WHERE r2.master_id = p.master_id AND r2.released <> '')
                    END AS master_first_released,
                    CASE WHEN p.via_label_id = $2 THEN NULL
                         ELSE l_via.name END AS sub_label_name
             FROM paged p
             LEFT JOIN master m ON m.id = NULLIF(p.master_id, 0)
             LEFT JOIN label l_via ON l_via.id = p.via_label_id
             ORDER BY p.id, p.released";

    let (count_row, rows) = if include_sublabels {
        let tx = client.transaction().await?;
        tx.batch_execute("SET LOCAL statement_timeout = '15s'")
            .await?;

        // UNION (not UNION ALL): defends against parent_label_id cycles in the
        // label tree. Run the tree first and pass a concrete int[] into the
        // release queries; otherwise Postgres badly overestimates the recursive
        // CTE and can scan/hash multi-million-row tables for tiny labels.
        let label_rows = match tx
            .query(
                "WITH RECURSIVE label_tree AS (
                 SELECT id FROM label WHERE id = $1
                 UNION
                 SELECT l.id FROM label l
                 JOIN label_tree lt ON l.parent_label_id = lt.id
             )
             SELECT id FROM label_tree",
                &[&label_id],
            )
            .await
        {
            Ok(rows) => rows,
            Err(err) => {
                let err = label_releases_query_error(err);
                return Err(rollback_label_releases_tx(tx, err).await);
            }
        };
        let label_ids: Vec<i32> = label_rows.iter().map(|row| row.get("id")).collect();

        let count_row = match tx.query_one(count_sql, &[&label_ids]).await {
            Ok(row) => row,
            Err(err) => {
                let err = label_releases_query_error(err);
                return Err(rollback_label_releases_tx(tx, err).await);
            }
        };

        let rows = match tx
            .query(
                fetch_sql_by_released,
                &[&label_ids, &label_id, &limit, &offset],
            )
            .await
        {
            Ok(rows) => rows,
            Err(err) => {
                let err = label_releases_query_error(err);
                return Err(rollback_label_releases_tx(tx, err).await);
            }
        };

        tx.commit().await?;
        (count_row, rows)
    } else {
        let label_ids = vec![label_id];
        let count_row = client.query_one(count_sql, &[&label_ids]).await?;
        let rows = client
            .query(fetch_sql_by_id, &[&label_ids, &label_id, &limit, &offset])
            .await?;
        (count_row, rows)
    };
    let total: i64 = count_row.get("cnt");

    let release_ids: Vec<i32> = rows.iter().map(|r| r.get("id")).collect();
    let (artist_map, label_map, format_map) = if release_ids.is_empty() {
        (HashMap::new(), HashMap::new(), HashMap::new())
    } else {
        fetch_release_enrichments(&client, &release_ids).await?
    };

    let results = rows
        .iter()
        .map(|r| {
            let id: i32 = r.get("id");
            let via_label_id: i32 = r.get("via_label_id");
            let formats = format_map.get(&id).cloned().unwrap_or_default();
            let descs: Vec<String> = formats.iter().map(|f| f.descriptions.clone()).collect();
            let primary_type = infer_primary_type(&descs);
            LabelReleaseEntry {
                id,
                title: r.get("title"),
                country: r.get("country"),
                released: r.get("released"),
                master_id: r.get("master_id"),
                master_title: r.get("master_title"),
                master_first_released: r.get("master_first_released"),
                primary_type,
                label_id: via_label_id,
                via_label_id,
                sub_label_name: r.get("sub_label_name"),
                artists: artist_map.get(&id).cloned().unwrap_or_default(),
                labels: label_map.get(&id).cloned().unwrap_or_default(),
                formats,
            }
        })
        .collect();

    let pages = if per_page > 0 {
        ((total as f64) / (per_page as f64)).ceil() as i32
    } else {
        1
    };

    Ok(Some(LabelReleasesResponse {
        results,
        pagination: Pagination {
            page,
            per_page,
            pages,
            items: total,
        },
        include_sublabels,
    }))
}

// Fetch release_format.descriptions rows grouped by release_id.
// Returns vec<description-string> per release in insertion order.
async fn fetch_format_descriptions(
    client: &Client,
    release_ids: &[i32],
) -> anyhow::Result<HashMap<i32, Vec<String>>> {
    let mut map: HashMap<i32, Vec<String>> = HashMap::new();
    if release_ids.is_empty() {
        return Ok(map);
    }
    let rows = client
        .query(
            "SELECT release_id, descriptions FROM release_format WHERE release_id = ANY($1)",
            &[&release_ids],
        )
        .await?;
    for r in &rows {
        let rid: i32 = r.get("release_id");
        let d: String = r.get("descriptions");
        map.entry(rid).or_default().push(d);
    }
    Ok(map)
}

type ArtistEntryPrimaryTypeMaps = (HashMap<i32, Vec<String>>, HashMap<i32, Vec<String>>);

/// Fetch structural type evidence for one artist endpoint result set.
///
/// Masters aggregate format descriptions across every child release, while
/// masterless entries use only their exact release. Both groups are fetched in
/// a single page-level query so endpoint cost does not grow by one query per
/// result row.
async fn fetch_artist_entry_primary_types(
    client: &Client,
    master_ids: &[i32],
    masterless_release_ids: &[i32],
) -> anyhow::Result<ArtistEntryPrimaryTypeMaps> {
    let mut master_descriptions: HashMap<i32, Vec<String>> = HashMap::new();
    let mut release_descriptions: HashMap<i32, Vec<String>> = HashMap::new();
    if master_ids.is_empty() && masterless_release_ids.is_empty() {
        return Ok((HashMap::new(), HashMap::new()));
    }

    let rows = client
        .query(
            "SELECT 'master'::text AS kind,
                    r.master_id AS entry_id,
                    rf.descriptions
             FROM release r
             JOIN release_format rf ON rf.release_id = r.id
             WHERE r.master_id = ANY($1)
             UNION ALL
             SELECT 'release'::text AS kind,
                    rf.release_id AS entry_id,
                    rf.descriptions
             FROM release_format rf
             WHERE rf.release_id = ANY($2)",
            &[&master_ids, &masterless_release_ids],
        )
        .await?;

    for row in rows {
        let kind: String = row.get("kind");
        let entry_id: i32 = row.get("entry_id");
        let descriptions: String = row.get("descriptions");
        if kind == "master" {
            master_descriptions
                .entry(entry_id)
                .or_default()
                .push(descriptions);
        } else {
            release_descriptions
                .entry(entry_id)
                .or_default()
                .push(descriptions);
        }
    }

    let master_types = master_descriptions
        .into_iter()
        .map(|(id, descriptions)| (id, structural_primary_types(&descriptions)))
        .collect();
    let release_types = release_descriptions
        .into_iter()
        .map(|(id, descriptions)| (id, structural_primary_types(&descriptions)))
        .collect();

    Ok((master_types, release_types))
}

// Returns master_id -> Vec<(artist_name, join_relation="", artist_id)> (master_artist has no join column).
async fn fetch_master_artist_credits(
    client: &Client,
    master_ids: &[i32],
) -> anyhow::Result<HashMap<i32, Vec<(String, String, i32)>>> {
    let mut map: HashMap<i32, Vec<(String, String, i32)>> = HashMap::new();
    if master_ids.is_empty() {
        return Ok(map);
    }
    let rows = client
        .query(
            "SELECT master_id, artist_id, artist_name FROM master_artist WHERE master_id = ANY($1)",
            &[&master_ids],
        )
        .await?;
    for r in &rows {
        let mid: i32 = r.get("master_id");
        let name: String = r.get("artist_name");
        let aid: i32 = r.get("artist_id");
        map.entry(mid).or_default().push((name, String::new(), aid));
    }
    Ok(map)
}

// Returns release_id -> Vec<(artist_name, join_relation, artist_id)>.
async fn fetch_release_artist_credits(
    client: &Client,
    release_ids: &[i32],
) -> anyhow::Result<HashMap<i32, Vec<(String, String, i32)>>> {
    let mut map: HashMap<i32, Vec<(String, String, i32)>> = HashMap::new();
    if release_ids.is_empty() {
        return Ok(map);
    }
    let rows = client
        .query(
            "SELECT release_id, artist_id, artist_name, join_relation
         FROM release_artist WHERE release_id = ANY($1)",
            &[&release_ids],
        )
        .await?;
    for r in &rows {
        let rid: i32 = r.get("release_id");
        let name: String = r.get("artist_name");
        let join: String = r.get("join_relation");
        let aid: i32 = r.get("artist_id");
        map.entry(rid).or_default().push((name, join, aid));
    }
    Ok(map)
}

// ---------------------------------------------------------------------------
// Discogs-compatible query helpers (for beets / python3-discogs-client)
// ---------------------------------------------------------------------------

fn build_search_text(title: &str, artists: &[CreditedArtist]) -> String {
    let artist_names: Vec<&str> = artists.iter().map(|a| a.artist_name.as_str()).collect();
    if artist_names.is_empty() {
        title.to_string()
    } else {
        format!("{} {}", artist_names.join(" "), title)
    }
}

// Given format description rows (each a ", "-joined string from release_format.descriptions),
// walk them in insertion order and return the first MB-equivalent primary_type keyword.
fn infer_primary_type(description_rows: &[String]) -> String {
    for row in description_rows {
        for d in row.split(", ") {
            match d {
                "Album" | "Compilation" => return "Album".to_string(),
                "Single" => return "Single".to_string(),
                "EP" | "Mini-Album" => return "EP".to_string(),
                _ => {}
            }
        }
    }
    "Other".to_string()
}

fn structural_primary_types(description_rows: &[String]) -> Vec<String> {
    let mut types = BTreeSet::new();
    for row in description_rows {
        for description in row.split(',').map(str::trim) {
            let normalized = match description {
                "Album" | "Compilation" => Some("Album"),
                "EP" | "Mini-Album" => Some("EP"),
                "Single" => Some("Single"),
                _ => None,
            };
            if let Some(primary_type) = normalized {
                types.insert(primary_type.to_string());
            }
        }
    }
    types.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::structural_primary_types;

    #[test]
    fn structural_primary_types_normalize_recognized_discogs_descriptions() {
        let descriptions = vec![
            "Compilation, Album".to_string(),
            "Mini-Album, EP".to_string(),
            "Single".to_string(),
            "Unofficial Release, Promo".to_string(),
        ];

        assert_eq!(
            structural_primary_types(&descriptions),
            vec!["Album", "EP", "Single"]
        );
    }

    #[test]
    fn structural_primary_types_are_sorted_deduplicated_and_exclude_unknowns() {
        let descriptions = vec![
            "Single, Album, Single".to_string(),
            "EP, Compilation, Mini-Album".to_string(),
            "Box Set".to_string(),
        ];

        assert_eq!(
            structural_primary_types(&descriptions),
            vec!["Album", "EP", "Single"]
        );
        assert!(structural_primary_types(&["Box Set, Promo".to_string()]).is_empty());
    }
}

// Join (name, join_relation) pairs into an MB-style artist-credit string.
// Discogs <join> sits between adjacent artists (e.g. "&", "feat.", ","). Falls
// back to " & " when the join field is empty.
fn join_artist_credit(pairs: &[(String, String)]) -> String {
    let mut out = String::new();
    for (i, (name, join)) in pairs.iter().enumerate() {
        if i > 0 {
            let j = join.trim();
            if j.is_empty() {
                out.push_str(" & ");
            } else if matches!(j, "," | ";") {
                out.push_str(j);
                out.push(' ');
            } else {
                out.push(' ');
                out.push_str(j);
                out.push(' ');
            }
        }
        out.push_str(name);
    }
    out
}

fn truncate_profile(s: &str, max_chars: usize) -> String {
    let mut count = 0;
    let mut end = s.len();
    for (i, _c) in s.char_indices() {
        if count >= max_chars {
            end = i;
            break;
        }
        count += 1;
    }
    if end < s.len() {
        let mut out = s[..end].to_string();
        out.push_str("...");
        out
    } else {
        s.to_string()
    }
}

fn parse_year(released: &str) -> Option<i32> {
    released
        .get(..4)
        .and_then(|s| s.parse::<i32>().ok())
        .filter(|&y| y > 0)
}

fn parse_descriptions(s: &str) -> Vec<String> {
    if s.is_empty() {
        vec![]
    } else {
        s.split(", ").map(|s| s.to_string()).collect()
    }
}

pub async fn query_discogs_release(pool: &Pool, id: i32) -> anyhow::Result<Option<DiscogsRelease>> {
    let client = get_client(pool).await?;
    let client = &**client;

    let row = match client.query_opt(
        "SELECT id, title, country, released, master_id, data_quality FROM release WHERE id = $1",
        &[&id],
    ).await? {
        Some(r) => r,
        None => return Ok(None),
    };

    let artist_rows = client.query(
        "SELECT artist_id, artist_name, role, anv, join_relation FROM release_artist WHERE release_id = $1",
        &[&id],
    ).await?;
    let artists: Vec<DiscogsArtistCredit> = artist_rows
        .iter()
        .map(|r| DiscogsArtistCredit {
            id: r.get("artist_id"),
            name: r.get("artist_name"),
            anv: r.get("anv"),
            join: r.get("join_relation"),
            role: r.get("role"),
            tracks: String::new(),
            resource_url: String::new(),
        })
        .collect();

    let label_rows = client
        .query(
            "SELECT label_id, label_name, catno FROM release_label WHERE release_id = $1",
            &[&id],
        )
        .await?;
    let labels: Vec<ApiLabel> = label_rows
        .iter()
        .map(|r| ApiLabel {
            id: r.get("label_id"),
            name: r.get("label_name"),
            catno: r.get("catno"),
        })
        .collect();

    let format_rows = client
        .query(
            "SELECT name, qty, descriptions FROM release_format WHERE release_id = $1",
            &[&id],
        )
        .await?;
    let formats: Vec<DiscogsFormat> = format_rows
        .iter()
        .map(|r| {
            let qty: i32 = r.get("qty");
            let desc: String = r.get("descriptions");
            DiscogsFormat {
                name: r.get("name"),
                qty: qty.to_string(),
                descriptions: parse_descriptions(&desc),
            }
        })
        .collect();

    let track_rows = client.query(
        "SELECT sequence, position, title, duration FROM release_track WHERE release_id = $1 ORDER BY sequence",
        &[&id],
    ).await?;

    let track_artist_rows = client.query(
        "SELECT sequence, artist_id, artist_name, role, anv FROM release_track_artist WHERE release_id = $1 ORDER BY sequence",
        &[&id],
    ).await?;
    let mut track_artist_map: HashMap<i32, Vec<DiscogsArtistCredit>> = HashMap::new();
    for r in &track_artist_rows {
        let seq: i32 = r.get("sequence");
        track_artist_map
            .entry(seq)
            .or_default()
            .push(DiscogsArtistCredit {
                id: r.get("artist_id"),
                name: r.get("artist_name"),
                anv: r.get("anv"),
                join: String::new(),
                role: r.get("role"),
                tracks: String::new(),
                resource_url: String::new(),
            });
    }

    let tracklist: Vec<DiscogsTrack> = track_rows
        .iter()
        .map(|r| {
            let seq: i32 = r.get("sequence");
            DiscogsTrack {
                position: r.get("position"),
                type_: "track".to_string(),
                title: r.get("title"),
                duration: r.get("duration"),
                artists: track_artist_map.remove(&seq).unwrap_or_default(),
                extraartists: vec![],
            }
        })
        .collect();

    let genre_rows = client
        .query(
            "SELECT genre FROM release_genre WHERE release_id = $1",
            &[&id],
        )
        .await?;
    let genres: Vec<String> = genre_rows.iter().map(|r| r.get("genre")).collect();

    let style_rows = client
        .query(
            "SELECT style FROM release_style WHERE release_id = $1",
            &[&id],
        )
        .await?;
    let styles: Vec<String> = style_rows.iter().map(|r| r.get("style")).collect();

    let released: String = row.get("released");

    Ok(Some(DiscogsRelease {
        id,
        title: row.get("title"),
        uri: format!("https://www.discogs.com/release/{id}"),
        year: parse_year(&released),
        country: row.get("country"),
        master_id: row.get("master_id"),
        data_quality: row.get("data_quality"),
        artists,
        tracklist,
        labels,
        formats,
        genres,
        styles,
        images: vec![],
    }))
}

pub async fn query_discogs_master(pool: &Pool, id: i32) -> anyhow::Result<Option<DiscogsMaster>> {
    let client = get_client(pool).await?;
    let client = &**client;

    let row = match client
        .query_opt("SELECT id, title, year FROM master WHERE id = $1", &[&id])
        .await?
    {
        Some(r) => r,
        None => return Ok(None),
    };

    Ok(Some(DiscogsMaster {
        id,
        title: row.get("title"),
        year: row.get("year"),
    }))
}

pub async fn query_discogs_search(
    pool: &Pool,
    q: &str,
    per_page: i32,
    page: i32,
) -> anyhow::Result<DiscogsSearchResponse> {
    let client = get_client(pool).await?;
    let client = &**client;

    let per_page = per_page.clamp(1, 100);
    let page = page.max(1);
    let limit = (per_page + 1) as i64; // fetch one extra to detect more pages
    let offset = ((page - 1) as i64) * (per_page as i64);

    let rows = client.query(
        "SELECT r.id, r.title,
                (SELECT ra.artist_name FROM release_artist ra WHERE ra.release_id = r.id LIMIT 1) as artist_name
         FROM release r
         WHERE to_tsvector('english', r.search_text) @@ plainto_tsquery('english', $1)
         ORDER BY ts_rank(to_tsvector('english', r.search_text), plainto_tsquery('english', $1)) DESC
         LIMIT $2 OFFSET $3",
        &[&q, &limit, &offset],
    ).await?;

    let has_more = rows.len() > per_page as usize;
    let result_rows = if has_more {
        &rows[..per_page as usize]
    } else {
        &rows[..]
    };

    let results: Vec<DiscogsSearchResult> = result_rows
        .iter()
        .map(|r| {
            let id: i32 = r.get("id");
            let title: String = r.get("title");
            let artist_name: Option<String> = r.get("artist_name");
            let display_title = match artist_name {
                Some(a) if !a.is_empty() => format!("{a} - {title}"),
                _ => title,
            };
            DiscogsSearchResult {
                id,
                type_: "release".to_string(),
                title: display_title,
            }
        })
        .collect();

    let total_on_page = result_rows.len() as i64;
    let items = if has_more {
        // More results exist beyond this page; estimate conservatively
        offset + total_on_page + 1
    } else {
        offset + total_on_page
    };
    let pages = if per_page > 0 {
        ((items as f64) / (per_page as f64)).ceil() as i32
    } else {
        1
    };

    Ok(DiscogsSearchResponse {
        pagination: DiscogsPagination { pages, items },
        results,
    })
}

// ---------------------------------------------------------------------------
// Shared enrichment helper
// ---------------------------------------------------------------------------

type EnrichmentMaps = (
    HashMap<i32, Vec<ApiArtistCredit>>,
    HashMap<i32, Vec<ApiLabel>>,
    HashMap<i32, Vec<ApiFormat>>,
);

async fn fetch_release_enrichments(
    client: &Client,
    release_ids: &[i32],
) -> anyhow::Result<EnrichmentMaps> {
    let artist_rows = client.query(
        "SELECT release_id, artist_id, artist_name, role, anv FROM release_artist WHERE release_id = ANY($1)",
        &[&release_ids],
    ).await?;
    let mut artist_map: HashMap<i32, Vec<ApiArtistCredit>> = HashMap::new();
    for r in &artist_rows {
        let rid: i32 = r.get("release_id");
        artist_map.entry(rid).or_default().push(ApiArtistCredit {
            id: r.get("artist_id"),
            name: r.get("artist_name"),
            role: r.get("role"),
            anv: r.get("anv"),
        });
    }

    let label_rows = client.query(
        "SELECT release_id, label_id, label_name, catno FROM release_label WHERE release_id = ANY($1)",
        &[&release_ids],
    ).await?;
    let mut label_map: HashMap<i32, Vec<ApiLabel>> = HashMap::new();
    for r in &label_rows {
        let rid: i32 = r.get("release_id");
        label_map.entry(rid).or_default().push(ApiLabel {
            id: r.get("label_id"),
            name: r.get("label_name"),
            catno: r.get("catno"),
        });
    }

    let format_rows = client.query(
        "SELECT release_id, name, qty, descriptions, free_text FROM release_format WHERE release_id = ANY($1)",
        &[&release_ids],
    ).await?;
    let mut format_map: HashMap<i32, Vec<ApiFormat>> = HashMap::new();
    for r in &format_rows {
        let rid: i32 = r.get("release_id");
        format_map.entry(rid).or_default().push(ApiFormat {
            name: r.get("name"),
            qty: r.get("qty"),
            descriptions: r.get("descriptions"),
            free_text: r.get("free_text"),
        });
    }

    Ok((artist_map, label_map, format_map))
}
