use std::collections::HashMap;
use std::pin::pin;

use tokio_postgres::binary_copy::BinaryCopyInWriter;
use tokio_postgres::types::{ToSql, Type};
use tokio_postgres::{Client, NoTls};

use crate::schema;
use crate::types::*;

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
    client.batch_execute(schema::VACUUM_ANALYZE).await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// COPY helpers (binary format for speed)
// ---------------------------------------------------------------------------

pub async fn copy_artists(client: &Client, artists: &[Artist]) -> anyhow::Result<()> {
    if artists.is_empty() { return Ok(()); }

    // artist table
    {
        let sink = client.copy_in(
            "COPY artist (id, name, realname, profile, data_quality) FROM STDIN WITH (FORMAT binary)"
        ).await?;
        let mut w = pin!(BinaryCopyInWriter::new(sink, &[Type::INT4, Type::TEXT, Type::TEXT, Type::TEXT, Type::TEXT]));
        for a in artists {
            w.as_mut().write(&[
                &a.id as &(dyn ToSql + Sync),
                &a.name as _, &a.realname as _, &a.profile as _, &a.data_quality as _,
            ]).await?;
        }
        w.as_mut().finish().await?;
    }

    // artist_alias
    if artists.iter().any(|a| !a.aliases.is_empty()) {
        let sink = client.copy_in(
            "COPY artist_alias (artist_id, alias_id, name) FROM STDIN WITH (FORMAT binary)"
        ).await?;
        let mut w = pin!(BinaryCopyInWriter::new(sink, &[Type::INT4, Type::INT4, Type::TEXT]));
        for a in artists {
            for al in &a.aliases {
                w.as_mut().write(&[
                    &a.id as &(dyn ToSql + Sync), &al.alias_id as _, &al.name as _,
                ]).await?;
            }
        }
        w.as_mut().finish().await?;
    }

    // artist_namevariation
    if artists.iter().any(|a| !a.namevariations.is_empty()) {
        let sink = client.copy_in(
            "COPY artist_namevariation (artist_id, name) FROM STDIN WITH (FORMAT binary)"
        ).await?;
        let mut w = pin!(BinaryCopyInWriter::new(sink, &[Type::INT4, Type::TEXT]));
        for a in artists {
            for nv in &a.namevariations {
                w.as_mut().write(&[&a.id as &(dyn ToSql + Sync), nv as _]).await?;
            }
        }
        w.as_mut().finish().await?;
    }

    Ok(())
}

pub async fn copy_labels(client: &Client, labels: &[Label]) -> anyhow::Result<()> {
    if labels.is_empty() { return Ok(()); }

    let sink = client.copy_in(
        "COPY label (id, name, contactinfo, profile, parent_label_id, data_quality) FROM STDIN WITH (FORMAT binary)"
    ).await?;
    let mut w = pin!(BinaryCopyInWriter::new(sink, &[Type::INT4, Type::TEXT, Type::TEXT, Type::TEXT, Type::INT4, Type::TEXT]));
    for l in labels {
        w.as_mut().write(&[
            &l.id as &(dyn ToSql + Sync),
            &l.name as _, &l.contactinfo as _, &l.profile as _,
            &l.parent_label_id as _, &l.data_quality as _,
        ]).await?;
    }
    w.as_mut().finish().await?;
    Ok(())
}

pub async fn copy_masters(client: &Client, masters: &[Master]) -> anyhow::Result<()> {
    if masters.is_empty() { return Ok(()); }

    // master table
    {
        let sink = client.copy_in(
            "COPY master (id, title, year, main_release_id, data_quality) FROM STDIN WITH (FORMAT binary)"
        ).await?;
        let mut w = pin!(BinaryCopyInWriter::new(sink, &[Type::INT4, Type::TEXT, Type::INT4, Type::INT4, Type::TEXT]));
        for m in masters {
            w.as_mut().write(&[
                &m.id as &(dyn ToSql + Sync),
                &m.title as _, &m.year as _, &m.main_release_id as _, &m.data_quality as _,
            ]).await?;
        }
        w.as_mut().finish().await?;
    }

    // master_artist
    if masters.iter().any(|m| !m.artists.is_empty()) {
        let sink = client.copy_in(
            "COPY master_artist (master_id, artist_id, artist_name, role, anv) FROM STDIN WITH (FORMAT binary)"
        ).await?;
        let mut w = pin!(BinaryCopyInWriter::new(sink, &[Type::INT4, Type::INT4, Type::TEXT, Type::TEXT, Type::TEXT]));
        for m in masters {
            for a in &m.artists {
                w.as_mut().write(&[
                    &m.id as &(dyn ToSql + Sync),
                    &a.artist_id as _, &a.artist_name as _, &a.role as _, &a.anv as _,
                ]).await?;
            }
        }
        w.as_mut().finish().await?;
    }

    Ok(())
}

pub async fn copy_releases(client: &Client, releases: &[Release]) -> anyhow::Result<()> {
    if releases.is_empty() { return Ok(()); }

    // release table
    {
        let sink = client.copy_in(
            "COPY release (id, title, country, released, notes, master_id, status, data_quality, search_text) FROM STDIN WITH (FORMAT binary)"
        ).await?;
        let mut w = pin!(BinaryCopyInWriter::new(sink, &[
            Type::INT4, Type::TEXT, Type::TEXT, Type::TEXT, Type::TEXT,
            Type::INT4, Type::TEXT, Type::TEXT, Type::TEXT,
        ]));
        for r in releases {
            let search_text = build_search_text(&r.title, &r.artists);
            w.as_mut().write(&[
                &r.id as &(dyn ToSql + Sync),
                &r.title as _, &r.country as _, &r.released as _, &r.notes as _,
                &r.master_id as _, &r.status as _, &r.data_quality as _,
                &search_text as _,
            ]).await?;
        }
        w.as_mut().finish().await?;
    }

    // release_artist
    if releases.iter().any(|r| !r.artists.is_empty()) {
        let sink = client.copy_in(
            "COPY release_artist (release_id, artist_id, artist_name, role, anv, join_relation) FROM STDIN WITH (FORMAT binary)"
        ).await?;
        let mut w = pin!(BinaryCopyInWriter::new(sink, &[
            Type::INT4, Type::INT4, Type::TEXT, Type::TEXT, Type::TEXT, Type::TEXT,
        ]));
        for r in releases {
            for a in &r.artists {
                w.as_mut().write(&[
                    &r.id as &(dyn ToSql + Sync),
                    &a.artist_id as _, &a.artist_name as _, &a.role as _, &a.anv as _,
                    &a.join_relation as _,
                ]).await?;
            }
        }
        w.as_mut().finish().await?;
    }

    // release_label
    if releases.iter().any(|r| !r.labels.is_empty()) {
        let sink = client.copy_in(
            "COPY release_label (release_id, label_id, label_name, catno) FROM STDIN WITH (FORMAT binary)"
        ).await?;
        let mut w = pin!(BinaryCopyInWriter::new(sink, &[Type::INT4, Type::INT4, Type::TEXT, Type::TEXT]));
        for r in releases {
            for l in &r.labels {
                w.as_mut().write(&[
                    &r.id as &(dyn ToSql + Sync),
                    &l.label_id as _, &l.label_name as _, &l.catno as _,
                ]).await?;
            }
        }
        w.as_mut().finish().await?;
    }

    // release_format
    if releases.iter().any(|r| !r.formats.is_empty()) {
        let sink = client.copy_in(
            "COPY release_format (release_id, name, qty, descriptions, free_text) FROM STDIN WITH (FORMAT binary)"
        ).await?;
        let mut w = pin!(BinaryCopyInWriter::new(sink, &[Type::INT4, Type::TEXT, Type::INT4, Type::TEXT, Type::TEXT]));
        for r in releases {
            for f in &r.formats {
                w.as_mut().write(&[
                    &r.id as &(dyn ToSql + Sync),
                    &f.name as _, &f.qty as _, &f.descriptions as _, &f.free_text as _,
                ]).await?;
            }
        }
        w.as_mut().finish().await?;
    }

    // release_track
    if releases.iter().any(|r| !r.tracks.is_empty()) {
        let sink = client.copy_in(
            "COPY release_track (release_id, sequence, position, title, duration) FROM STDIN WITH (FORMAT binary)"
        ).await?;
        let mut w = pin!(BinaryCopyInWriter::new(sink, &[Type::INT4, Type::INT4, Type::TEXT, Type::TEXT, Type::TEXT]));
        for r in releases {
            for t in &r.tracks {
                w.as_mut().write(&[
                    &r.id as &(dyn ToSql + Sync),
                    &t.sequence as _, &t.position as _, &t.title as _, &t.duration as _,
                ]).await?;
            }
        }
        w.as_mut().finish().await?;
    }

    // release_track_artist
    if releases.iter().any(|r| r.tracks.iter().any(|t| !t.artists.is_empty())) {
        let sink = client.copy_in(
            "COPY release_track_artist (release_id, sequence, artist_id, artist_name, role, anv) FROM STDIN WITH (FORMAT binary)"
        ).await?;
        let mut w = pin!(BinaryCopyInWriter::new(sink, &[
            Type::INT4, Type::INT4, Type::INT4, Type::TEXT, Type::TEXT, Type::TEXT,
        ]));
        for r in releases {
            for t in &r.tracks {
                for a in &t.artists {
                    w.as_mut().write(&[
                        &r.id as &(dyn ToSql + Sync),
                        &t.sequence as _, &a.artist_id as _, &a.artist_name as _,
                        &a.role as _, &a.anv as _,
                    ]).await?;
                }
            }
        }
        w.as_mut().finish().await?;
    }

    // release_genre
    if releases.iter().any(|r| !r.genres.is_empty()) {
        let sink = client.copy_in(
            "COPY release_genre (release_id, genre) FROM STDIN WITH (FORMAT binary)"
        ).await?;
        let mut w = pin!(BinaryCopyInWriter::new(sink, &[Type::INT4, Type::TEXT]));
        for r in releases {
            for g in &r.genres {
                w.as_mut().write(&[&r.id as &(dyn ToSql + Sync), g as _]).await?;
            }
        }
        w.as_mut().finish().await?;
    }

    // release_style
    if releases.iter().any(|r| !r.styles.is_empty()) {
        let sink = client.copy_in(
            "COPY release_style (release_id, style) FROM STDIN WITH (FORMAT binary)"
        ).await?;
        let mut w = pin!(BinaryCopyInWriter::new(sink, &[Type::INT4, Type::TEXT]));
        for r in releases {
            for s in &r.styles {
                w.as_mut().write(&[&r.id as &(dyn ToSql + Sync), s as _]).await?;
            }
        }
        w.as_mut().finish().await?;
    }

    // release_identifier
    if releases.iter().any(|r| !r.identifiers.is_empty()) {
        let sink = client.copy_in(
            "COPY release_identifier (release_id, type, value, description) FROM STDIN WITH (FORMAT binary)"
        ).await?;
        let mut w = pin!(BinaryCopyInWriter::new(sink, &[Type::INT4, Type::TEXT, Type::TEXT, Type::TEXT]));
        for r in releases {
            for i in &r.identifiers {
                w.as_mut().write(&[
                    &r.id as &(dyn ToSql + Sync),
                    &i.type_ as _, &i.value as _, &i.description as _,
                ]).await?;
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

pub async fn query_health(client: &Client) -> anyhow::Result<HealthResponse> {
    // Tables may not exist before first import — handle gracefully
    let releases = match client.query_one(
        "SELECT count(*) AS cnt FROM release", &[],
    ).await {
        Ok(row) => row.get::<_, i64>("cnt"),
        Err(_) => 0,
    };

    let last_import = match client.query_opt(
        "SELECT value FROM import_meta WHERE key = 'last_import'", &[],
    ).await {
        Ok(Some(r)) => r.get::<_, String>("value"),
        _ => String::new(),
    };

    let dump_date = match client.query_opt(
        "SELECT value FROM import_meta WHERE key = 'dump_date'", &[],
    ).await {
        Ok(Some(r)) => r.get::<_, String>("value"),
        _ => String::new(),
    };

    let status = if releases > 0 { "ok" } else { "awaiting_import" };
    Ok(HealthResponse {
        status: status.to_string(),
        releases,
        last_import,
        dump_date,
    })
}

pub async fn query_search(
    client: &Client,
    title: Option<&str>,
    artist: Option<&str>,
    page: i32,
    per_page: i32,
) -> anyhow::Result<SearchResponse> {
    let per_page = per_page.clamp(1, 100);
    let page = page.max(1);
    let limit = per_page as i64;
    let offset = (page as i64 - 1) * limit;

    if title.is_none() && artist.is_none() {
        return Ok(SearchResponse { results: vec![], page, per_page });
    }

    let rows = client.query(
        "SELECT r.id, r.title, r.country, r.released, r.master_id
         FROM release r
         WHERE ($1::text IS NULL OR to_tsvector('english', r.title) @@ plainto_tsquery('english', $1))
           AND ($2::text IS NULL OR EXISTS (
                SELECT 1 FROM release_artist ra
                JOIN artist a ON ra.artist_id = a.id
                WHERE ra.release_id = r.id
                AND to_tsvector('english', a.name) @@ plainto_tsquery('english', $2)))
         ORDER BY r.id LIMIT $3 OFFSET $4",
        &[&title, &artist, &limit, &offset],
    ).await?;

    let release_ids: Vec<i32> = rows.iter().map(|r| r.get("id")).collect();
    if release_ids.is_empty() {
        return Ok(SearchResponse { results: vec![], page, per_page });
    }

    let (artist_map, label_map, format_map) = fetch_release_enrichments(client, &release_ids).await?;

    let results = rows.iter().map(|r| {
        let id: i32 = r.get("id");
        SearchResult {
            id,
            title: r.get("title"),
            country: r.get("country"),
            released: r.get("released"),
            master_id: r.get("master_id"),
            artists: artist_map.get(&id).cloned().unwrap_or_default(),
            labels: label_map.get(&id).cloned().unwrap_or_default(),
            formats: format_map.get(&id).cloned().unwrap_or_default(),
        }
    }).collect();

    Ok(SearchResponse { results, page, per_page })
}

pub async fn query_release(client: &Client, id: i32) -> anyhow::Result<Option<ReleaseDetail>> {
    let row = match client.query_opt(
        "SELECT id, title, country, released, master_id FROM release WHERE id = $1",
        &[&id],
    ).await? {
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
        track_artist_map.entry(seq).or_default().push(ApiArtistCredit {
            id: r.get("artist_id"),
            name: r.get("artist_name"),
            role: r.get("role"),
            anv: r.get("anv"),
        });
    }

    let tracks: Vec<ApiTrack> = track_rows.iter().map(|r| {
        let seq: i32 = r.get("sequence");
        ApiTrack {
            position: r.get("position"),
            title: r.get("title"),
            duration: r.get("duration"),
            artists: track_artist_map.remove(&seq).unwrap_or_default(),
        }
    }).collect();

    let genre_rows = client.query(
        "SELECT genre FROM release_genre WHERE release_id = $1", &[&id],
    ).await?;
    let genres: Vec<String> = genre_rows.iter().map(|r| r.get("genre")).collect();

    let style_rows = client.query(
        "SELECT style FROM release_style WHERE release_id = $1", &[&id],
    ).await?;
    let styles: Vec<String> = style_rows.iter().map(|r| r.get("style")).collect();

    let ident_rows = client.query(
        "SELECT type, value, description FROM release_identifier WHERE release_id = $1", &[&id],
    ).await?;
    let identifiers: Vec<ApiIdentifier> = ident_rows.iter().map(|r| ApiIdentifier {
        type_: r.get("type"),
        value: r.get("value"),
        description: r.get("description"),
    }).collect();

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

pub async fn query_master(client: &Client, id: i32) -> anyhow::Result<Option<MasterDetail>> {
    let row = match client.query_opt(
        "SELECT id, title, year, main_release_id FROM master WHERE id = $1",
        &[&id],
    ).await? {
        Some(r) => r,
        None => return Ok(None),
    };

    let artist_rows = client.query(
        "SELECT artist_id, artist_name, role, anv FROM master_artist WHERE master_id = $1",
        &[&id],
    ).await?;
    let artists: Vec<ApiArtistCredit> = artist_rows.iter().map(|r| ApiArtistCredit {
        id: r.get("artist_id"),
        name: r.get("artist_name"),
        role: r.get("role"),
        anv: r.get("anv"),
    }).collect();

    let release_rows = client.query(
        "SELECT id, title, country FROM release WHERE master_id = $1 ORDER BY id",
        &[&id],
    ).await?;
    let release_ids: Vec<i32> = release_rows.iter().map(|r| r.get("id")).collect();

    let (_, label_map, format_map) = if release_ids.is_empty() {
        (HashMap::new(), HashMap::new(), HashMap::new())
    } else {
        fetch_release_enrichments(client, &release_ids).await?
    };

    let releases: Vec<MasterRelease> = release_rows.iter().map(|r| {
        let rid: i32 = r.get("id");
        MasterRelease {
            id: rid,
            title: r.get("title"),
            country: r.get("country"),
            formats: format_map.get(&rid).cloned().unwrap_or_default(),
            labels: label_map.get(&rid).cloned().unwrap_or_default(),
        }
    }).collect();

    Ok(Some(MasterDetail {
        id,
        title: row.get("title"),
        year: row.get("year"),
        main_release_id: row.get("main_release_id"),
        artists,
        releases,
    }))
}

pub async fn query_artist(client: &Client, id: i32) -> anyhow::Result<Option<ArtistDetail>> {
    let row = match client.query_opt(
        "SELECT id, name, realname, profile FROM artist WHERE id = $1",
        &[&id],
    ).await? {
        Some(r) => r,
        None => return Ok(None),
    };

    let alias_rows = client.query(
        "SELECT alias_id, name FROM artist_alias WHERE artist_id = $1", &[&id],
    ).await?;
    let aliases: Vec<ApiAlias> = alias_rows.iter().map(|r| ApiAlias {
        id: r.get("alias_id"),
        name: r.get("name"),
    }).collect();

    let nv_rows = client.query(
        "SELECT name FROM artist_namevariation WHERE artist_id = $1", &[&id],
    ).await?;
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

fn parse_year(released: &str) -> Option<i32> {
    released.get(..4)
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

pub async fn query_discogs_release(client: &Client, id: i32) -> anyhow::Result<Option<DiscogsRelease>> {
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
    let artists: Vec<DiscogsArtistCredit> = artist_rows.iter().map(|r| DiscogsArtistCredit {
        id: r.get("artist_id"),
        name: r.get("artist_name"),
        anv: r.get("anv"),
        join: r.get("join_relation"),
        role: r.get("role"),
        tracks: String::new(),
        resource_url: String::new(),
    }).collect();

    let label_rows = client.query(
        "SELECT label_id, label_name, catno FROM release_label WHERE release_id = $1",
        &[&id],
    ).await?;
    let labels: Vec<ApiLabel> = label_rows.iter().map(|r| ApiLabel {
        id: r.get("label_id"),
        name: r.get("label_name"),
        catno: r.get("catno"),
    }).collect();

    let format_rows = client.query(
        "SELECT name, qty, descriptions FROM release_format WHERE release_id = $1",
        &[&id],
    ).await?;
    let formats: Vec<DiscogsFormat> = format_rows.iter().map(|r| {
        let qty: i32 = r.get("qty");
        let desc: String = r.get("descriptions");
        DiscogsFormat {
            name: r.get("name"),
            qty: qty.to_string(),
            descriptions: parse_descriptions(&desc),
        }
    }).collect();

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
        track_artist_map.entry(seq).or_default().push(DiscogsArtistCredit {
            id: r.get("artist_id"),
            name: r.get("artist_name"),
            anv: r.get("anv"),
            join: String::new(),
            role: r.get("role"),
            tracks: String::new(),
            resource_url: String::new(),
        });
    }

    let tracklist: Vec<DiscogsTrack> = track_rows.iter().map(|r| {
        let seq: i32 = r.get("sequence");
        DiscogsTrack {
            position: r.get("position"),
            type_: "track".to_string(),
            title: r.get("title"),
            duration: r.get("duration"),
            artists: track_artist_map.remove(&seq).unwrap_or_default(),
            extraartists: vec![],
        }
    }).collect();

    let genre_rows = client.query(
        "SELECT genre FROM release_genre WHERE release_id = $1", &[&id],
    ).await?;
    let genres: Vec<String> = genre_rows.iter().map(|r| r.get("genre")).collect();

    let style_rows = client.query(
        "SELECT style FROM release_style WHERE release_id = $1", &[&id],
    ).await?;
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

pub async fn query_discogs_master(client: &Client, id: i32) -> anyhow::Result<Option<DiscogsMaster>> {
    let row = match client.query_opt(
        "SELECT id, title, year FROM master WHERE id = $1",
        &[&id],
    ).await? {
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
    client: &Client,
    q: &str,
    per_page: i32,
    page: i32,
) -> anyhow::Result<DiscogsSearchResponse> {
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
    let result_rows = if has_more { &rows[..per_page as usize] } else { &rows[..] };

    let results: Vec<DiscogsSearchResult> = result_rows.iter().map(|r| {
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
    }).collect();

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
