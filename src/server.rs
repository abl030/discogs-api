use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use clap::Parser;
use serde::Deserialize;

use discogs_api::db;
use discogs_api::types::*;

#[derive(Parser)]
#[command(name = "discogs-api", about = "Discogs mirror JSON API server")]
struct Args {
    /// PostgreSQL connection string
    #[arg(long)]
    dsn: String,

    /// Port to listen on
    #[arg(long, default_value = "8086")]
    port: u16,
}

struct AppState {
    client: tokio_postgres::Client,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    let client = db::connect(&args.dsn).await?;
    let state = Arc::new(AppState { client });

    let app = Router::new()
        .route("/health", get(health))
        .route("/api/search", get(search))
        .route("/api/releases/{id}", get(get_release))
        .route("/api/masters/{id}", get(get_master))
        .route("/api/artists", get(search_artists))
        .route("/api/artists/{id}", get(get_artist))
        .route("/api/artists/{id}/releases", get(get_artist_releases))
        .route("/api/artists/{id}/masters", get(get_artist_masters))
        .route("/api/labels", get(search_labels))
        .route("/api/labels/{id}", get(get_label))
        .route("/api/labels/{id}/releases", get(get_label_releases))
        // Discogs-API-compatible routes (for beets / python3-discogs-client)
        .route("/releases/{id}", get(discogs_get_release))
        .route("/masters/{id}", get(discogs_get_master))
        .route("/database/search", get(discogs_search))
        .with_state(state);

    let addr = format!("0.0.0.0:{}", args.port);
    tracing::info!("listening on {addr}");
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn health(
    State(state): State<Arc<AppState>>,
) -> Result<Json<HealthResponse>, StatusCode> {
    db::query_health(&state.client).await
        .map(Json)
        .map_err(|e| { tracing::error!("health error: {e}"); StatusCode::INTERNAL_SERVER_ERROR })
}

#[derive(Deserialize)]
struct SearchParams {
    artist: Option<String>,
    title: Option<String>,
    page: Option<i32>,
    per_page: Option<i32>,
}

#[derive(Deserialize)]
struct DiscogsSearchParams {
    q: Option<String>,
    per_page: Option<i32>,
    page: Option<i32>,
}

async fn search(
    State(state): State<Arc<AppState>>,
    Query(params): Query<SearchParams>,
) -> Result<Json<SearchResponse>, StatusCode> {
    db::query_search(
        &state.client,
        params.title.as_deref(),
        params.artist.as_deref(),
        params.page.unwrap_or(1),
        params.per_page.unwrap_or(25),
    ).await
        .map(Json)
        .map_err(|e| { tracing::error!("search error: {e}"); StatusCode::INTERNAL_SERVER_ERROR })
}

async fn get_release(
    State(state): State<Arc<AppState>>,
    Path(id): Path<i32>,
) -> Result<Json<ReleaseDetail>, StatusCode> {
    match db::query_release(&state.client, id).await {
        Ok(Some(r)) => Ok(Json(r)),
        Ok(None) => Err(StatusCode::NOT_FOUND),
        Err(e) => { tracing::error!("release query error: {e}"); Err(StatusCode::INTERNAL_SERVER_ERROR) }
    }
}

async fn get_master(
    State(state): State<Arc<AppState>>,
    Path(id): Path<i32>,
) -> Result<Json<MasterDetail>, StatusCode> {
    match db::query_master(&state.client, id).await {
        Ok(Some(m)) => Ok(Json(m)),
        Ok(None) => Err(StatusCode::NOT_FOUND),
        Err(e) => { tracing::error!("master query error: {e}"); Err(StatusCode::INTERNAL_SERVER_ERROR) }
    }
}

async fn get_artist(
    State(state): State<Arc<AppState>>,
    Path(id): Path<i32>,
) -> Result<Json<ArtistDetail>, StatusCode> {
    match db::query_artist(&state.client, id).await {
        Ok(Some(a)) => Ok(Json(a)),
        Ok(None) => Err(StatusCode::NOT_FOUND),
        Err(e) => { tracing::error!("artist query error: {e}"); Err(StatusCode::INTERNAL_SERVER_ERROR) }
    }
}

#[derive(Deserialize)]
struct PaginationParams {
    page: Option<i32>,
    per_page: Option<i32>,
}

async fn get_artist_releases(
    State(state): State<Arc<AppState>>,
    Path(id): Path<i32>,
    Query(params): Query<PaginationParams>,
) -> Result<Json<ArtistReleasesResponse>, StatusCode> {
    match db::query_artist_releases(
        &state.client,
        id,
        params.page.unwrap_or(1),
        params.per_page.unwrap_or(100),
    ).await {
        Ok(Some(r)) => Ok(Json(r)),
        Ok(None) => Err(StatusCode::NOT_FOUND),
        Err(e) => { tracing::error!("artist releases query error: {e}"); Err(StatusCode::INTERNAL_SERVER_ERROR) }
    }
}

async fn get_artist_masters(
    State(state): State<Arc<AppState>>,
    Path(id): Path<i32>,
    Query(params): Query<PaginationParams>,
) -> Result<Json<ArtistMastersResponse>, StatusCode> {
    match db::query_artist_masters(
        &state.client,
        id,
        params.page.unwrap_or(1),
        params.per_page.unwrap_or(100),
    ).await {
        Ok(Some(r)) => Ok(Json(r)),
        Ok(None) => Err(StatusCode::NOT_FOUND),
        Err(e) => { tracing::error!("artist masters query error: {e}"); Err(StatusCode::INTERNAL_SERVER_ERROR) }
    }
}

#[derive(Deserialize)]
struct ArtistSearchParams {
    name: Option<String>,
    page: Option<i32>,
    per_page: Option<i32>,
}

async fn search_artists(
    State(state): State<Arc<AppState>>,
    Query(params): Query<ArtistSearchParams>,
) -> Result<Json<ArtistSearchResponse>, StatusCode> {
    let name = match params.name {
        Some(ref n) if !n.is_empty() => n.as_str(),
        _ => return Ok(Json(ArtistSearchResponse {
            results: vec![],
            total: 0,
            page: params.page.unwrap_or(1),
            per_page: params.per_page.unwrap_or(25),
        })),
    };
    db::query_artist_search(
        &state.client,
        name,
        params.page.unwrap_or(1),
        params.per_page.unwrap_or(25),
    ).await
        .map(Json)
        .map_err(|e| { tracing::error!("artist search error: {e}"); StatusCode::INTERNAL_SERVER_ERROR })
}

// ---------------------------------------------------------------------------
// Label handlers
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct LabelSearchParams {
    name: Option<String>,
    page: Option<i32>,
    per_page: Option<i32>,
}

async fn search_labels(
    State(state): State<Arc<AppState>>,
    Query(params): Query<LabelSearchParams>,
) -> Result<Json<LabelSearchResponse>, StatusCode> {
    let name = match params.name {
        Some(ref n) if !n.is_empty() => n.as_str(),
        _ => return Ok(Json(LabelSearchResponse {
            results: vec![],
            total: 0,
            page: params.page.unwrap_or(1),
            per_page: params.per_page.unwrap_or(25),
        })),
    };
    db::query_label_search(
        &state.client,
        name,
        params.page.unwrap_or(1),
        params.per_page.unwrap_or(25),
    ).await
        .map(Json)
        .map_err(|e| { tracing::error!("label search error: {e}"); StatusCode::INTERNAL_SERVER_ERROR })
}

async fn get_label(
    State(state): State<Arc<AppState>>,
    Path(id): Path<i32>,
) -> Result<Json<LabelDetail>, StatusCode> {
    match db::query_label(&state.client, id).await {
        Ok(Some(l)) => Ok(Json(l)),
        Ok(None) => Err(StatusCode::NOT_FOUND),
        Err(e) => { tracing::error!("label query error: {e}"); Err(StatusCode::INTERNAL_SERVER_ERROR) }
    }
}

#[derive(Deserialize)]
struct LabelReleasesParams {
    page: Option<i32>,
    per_page: Option<i32>,
    include_sublabels: Option<bool>,
}

async fn get_label_releases(
    State(state): State<Arc<AppState>>,
    Path(id): Path<i32>,
    Query(params): Query<LabelReleasesParams>,
) -> Result<Json<LabelReleasesResponse>, StatusCode> {
    match db::query_label_releases(
        &state.client,
        id,
        params.page.unwrap_or(1),
        params.per_page.unwrap_or(100),
        params.include_sublabels.unwrap_or(true),
    ).await {
        Ok(Some(r)) => Ok(Json(r)),
        Ok(None) => Err(StatusCode::NOT_FOUND),
        Err(e) => {
            // The recursive-CTE branch installs SET LOCAL statement_timeout
            // and surfaces query_canceled as `LabelReleasesTimeout`. Map
            // that to 503 so the caller can retry with include_sublabels=false
            // instead of seeing a generic 500.
            if e.downcast_ref::<db::LabelReleasesTimeout>().is_some() {
                tracing::warn!("label releases timed out for id={id}: {e}");
                return Err(StatusCode::SERVICE_UNAVAILABLE);
            }
            tracing::error!("label releases query error: {e}");
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

// ---------------------------------------------------------------------------
// Discogs-API-compatible handlers (for beets / python3-discogs-client)
// ---------------------------------------------------------------------------

async fn discogs_get_release(
    State(state): State<Arc<AppState>>,
    Path(id): Path<i32>,
) -> Result<Json<DiscogsRelease>, StatusCode> {
    match db::query_discogs_release(&state.client, id).await {
        Ok(Some(r)) => Ok(Json(r)),
        Ok(None) => Err(StatusCode::NOT_FOUND),
        Err(e) => { tracing::error!("discogs release query error: {e}"); Err(StatusCode::INTERNAL_SERVER_ERROR) }
    }
}

async fn discogs_get_master(
    State(state): State<Arc<AppState>>,
    Path(id): Path<i32>,
) -> Result<Json<DiscogsMaster>, StatusCode> {
    match db::query_discogs_master(&state.client, id).await {
        Ok(Some(m)) => Ok(Json(m)),
        Ok(None) => Err(StatusCode::NOT_FOUND),
        Err(e) => { tracing::error!("discogs master query error: {e}"); Err(StatusCode::INTERNAL_SERVER_ERROR) }
    }
}

async fn discogs_search(
    State(state): State<Arc<AppState>>,
    Query(params): Query<DiscogsSearchParams>,
) -> Result<Json<DiscogsSearchResponse>, StatusCode> {
    let q = match params.q {
        Some(ref q) if !q.is_empty() => q.as_str(),
        _ => return Ok(Json(DiscogsSearchResponse {
            pagination: DiscogsPagination { pages: 0, items: 0 },
            results: vec![],
        })),
    };
    db::query_discogs_search(
        &state.client,
        q,
        params.per_page.unwrap_or(5),
        params.page.unwrap_or(1),
    ).await
        .map(Json)
        .map_err(|e| { tracing::error!("discogs search error: {e}"); StatusCode::INTERNAL_SERVER_ERROR })
}
