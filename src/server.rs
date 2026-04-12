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
        .route("/api/artists/{id}", get(get_artist))
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
