use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Import types — built by the XML parser, flushed to Postgres via COPY
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
pub struct Artist {
    pub id: i32,
    pub name: String,
    pub realname: String,
    pub profile: String,
    pub data_quality: String,
    pub aliases: Vec<ArtistAlias>,
    pub namevariations: Vec<String>,
}

#[derive(Debug, Default)]
pub struct ArtistAlias {
    pub alias_id: i32,
    pub name: String,
}

#[derive(Debug, Default)]
pub struct Label {
    pub id: i32,
    pub name: String,
    pub contactinfo: String,
    pub profile: String,
    pub parent_label_id: Option<i32>,
    pub data_quality: String,
}

#[derive(Debug, Default)]
pub struct Master {
    pub id: i32,
    pub title: String,
    pub year: Option<i32>,
    pub main_release_id: Option<i32>,
    pub data_quality: String,
    pub artists: Vec<CreditedArtist>,
}

#[derive(Debug, Default)]
pub struct Release {
    pub id: i32,
    pub title: String,
    pub country: String,
    pub released: String,
    pub notes: String,
    pub master_id: Option<i32>,
    pub status: String,
    pub data_quality: String,
    pub artists: Vec<CreditedArtist>,
    pub labels: Vec<ReleaseLabel>,
    pub formats: Vec<ReleaseFormat>,
    pub tracks: Vec<ReleaseTrack>,
    pub genres: Vec<String>,
    pub styles: Vec<String>,
    pub identifiers: Vec<ReleaseIdentifier>,
}

#[derive(Debug, Default, Clone)]
pub struct CreditedArtist {
    pub artist_id: i32,
    pub artist_name: String,
    pub role: String,
    pub anv: String,
    pub join_relation: String,
}

#[derive(Debug, Default)]
pub struct ReleaseLabel {
    pub label_id: i32,
    pub label_name: String,
    pub catno: String,
}

#[derive(Debug, Default)]
pub struct ReleaseFormat {
    pub name: String,
    pub qty: i32,
    pub descriptions: String,
    pub free_text: String,
}

#[derive(Debug, Default)]
pub struct ReleaseTrack {
    pub sequence: i32,
    pub position: String,
    pub title: String,
    pub duration: String,
    pub artists: Vec<CreditedArtist>,
}

#[derive(Debug, Default)]
pub struct ReleaseIdentifier {
    pub type_: String,
    pub value: String,
    pub description: String,
}

// ---------------------------------------------------------------------------
// API response types — returned as JSON by the axum server
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize, Deserialize)]
pub struct HealthResponse {
    pub status: String,
    pub releases: i64,
    pub last_import: String,
    pub dump_date: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SearchResponse {
    pub results: Vec<SearchResult>,
    pub page: i32,
    pub per_page: i32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SearchResult {
    pub id: i32,
    pub title: String,
    pub country: String,
    pub released: String,
    pub master_id: Option<i32>,
    pub artists: Vec<ApiArtistCredit>,
    pub labels: Vec<ApiLabel>,
    pub formats: Vec<ApiFormat>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ReleaseDetail {
    pub id: i32,
    pub title: String,
    pub country: String,
    pub released: String,
    pub master_id: Option<i32>,
    pub artists: Vec<ApiArtistCredit>,
    pub labels: Vec<ApiLabel>,
    pub formats: Vec<ApiFormat>,
    pub tracks: Vec<ApiTrack>,
    pub genres: Vec<String>,
    pub styles: Vec<String>,
    pub identifiers: Vec<ApiIdentifier>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MasterDetail {
    pub id: i32,
    pub title: String,
    pub year: Option<i32>,
    pub main_release_id: Option<i32>,
    pub artists: Vec<ApiArtistCredit>,
    pub releases: Vec<MasterRelease>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MasterRelease {
    pub id: i32,
    pub title: String,
    pub country: String,
    pub formats: Vec<ApiFormat>,
    pub labels: Vec<ApiLabel>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ArtistDetail {
    pub id: i32,
    pub name: String,
    pub realname: String,
    pub profile: String,
    pub aliases: Vec<ApiAlias>,
    pub namevariations: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ArtistReleasesResponse {
    pub results: Vec<SearchResult>,
    pub pagination: Pagination,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Pagination {
    pub page: i32,
    pub per_page: i32,
    pub pages: i32,
    pub items: i64,
}

// Shared API sub-types

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiArtistCredit {
    pub id: i32,
    pub name: String,
    pub role: String,
    pub anv: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiLabel {
    pub id: i32,
    pub name: String,
    pub catno: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiFormat {
    pub name: String,
    pub qty: i32,
    pub descriptions: String,
    pub free_text: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ApiTrack {
    pub position: String,
    pub title: String,
    pub duration: String,
    pub artists: Vec<ApiArtistCredit>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ApiIdentifier {
    #[serde(rename = "type")]
    pub type_: String,
    pub value: String,
    pub description: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ApiAlias {
    pub id: i32,
    pub name: String,
}

// ---------------------------------------------------------------------------
// Discogs-API-compatible response types (for beets/python3-discogs-client)
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
pub struct DiscogsRelease {
    pub id: i32,
    pub title: String,
    pub uri: String,
    pub year: Option<i32>,
    pub country: String,
    pub master_id: Option<i32>,
    pub data_quality: String,
    pub artists: Vec<DiscogsArtistCredit>,
    pub tracklist: Vec<DiscogsTrack>,
    pub labels: Vec<ApiLabel>,
    pub formats: Vec<DiscogsFormat>,
    pub genres: Vec<String>,
    pub styles: Vec<String>,
    pub images: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DiscogsArtistCredit {
    pub id: i32,
    pub name: String,
    pub anv: String,
    pub join: String,
    pub role: String,
    pub tracks: String,
    pub resource_url: String,
}

#[derive(Debug, Serialize)]
pub struct DiscogsTrack {
    pub position: String,
    pub type_: String,
    pub title: String,
    pub duration: String,
    pub artists: Vec<DiscogsArtistCredit>,
    pub extraartists: Vec<DiscogsArtistCredit>,
}

#[derive(Debug, Serialize)]
pub struct DiscogsFormat {
    pub name: String,
    pub qty: String,
    pub descriptions: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct DiscogsMaster {
    pub id: i32,
    pub year: Option<i32>,
    pub title: String,
}

#[derive(Debug, Serialize)]
pub struct DiscogsSearchResponse {
    pub pagination: DiscogsPagination,
    pub results: Vec<DiscogsSearchResult>,
}

#[derive(Debug, Serialize)]
pub struct DiscogsPagination {
    pub pages: i32,
    pub items: i64,
}

#[derive(Debug, Serialize)]
pub struct DiscogsSearchResult {
    pub id: i32,
    #[serde(rename = "type")]
    pub type_: String,
    pub title: String,
}
