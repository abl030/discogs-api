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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub master_title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub master_first_released: Option<String>,
    pub primary_type: String,
    pub score: f32,
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
    pub primary_type: String,
    pub first_release_date: String,
    pub artist_credit: String,
    pub primary_artist_id: Option<i32>,
    pub artists: Vec<ApiArtistCredit>,
    pub releases: Vec<MasterRelease>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MasterRelease {
    pub id: i32,
    pub title: String,
    pub country: String,
    pub released: String,
    pub track_count: i32,
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
pub struct ArtistSearchResponse {
    pub results: Vec<ArtistSearchResult>,
    pub total: i64,
    pub page: i32,
    pub per_page: i32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ArtistSearchResult {
    pub id: i32,
    pub name: String,
    pub profile: String,
    pub score: f32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ArtistMastersResponse {
    pub results: Vec<ArtistMasterEntry>,
    pub total: i64,
    pub page: i32,
    pub per_page: i32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ArtistMasterEntry {
    pub id: MasterEntryId,
    pub title: String,
    #[serde(rename = "type")]
    pub type_: String,
    pub first_release_date: String,
    pub artist_credit: String,
    pub primary_artist_id: Option<i32>,
    pub is_masterless: bool,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum MasterEntryId {
    Master(i32),
    Release(String),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Pagination {
    pub page: i32,
    pub per_page: i32,
    pub pages: i32,
    pub items: i64,
}

// ---------------------------------------------------------------------------
// Label endpoints — search, detail, releases
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize, Deserialize)]
pub struct LabelSearchResponse {
    pub results: Vec<LabelHit>,
    pub total: i64,
    pub page: i32,
    pub per_page: i32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LabelHit {
    pub id: i32,
    pub name: String,
    pub profile: String,
    pub parent_label_id: Option<i32>,
    pub parent_label_name: Option<String>,
    pub release_count: i64,
    pub score: f32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LabelDetail {
    pub id: i32,
    pub name: String,
    pub profile: String,
    pub contactinfo: String,
    pub data_quality: String,
    pub parent_label_id: Option<i32>,
    pub parent_label_name: Option<String>,
    pub total_releases: i64,
    pub sub_labels: Vec<SubLabel>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SubLabel {
    pub id: i32,
    pub name: String,
    pub release_count: i64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LabelReleasesResponse {
    pub results: Vec<LabelReleaseEntry>,
    pub pagination: Pagination,
    pub include_sublabels: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LabelReleaseEntry {
    pub id: i32,
    pub title: String,
    pub country: String,
    pub released: String,
    pub master_id: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub master_title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub master_first_released: Option<String>,
    pub primary_type: String,
    pub label_id: i32,
    pub via_label_id: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sub_label_name: Option<String>,
    pub artists: Vec<ApiArtistCredit>,
    pub labels: Vec<ApiLabel>,
    pub formats: Vec<ApiFormat>,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn label_release_entry_serializes_additive_label_id_fields() {
        let entry = LabelReleaseEntry {
            id: 83182,
            title: "OK Computer".to_string(),
            country: "Europe".to_string(),
            released: "1997-06-16".to_string(),
            master_id: Some(21491),
            master_title: Some("OK Computer".to_string()),
            master_first_released: Some("1997".to_string()),
            primary_type: "Album".to_string(),
            label_id: 2294,
            via_label_id: 2294,
            sub_label_name: None,
            artists: vec![],
            labels: vec![],
            formats: vec![],
        };

        let value = serde_json::to_value(entry).expect("serialize label release entry");
        assert_eq!(value["label_id"], 2294);
        assert_eq!(value["via_label_id"], 2294);
    }
}
