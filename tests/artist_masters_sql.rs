use std::collections::BTreeSet;
use std::fs;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use discogs_api::db;
use proptest::prelude::*;
use proptest::test_runner::{Config as ProptestConfig, TestCaseError, TestRunner};
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::runtime::Runtime;

const ARTIST_ID: i32 = 1;

const NULL_PRIMARY_ARTIST_FIXTURE_JSON: &str = r#"{"id":60,"title":"Mixed appearance master","type":"EP","primary_types":["EP","Single"],"first_release_date":"2005","artist_credit":"","primary_artist_id":null,"is_masterless":false}"#;

struct EphemeralPostgres {
    child: Child,
    _root: TempDir,
    dsn: String,
}

impl EphemeralPostgres {
    fn start() -> Self {
        let root = tempfile::Builder::new()
            .prefix("discogs-pg-")
            .tempdir()
            .expect("create postgres tempdir");
        let data = root.path().join("data");
        let socket = root.path().join("socket");
        fs::create_dir(&socket).expect("create postgres socket directory");

        let init = Command::new("initdb")
            .args([
                "--no-sync",
                "--no-locale",
                "--encoding=UTF8",
                "--auth=trust",
                "--username=postgres",
                "-D",
            ])
            .arg(&data)
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .output()
            .expect("run initdb (use the documented Nix test shell)");
        assert!(
            init.status.success(),
            "initdb failed: {}",
            String::from_utf8_lossy(&init.stderr)
        );

        let mut child = Command::new("postgres")
            .arg("-D")
            .arg(&data)
            .arg("-k")
            .arg(&socket)
            .arg("-h")
            .arg("")
            .arg("-F")
            .args(["-c", "synchronous_commit=off", "-c", "full_page_writes=off"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("start ephemeral postgres");

        let started = Instant::now();
        loop {
            let ready = Command::new("pg_isready")
                .arg("-h")
                .arg(&socket)
                .arg("-U")
                .arg("postgres")
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .is_ok_and(|status| status.success());
            if ready {
                break;
            }
            if started.elapsed() > Duration::from_secs(10) {
                let _ = child.kill();
                panic!("ephemeral postgres did not become ready");
            }
            thread::sleep(Duration::from_millis(25));
        }

        Self {
            child,
            dsn: format!(
                "host={} user=postgres dbname=postgres",
                socket.to_string_lossy()
            ),
            _root: root,
        }
    }
}

impl Drop for EphemeralPostgres {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

async fn initialize(dsn: &str) -> deadpool_postgres::Pool {
    let client = db::connect(dsn).await.expect("connect to test postgres");
    db::init_schema(&client).await.expect("initialize schema");
    db::create_pool(dsn).await.expect("create test pool")
}

async fn seed_contract_fixture(pool: &deadpool_postgres::Pool) {
    let client = pool.get().await.expect("get seed connection");
    client
        .batch_execute(
            "
            INSERT INTO artist (id, name) VALUES
                (1, 'Target'), (2, 'Guest'), (3, 'Empty Artist'),
                (4, 'Master-only Appearance'),
                (5, 'Masterless-only Appearance'),
                (6, 'No Appearances');
            INSERT INTO master (id, title, main_release_id) VALUES
                (10, 'Main release wins', 101),
                (20, 'Fallback tie', NULL),
                (30, 'Compilation only', 301),
                (50, 'Numeric namespace master', 501),
                (60, 'Mixed appearance master', 601),
                (70, 'Compilation appearance master', 701);
            INSERT INTO release (id, title, released, master_id) VALUES
                (101, 'Uncredited main release', '2002', 10),
                (102, 'Artist subset release', '1999', 10),
                (201, 'Tie winner', '2000', 20),
                (202, 'Tie loser', '2000', 20),
                (301, 'Compilation child', '', 30),
                (501, 'Master namespace child', '2001', 50),
                (50, 'Numeric namespace release', '2001', NULL),
                (400, 'Duplicate masterless zero', '2003', 0),
                (401, 'Mini album masterless null', '2004', NULL),
                (402, 'Compilation masterless null', '', NULL),
                (601, 'EP appearance child', '2005', 60),
                (602, 'Single appearance child', '2006', 60),
                (60, 'Numeric appearance release', '2007', NULL),
                (700, 'Zero sentinel appearance', '2008', 0),
                (701, 'Compilation appearance child', '', 70);
            INSERT INTO release_artist
                (release_id, artist_id, artist_name, join_relation)
            VALUES
                (102, 1, 'Target', ''),
                (201, 1, 'Target', ''),
                (202, 1, 'Target', ''),
                (301, 1, 'Target', ''),
                (501, 1, 'Target', ''),
                (50, 1, 'Target', ''),
                (50, 2, 'Guest', 'feat.'),
                (400, 1, 'Target', ''),
                (400, 1, 'Target', ''),
                (401, 1, 'Target', ''),
                (401, 2, 'Guest', 'feat.'),
                (402, 1, 'Target', ''),
                (60, 2, 'Guest', ''),
                (700, 2, 'Guest', '');
            INSERT INTO master_artist (master_id, artist_id, artist_name) VALUES
                (10, 1, 'Target'),
                (10, 2, 'Guest'),
                (30, 1, 'Target'),
                (50, 1, 'Target'),
                (70, 2, 'Guest');
            INSERT INTO release_format (release_id, descriptions) VALUES
                (101, 'Album, Compilation'),
                (102, 'Single'),
                (201, 'EP'),
                (202, 'Single'),
                (301, 'Compilation'),
                (501, 'Single'),
                (50, 'Album'),
                (400, 'Single, Single'),
                (401, 'Mini-Album, Compilation'),
                (402, 'Compilation'),
                (601, 'EP, Compilation'),
                (602, 'Single, Promo'),
                (60, 'Album, Compilation'),
                (700, 'Single, Unofficial Release'),
                (701, 'Compilation');
            INSERT INTO release_track_artist
                (release_id, sequence, artist_id, artist_name)
            VALUES
                (601, 1, 1, 'Target'),
                (602, 1, 1, 'Target'),
                (60, 1, 1, 'Target'),
                (700, 1, 1, 'Target'),
                (701, 1, 1, 'Target'),
                (601, 2, 4, 'Master-only Appearance'),
                (60, 2, 5, 'Masterless-only Appearance');
            ",
        )
        .await
        .expect("seed contract fixture");
}

fn response_rows(response: &discogs_api::types::ArtistMastersResponse) -> Vec<Value> {
    response
        .results
        .iter()
        .map(|entry| serde_json::to_value(entry).expect("serialize artist entry"))
        .collect()
}

async fn legacy_rows(pool: &deadpool_postgres::Pool, per_page: i32) -> (Vec<Value>, i64) {
    let first = db::query_artist_masters(pool, ARTIST_ID, 1, per_page)
        .await
        .expect("query first legacy page")
        .expect("artist exists");
    let total = first.total;
    let pages = ((total + i64::from(per_page) - 1) / i64::from(per_page)).max(1);
    let mut rows = response_rows(&first);
    for page in 2..=pages {
        let response = db::query_artist_masters(pool, ARTIST_ID, page as i32, per_page)
            .await
            .expect("query legacy page")
            .expect("artist exists");
        assert_eq!(response.total, total);
        rows.extend(response_rows(&response));
    }
    (rows, total)
}

fn check_conserved(
    legacy: &[Value],
    legacy_total: i64,
    bulk: &[Value],
    bulk_total: i64,
) -> Result<(), String> {
    if legacy_total != bulk_total {
        return Err(format!(
            "reported totals differ: legacy={legacy_total}, bulk={bulk_total}"
        ));
    }
    if legacy_total as usize != legacy.len() {
        return Err(format!(
            "legacy total {legacy_total} does not match {} serialized rows",
            legacy.len()
        ));
    }
    if bulk_total as usize != bulk.len() {
        return Err(format!(
            "bulk total {bulk_total} does not match {} serialized rows",
            bulk.len()
        ));
    }
    if legacy != bulk {
        return Err("serialized rows differ row-for-row".to_string());
    }
    Ok(())
}

fn assert_conserved(legacy: &[Value], legacy_total: i64, bulk: &[Value], bulk_total: i64) {
    if let Err(error) = check_conserved(legacy, legacy_total, bulk, bulk_total) {
        panic!("artist masters conservation failed: {error}");
    }
}

fn expected_appearance_rows() -> Vec<Value> {
    vec![
        json!({
            "id": 60, "title": "Mixed appearance master", "type": "EP",
            "primary_types": ["EP", "Single"],
            "first_release_date": "2005", "artist_credit": "",
            "primary_artist_id": null, "is_masterless": false
        }),
        json!({
            "id": "release-60", "title": "Numeric appearance release", "type": "Album",
            "primary_types": ["Album"],
            "first_release_date": "2007", "artist_credit": "Guest",
            "primary_artist_id": 2, "is_masterless": true
        }),
        json!({
            "id": "release-700", "title": "Zero sentinel appearance", "type": "Single",
            "primary_types": ["Single"],
            "first_release_date": "2008", "artist_credit": "Guest",
            "primary_artist_id": 2, "is_masterless": true
        }),
        json!({
            "id": 70, "title": "Compilation appearance master", "type": "Album",
            "primary_types": [],
            "first_release_date": "", "artist_credit": "Guest",
            "primary_artist_id": 2, "is_masterless": false
        }),
    ]
}

fn check_appearance_rows(actual: &[Value], expected: &[Value]) -> Result<(), String> {
    if actual.len() != expected.len() {
        return Err(format!(
            "appearance row count differs: actual={}, expected={}",
            actual.len(),
            expected.len()
        ));
    }
    for (index, (actual, expected)) in actual.iter().zip(expected).enumerate() {
        if actual != expected {
            return Err(format!("appearance row {index} differs"));
        }
    }
    Ok(())
}

#[test]
fn deterministic_appearances_contract_pins_structural_types_and_namespaces() {
    let postgres = EphemeralPostgres::start();
    let runtime = Runtime::new().expect("create runtime");
    runtime.block_on(async {
        let pool = initialize(&postgres.dsn).await;
        seed_contract_fixture(&pool).await;

        let response = db::query_artist_appearances(&pool, ARTIST_ID)
            .await
            .expect("query appearances")
            .expect("artist exists");
        let rows = response_rows(&response);
        assert_eq!(response.total, 4);
        let expected = expected_appearance_rows();
        check_appearance_rows(&rows, &expected).expect("appearance rows match strict fixture");
        assert_eq!(
            serde_json::to_string(&response.results[0]).expect("serialize nullable fixture"),
            NULL_PRIMARY_ARTIST_FIXTURE_JSON
        );

        // One-sided result sets exercise the same real enrichment query with
        // an empty int4[] in the opposite namespace. The empty artist pins the
        // early return for two empty arrays.
        let master_only = db::query_artist_appearances(&pool, 4)
            .await
            .expect("query master-only appearances")
            .expect("master-only artist exists");
        assert_eq!(master_only.total, 1);
        assert_eq!(
            response_rows(&master_only)[0],
            json!({
                "id": 60, "title": "Mixed appearance master", "type": "EP",
                "primary_types": ["EP", "Single"],
                "first_release_date": "2005", "artist_credit": "",
                "primary_artist_id": null, "is_masterless": false
            })
        );

        let masterless_only = db::query_artist_appearances(&pool, 5)
            .await
            .expect("query masterless-only appearances")
            .expect("masterless-only artist exists");
        assert_eq!(masterless_only.total, 1);
        assert_eq!(
            response_rows(&masterless_only)[0],
            json!({
                "id": "release-60", "title": "Numeric appearance release", "type": "Album",
                "primary_types": ["Album"],
                "first_release_date": "2007", "artist_credit": "Guest",
                "primary_artist_id": 2, "is_masterless": true
            })
        );

        let empty = db::query_artist_appearances(&pool, 6)
            .await
            .expect("query empty appearances")
            .expect("empty artist exists");
        assert_eq!(empty.total, 0);
        assert!(empty.results.is_empty());

        let mut namespace_mutant = rows.clone();
        namespace_mutant[0]["id"] = json!("release-60");
        assert!(
            check_appearance_rows(&namespace_mutant, &expected).is_err(),
            "checker accepted a master/release namespace mutant"
        );

        let mut typing_mutant = rows.clone();
        typing_mutant[0]["primary_types"] = json!(["EP"]);
        assert!(
            check_appearance_rows(&typing_mutant, &expected).is_err(),
            "checker accepted a child-pressing type-loss mutant"
        );

        let mut missing_field_mutant = rows.clone();
        missing_field_mutant[0]
            .as_object_mut()
            .expect("serialized row is an object")
            .remove("primary_types");
        assert!(
            check_appearance_rows(&missing_field_mutant, &expected).is_err(),
            "checker accepted a missing additive primary_types field"
        );
    });
}

#[test]
fn deterministic_bulk_contract_preserves_wire_semantics_and_multiplicity() {
    let postgres = EphemeralPostgres::start();
    let runtime = Runtime::new().expect("create runtime");
    runtime.block_on(async {
        let pool = initialize(&postgres.dsn).await;
        seed_contract_fixture(&pool).await;

        let (legacy, legacy_total) = legacy_rows(&pool, 3).await;
        let response = db::query_artist_masters_all(&pool, ARTIST_ID)
            .await
            .expect("query bulk route owner")
            .expect("artist exists");
        let bulk = response_rows(&response);
        assert_eq!(response.page, 1);
        assert_eq!(response.per_page, response.total.max(1) as i32);
        assert_conserved(&legacy, legacy_total, &bulk, response.total);

        let beyond_last_page = db::query_artist_masters(&pool, ARTIST_ID, 99, 3)
            .await
            .expect("query beyond final legacy page")
            .expect("artist exists");
        assert_eq!(beyond_last_page.total, response.total);
        assert!(beyond_last_page.results.is_empty());

        assert!(
            db::query_artist_masters_all(&pool, 999)
                .await
                .expect("query missing artist")
                .is_none()
        );
        let empty = db::query_artist_masters_all(&pool, 3)
            .await
            .expect("query empty artist")
            .expect("empty artist exists");
        assert_eq!(empty.total, 0);
        assert_eq!(empty.page, 1);
        assert_eq!(empty.per_page, 1);
        assert!(empty.results.is_empty());

        assert_eq!(response.total, 9);
        assert_eq!(
            bulk,
            vec![
                json!({
                    "id": 10, "title": "Main release wins", "type": "Album",
                    "primary_types": ["Album", "Single"],
                    "first_release_date": "1999", "artist_credit": "Target & Guest",
                    "primary_artist_id": 1, "is_masterless": false
                }),
                json!({
                    "id": 20, "title": "Fallback tie", "type": "EP",
                    "primary_types": ["EP", "Single"],
                    "first_release_date": "2000", "artist_credit": "",
                    "primary_artist_id": null, "is_masterless": false
                }),
                json!({
                    "id": 50, "title": "Numeric namespace master", "type": "Single",
                    "primary_types": ["Single"],
                    "first_release_date": "2001", "artist_credit": "Target",
                    "primary_artist_id": 1, "is_masterless": false
                }),
                json!({
                    "id": "release-50", "title": "Numeric namespace release", "type": "Album",
                    "primary_types": ["Album"],
                    "first_release_date": "2001", "artist_credit": "Target feat. Guest",
                    "primary_artist_id": 1, "is_masterless": true
                }),
                json!({
                    "id": "release-400", "title": "Duplicate masterless zero", "type": "Single",
                    "primary_types": ["Single"],
                    "first_release_date": "2003", "artist_credit": "Target & Target",
                    "primary_artist_id": 1, "is_masterless": true
                }),
                json!({
                    "id": "release-400", "title": "Duplicate masterless zero", "type": "Single",
                    "primary_types": ["Single"],
                    "first_release_date": "2003", "artist_credit": "Target & Target",
                    "primary_artist_id": 1, "is_masterless": true
                }),
                json!({
                    "id": "release-401", "title": "Mini album masterless null", "type": "EP",
                    "primary_types": ["EP"],
                    "first_release_date": "2004", "artist_credit": "Target feat. Guest",
                    "primary_artist_id": 1, "is_masterless": true
                }),
                json!({
                    "id": 30, "title": "Compilation only", "type": "Album",
                    "primary_types": [], "first_release_date": "",
                    "artist_credit": "Target", "primary_artist_id": 1,
                    "is_masterless": false
                }),
                json!({
                    "id": "release-402", "title": "Compilation masterless null", "type": "Album",
                    "primary_types": [], "first_release_date": "",
                    "artist_credit": "Target", "primary_artist_id": 1,
                    "is_masterless": true
                }),
            ]
        );

        let mut distinct_mutant = bulk.clone();
        distinct_mutant.dedup();
        assert!(
            check_conserved(
                &legacy,
                legacy_total,
                &distinct_mutant,
                distinct_mutant.len() as i64,
            )
            .is_err(),
            "checker accepted a DISTINCT-style duplicate-collapse mutant"
        );

        let mut representative_only_type_mutant = bulk.clone();
        let master = representative_only_type_mutant
            .iter_mut()
            .find(|row| row["id"] == 10)
            .expect("fixture includes multi-pressing master");
        master["primary_types"] = json!(["Album"]);
        assert!(
            check_conserved(
                &legacy,
                legacy_total,
                &representative_only_type_mutant,
                response.total,
            )
            .is_err(),
            "checker accepted a representative-only master type mutant"
        );
    });
}

#[derive(Clone, Debug)]
enum Evidence {
    Album,
    Ep,
    MiniAlbum,
    Single,
    Compilation,
    Unknown,
}

impl Evidence {
    fn description(&self) -> &'static str {
        match self {
            Self::Album => "Album, Compilation",
            Self::Ep => "EP, Compilation",
            Self::MiniAlbum => "Mini-Album, Compilation",
            Self::Single => "Single, Promo",
            Self::Compilation => "Compilation",
            Self::Unknown => "Box Set, Promo",
        }
    }

    fn scalar_type(&self) -> &'static str {
        match self {
            Self::Album | Self::Compilation => "Album",
            Self::Ep | Self::MiniAlbum => "EP",
            Self::Single => "Single",
            Self::Unknown => "Other",
        }
    }

    fn structural_type(&self) -> Option<&'static str> {
        match self {
            Self::Album => Some("Album"),
            Self::Ep | Self::MiniAlbum => Some("EP"),
            Self::Single => Some("Single"),
            Self::Compilation | Self::Unknown => None,
        }
    }
}

#[derive(Clone, Debug)]
struct MasterSpec {
    main_release: bool,
    first_date: String,
    second_date: String,
    first_evidence: Evidence,
    second_evidence: Evidence,
    credit_second_release: bool,
    has_master_credit: bool,
    guest_master_credit: bool,
}

#[derive(Clone, Debug)]
struct MasterlessSpec {
    zero_sentinel: bool,
    duplicate_credits: u8,
    date: String,
    evidence: Vec<Evidence>,
    guest_credit: bool,
}

#[derive(Clone, Debug)]
struct GeneratedWorld {
    masters: Vec<MasterSpec>,
    masterless: Vec<MasterlessSpec>,
    per_page: i32,
}

fn evidence_strategy() -> impl Strategy<Value = Evidence> {
    prop_oneof![
        Just(Evidence::Album),
        Just(Evidence::Ep),
        Just(Evidence::MiniAlbum),
        Just(Evidence::Single),
        Just(Evidence::Compilation),
        Just(Evidence::Unknown),
    ]
}

fn date_strategy() -> impl Strategy<Value = String> {
    prop_oneof![
        Just(String::new()),
        Just("1999".to_string()),
        Just("2000".to_string()),
        Just("2001".to_string()),
        Just("2001-01-01".to_string()),
    ]
}

fn master_strategy() -> impl Strategy<Value = MasterSpec> {
    (
        any::<bool>(),
        date_strategy(),
        date_strategy(),
        evidence_strategy(),
        evidence_strategy(),
        any::<bool>(),
        any::<bool>(),
        any::<bool>(),
    )
        .prop_map(
            |(
                main_release,
                first_date,
                second_date,
                first_evidence,
                second_evidence,
                credit_second_release,
                has_master_credit,
                guest_master_credit,
            )| MasterSpec {
                main_release,
                first_date,
                second_date,
                first_evidence,
                second_evidence,
                credit_second_release,
                has_master_credit,
                guest_master_credit,
            },
        )
}

fn masterless_strategy() -> impl Strategy<Value = MasterlessSpec> {
    (
        any::<bool>(),
        1_u8..4,
        date_strategy(),
        prop::collection::vec(evidence_strategy(), 1..4),
        any::<bool>(),
    )
        .prop_map(
            |(zero_sentinel, duplicate_credits, date, evidence, guest_credit)| MasterlessSpec {
                zero_sentinel,
                duplicate_credits,
                date,
                evidence,
                guest_credit,
            },
        )
}

fn generated_world_strategy() -> impl Strategy<Value = GeneratedWorld> {
    (
        prop::collection::vec(master_strategy(), 3..6),
        prop::collection::vec(masterless_strategy(), 3..7),
        1_i32..6,
    )
        .prop_map(|(mut masters, mut masterless, per_page)| {
            // Every generated world contains the high-risk shapes, while the
            // surrounding dates, credits, and extra rows remain generated.
            masters[0].main_release = true;
            masters[0].first_date = "2001".to_string();
            masters[0].second_date = "1999".to_string();
            masters[0].first_evidence = Evidence::Album;
            masters[0].second_evidence = Evidence::Single;
            masters[0].credit_second_release = false;
            masters[0].has_master_credit = false;

            masters[1].main_release = false;
            masters[1].first_date = "2000".to_string();
            masters[1].second_date = "2000".to_string();
            masters[1].first_evidence = Evidence::Ep;
            masters[1].second_evidence = Evidence::Single;
            masters[1].credit_second_release = true;

            masters[2].first_evidence = Evidence::Compilation;
            masters[2].second_evidence = Evidence::Unknown;

            masterless[0].zero_sentinel = true;
            masterless[0].duplicate_credits = masterless[0].duplicate_credits.max(2);
            masterless[0].date = "2001".to_string();
            masterless[0].evidence = vec![Evidence::MiniAlbum];

            masterless[1].zero_sentinel = false;
            masterless[1].evidence = vec![Evidence::Compilation];

            masterless[2].evidence = vec![Evidence::Single, Evidence::Album, Evidence::Ep];

            GeneratedWorld {
                masters,
                masterless,
                per_page,
            }
        })
}

fn master_id(index: usize) -> i32 {
    10 + index as i32
}

fn master_release_ids(index: usize) -> (i32, i32) {
    let first = 1_000 + index as i32 * 10;
    (first, first + 1)
}

fn masterless_release_id(index: usize) -> i32 {
    if index == 0 {
        // Deliberate master/release numeric namespace collision.
        master_id(0)
    } else {
        2_000 + index as i32
    }
}

async fn seed_generated_world(pool: &deadpool_postgres::Pool, world: &GeneratedWorld) {
    let mut client = pool.get().await.expect("get generated seed connection");
    let transaction = client.transaction().await.expect("start seed transaction");
    transaction
        .batch_execute(
            "TRUNCATE master_artist, release_track_artist, release_format,
                      release_artist, release, master, artist;
             INSERT INTO artist (id, name) VALUES
                 (1, 'Target'), (2, 'Guest'), (3, 'Appearing'),
                 (4, 'Master-only Appearance'),
                 (5, 'Masterless-only Appearance'),
                 (6, 'No Appearances');",
        )
        .await
        .expect("reset generated world");

    for (index, spec) in world.masters.iter().enumerate() {
        let id = master_id(index);
        let (first_release_id, second_release_id) = master_release_ids(index);
        let title = format!("Master {id}");
        let main_release_id = spec.main_release.then_some(first_release_id);
        transaction
            .execute(
                "INSERT INTO master (id, title, main_release_id) VALUES ($1, $2, $3)",
                &[&id, &title, &main_release_id],
            )
            .await
            .expect("insert generated master");

        for (release_id, date, evidence, credited) in [
            (
                first_release_id,
                &spec.first_date,
                &spec.first_evidence,
                true,
            ),
            (
                second_release_id,
                &spec.second_date,
                &spec.second_evidence,
                spec.credit_second_release,
            ),
        ] {
            let release_title = format!("Release {release_id}");
            transaction
                .execute(
                    "INSERT INTO release (id, title, released, master_id) VALUES ($1, $2, $3, $4)",
                    &[&release_id, &release_title, date, &id],
                )
                .await
                .expect("insert generated master child");
            transaction
                .execute(
                    "INSERT INTO release_format (release_id, descriptions) VALUES ($1, $2)",
                    &[&release_id, &evidence.description()],
                )
                .await
                .expect("insert generated master format");
            transaction
                .execute(
                    "INSERT INTO release_track_artist
                     (release_id, sequence, artist_id, artist_name)
                     VALUES ($1, 1, 3, 'Appearing')",
                    &[&release_id],
                )
                .await
                .expect("insert generated appearance credit");
            if credited {
                transaction
                    .execute(
                        "INSERT INTO release_artist
                         (release_id, artist_id, artist_name, join_relation)
                         VALUES ($1, 1, 'Target', '')",
                        &[&release_id],
                    )
                    .await
                    .expect("insert generated artist release credit");
            }
        }

        if index == 0 {
            transaction
                .execute(
                    "INSERT INTO release_track_artist
                     (release_id, sequence, artist_id, artist_name)
                     VALUES ($1, 2, 4, 'Master-only Appearance')",
                    &[&first_release_id],
                )
                .await
                .expect("insert generated master-only appearance credit");
        }

        if spec.has_master_credit {
            transaction
                .execute(
                    "INSERT INTO master_artist (master_id, artist_id, artist_name)
                     VALUES ($1, 1, 'Target')",
                    &[&id],
                )
                .await
                .expect("insert generated master credit");
            if spec.guest_master_credit {
                transaction
                    .execute(
                        "INSERT INTO master_artist (master_id, artist_id, artist_name)
                         VALUES ($1, 2, 'Guest')",
                        &[&id],
                    )
                    .await
                    .expect("insert generated guest master credit");
            }
        }
    }

    for (index, spec) in world.masterless.iter().enumerate() {
        let release_id = masterless_release_id(index);
        let title = format!("Masterless {release_id}");
        let master_id = spec.zero_sentinel.then_some(0_i32);
        transaction
            .execute(
                "INSERT INTO release (id, title, released, master_id) VALUES ($1, $2, $3, $4)",
                &[&release_id, &title, &spec.date, &master_id],
            )
            .await
            .expect("insert generated masterless release");
        for evidence in &spec.evidence {
            transaction
                .execute(
                    "INSERT INTO release_format (release_id, descriptions) VALUES ($1, $2)",
                    &[&release_id, &evidence.description()],
                )
                .await
                .expect("insert generated masterless format");
        }
        transaction
            .execute(
                "INSERT INTO release_track_artist
                 (release_id, sequence, artist_id, artist_name)
                 VALUES ($1, 1, 3, 'Appearing')",
                &[&release_id],
            )
            .await
            .expect("insert generated masterless appearance credit");
        if index == 0 {
            transaction
                .execute(
                    "INSERT INTO release_track_artist
                     (release_id, sequence, artist_id, artist_name)
                     VALUES ($1, 2, 5, 'Masterless-only Appearance')",
                    &[&release_id],
                )
                .await
                .expect("insert generated masterless-only appearance credit");
        }
        for _ in 0..spec.duplicate_credits {
            transaction
                .execute(
                    "INSERT INTO release_artist
                     (release_id, artist_id, artist_name, join_relation)
                     VALUES ($1, 1, 'Target', '')",
                    &[&release_id],
                )
                .await
                .expect("insert generated duplicate artist credit");
        }
        if spec.guest_credit {
            transaction
                .execute(
                    "INSERT INTO release_artist
                     (release_id, artist_id, artist_name, join_relation)
                     VALUES ($1, 2, 'Guest', 'feat.')",
                    &[&release_id],
                )
                .await
                .expect("insert generated guest release credit");
        }
    }

    transaction.commit().await.expect("commit generated world");
}

fn normalized_date(date: &str) -> Option<String> {
    (!date.is_empty()).then(|| date.to_string())
}

fn first_artist_date(spec: &MasterSpec) -> Option<String> {
    let mut dates = vec![normalized_date(&spec.first_date)];
    if spec.credit_second_release {
        dates.push(normalized_date(&spec.second_date));
    }
    dates.into_iter().flatten().min()
}

fn fallback_representative_index(spec: &MasterSpec) -> usize {
    if spec.main_release {
        return 0;
    }
    if !spec.credit_second_release {
        return 0;
    }
    match (
        normalized_date(&spec.first_date),
        normalized_date(&spec.second_date),
    ) {
        (Some(first), Some(second)) if second < first => 1,
        (None, Some(_)) => 1,
        _ => 0,
    }
}

fn structural_types<'a>(evidence: impl IntoIterator<Item = &'a Evidence>) -> Vec<String> {
    let mut types = BTreeSet::new();
    for item in evidence {
        if let Some(type_) = item.structural_type() {
            types.insert(type_.to_string());
        }
    }
    types.into_iter().collect()
}

fn scalar_type<'a>(evidence: impl IntoIterator<Item = &'a Evidence>) -> &'static str {
    evidence
        .into_iter()
        .find(|item| !matches!(item, Evidence::Unknown))
        .map(Evidence::scalar_type)
        .unwrap_or("Other")
}

#[derive(Debug)]
struct ExpectedRow {
    sort_date: Option<String>,
    entry_id: i32,
    kind_order: u8,
    value: Value,
}

fn expected_generated_rows(world: &GeneratedWorld) -> Vec<Value> {
    let mut rows = Vec::new();
    for (index, spec) in world.masters.iter().enumerate() {
        let id = master_id(index);
        let rep = if fallback_representative_index(spec) == 0 {
            &spec.first_evidence
        } else {
            &spec.second_evidence
        };
        let date = first_artist_date(spec);
        let (artist_credit, primary_artist_id) = if spec.has_master_credit {
            (
                if spec.guest_master_credit {
                    "Target & Guest"
                } else {
                    "Target"
                },
                json!(1),
            )
        } else {
            ("", Value::Null)
        };
        rows.push(ExpectedRow {
            sort_date: date.clone(),
            entry_id: id,
            kind_order: 0,
            value: json!({
                "id": id,
                "title": format!("Master {id}"),
                "type": rep.scalar_type(),
                "primary_types": structural_types([&spec.first_evidence, &spec.second_evidence]),
                "first_release_date": date.unwrap_or_default(),
                "artist_credit": artist_credit,
                "primary_artist_id": primary_artist_id,
                "is_masterless": false,
            }),
        });
    }

    for (index, spec) in world.masterless.iter().enumerate() {
        let id = masterless_release_id(index);
        let date = normalized_date(&spec.date);
        let duplicate_names = std::iter::repeat_n("Target", spec.duplicate_credits as usize)
            .collect::<Vec<_>>()
            .join(" & ");
        let artist_credit = if spec.guest_credit {
            format!("{duplicate_names} feat. Guest")
        } else {
            duplicate_names
        };
        let scalar = scalar_type(&spec.evidence);
        let value = json!({
            "id": format!("release-{id}"),
            "title": format!("Masterless {id}"),
            "type": scalar,
            "primary_types": structural_types(&spec.evidence),
            "first_release_date": date.clone().unwrap_or_default(),
            "artist_credit": artist_credit,
            "primary_artist_id": 1,
            "is_masterless": true,
        });
        for _ in 0..spec.duplicate_credits {
            rows.push(ExpectedRow {
                sort_date: date.clone(),
                entry_id: id,
                kind_order: 1,
                value: value.clone(),
            });
        }
    }

    rows.sort_by(|left, right| {
        left.sort_date
            .is_none()
            .cmp(&right.sort_date.is_none())
            .then_with(|| left.sort_date.cmp(&right.sort_date))
            .then_with(|| left.entry_id.cmp(&right.entry_id))
            .then_with(|| left.kind_order.cmp(&right.kind_order))
    });
    rows.into_iter().map(|row| row.value).collect()
}

fn first_appearance_date(spec: &MasterSpec) -> Option<String> {
    [
        normalized_date(&spec.first_date),
        normalized_date(&spec.second_date),
    ]
    .into_iter()
    .flatten()
    .min()
}

fn appearance_representative_index(spec: &MasterSpec) -> usize {
    if spec.main_release {
        return 0;
    }
    match (
        normalized_date(&spec.first_date),
        normalized_date(&spec.second_date),
    ) {
        (Some(first), Some(second)) if second < first => 1,
        (None, Some(_)) => 1,
        _ => 0,
    }
}

fn expected_generated_appearance_rows(world: &GeneratedWorld) -> Vec<Value> {
    let mut rows = Vec::new();
    for (index, spec) in world.masters.iter().enumerate() {
        let id = master_id(index);
        let rep = if appearance_representative_index(spec) == 0 {
            &spec.first_evidence
        } else {
            &spec.second_evidence
        };
        let date = first_appearance_date(spec);
        let (artist_credit, primary_artist_id) = if spec.has_master_credit {
            (
                if spec.guest_master_credit {
                    "Target & Guest"
                } else {
                    "Target"
                },
                json!(1),
            )
        } else {
            ("", Value::Null)
        };
        rows.push(ExpectedRow {
            sort_date: date.clone(),
            entry_id: id,
            kind_order: 0,
            value: json!({
                "id": id,
                "title": format!("Master {id}"),
                "type": rep.scalar_type(),
                "primary_types": structural_types([&spec.first_evidence, &spec.second_evidence]),
                "first_release_date": date.unwrap_or_default(),
                "artist_credit": artist_credit,
                "primary_artist_id": primary_artist_id,
                "is_masterless": false,
            }),
        });
    }

    for (index, spec) in world.masterless.iter().enumerate() {
        let id = masterless_release_id(index);
        let date = normalized_date(&spec.date);
        let duplicate_names = std::iter::repeat_n("Target", spec.duplicate_credits as usize)
            .collect::<Vec<_>>()
            .join(" & ");
        let artist_credit = if spec.guest_credit {
            format!("{duplicate_names} feat. Guest")
        } else {
            duplicate_names
        };
        rows.push(ExpectedRow {
            sort_date: date.clone(),
            entry_id: id,
            kind_order: 1,
            value: json!({
                "id": format!("release-{id}"),
                "title": format!("Masterless {id}"),
                "type": scalar_type(&spec.evidence),
                "primary_types": structural_types(&spec.evidence),
                "first_release_date": date.unwrap_or_default(),
                "artist_credit": artist_credit,
                "primary_artist_id": 1,
                "is_masterless": true,
            }),
        });
    }

    rows.sort_by(|left, right| {
        left.sort_date
            .is_none()
            .cmp(&right.sort_date.is_none())
            .then_with(|| left.sort_date.cmp(&right.sort_date))
            .then_with(|| left.entry_id.cmp(&right.entry_id))
            .then_with(|| left.kind_order.cmp(&right.kind_order))
    });
    rows.into_iter().map(|row| row.value).collect()
}

fn expected_master_only_appearance(world: &GeneratedWorld) -> Value {
    let spec = &world.masters[0];
    let (artist_credit, primary_artist_id) = if spec.has_master_credit {
        (
            if spec.guest_master_credit {
                "Target & Guest"
            } else {
                "Target"
            },
            json!(1),
        )
    } else {
        ("", Value::Null)
    };
    json!({
        "id": master_id(0),
        "title": format!("Master {}", master_id(0)),
        "type": spec.first_evidence.scalar_type(),
        "primary_types": structural_types([&spec.first_evidence, &spec.second_evidence]),
        "first_release_date": spec.first_date,
        "artist_credit": artist_credit,
        "primary_artist_id": primary_artist_id,
        "is_masterless": false,
    })
}

#[test]
fn generated_artist_queries_match_real_sql_contracts() {
    let postgres = EphemeralPostgres::start();
    let runtime = Runtime::new().expect("create generated runtime");
    let pool = runtime.block_on(initialize(&postgres.dsn));
    let mut config = ProptestConfig::with_cases(32);
    config.failure_persistence = None;
    let mut runner = TestRunner::new(config);
    runner
        .run(&generated_world_strategy(), |world| {
            runtime.block_on(async {
                seed_generated_world(&pool, &world).await;
                let (legacy, legacy_total) = legacy_rows(&pool, world.per_page).await;
                let response = db::query_artist_masters_all(&pool, ARTIST_ID)
                    .await
                    .map_err(|error| TestCaseError::fail(error.to_string()))?
                    .ok_or_else(|| TestCaseError::fail("generated artist disappeared"))?;
                let bulk = response_rows(&response);
                check_conserved(&legacy, legacy_total, &bulk, response.total)
                    .map_err(TestCaseError::fail)?;
                prop_assert_eq!(bulk, expected_generated_rows(&world));

                let appearances = db::query_artist_appearances(&pool, 3)
                    .await
                    .map_err(|error| TestCaseError::fail(error.to_string()))?
                    .ok_or_else(|| {
                        TestCaseError::fail("generated appearance artist disappeared")
                    })?;
                let appearance_rows = response_rows(&appearances);
                let expected_appearances = expected_generated_appearance_rows(&world);
                check_appearance_rows(&appearance_rows, &expected_appearances)
                    .map_err(TestCaseError::fail)?;
                prop_assert_eq!(appearances.total as usize, appearance_rows.len());

                let master_only = db::query_artist_appearances(&pool, 4)
                    .await
                    .map_err(|error| TestCaseError::fail(error.to_string()))?
                    .ok_or_else(|| TestCaseError::fail("master-only artist disappeared"))?;
                let master_only_rows = response_rows(&master_only);
                prop_assert_eq!(master_only_rows.len(), 1);
                prop_assert_eq!(
                    &master_only_rows[0],
                    &expected_master_only_appearance(&world)
                );

                let masterless_only = db::query_artist_appearances(&pool, 5)
                    .await
                    .map_err(|error| TestCaseError::fail(error.to_string()))?
                    .ok_or_else(|| TestCaseError::fail("masterless-only artist disappeared"))?;
                let masterless_only_rows = response_rows(&masterless_only);
                prop_assert_eq!(masterless_only_rows.len(), 1);
                let expected_masterless = expected_appearances
                    .iter()
                    .find(|row| row["id"] == format!("release-{}", masterless_release_id(0)))
                    .ok_or_else(|| TestCaseError::fail("expected collision release missing"))?;
                prop_assert_eq!(&masterless_only_rows[0], expected_masterless);

                let empty = db::query_artist_appearances(&pool, 6)
                    .await
                    .map_err(|error| TestCaseError::fail(error.to_string()))?
                    .ok_or_else(|| TestCaseError::fail("empty artist disappeared"))?;
                prop_assert_eq!(empty.total, 0);
                prop_assert!(empty.results.is_empty());
                Ok(())
            })
        })
        .expect("generated real-SQL artist query contracts");
}
