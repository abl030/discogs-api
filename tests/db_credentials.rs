use std::fs;
use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use discogs_api::db;
use tempfile::TempDir;

fn credential_file(contents: &str) -> (TempDir, std::path::PathBuf) {
    let root = tempfile::tempdir().expect("create credential tempdir");
    let path = root.path().join("postgres-password");
    fs::write(&path, contents).expect("write credential fixture");
    (root, path)
}

#[test]
fn config_reads_pgpassword_from_credential_file() {
    let (_root, credential) =
        credential_file("POSTGRES_PASSWORD=server-copy\nPGPASSWORD=file-secret\n");

    let config = db::connection_config("postgresql://discogs@127.0.0.1:5432/discogs", &credential)
        .expect("build passwordless config from credential");

    assert_eq!(config.get_user(), Some("discogs"));
    assert_eq!(config.get_dbname(), Some("discogs"));
    assert_eq!(config.get_password(), Some(b"file-secret".as_slice()));
}

#[test]
fn config_rejects_password_in_dsn_before_reading_credentials() {
    let missing_credential = std::path::Path::new("/definitely/missing/credential");

    for dsn in [
        "postgresql://discogs:secret@127.0.0.1:5432/discogs",
        "host=127.0.0.1 port=5432 user=discogs dbname=discogs password=secret",
        "postgresql://discogs:@127.0.0.1:5432/discogs",
    ] {
        let error = db::connection_config(dsn, missing_credential)
            .expect_err("reject password-bearing DSN");

        assert_eq!(
            error.to_string(),
            "PostgreSQL DSN must not contain a password"
        );
    }
}

#[test]
fn malformed_credential_file_fails_without_echoing_contents() {
    let (_root, credential) = credential_file("PGPASSWORD=\"unterminated-sensitive-fixture\n");

    let error = db::connection_config(
        "postgresql://discogs@127.0.0.1:5432/discogs",
        &credential,
    )
    .expect_err("reject malformed dotenv");

    assert_eq!(error.to_string(), "Invalid PostgreSQL credential file");
}

#[test]
fn credential_file_values_are_literal_and_never_expand_environment() {
    let (_root, credential) = credential_file("PGPASSWORD=$PATH\n");

    let config = db::connection_config(
        "postgresql://discogs@127.0.0.1:5432/discogs",
        &credential,
    )
    .expect("parse literal credential");

    assert_eq!(config.get_password(), Some(b"$PATH".as_slice()));
}

struct AuthenticatedPostgres {
    child: Child,
    _root: TempDir,
    dsn: String,
    credential: std::path::PathBuf,
}

impl AuthenticatedPostgres {
    fn start() -> Self {
        let root = tempfile::Builder::new()
            .prefix("discogs-auth-pg-")
            .tempdir()
            .expect("create postgres tempdir");
        let data = root.path().join("data");
        let socket = root.path().join("socket");
        let password_file = root.path().join("initdb-password");
        let credential = root.path().join("postgres-credential");
        fs::create_dir(&socket).expect("create postgres socket directory");
        fs::write(&password_file, "file-secret\n").expect("write initdb password");
        fs::write(&credential, "PGPASSWORD=file-secret\n").expect("write app credential");

        let init = Command::new("initdb")
            .args([
                "--no-sync",
                "--no-locale",
                "--encoding=UTF8",
                "--auth-local=trust",
                "--auth-host=scram-sha-256",
                "--username=postgres",
                "--pwfile",
            ])
            .arg(&password_file)
            .arg("-D")
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

        let port = TcpListener::bind(("127.0.0.1", 0))
            .expect("reserve postgres port")
            .local_addr()
            .expect("read reserved port")
            .port();
        let mut child = Command::new("postgres")
            .arg("-D")
            .arg(&data)
            .arg("-k")
            .arg(&socket)
            .arg("-h")
            .arg("127.0.0.1")
            .arg("-p")
            .arg(port.to_string())
            .arg("-F")
            .args(["-c", "synchronous_commit=off", "-c", "full_page_writes=off"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("start ephemeral postgres");

        let started = Instant::now();
        loop {
            let ready = Command::new("pg_isready")
                .args(["-h", "127.0.0.1", "-p"])
                .arg(port.to_string())
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
            dsn: format!("postgresql://postgres@127.0.0.1:{port}/postgres"),
            credential,
            _root: root,
        }
    }
}

impl Drop for AuthenticatedPostgres {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[test]
fn importer_and_api_paths_authenticate_with_file_backed_password() {
    let postgres = AuthenticatedPostgres::start();
    let runtime = tokio::runtime::Runtime::new().expect("create runtime");

    runtime.block_on(async {
        let importer = db::connect_with_credential(&postgres.dsn, &postgres.credential)
            .await
            .expect("importer connection authenticates");
        let importer_value: i32 = importer
            .query_one("SELECT 49", &[])
            .await
            .expect("query through importer connection")
            .get(0);
        assert_eq!(importer_value, 49);

        let api_pool = db::create_pool_with_credential(&postgres.dsn, &postgres.credential)
            .await
            .expect("API pool authenticates");
        let api_value: i32 = api_pool
            .get()
            .await
            .expect("get API pooled connection")
            .query_one("SELECT 49", &[])
            .await
            .expect("query through API pool")
            .get(0);
        assert_eq!(api_value, 49);
    });
}
