use anyhow::Result;
use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::types::{Delete, ObjectIdentifier};
use aws_sdk_s3::Client;
use futures_core::Future;
use itertools::Itertools;
use libsql_client::{Connection, QueryResult, Statement, Value};
use s3s::auth::SimpleAuth;
use s3s::service::S3ServiceBuilder;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Mutex, OnceLock};
use std::time::Instant;
use tokio::io::AsyncReadExt;
use tokio::time::sleep;
use tokio::time::Duration;
use url::Url;
use uuid::Uuid;

use crate::auth::user_auth_strategies::Disabled;
use crate::auth::Auth;
use crate::config::{DbConfig, UserApiConfig};
use crate::net::AddrIncoming;
use crate::Server;

/// A process-local registry assigning each named resource a unique, stable
/// listener. The registry binds an OS-assigned ephemeral port (`:0`) and keeps
/// the resulting `std::net::TcpListener` alive for the whole process, so the
/// port stays bound for the test's lifetime. Rebinding by reusing a port number
/// is a race (another process can grab the port in between), so instead every
/// bind hands out a `try_clone` of the single held socket — the port is never
/// released, only shared.
///
/// Each `key` is cached, so a test can reuse the same listener across restarts
/// (e.g. the S3 outage test restarts its S3 server on the same port) without
/// another test in this process grabbing it.
static LISTENERS: OnceLock<Mutex<HashMap<String, std::net::TcpListener>>> = OnceLock::new();

/// Bind an ephemeral port for a `key` and return the tokio listener + port,
/// where the listener is a clone of the registry's held socket.
fn test_listener(key: &str) -> (tokio::net::TcpListener, u16) {
    let mut listeners = LISTENERS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap();
    let entry = listeners
        .entry(key.to_string())
        .or_insert_with(|| std::net::TcpListener::bind("127.0.0.1:0").unwrap());
    let port = entry.local_addr().unwrap().port();
    let shared = entry
        .try_clone()
        .expect("failed to clone bound test listener");
    shared.set_nonblocking(true).unwrap();
    let tokio_listener =
        tokio::net::TcpListener::from_std(shared).expect("failed to convert test listener");
    (tokio_listener, port)
}

/// Allocate a per-`key` listener and start a mock S3 server sharing that socket.
/// Returns the graceful-shutdown handle plus the bound port. The listener given
/// to the server is a clone of the registry's socket, so the port stays bound
/// for the whole test.
fn start_s3_server(key: &str) -> (S3ServerHandle, u16) {
    let (listener, port) = test_listener(&format!("s3-{key}"));
    let tmp = std::env::temp_dir().join(format!("s3s-{}", Uuid::now_v7().as_simple()));
    let handle = start_stoppable_s3_server(listener, tmp);
    (handle, port)
}

/// Build a sqld `Server` listening on a fresh clone of the `db-<key>` socket.
/// Each call clones the still-bound socket, so a multi-step test reuses one port
/// without ever releasing it or rebinding it by number.
async fn make_db_server(
    key: &str,
    options: &bottomless::replicator::Options,
    path: impl Into<PathBuf>,
) -> Server {
    let (listener, _port) = test_listener(&format!("db-{key}"));
    configure_server(options, listener, path).await
}

/// returns a future that once polled will shutdown the server and wait for cleanup
fn start_db(step: u32, server: Server) -> impl Future<Output = ()> {
    let notify = server.shutdown.clone();
    let handle = tokio::spawn(async move {
        if let Err(e) = server.start().await {
            panic!("Failed step {}: {}", step, e);
        }
    });

    async move {
        notify.notify_waiters();
        handle.await.unwrap();
    }
}

async fn configure_server(
    options: &bottomless::replicator::Options,
    listener: tokio::net::TcpListener,
    path: impl Into<PathBuf>,
) -> Server {
    let http_acceptor = AddrIncoming::new(listener);
    Server {
        db_config: DbConfig {
            extensions_path: None,
            bottomless_replication: Some(options.clone()),
            max_log_size: 200 * 4046,
            max_log_duration: None,
            soft_heap_limit_mb: None,
            hard_heap_limit_mb: None,
            max_response_size: 10000000 * 4096,
            max_total_response_size: 10000000 * 4096,
            snapshot_exec: None,
            checkpoint_interval: Some(Duration::from_secs(3)),
            snapshot_at_shutdown: false,
            encryption_config: None,
            max_concurrent_requests: 128,
            connection_creation_timeout: None,
            disable_intelligent_throttling: false,
        },
        admin_api_config: None,
        disable_namespaces: true,
        user_api_config: UserApiConfig {
            hrana_ws_acceptor: None,
            http_acceptor: Some(http_acceptor),
            enable_http_console: false,
            self_url: None,
            primary_url: None,
            auth_strategy: Auth::new(Disabled::new()),
        },
        path: path.into().into(),
        disable_default_namespace: false,
        max_active_namespaces: 100,
        heartbeat_config: None,
        idle_shutdown_timeout: None,
        initial_idle_shutdown_timeout: None,
        rpc_server_config: None,
        rpc_client_config: None,
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn backup_restore() {
    let _ = tracing_subscriber::fmt::try_init();

    let (_s3, s3_port) = start_s3_server("backup_restore");

    const DB_ID: &str = "testbackuprestore";
    const BUCKET: &str = "testbackuprestore";
    const PATH: &str = "backup_restore.sqld";
    let (_, port) = test_listener("db-backup_restore");
    const OPS: usize = 2000;
    const ROWS: usize = 10;

    let s3_endpoint = format!("http://127.0.0.1:{s3_port}");
    let _ = S3BucketCleaner::new(&s3_endpoint, BUCKET).await;
    assert_bucket_occupancy_with_endpoint(BUCKET, &format!("{s3_endpoint}/"), true).await;

    let options = bottomless::replicator::Options {
        db_id: Some(DB_ID.to_string()),
        create_bucket_if_not_exists: true,
        verify_crc: true,
        use_compression: bottomless::replicator::CompressionKind::Gzip,
        encryption_config: None,
        aws_endpoint: Some(s3_endpoint.clone()),
        access_key_id: Some("bar".to_string()),
        secret_access_key: Some("foo".to_string()),
        session_token: None,
        region: Some("us-east-1".to_string()),
        bucket_name: BUCKET.to_string(),
        max_frames_per_batch: 10000,
        max_batch_interval: Duration::from_millis(250),
        s3_max_parallelism: 32,
        s3_max_retries: 10,
        s3_read_timeout_secs: 5,
        s3_connect_timeout_secs: 5,
        skip_snapshot: false,
        skip_shutdown_upload: false,
    };
    let connection_addr = Url::parse(&format!("http://localhost:{}", port)).unwrap();

    let make_server = || make_db_server("backup_restore", &options, PATH);

    {
        tracing::info!(
            "---STEP 1: create a local database, fill it with data, wait for WAL backup---"
        );
        let cleaner = DbFileCleaner::new(PATH);
        let db_job = start_db(1, make_server().await);

        sleep(Duration::from_secs(2)).await;

        let _ = sql(
            &connection_addr,
            ["CREATE TABLE IF NOT EXISTS t(id INT PRIMARY KEY, name TEXT);"],
        )
        .await
        .unwrap();

        perform_updates(&connection_addr, ROWS, OPS, "A").await;

        assert_updates(&connection_addr, ROWS, OPS, "A").await;

        sleep(Duration::from_secs(2)).await;

        db_job.await;
        drop(cleaner);
    }

    // make sure that db file doesn't exist, and that the bucket contains backup
    assert!(!std::path::Path::new(PATH).exists());
    assert_bucket_occupancy(&s3_endpoint, BUCKET, false).await;

    {
        tracing::info!(
            "---STEP 2: recreate the database from WAL - create a snapshot at the end---"
        );
        let cleaner = DbFileCleaner::new(PATH);
        let db_job = start_db(2, make_server().await);

        sleep(Duration::from_secs(2)).await;

        assert_updates(&connection_addr, ROWS, OPS, "A").await;

        db_job.await;
        drop(cleaner);
    }

    assert!(!std::path::Path::new(PATH).exists());

    {
        tracing::info!("---STEP 3: recreate database from snapshot alone---");
        let cleaner = DbFileCleaner::new(PATH);
        let db_job = start_db(3, make_server().await);

        sleep(Duration::from_secs(2)).await;

        // override existing entries, this will generate WAL
        perform_updates(&connection_addr, ROWS, OPS, "B").await;

        // wait for WAL to backup
        sleep(Duration::from_secs(2)).await;
        db_job.await;
        drop(cleaner);
    }

    assert!(!std::path::Path::new(PATH).exists());

    {
        tracing::info!("---STEP 4: recreate the database from snapshot + WAL---");
        let cleaner = DbFileCleaner::new(PATH);
        let db_job = start_db(4, make_server().await);

        sleep(Duration::from_secs(2)).await;

        assert_updates(&connection_addr, ROWS, OPS, "B").await;

        db_job.await;
        drop(cleaner);
    }

    {
        // make sure that we can follow back until the generation from which snapshot could be possible
        tracing::info!("---STEP 5: recreate database from generation missing snapshot ---");

        // manually remove snapshots from all generations, this will force restore across generations
        // from the very beginning
        remove_snapshots(&s3_endpoint, BUCKET).await;

        let cleaner = DbFileCleaner::new(PATH);
        let db_job = start_db(4, make_server().await);

        sleep(Duration::from_secs(2)).await;

        assert_updates(&connection_addr, ROWS, OPS, "B").await;

        db_job.await;
        drop(cleaner);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn rollback_restore() {
    let _ = tracing_subscriber::fmt::try_init();

    let (_s3, s3_port) = start_s3_server("rollback_restore");

    const DB_ID: &str = "testrollbackrestore";
    const BUCKET: &str = "testrollbackrestore";
    const PATH: &str = "rollback_restore.sqld";
    let (_, port) = test_listener("db-rollback_restore");

    async fn get_data(conn: &Url) -> Result<Vec<(Value, Value)>> {
        let result = sql(conn, ["SELECT * FROM t"]).await?;
        let rows = result
            .into_iter()
            .next()
            .unwrap()
            .into_result_set()?
            .rows
            .into_iter()
            .map(|row| (row.cells["id"].clone(), row.cells["name"].clone()))
            .collect();
        Ok(rows)
    }

    let s3_endpoint = format!("http://127.0.0.1:{s3_port}");
    let _ = S3BucketCleaner::new(&s3_endpoint, BUCKET).await;
    assert_bucket_occupancy(&s3_endpoint, BUCKET, true).await;

    let conn = Url::parse(&format!("http://localhost:{}", port)).unwrap();
    let options = bottomless::replicator::Options {
        db_id: Some(DB_ID.to_string()),
        create_bucket_if_not_exists: true,
        verify_crc: true,
        use_compression: bottomless::replicator::CompressionKind::Gzip,
        encryption_config: None,
        aws_endpoint: Some(s3_endpoint.clone()),
        access_key_id: Some("bar".to_string()),
        secret_access_key: Some("foo".to_string()),
        session_token: None,
        region: Some("us-east-1".to_string()),
        bucket_name: BUCKET.to_string(),
        max_frames_per_batch: 10000,
        max_batch_interval: Duration::from_millis(250),
        s3_max_parallelism: 32,
        s3_max_retries: 10,
        s3_read_timeout_secs: 5,
        s3_connect_timeout_secs: 5,
        skip_snapshot: false,
        skip_shutdown_upload: false,
    };
    let make_server = || make_db_server("rollback_restore", &options, PATH);

    {
        tracing::info!("---STEP 1: create db, write row, rollback---");
        let cleaner = DbFileCleaner::new(PATH);
        let db_job = start_db(1, make_server().await);

        sleep(Duration::from_secs(2)).await;

        let _ = sql(
            &conn,
            [
                "CREATE TABLE IF NOT EXISTS t(id INT PRIMARY KEY, name TEXT);",
                "INSERT INTO t(id, name) VALUES(1, 'A')",
            ],
        )
        .await
        .unwrap();

        let _ = sql(
            &conn,
            [
                "BEGIN",
                "UPDATE t SET name = 'B' WHERE id = 1",
                "ROLLBACK",
                "INSERT INTO t(id, name) VALUES(2, 'B')",
            ],
        )
        .await
        .unwrap();

        // wait for backup
        sleep(Duration::from_secs(2)).await;
        assert_bucket_occupancy(&s3_endpoint, BUCKET, false).await;

        let rs = get_data(&conn).await.unwrap();
        assert_eq!(
            rs,
            vec![
                (Value::Integer(1), Value::Text("A".into())),
                (Value::Integer(2), Value::Text("B".into()))
            ],
            "rollback value should not be updated"
        );

        db_job.await;
        drop(cleaner);
    }

    {
        tracing::info!("---STEP 2: recreate database, read modify, read again ---");
        let cleaner = DbFileCleaner::new(PATH);
        let db_job = start_db(2, make_server().await);
        sleep(Duration::from_secs(2)).await;

        let rs = get_data(&conn).await.unwrap();
        assert_eq!(
            rs,
            vec![
                (Value::Integer(1), Value::Text("A".into())),
                (Value::Integer(2), Value::Text("B".into()))
            ],
            "restored value should not contain rollbacked update"
        );
        let _ = sql(&conn, ["UPDATE t SET name = 'C'"]).await.unwrap();
        let rs = get_data(&conn).await.unwrap();
        assert_eq!(
            rs,
            vec![
                (Value::Integer(1), Value::Text("C".into())),
                (Value::Integer(2), Value::Text("C".into()))
            ]
        );

        db_job.await;
        drop(cleaner);
    }
}

async fn perform_updates(connection_addr: &Url, row_count: usize, ops_count: usize, update: &str) {
    let stmts: Vec<_> = (0..ops_count)
        .map(|i| {
            format!(
                "INSERT INTO t(id, name) VALUES({}, '{}-{}') ON CONFLICT (id) DO UPDATE SET name = '{}-{}';",
                i % row_count,
                i,
                update,
                i,
                update
            )
        })
        .collect();
    let _ = sql(connection_addr, stmts).await.unwrap();
}

async fn assert_updates(connection_addr: &Url, row_count: usize, ops_count: usize, update: &str) {
    let result = sql(connection_addr, ["SELECT id, name FROM t ORDER BY id;"])
        .await
        .unwrap();
    let rs = result
        .into_iter()
        .next()
        .unwrap()
        .into_result_set()
        .unwrap();
    assert_eq!(rs.rows.len(), row_count, "unexpected number of rows");
    let base = if ops_count < 10 { 0 } else { ops_count - 10 } as i64;
    for (i, row) in rs.rows.iter().enumerate() {
        let i = i as i64;
        let id = row.cells["id"].clone();
        let name = row.cells["name"].clone();
        assert_eq!(
            (&id, &name),
            (
                &Value::Integer(i),
                &Value::Text(format!("{}-{}", base + i, update))
            ),
            "unexpected values for row {}: ({})",
            i,
            name
        );
    }
}

async fn sql<I, S>(url: &Url, stmts: I) -> Result<Vec<QueryResult>>
where
    I: IntoIterator<Item = S>,
    S: Into<Statement>,
{
    let db = libsql_client::reqwest::Connection::connect_from_url(url)?;
    db.batch(stmts).await
}

async fn s3_config(endpoint: &str) -> aws_sdk_s3::config::Config {
    let loader = aws_config::from_env().endpoint_url(endpoint);
    aws_sdk_s3::config::Builder::from(&loader.load().await)
        .force_path_style(true)
        .region(Region::new("us-east-1".to_string()))
        .credentials_provider(Credentials::new("bar", "foo", None, None, "Static"))
        .build()
}

async fn s3_client(endpoint: &str) -> Result<Client> {
    let conf = s3_config(endpoint).await;
    let client = Client::from_conf(conf);
    Ok(client)
}

/// Remove a snapshot objects from all generation. This may trigger bottomless to do rollup restore
/// across all generations.
async fn remove_snapshots(endpoint: &str, bucket: &str) {
    let client = s3_client(endpoint).await.unwrap();
    if let Ok(out) = client.list_objects().bucket(bucket).send().await {
        let keys = out
            .contents()
            .iter()
            .map(|o| {
                let key = o.key().unwrap();
                let prefix = key.split('/').next().unwrap();
                format!("{}/db.gz", prefix)
            })
            .unique()
            .map(|key| ObjectIdentifier::builder().key(key).build().unwrap())
            .collect();

        client
            .delete_objects()
            .bucket(bucket)
            .delete(
                Delete::builder()
                    .set_objects(Some(keys))
                    .quiet(true)
                    .build()
                    .unwrap(),
            )
            .send()
            .await
            .unwrap();
    }
}

/// Checks if the corresponding bucket is empty (has any elements) or not.
/// If bucket was not found, it's equivalent of an empty one.
async fn assert_bucket_occupancy(endpoint: &str, bucket: &str, expect_empty: bool) {
    assert_bucket_occupancy_with_endpoint(bucket, endpoint, expect_empty).await;
}

async fn assert_bucket_occupancy_with_endpoint(bucket: &str, endpoint: &str, expect_empty: bool) {
    let loader = aws_config::from_env().endpoint_url(endpoint);
    let conf = aws_sdk_s3::config::Builder::from(&loader.load().await)
        .force_path_style(true)
        .region(Region::new("us-east-1".to_string()))
        .credentials_provider(Credentials::new("bar", "foo", None, None, "Static"))
        .build();
    let client = Client::from_conf(conf);

    if let Ok(out) = client.list_objects().bucket(bucket).send().await {
        let contents = out.contents();
        if expect_empty {
            assert!(
                contents.is_empty(),
                "expected S3 bucket to be empty but {} were found",
                contents.len()
            );
        } else {
            assert!(
                !contents.is_empty(),
                "expected S3 bucket to be filled with backup data but it was empty"
            );
        }
    } else if !expect_empty {
        panic!("bucket '{}' doesn't exist", bucket);
    }
}

/// Guardian struct used for cleaning up the test data from
/// database file dir at the beginning and end of a test.
struct DbFileCleaner(PathBuf);

impl DbFileCleaner {
    fn new<P: Into<PathBuf>>(path: P) -> Self {
        let path = path.into();
        Self::cleanup(&path);
        DbFileCleaner(path)
    }

    fn cleanup(path: &PathBuf) {
        let _ = std::fs::remove_dir_all(path);
    }
}

impl Drop for DbFileCleaner {
    fn drop(&mut self) {
        Self::cleanup(&self.0)
    }
}

/// Guardian struct used for cleaning up the test data from
/// S3 bucket dir at the beginning and end of a test.
#[allow(dead_code)]
struct S3BucketCleaner(&'static str);

impl S3BucketCleaner {
    async fn new(endpoint: &str, bucket: &'static str) -> Self {
        let _ = Self::cleanup(endpoint, bucket).await; // cleanup the bucket before test
        S3BucketCleaner(bucket)
    }

    /// Delete all objects from S3 bucket with provided name (doesn't delete bucket itself).
    async fn cleanup(endpoint: &str, bucket: &str) -> Result<()> {
        let client = s3_client(endpoint).await?;
        let objects = client.list_objects().bucket(bucket).send().await?;
        let mut delete_keys = Vec::new();
        for o in objects.contents() {
            let id = ObjectIdentifier::builder()
                .set_key(o.key().map(String::from))
                .build()
                .unwrap();
            delete_keys.push(id);
        }

        let _ = client
            .delete_objects()
            .bucket(bucket)
            .delete(
                Delete::builder()
                    .set_objects(Some(delete_keys))
                    .build()
                    .unwrap(),
            )
            .send()
            .await?;

        Ok(())
    }
}

impl Drop for S3BucketCleaner {
    fn drop(&mut self) {
        //FIXME: running line below on tokio::test runtime will hang.
        //let _ = block_on(Self::cleanup(self.0));
    }
}

struct S3ServerHandle {
    shutdown_tx: tokio::sync::oneshot::Sender<()>,
}

impl S3ServerHandle {
    fn shutdown(self) {
        let _ = self.shutdown_tx.send(());
    }
}

/// Start a mock S3 server on a dedicated thread, serving a clone of the
/// registered listener so the port stays bound for the test's lifetime. The
/// `dir` path is reused across restarts so S3 state persists; restarting on the
/// same key serves a new clone of the same still-bound socket.
fn start_stoppable_s3_server(listener: tokio::net::TcpListener, dir: PathBuf) -> S3ServerHandle {
    std::fs::create_dir_all(&dir).unwrap();

    let s3_impl = s3s_fs::FileSystem::new(dir).unwrap();
    let auth = SimpleAuth::from_single("bar", "foo");
    let mut s3 = S3ServiceBuilder::new(s3_impl);
    s3.set_auth(auth);
    let s3 = s3.build().into_shared().into_make_service();

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();

    std::thread::spawn(move || {
        let listener = listener
            .into_std()
            .expect("failed to convert test listener");
        listener.set_nonblocking(true).unwrap();
        let addr = listener.local_addr().unwrap();
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let server = hyper::Server::from_tcp(listener).unwrap().serve(s3);
            let graceful = server.with_graceful_shutdown(async move {
                let _ = shutdown_rx.await;
            });
            tracing::debug!(%addr, "mock s3 server listening");
            graceful.await.unwrap();
        });
    });

    std::thread::sleep(Duration::from_millis(500));

    S3ServerHandle { shutdown_tx }
}

/// Start a TCP server on a per-`key` port that accepts connections but never
/// sends a response, returning the bound port. Used to test read_timeout behavior.
async fn start_stall_server(key: &str) -> u16 {
    let (listener, port) = test_listener(&format!("stall-{key}"));
    tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            // Accept connection, read the request, then hang — this causes read_timeout to fire
            tokio::spawn(async move {
                let mut buf = [0u8; 1024];
                let _ = stream.read(&mut buf).await;
                loop {
                    tokio::time::sleep(Duration::from_secs(60)).await;
                }
            });
        }
    });
    // Give the server a moment to start listening
    tokio::time::sleep(Duration::from_millis(100)).await;
    port
}

#[tokio::test]
async fn s3_read_timeout_fires() {
    let _ = tracing_subscriber::fmt::try_init();

    // Start a stall server on a different port
    let stall_port = start_stall_server("s3_read_timeout").await;

    let options = bottomless::replicator::Options {
        db_id: None,
        create_bucket_if_not_exists: true,
        verify_crc: true,
        use_compression: bottomless::replicator::CompressionKind::Gzip,
        encryption_config: None,
        aws_endpoint: Some(format!("http://127.0.0.1:{stall_port}")),
        access_key_id: Some("test".to_string()),
        secret_access_key: Some("test".to_string()),
        session_token: None,
        region: Some("us-east-1".to_string()),
        bucket_name: "test-bucket".to_string(),
        max_frames_per_batch: 10000,
        max_batch_interval: Duration::from_millis(250),
        s3_max_parallelism: 32,
        s3_max_retries: 1,
        s3_read_timeout_secs: 1,
        s3_connect_timeout_secs: 1,
        skip_snapshot: false,
        skip_shutdown_upload: false,
    };
    let client = Client::from_conf(options.client_config().await.unwrap());

    let start = Instant::now();
    let result = client.head_bucket().bucket("test-bucket").send().await;
    let elapsed = start.elapsed();

    // Should fail — note: AWS SDK internal retry/backoff may add significant time,
    // so we just verify it does eventually timeout rather than hanging indefinitely
    assert!(result.is_err(), "Expected timeout error, got {:?}", result);
    assert!(
        elapsed < Duration::from_secs(60),
        "Should have timed out, took {:?}",
        elapsed
    );
}

#[tokio::test]
async fn s3_connect_timeout_fires() {
    let _ = tracing_subscriber::fmt::try_init();

    // Use a blackhole IP (TEST-NET-1) so the TCP handshake stalls and
    // connect_timeout fires. A listening socket that never calls accept()
    // would still complete the handshake at the kernel level, so it would
    // not trigger connect_timeout.
    let options = bottomless::replicator::Options {
        db_id: None,
        create_bucket_if_not_exists: true,
        verify_crc: true,
        use_compression: bottomless::replicator::CompressionKind::Gzip,
        encryption_config: None,
        aws_endpoint: Some("http://192.0.2.1:12345".to_string()),
        access_key_id: Some("test".to_string()),
        secret_access_key: Some("test".to_string()),
        session_token: None,
        region: Some("us-east-1".to_string()),
        bucket_name: "test-bucket".to_string(),
        max_frames_per_batch: 10000,
        max_batch_interval: Duration::from_millis(250),
        s3_max_parallelism: 32,
        s3_max_retries: 1,
        s3_read_timeout_secs: 60,
        s3_connect_timeout_secs: 2,
        skip_snapshot: false,
        skip_shutdown_upload: false,
    };
    let client = Client::from_conf(options.client_config().await.unwrap());

    let start = Instant::now();
    let result = client.head_bucket().bucket("test-bucket").send().await;
    let elapsed = start.elapsed();

    assert!(result.is_err(), "Expected timeout error, got {:?}", result);
    assert!(
        elapsed < Duration::from_secs(60),
        "Should have timed out, took {:?}",
        elapsed
    );
}

/// Test that the server fails to start (rather than hanging indefinitely) when
/// bottomless restore starts but the S3 connection is interrupted (stalls).
/// This simulates an S3 server that accepts the TCP connection but never
/// sends a response, causing read_timeout to fire.
#[tokio::test(flavor = "multi_thread")]
async fn restore_fails_quickly_when_s3_interrupted() {
    let _ = tracing_subscriber::fmt::try_init();

    const DB_ID: &str = "testrestoretimeout";
    const BUCKET: &str = "testrestoretimeout";
    const PATH: &str = "restore_timeout.sqld";
    // Step 1: Start the mock S3 server; keep it alive through step 1, then drop
    // it so the stall server can take over the S3 endpoint.
    let (_s3, s3_port) = start_s3_server("restore_fails_quickly");
    let (_, port) = test_listener("db-restore_fails_quickly");

    // Build options without from_env() to avoid cross-test env var pollution.
    let options = bottomless::replicator::Options {
        db_id: Some(DB_ID.to_string()),
        create_bucket_if_not_exists: true,
        verify_crc: true,
        use_compression: bottomless::replicator::CompressionKind::Gzip,
        encryption_config: None,
        aws_endpoint: Some(format!("http://127.0.0.1:{s3_port}")),
        access_key_id: Some("bar".to_string()),
        secret_access_key: Some("foo".to_string()),
        session_token: None,
        region: Some("us-east-1".to_string()),
        bucket_name: BUCKET.to_string(),
        max_frames_per_batch: 10_000,
        max_batch_interval: Duration::from_millis(250),
        s3_max_parallelism: 32,
        s3_max_retries: 10,
        s3_read_timeout_secs: 5,
        s3_connect_timeout_secs: 5,
        skip_snapshot: false,
        skip_shutdown_upload: false,
    };
    let connection_addr = Url::parse(&format!("http://localhost:{}", port)).unwrap();

    {
        let cleaner = DbFileCleaner::new(PATH);
        let db_job = start_db(
            1,
            make_db_server("restore_fails_quickly", &options, PATH).await,
        );

        sleep(Duration::from_secs(2)).await;

        let _ = sql(
            &connection_addr,
            ["CREATE TABLE IF NOT EXISTS t(id INT PRIMARY KEY, name TEXT);"],
        )
        .await
        .unwrap();

        let _ = sql(&connection_addr, ["INSERT INTO t(id, name) VALUES(1, 'A')"])
            .await
            .unwrap();

        sleep(Duration::from_secs(3)).await;
        db_job.await;
        drop(cleaner);
    }

    // Step 2: Delete local database file and replace the S3 endpoint with a
    // stall server (accepts connections but never responds). This simulates an
    // S3 connection that starts but is interrupted.
    assert!(!std::path::Path::new(PATH).exists());

    let stall_port = start_stall_server("restore_fails_quickly").await;

    let stall_options = bottomless::replicator::Options {
        db_id: Some(DB_ID.to_string()),
        create_bucket_if_not_exists: true,
        verify_crc: true,
        use_compression: bottomless::replicator::CompressionKind::Gzip,
        encryption_config: None,
        aws_endpoint: Some(format!("http://127.0.0.1:{stall_port}")),
        access_key_id: Some("bar".to_string()),
        secret_access_key: Some("foo".to_string()),
        session_token: None,
        region: Some("us-east-1".to_string()),
        bucket_name: BUCKET.to_string(),
        max_frames_per_batch: 10_000,
        max_batch_interval: Duration::from_millis(250),
        s3_max_parallelism: 32,
        s3_max_retries: 1,
        s3_read_timeout_secs: 2,
        s3_connect_timeout_secs: 2,
        skip_snapshot: false,
        skip_shutdown_upload: false,
    };

    let server = make_db_server("restore_fails_quickly", &stall_options, PATH).await;
    let start = Instant::now();
    let result = tokio::time::timeout(Duration::from_secs(30), server.start()).await;
    let elapsed = start.elapsed();

    match result {
        Ok(Ok(())) => panic!("Server should not have started successfully with stalled S3"),
        Ok(Err(_)) => {
            // Server returned an error (expected)
        }
        Err(_) => {
            panic!("Server start hung for too long when S3 connection was interrupted");
        }
    }
    assert!(
        elapsed < Duration::from_secs(60),
        "Server start should have completed quickly, took {:?}",
        elapsed
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn replication_resumes_after_s3_outage() {
    let _ = tracing_subscriber::fmt::try_init();

    const DB_ID: &str = "testreplicationresumes";
    const BUCKET: &str = "testreplicationresumes";
    const PATH: &str = "replication_resumes.sqld";
    let s3_key = "replication_resumes";
    let (_, s3_port) = test_listener(&format!("s3-{s3_key}"));
    let (_, port) = test_listener("db-replication_resumes");

    let s3_dir = std::env::temp_dir().join(format!("s3s-{}", DB_ID));

    let s3_endpoint = format!("http://127.0.0.1:{s3_port}/");

    // Clean up any leftover data from previous test runs.
    let _ = std::fs::remove_dir_all(&s3_dir);

    // Step 1: Start S3, create DB, write data, verify replication.
    let (listener, _) = test_listener(&format!("s3-{s3_key}"));
    let s3 = start_stoppable_s3_server(listener, s3_dir.clone());

    assert_bucket_occupancy_with_endpoint(BUCKET, &s3_endpoint, true).await;

    let options = bottomless::replicator::Options {
        db_id: Some(DB_ID.to_string()),
        create_bucket_if_not_exists: true,
        verify_crc: true,
        use_compression: bottomless::replicator::CompressionKind::Gzip,
        encryption_config: None,
        aws_endpoint: Some(format!("http://127.0.0.1:{s3_port}")),
        access_key_id: Some("bar".to_string()),
        secret_access_key: Some("foo".to_string()),
        session_token: None,
        region: Some("us-east-1".to_string()),
        bucket_name: BUCKET.to_string(),
        max_frames_per_batch: 10_000,
        max_batch_interval: Duration::from_millis(250),
        s3_max_parallelism: 32,
        s3_max_retries: 1,
        s3_read_timeout_secs: 2,
        s3_connect_timeout_secs: 2,
        skip_snapshot: false,
        skip_shutdown_upload: true,
    };

    let connection_addr = Url::parse(&format!("http://localhost:{}", port)).unwrap();

    // Step 1: Start S3, start server, write initial data.
    tracing::info!("---STEP 1: start db and write initial data---");
    let cleaner = DbFileCleaner::new(PATH);
    let db_job = start_db(
        1,
        make_db_server("replication_resumes", &options, PATH).await,
    );

    sleep(Duration::from_secs(5)).await;

    match sql(
        &connection_addr,
        ["CREATE TABLE IF NOT EXISTS t(id INT PRIMARY KEY, name TEXT);"],
    )
    .await
    {
        Ok(results) => tracing::info!("CREATE TABLE succeeded: {:?}", results),
        Err(e) => {
            tracing::error!("CREATE TABLE failed: {:?}", e);
            panic!("CREATE TABLE failed: {:?}", e);
        }
    }

    sleep(Duration::from_secs(2)).await;

    match sql(&connection_addr, ["INSERT INTO t(id, name) VALUES(1, 'A')"]).await {
        Ok(results) => tracing::info!("INSERT succeeded: {:?}", results),
        Err(e) => {
            tracing::error!("INSERT failed: {:?}", e);
            panic!("INSERT failed: {:?}", e);
        }
    }

    sleep(Duration::from_secs(3)).await;

    assert_bucket_occupancy_with_endpoint(BUCKET, &s3_endpoint, false).await;

    // Step 2: Shut down S3 while server is still running, write more data locally.
    tracing::info!("---STEP 2: shut down S3, write more data locally---");
    s3.shutdown();
    sleep(Duration::from_secs(1)).await;

    match sql(&connection_addr, ["INSERT INTO t(id, name) VALUES(2, 'B')"]).await {
        Ok(results) => tracing::info!("INSERT while S3 down succeeded: {:?}", results),
        Err(e) => {
            tracing::error!("INSERT while S3 down failed: {:?}", e);
            panic!("INSERT while S3 down failed: {:?}", e);
        }
    }

    sleep(Duration::from_secs(2)).await;

    // Step 3: Restart S3, write more data to trigger WAL flush, verify catch-up.
    tracing::info!("---STEP 3: restart S3, verify replication resumes---");
    let (listener, _) = test_listener(&format!("s3-{s3_key}"));
    let _s3 = start_stoppable_s3_server(listener, s3_dir);
    sleep(Duration::from_secs(1)).await;

    match sql(&connection_addr, ["INSERT INTO t(id, name) VALUES(3, 'C')"]).await {
        Ok(results) => tracing::info!("INSERT after S3 restart succeeded: {:?}", results),
        Err(e) => {
            tracing::error!("INSERT after S3 restart failed: {:?}", e);
            panic!("INSERT after S3 restart failed: {:?}", e);
        }
    }

    sleep(Duration::from_secs(5)).await;

    assert_bucket_occupancy_with_endpoint(BUCKET, &s3_endpoint, false).await;

    // Shut down the server now that replication has caught up.
    db_job.await;
    drop(cleaner);

    // Step 4: Restore from scratch and verify all three rows are present.
    tracing::info!("---STEP 4: restore from backup and verify all rows---");
    let cleaner = DbFileCleaner::new(PATH);
    let db_job = start_db(
        4,
        make_db_server("replication_resumes", &options, PATH).await,
    );

    sleep(Duration::from_secs(5)).await;

    match sql(&connection_addr, ["SELECT id, name FROM t ORDER BY id"]).await {
        Ok(rows) => {
            tracing::info!("SELECT returned {} results", rows.len());
            for (i, row) in rows.iter().enumerate() {
                tracing::info!("Result {}: {:?}", i, row);
            }
            let first = rows
                .into_iter()
                .next()
                .expect("SELECT should return at least one result");
            let rs = first
                .into_result_set()
                .expect("SELECT result should be a result set")
                .rows
                .into_iter()
                .map(|row| (row.cells["id"].clone(), row.cells["name"].clone()))
                .collect::<Vec<_>>();

            assert_eq!(
                rs,
                vec![
                    (Value::Integer(1), Value::Text("A".into())),
                    (Value::Integer(2), Value::Text("B".into())),
                    (Value::Integer(3), Value::Text("C".into())),
                ],
                "all rows should be present after restoring from backup"
            );
        }
        Err(e) => {
            tracing::error!("SELECT failed: {:?}", e);
            panic!("SELECT failed: {:?}", e);
        }
    }

    db_job.await;
    drop(cleaner);
}
