#[macro_use]
mod macros;

mod connection;
mod id;
mod migrations;
mod mutex;
mod transaction;

pub use id::DatabaseId;
pub use migrations::SCHEMA_VERSION;

use tracing::Span;

use self::{
    mutex::{CommittedMutexTransaction, ConnectionMutex},
    transaction::TransactionWrapper,
};
use deadlock::ExpectShortLifetime;
use ref_cast::RefCast;
use sqlx::{
    sqlite::{
        Sqlite, SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous,
    },
    Row, SqlitePool,
};
use std::{
    fmt,
    future::Future,
    io,
    ops::{Deref, DerefMut},
    panic::Location,
    path::Path,
    time::Duration,
};
#[cfg(test)]
use tempfile::TempDir;
use thiserror::Error;
use tokio::{fs, task};

const WARN_AFTER_TRANSACTION_LIFETIME: Duration = Duration::from_secs(3);

pub(crate) use self::connection::Connection;

/// Database connection pool.
#[derive(Clone)]
pub(crate) struct Pool {
    // Pool with multiple read-only connections
    reads: SqlitePool,
    // Single writable connection.
    write: ConnectionMutex,
}

impl Pool {
    async fn create(connect_options: SqliteConnectOptions) -> Result<Self, sqlx::Error> {
        let common_options = connect_options
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal)
            .pragma("recursive_triggers", "ON")
            .optimize_on_close(true, Some(1000));

        let write_options = common_options.clone();
        let write = ConnectionMutex::connect(write_options).await?;

        let read_options = common_options.read_only(true);
        let reads = SqlitePoolOptions::new()
            .max_connections(8)
            .test_before_acquire(false)
            .connect_with(read_options)
            .await?;

        Ok(Self { reads, write })
    }

    /// Acquire a read-only database connection.
    #[track_caller]
    pub fn acquire(&self) -> impl Future<Output = Result<PoolConnection, sqlx::Error>> + '_ {
        let location = Location::caller();

        async move {
            let conn = self.reads.acquire().await?;

            let track_lifetime =
                ExpectShortLifetime::new_in(WARN_AFTER_TRANSACTION_LIFETIME, location);

            Ok(PoolConnection {
                inner: conn,
                _track_lifetime: track_lifetime,
            })
        }
    }

    /// Begin a read-only transaction. See [`ReadTransaction`] for more details.
    #[track_caller]
    pub fn begin_read(&self) -> impl Future<Output = Result<ReadTransaction, sqlx::Error>> + '_ {
        let location = Location::caller();

        async move {
            let tx = self.reads.begin().await?;

            let track_lifetime =
                ExpectShortLifetime::new_in(WARN_AFTER_TRANSACTION_LIFETIME, location);

            Ok(ReadTransaction {
                inner: TransactionWrapper::Pool(tx),
                _track_lifetime: Some(track_lifetime),
            })
        }
    }

    /// Begin a write transaction. See [`WriteTransaction`] for more details.
    #[track_caller]
    pub fn begin_write(&self) -> impl Future<Output = Result<WriteTransaction, sqlx::Error>> + '_ {
        let location = Location::caller();

        async move {
            let tx = self.write.begin().await?;

            let track_lifetime =
                ExpectShortLifetime::new_in(WARN_AFTER_TRANSACTION_LIFETIME, location);

            Ok(WriteTransaction {
                inner: ReadTransaction {
                    inner: TransactionWrapper::Mutex(tx),
                    _track_lifetime: Some(track_lifetime),
                },
            })
        }
    }

    pub(crate) async fn close(&self) -> Result<(), sqlx::Error> {
        self.write.close().await;
        self.reads.close().await;

        Ok(())
    }
}

/// Database connection from pool
pub(crate) struct PoolConnection {
    inner: sqlx::pool::PoolConnection<Sqlite>,
    _track_lifetime: ExpectShortLifetime,
}

impl Deref for PoolConnection {
    type Target = Connection;

    fn deref(&self) -> &Self::Target {
        Connection::ref_cast(self.inner.deref())
    }
}

impl DerefMut for PoolConnection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        Connection::ref_cast_mut(self.inner.deref_mut())
    }
}

/// Transaction that allows only reading.
///
/// This is useful if one wants to make sure the observed database content doesn't change for the
/// duration of the transaction even in the presence of concurrent writes. In other words - a read
/// transaction represents an immutable snapshot of the database at the point the transaction was
/// created. A read transaction doesn't need to be committed or rolled back - it's implicitly ended
/// when the `ReadTransaction` instance drops.
pub(crate) struct ReadTransaction {
    inner: TransactionWrapper,
    _track_lifetime: Option<ExpectShortLifetime>,
}

impl Deref for ReadTransaction {
    type Target = Connection;

    fn deref(&self) -> &Self::Target {
        Connection::ref_cast(self.inner.deref())
    }
}

impl DerefMut for ReadTransaction {
    fn deref_mut(&mut self) -> &mut Self::Target {
        Connection::ref_cast_mut(self.inner.deref_mut())
    }
}

impl fmt::Debug for ReadTransaction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("ReadTransaction").finish_non_exhaustive()
    }
}

impl_executor_by_deref!(ReadTransaction);

/// Transaction that allows both reading and writing.
///
/// At most one task can hold a write transaction at any time. Any other tasks are blocked on
/// calling `begin_write` until the task that currently holds it is done with it (commits it or
/// rolls it back). Performing read-only operations concurrently while a write transaction is in
/// use is still allowed. Those operations will not see the writes performed via the write
/// transaction until that transaction is committed however.
pub(crate) struct WriteTransaction {
    inner: ReadTransaction,
}

impl WriteTransaction {
    /// Commits the transaction.
    ///
    /// # Cancel safety
    ///
    /// If the future returned by this function is cancelled before completion, the transaction
    /// is guaranteed to be either committed or rolled back but there is no way to tell in advance
    /// which of the two operations happens.
    pub async fn commit(self) -> Result<(), sqlx::Error> {
        self.commit_inner().await?;
        Ok(())
    }

    /// Commits the transaction and if (and only if) the commit completes successfully, runs the
    /// given closure.
    ///
    /// # Atomicity
    ///
    /// If the commit succeeds, the closure is guaranteed to complete before another write
    /// transaction begins.
    ///
    /// # Cancel safety
    ///
    /// The commits completes and if it succeeds the closure gets called. This is guaranteed to
    /// happen even if the future returned from this function is cancelled before completion.
    ///
    /// # Insufficient alternatives
    ///
    /// ## Calling `commit().await?` and then calling `f()`
    ///
    /// This is not enough because it has these possible outcomes depending on whether and when
    /// cancellation happened:
    ///
    /// 1. `commit` completes successfully and `f` is called
    /// 2. `commit` completes with error and `f` is not called
    /// 3. `commit` is cancelled but the transaction is still committed and `f` is not called
    /// 4. `commit` is cancelled and the transaction rolls back and `f` is not called
    ///
    /// Number 3 is typically not desirable.
    ///
    /// ## Calling `f` using a RAII guard
    ///
    /// This is still not enough because it has the following possible outcomes:
    ///
    /// 1. `commit` completes successfully and `f` is called
    /// 2. `commit` completes with error and `f` is called
    /// 3. `commit` is cancelled but the transaction is still committed and `f` is called
    /// 4. `commit` is cancelled and the transaction rolls back and `f` is called
    ///
    /// Numbers 2 and 4 are not desirable. Number 2 can be handled by explicitly handling the error
    /// case and disabling the guard but there is nothing to do about number 4.
    pub async fn commit_and_then<F, R>(self, f: F) -> Result<R, sqlx::Error>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let span = Span::current();

        task::spawn(async move {
            // Make sure `_committed_tx` is alive until `f` completes,
            let _committed_tx = self.commit_inner().await?;
            let result = span.in_scope(f);
            Ok(result)
        })
        .await
        .unwrap()
    }

    async fn commit_inner(self) -> Result<CommittedMutexTransaction, sqlx::Error> {
        let tx = match self.inner.inner {
            TransactionWrapper::Mutex(tx) => tx.commit().await?,
            TransactionWrapper::Pool(_) => unreachable!(),
        };

        Ok(tx)
    }
}

impl Deref for WriteTransaction {
    type Target = ReadTransaction;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for WriteTransaction {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl std::fmt::Debug for WriteTransaction {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "WriteTransaction{{ inner:{:?} }}", self.inner)
    }
}

impl_executor_by_deref!(WriteTransaction);

/// Creates a new database and opens a connection to it.
pub(crate) async fn create(path: impl AsRef<Path>) -> Result<Pool, Error> {
    let path = path.as_ref();

    if fs::metadata(path).await.is_ok() {
        return Err(Error::Exists);
    }

    create_directory(path).await?;

    let connect_options = SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(true);

    let pool = Pool::create(connect_options).await.map_err(Error::Open)?;

    migrations::run(&pool).await?;

    Ok(pool)
}

/// Creates a new database in a temporary directory. Useful for tests.
#[cfg(test)]
pub(crate) async fn create_temp() -> Result<(TempDir, Pool), Error> {
    let temp_dir = TempDir::new().map_err(Error::CreateDirectory)?;
    let pool = create(temp_dir.path().join("temp.db")).await?;

    Ok((temp_dir, pool))
}

/// Opens a connection to the specified database. Fails if the db doesn't exist.
pub(crate) async fn open(path: impl AsRef<Path>) -> Result<Pool, Error> {
    let connect_options = SqliteConnectOptions::new().filename(path);
    let pool = Pool::create(connect_options).await.map_err(Error::Open)?;

    migrations::run(&pool).await?;

    Ok(pool)
}

async fn create_directory(path: &Path) -> Result<(), Error> {
    if let Some(dir) = path.parent() {
        fs::create_dir_all(dir)
            .await
            .map_err(Error::CreateDirectory)?
    }

    Ok(())
}

// Explicit cast from `i64` to `u64` to work around the lack of native `u64` support in the sqlx
// crate.
pub(crate) const fn decode_u64(i: i64) -> u64 {
    i as u64
}

// Explicit cast from `u64` to `i64` to work around the lack of native `u64` support in the sqlx
// crate.
pub(crate) const fn encode_u64(u: u64) -> i64 {
    u as i64
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("failed to create database directory")]
    CreateDirectory(#[source] io::Error),
    #[error("database already exists")]
    Exists,
    #[error("failed to open database")]
    Open(#[source] sqlx::Error),
    #[error("failed to execute database query")]
    Query(#[from] sqlx::Error),
}

async fn get_pragma(conn: &mut Connection, name: &str) -> Result<u32, Error> {
    Ok(sqlx::query(&format!("PRAGMA {}", name))
        .fetch_one(&mut *conn)
        .await?
        .get(0))
}

async fn set_pragma(conn: &mut Connection, name: &str, value: u32) -> Result<(), Error> {
    // `bind` doesn't seem to be supported for setting PRAGMAs...
    sqlx::query(&format!("PRAGMA {} = {}", name, value))
        .execute(&mut *conn)
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // Check the casts are lossless

    #[test]
    fn decode_u64_sanity_check() {
        // [0i64,     i64::MAX] -> [0u64,             u64::MAX / 2]
        // [i64::MIN,    -1i64] -> [u64::MAX / 2 + 1,     u64::MAX]

        assert_eq!(decode_u64(0), 0);
        assert_eq!(decode_u64(1), 1);
        assert_eq!(decode_u64(-1), u64::MAX);
        assert_eq!(decode_u64(i64::MIN), u64::MAX / 2 + 1);
        assert_eq!(decode_u64(i64::MAX), u64::MAX / 2);
    }

    #[test]
    fn encode_u64_sanity_check() {
        assert_eq!(encode_u64(0), 0);
        assert_eq!(encode_u64(1), 1);
        assert_eq!(encode_u64(u64::MAX / 2), i64::MAX);
        assert_eq!(encode_u64(u64::MAX / 2 + 1), i64::MIN);
        assert_eq!(encode_u64(u64::MAX), -1);
    }
}
