use ouisync::{
    crypto::sign::PublicKey, network::Network, Access, AccessSecrets, ConfigStore, EntryType,
    Error, Event, File, Payload, PeerAddr, Repository, RepositoryDb, Result,
};
use std::{
    future::Future,
    net::{Ipv4Addr, SocketAddr},
    path::PathBuf,
    thread,
    time::Duration,
};
use tempfile::TempDir;
use tokio::{
    sync::broadcast::{self, error::RecvError},
    time,
};
use tracing::{instrument, Instrument, Span};

// Timeout for running a whole test case
pub(crate) const TEST_TIMEOUT: Duration = Duration::from_secs(120);

// Timeout for waiting for an event
pub(crate) const EVENT_TIMEOUT: Duration = Duration::from_secs(60);

// Test environment
pub(crate) struct Env {
    base_dir: Option<TempDir>,
    next_repo_num: u64,
    next_peer_num: u64,
    _span: tracing::span::EnteredSpan,
}

impl Env {
    #[allow(unused)] // https://github.com/rust-lang/rust/issues/46379
    pub fn new() -> Self {
        init_log();

        let span = tracing::info_span!("test", name = thread::current().name()).entered();
        let base_dir = TempDir::new().unwrap();

        Self {
            base_dir: Some(base_dir),
            next_repo_num: 0,
            next_peer_num: 0,
            _span: span,
        }
    }

    pub fn next_store(&mut self) -> PathBuf {
        let num = self.next_repo_num;
        self.next_repo_num += 1;

        self.base_dir
            .as_ref()
            .unwrap()
            .path()
            .join(format!("repo-{}.db", num))
    }

    #[allow(unused)] // https://github.com/rust-lang/rust/issues/46379
    pub async fn create_repo(&mut self) -> Repository {
        let secrets = AccessSecrets::random_write();
        self.create_repo_with_secrets(secrets).await
    }

    pub async fn create_repo_with_secrets(&mut self, secrets: AccessSecrets) -> Repository {
        Repository::create(
            RepositoryDb::create(&self.next_store()).await.unwrap(),
            rand::random(),
            Access::new(None, None, secrets),
        )
        .await
        .unwrap()
    }

    #[allow(unused)] // https://github.com/rust-lang/rust/issues/46379
    pub async fn create_linked_repos(&mut self) -> (Repository, Repository) {
        let repo_a = self.create_repo().await;
        let repo_b = self
            .create_repo_with_secrets(repo_a.secrets().clone())
            .await;

        (repo_a, repo_b)
    }

    #[allow(unused)] // https://github.com/rust-lang/rust/issues/46379
    pub(crate) async fn create_node(&mut self, bind: PeerAddr) -> Network {
        let id = self.next_peer_num();
        let span = tracing::info_span!("peer", id);

        let config_store = self
            .base_dir
            .as_ref()
            .unwrap()
            .path()
            .join(format!("config-{}", id));
        let config_store = ConfigStore::new(config_store);

        let network = {
            let _enter = span.enter();
            Network::new(config_store)
        };

        network.handle().bind(&[bind]).instrument(span).await;
        network
    }

    // Create two nodes connected together.
    #[allow(unused)] // https://github.com/rust-lang/rust/issues/46379
    pub(crate) async fn create_connected_nodes(&mut self, proto: Proto) -> (Network, Network) {
        let a = self.create_node(proto.wrap((Ipv4Addr::LOCALHOST, 0))).await;
        let b = self.create_node(proto.wrap((Ipv4Addr::LOCALHOST, 0))).await;

        b.add_user_provided_peer(&proto.listener_local_addr_v4(&a));

        (a, b)
    }

    fn next_peer_num(&mut self) -> u64 {
        let num = self.next_peer_num;
        self.next_peer_num += 1;
        num
    }
}

impl Drop for Env {
    fn drop(&mut self) {
        // Preserve the base dir in case of panic, so it can be inspected to help debug test
        // failures.
        if thread::panicking() {
            let path = self.base_dir.take().unwrap().into_path();
            tracing::warn!("preserving base_dir in '{}'", path.display());
        }
    }
}

#[cfg(feature = "simulation")]
pub(crate) mod sim {
    use super::*;
    use ouisync::device_id::DeviceId;
    use std::{
        cell::Cell,
        future::Future,
        net::{Ipv4Addr, SocketAddr},
        path::PathBuf,
    };
    use tokio::task_local;
    use tracing::Instrument;

    const PORT: u16 = 12345;
    const PROTO: Proto = Proto::Tcp;

    /// Network simulator
    #[allow(unused)] // https://github.com/rust-lang/rust/issues/46379
    pub(crate) struct Sim<'a> {
        base_dir: Option<TempDir>,
        inner: turmoil::Sim<'a>,
    }

    impl<'a> Sim<'a> {
        #[allow(unused)] // https://github.com/rust-lang/rust/issues/46379
        pub fn new() -> Self {
            init_log();

            let base_dir = TempDir::new().unwrap();
            let inner = turmoil::Builder::new().build_with_rng(Box::new(rand::thread_rng()));

            Self {
                base_dir: Some(base_dir),
                inner,
            }
        }

        #[allow(unused)] // https://github.com/rust-lang/rust/issues/46379
        pub fn actor<Fut>(&mut self, name: &str, f: Fut)
        where
            Fut: Future<Output = ()> + 'static,
        {
            let actor = Actor::new(self.base_dir.as_ref().unwrap().path().join(name));
            let span = tracing::info_span!("actor", name);

            let f = async move {
                f.await;
                Ok(())
            };
            let f = ACTOR.scope(actor, f).instrument(span);

            self.inner.client(name, f);
        }

        #[allow(unused)] // https://github.com/rust-lang/rust/issues/46379
        pub fn run(&mut self) {
            self.inner.run().unwrap()
        }
    }

    #[allow(unused)] // https://github.com/rust-lang/rust/issues/46379
    pub(crate) async fn create_network() -> Network {
        let config_store = ACTOR.with(|actor| actor.base_dir.join("config"));
        let config_store = ConfigStore::new(config_store);

        let network = Network::new(config_store);

        let bind = PROTO.wrap((Ipv4Addr::UNSPECIFIED, PORT));
        network.handle().bind(&[bind]).await;

        network
    }

    #[allow(unused)] // https://github.com/rust-lang/rust/issues/46379
    pub(crate) async fn create_repo(secrets: AccessSecrets) -> Repository {
        let (path, device_id) = ACTOR.with(|actor| (actor.next_repo_path(), actor.device_id));

        Repository::create(
            RepositoryDb::create(&path).await.unwrap(),
            device_id,
            Access::new(None, None, secrets),
        )
        .await
        .unwrap()
    }

    task_local! {
        static ACTOR: Actor;
    }

    struct Actor {
        base_dir: PathBuf,
        device_id: DeviceId,
        repo_counter: Cell<u32>,
    }

    impl Actor {
        fn new(base_dir: PathBuf) -> Self {
            Actor {
                base_dir,
                device_id: rand::random(),
                repo_counter: Cell::new(0),
            }
        }

        fn next_repo_path(&self) -> PathBuf {
            let num = self.repo_counter.get();
            self.repo_counter.set(num + 1);

            self.base_dir.join(format!("repo-{}.db", num))
        }
    }

    pub(crate) trait NetworkExt {
        fn connect(&self, to: &str);
    }

    impl NetworkExt for Network {
        fn connect(&self, to: &str) {
            self.add_user_provided_peer(&PROTO.wrap(SocketAddr::new(turmoil::lookup(to), PORT)))
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) enum Proto {
    #[allow(unused)] // https://github.com/rust-lang/rust/issues/46379
    Tcp,
    #[allow(unused)] // https://github.com/rust-lang/rust/issues/46379
    Quic,
}

impl Proto {
    pub fn wrap(&self, addr: impl Into<SocketAddr>) -> PeerAddr {
        match self {
            Self::Tcp => PeerAddr::Tcp(addr.into()),
            Self::Quic => PeerAddr::Quic(addr.into()),
        }
    }

    #[track_caller]
    pub fn listener_local_addr_v4(&self, network: &Network) -> PeerAddr {
        match self {
            Self::Tcp => PeerAddr::Tcp(network.tcp_listener_local_addr_v4().unwrap()),
            Self::Quic => PeerAddr::Quic(network.quic_listener_local_addr_v4().unwrap()),
        }
    }
}

// Keep calling `f` until it returns `true`. Wait for repo notification between calls.
#[allow(unused)] // https://github.com/rust-lang/rust/issues/46379
pub(crate) async fn eventually<F, Fut>(repo: &Repository, mut f: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let mut rx = repo.subscribe();

    time::timeout(TEST_TIMEOUT, async {
        loop {
            if f().await {
                break;
            }

            wait(&mut rx).await
        }
    })
    .await
    .unwrap()
}

#[allow(unused)] // https://github.com/rust-lang/rust/issues/46379
pub(crate) async fn wait(rx: &mut broadcast::Receiver<Event>) {
    loop {
        match time::timeout(EVENT_TIMEOUT, rx.recv()).await {
            Ok(Ok(Event {
                payload: Payload::BranchChanged(_) | Payload::BlockReceived { .. },
                ..
            }))
            | Ok(Err(RecvError::Lagged(_))) => return,
            Ok(Ok(Event {
                payload: Payload::FileClosed,
                ..
            })) => continue,
            Ok(Err(RecvError::Closed)) => panic!("notification channel unexpectedly closed"),
            Err(_) => panic!("timeout waiting for notification"),
        }
    }
}

/// Wait until the file at `path` has the expected content. Panics if timeout elapses before the
/// file content matches.
#[allow(unused)] // https://github.com/rust-lang/rust/issues/46379
pub(crate) async fn expect_file_content(repo: &Repository, path: &str, expected_content: &[u8]) {
    expect_file_version_content(repo, path, None, expected_content).await
}

#[instrument(skip(expected_content))]
pub(crate) async fn expect_file_version_content(
    repo: &Repository,
    path: &str,
    branch_id: Option<&PublicKey>,
    expected_content: &[u8],
) {
    eventually(repo, || {
        check_file_version_content(repo, path, branch_id, expected_content)
            .instrument(Span::current())
    })
    .await
}

pub(crate) async fn check_file_version_content(
    repo: &Repository,
    path: &str,
    branch_id: Option<&PublicKey>,
    expected_content: &[u8],
) -> bool {
    tracing::debug!(path, "opening");

    let result = if let Some(branch_id) = branch_id {
        repo.open_file_version(path, branch_id).await
    } else {
        repo.open_file(path).await
    };

    let mut file = match result {
        Ok(file) => file,
        // `EntryNotFound` likely means that the parent directory hasn't yet been fully synced
        // and so the file entry is not in it yet.
        //
        // `BlockNotFound` means the first block of the file hasn't been downloaded yet.
        Err(error @ (Error::EntryNotFound | Error::BlockNotFound(_))) => {
            tracing::warn!(path, ?error, "open failed");
            return false;
        }
        Err(error) => panic!("unexpected error: {:?}", error),
    };

    tracing::debug!(path, branch.id = ?file.branch().id(), "opened");

    let actual_content = match read_in_chunks(&mut file, 4096).await {
        Ok(content) => content,
        // `BlockNotFound` means just the some block of the file hasn't been downloaded yet.
        Err(error @ Error::BlockNotFound(_)) => {
            tracing::warn!(path, ?error, "read failed");
            return false;
        }
        Err(error) => panic!("unexpected error: {:?}", error),
    };

    if actual_content == expected_content {
        tracing::debug!(path, "content matches");
        true
    } else {
        tracing::warn!(path, "content does not match");
        false
    }
}

#[instrument]
pub(crate) async fn expect_entry_exists(repo: &Repository, path: &str, entry_type: EntryType) {
    eventually(repo, || check_entry_exists(repo, path, entry_type)).await
}

#[allow(unused)] // https://github.com/rust-lang/rust/issues/46379
pub(crate) async fn check_entry_exists(
    repo: &Repository,
    path: &str,
    entry_type: EntryType,
) -> bool {
    let result = match entry_type {
        EntryType::File => repo.open_file(path).await.map(|_| ()),
        EntryType::Directory => repo.open_directory(path).await.map(|_| ()),
    };

    match result {
        Ok(()) => true,
        Err(Error::EntryNotFound | Error::BlockNotFound(_)) => false,
        Err(error) => panic!("unexpected error: {:?}", error),
    }
}

#[allow(unused)] // https://github.com/rust-lang/rust/issues/46379
pub(crate) async fn write_in_chunks(file: &mut File, content: &[u8], chunk_size: usize) {
    for offset in (0..content.len()).step_by(chunk_size) {
        let end = (offset + chunk_size).min(content.len());
        file.write(&content[offset..end]).await.unwrap();

        if to_megabytes(end) > to_megabytes(offset) {
            tracing::debug!(
                "file write progress: {}/{} MB",
                to_megabytes(end),
                to_megabytes(content.len())
            );
        }
    }
}

pub(crate) async fn read_in_chunks(file: &mut File, chunk_size: usize) -> Result<Vec<u8>, Error> {
    let mut content = vec![0; file.len() as usize];
    let mut offset = 0;

    while offset < content.len() {
        let end = (offset + chunk_size).min(content.len());
        let size = file.read(&mut content[offset..end]).await?;
        offset += size;
    }

    Ok(content)
}

fn to_megabytes(bytes: usize) -> usize {
    bytes / 1024 / 1024
}

pub(crate) fn init_log() {
    use tracing::metadata::LevelFilter;

    tracing_subscriber::fmt()
        .pretty()
        .with_env_filter(
            tracing_subscriber::EnvFilter::builder()
                // Only show the logs if explicitly enabled with the `RUST_LOG` env variable.
                .with_default_directive(LevelFilter::OFF.into())
                .from_env_lossy(),
        )
        // log output is captured by default and only shown on failure. Run tests with
        // `--nocapture` to override.
        .with_test_writer()
        .try_init()
        // error here most likely means the logger is already initialized. We can ignore that.
        .ok();
}
