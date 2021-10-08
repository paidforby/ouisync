use super::{
    client::Client,
    message::{Message, RepositoryId, Request, Response},
    object_stream::{TcpObjectReader, TcpObjectStream, TcpObjectWriter},
    server::Server,
};
use crate::{
    error::Result,
    index::Index,
    replica_id::ReplicaId,
    scoped_task::ScopedJoinHandle,
    tagged::{Local, Remote},
};
use std::{
    collections::{hash_map::Entry, HashMap},
    fmt,
    future::Future,
    pin::Pin,
    sync::Arc,
};
use tokio::{
    select,
    sync::{
        mpsc::{self, error::SendError},
        Mutex, RwLock,
    },
    task,
};

/// A stream for receiving Requests and sending Responses
pub(crate) struct ServerStream {
    tx: mpsc::Sender<Command>,
    rx: mpsc::Receiver<Request>,
    remote_id: Remote<RepositoryId>,
}

impl ServerStream {
    pub(super) fn new(
        tx: mpsc::Sender<Command>,
        rx: mpsc::Receiver<Request>,
        remote_id: Remote<RepositoryId>,
    ) -> Self {
        Self { tx, rx, remote_id }
    }

    pub async fn recv(&mut self) -> Option<Request> {
        let rq = self.rx.recv().await?;
        log::trace!("server: recv {:?}", rq);
        Some(rq)
    }

    pub async fn send(&self, response: Response) -> Result<(), SendError<Response>> {
        log::trace!("server: send {:?}", response);
        self.tx
            .send(Command::SendMessage(Message::Response {
                dst_id: self.remote_id.into_inner(),
                response,
            }))
            .await
            .map_err(|e| SendError(into_message(e.0)))
    }
}

/// A stream for sending Requests and receiving Responses
pub(crate) struct ClientStream {
    tx: mpsc::Sender<Command>,
    rx: mpsc::Receiver<Response>,
    remote_id: Remote<RepositoryId>,
}

impl ClientStream {
    pub(super) fn new(
        tx: mpsc::Sender<Command>,
        rx: mpsc::Receiver<Response>,
        remote_id: Remote<RepositoryId>,
    ) -> Self {
        Self { tx, rx, remote_id }
    }

    pub async fn recv(&mut self) -> Option<Response> {
        let rs = self.rx.recv().await?;
        log::trace!("client: recv {:?}", rs);
        Some(rs)
    }

    pub async fn send(&self, request: Request) -> Result<(), SendError<Request>> {
        log::trace!("client: send {:?}", request);
        self.tx
            .send(Command::SendMessage(Message::Request {
                dst_id: self.remote_id.into_inner(),
                request,
            }))
            .await
            .map_err(|e| SendError(into_message(e.0)))
    }
}

fn into_message<T: From<Message>>(command: Command) -> T {
    command.into_send_message().into()
}

type OnFinish = Pin<Box<dyn Future<Output = ()> + Send>>;

/// Maintains one or more connections to a peer, listening on all of them at the same time. Note
/// that at the present all the connections are TCP based and so dropping some of them would make
/// sense. However, in the future we may also have other transports (e.g. Bluetooth) and thus
/// keeping all may make sence because even if one is dropped, the others may still function.
///
/// Once a message is received, it is determined whether it is a request or a response. Based on
/// that it either goes to the ClientStream or ServerStream for processing by the Client and Server
/// structures respectively.
pub(crate) struct MessageBroker {
    command_tx: mpsc::Sender<Command>,
    _join_handle: ScopedJoinHandle<()>,
}

impl MessageBroker {
    pub async fn new(
        their_replica_id: ReplicaId,
        stream: TcpObjectStream,
        on_finish: OnFinish,
    ) -> Self {
        let (command_tx, command_rx) = mpsc::channel(1);

        let inner = Arc::new(Inner {
            their_replica_id,
            command_tx: command_tx.clone(),
            reader: MultiReader::new(),
            writer: MultiWriter::new(),
            links: RwLock::new(Links::new()),
        });

        inner.add_connection(stream);

        let handle = task::spawn(inner.run(command_rx, on_finish));

        Self {
            command_tx,
            _join_handle: ScopedJoinHandle(handle),
        }
    }

    pub async fn add_connection(&self, stream: TcpObjectStream) {
        self.send_command(Command::AddConnection(stream)).await
    }

    /// Try to establish a link between a local repository and a remote repository. The remote
    /// counterpart needs to call this too with matching `local_name` and `remote_name` for the link
    /// to actually be created.
    pub async fn create_link(
        &self,
        index: Index,
        local_id: Local<RepositoryId>,
        local_name: Local<String>,
        remote_name: Remote<String>,
    ) {
        self.send_command(Command::CreateLink {
            index,
            local_id,
            local_name,
            remote_name,
        })
        .await
    }

    /// Destroy the link between a local repository with the specified id and its remote
    /// counterpart (if one exists).
    pub async fn destroy_link(&self, local_id: Local<RepositoryId>) {
        self.send_command(Command::DestroyLink { local_id }).await
    }

    async fn send_command(&self, command: Command) {
        if let Err(command) = self.command_tx.send(command).await {
            log::error!(
                "failed to send command {:?} - broker already finished",
                command
            );
        }
    }
}

struct Inner {
    their_replica_id: ReplicaId,
    command_tx: mpsc::Sender<Command>,
    reader: MultiReader,
    writer: MultiWriter,
    links: RwLock<Links>,
}

impl Inner {
    async fn run(self: Arc<Self>, mut command_rx: mpsc::Receiver<Command>, on_finish: OnFinish) {
        // Note that we need to spawn here (as opposed to doing a select), because selecting could
        // result in a deadlock. The deadlock could happen this way:
        //
        // * We receive a Request from a peer and we send it to the Server.
        // * We receive another Request, but because the queue to the server has size 1, we block.
        // * Server processes the Request and sends the Response back to us.
        // * We're unable to process the Response because we're waiting for the second Request to
        //   go through.

        let (done_tx, mut done_rx) = mpsc::channel(1);

        let this = self.clone();
        let done = done_tx.clone();
        let handle1 = task::spawn(async move {
            loop {
                if let Some(command) = command_rx.recv().await {
                    if !this.handle_command(command).await {
                        break;
                    }
                } else {
                    break;
                }
            }
            done.send(1).await.unwrap_or_default();
        });

        let this = self.clone();
        let done = done_tx.clone();
        let handle2 = task::spawn(async move {
            loop {
                if let Some(message) = this.reader.read().await {
                    this.handle_message(message).await;
                } else {
                    break;
                }
            }
            done.send(2).await.unwrap_or_default();
        });

        // Wait for either to finish, then destroy the other.
        match done_rx.recv().await {
            Some(1) => handle2.abort(),
            Some(2) => handle1.abort(),
            _ => unreachable!(),
        }

        on_finish.await
    }

    async fn handle_command(&self, command: Command) -> bool {
        match command {
            Command::AddConnection(stream) => {
                self.add_connection(stream);
                true
            }
            Command::SendMessage(message) => self.send_message(message).await,
            Command::CreateLink {
                index,
                local_id,
                local_name,
                remote_name,
            } => {
                self.create_outgoing_link(index, local_id, local_name, remote_name)
                    .await
            }
            Command::DestroyLink { local_id } => {
                self.links.write().await.destroy_one(&local_id, None);
                true
            }
        }
    }

    async fn handle_message(&self, message: Message) {
        match message {
            Message::Request { dst_id, request } => {
                self.handle_request(&Local::new(dst_id), request).await
            }
            Message::Response { dst_id, response } => {
                self.handle_response(&Local::new(dst_id), response).await
            }
            Message::CreateLink { src_id, dst_name } => {
                self.create_incoming_link(Local::new(dst_name), Remote::new(src_id))
                    .await
            }
        }
    }

    fn add_connection(&self, stream: TcpObjectStream) {
        let (reader, writer) = stream.into_split();
        self.reader.add(reader);
        self.writer.add(writer);
    }

    async fn send_message(&self, message: Message) -> bool {
        self.writer.write(&message).await
    }

    async fn create_outgoing_link(
        &self,
        index: Index,
        local_id: Local<RepositoryId>,
        local_name: Local<String>,
        remote_name: Remote<String>,
    ) -> bool {
        let mut links = self.links.write().await;

        if links.active.contains_key(&local_id) {
            log::warn!("not creating link from {:?} - already exists", local_name);
            return true;
        }

        if links.pending_outgoing.contains_key(&local_name) {
            log::warn!("not creating link from {:?} - already pending", local_name);
            return true;
        }

        if !self
            .writer
            .write(&Message::CreateLink {
                src_id: local_id.into_inner(),
                dst_name: remote_name.into_inner(),
            })
            .await
        {
            log::warn!(
                "not creating link from {:?} - \
                 failed to send CreateLink message - all writers closed",
                local_name,
            );
            return false;
        }

        if let Some(pending) = links.pending_incoming.remove(&local_name) {
            self.create_link(&mut *links, index, local_id, pending.remote_id)
        } else {
            links
                .pending_outgoing
                .insert(local_name, PendingOutgoingLink { index, local_id });
        }

        true
    }

    async fn create_incoming_link(
        &self,
        local_name: Local<String>,
        remote_id: Remote<RepositoryId>,
    ) {
        let mut links = self.links.write().await;

        if let Some(pending) = links.pending_outgoing.remove(&local_name) {
            self.create_link(&mut *links, pending.index, pending.local_id, remote_id)
        } else {
            links
                .pending_incoming
                .insert(local_name, PendingIncomingLink { remote_id });
        }
    }

    fn create_link(
        &self,
        links: &mut Links,
        index: Index,
        local_id: Local<RepositoryId>,
        remote_id: Remote<RepositoryId>,
    ) {
        log::debug!("creating link {:?} -> {:?}", local_id, remote_id);

        let (request_tx, request_rx) = mpsc::channel(1);
        let (response_tx, response_rx) = mpsc::channel(1);

        links.insert_active(local_id, request_tx, response_tx);

        // NOTE: we just fire-and-forget the tasks which should be OK because when this
        // `MessageBroker` instance is dropped, the associated senders (`request_tx`, `response_tx`)
        // are dropped as well which closes the corresponding receivers which then terminates the
        // tasks.

        let client_stream = ClientStream::new(self.command_tx.clone(), response_rx, remote_id);
        let mut client = Client::new(index.clone(), self.their_replica_id, client_stream);
        task::spawn(async move { log_error(client.run(), "client failed: ").await });

        let server_stream = ServerStream::new(self.command_tx.clone(), request_rx, remote_id);
        let mut server = Server::new(index, server_stream);
        task::spawn(async move { log_error(server.run(), "server failed: ").await });
    }

    async fn handle_request(&self, local_id: &Local<RepositoryId>, request: Request) {
        if let Some((link_id, request_tx)) = self.links.read().await.get_request_link(local_id) {
            if request_tx.lock().await.send(request).await.is_err() {
                log::warn!("server unexpectedly terminated - destroying the link");
                self.links
                    .write()
                    .await
                    .destroy_one(local_id, Some(link_id));
            }
        } else {
            log::warn!(
                "received request {:?} for unlinked repository {:?}",
                request,
                local_id
            );
        }
    }

    async fn handle_response(&self, local_id: &Local<RepositoryId>, response: Response) {
        if let Some((link_id, response_tx)) = self.links.read().await.get_response_link(local_id) {
            if response_tx.lock().await.send(response).await.is_err() {
                log::warn!("client unexpectedly terminated - destroying the link");
                self.links
                    .write()
                    .await
                    .destroy_one(local_id, Some(link_id));
            }
        } else {
            log::warn!(
                "received response {:?} for unlinked repository {:?}",
                response,
                local_id
            );
        }
    }
}

async fn log_error<F>(fut: F, prefix: &'static str)
where
    F: Future<Output = Result<()>>,
{
    if let Err(error) = fut.await {
        log::error!("{}{}", prefix, error.verbose())
    }
}

pub(super) enum Command {
    AddConnection(TcpObjectStream),
    SendMessage(Message),
    CreateLink {
        index: Index,
        local_id: Local<RepositoryId>,
        local_name: Local<String>,
        remote_name: Remote<String>,
    },
    DestroyLink {
        local_id: Local<RepositoryId>,
    },
}

impl Command {
    pub(super) fn into_send_message(self) -> Message {
        match self {
            Self::SendMessage(message) => message,
            _ => panic!("Command is not SendMessage"),
        }
    }
}

impl fmt::Debug for Command {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::AddConnection(_) => f
                .debug_tuple("AddConnection")
                .field(&format_args!("_"))
                .finish(),
            Self::SendMessage(message) => f.debug_tuple("SendMessage").field(message).finish(),
            Self::CreateLink {
                local_name,
                remote_name,
                ..
            } => f
                .debug_struct("CreateLink")
                .field("local_name", local_name)
                .field("remote_name", remote_name)
                .finish_non_exhaustive(),
            Self::DestroyLink { local_id } => f
                .debug_struct("DestroyLink")
                .field("local_id", local_id)
                .finish(),
        }
    }
}

/// Wrapper for arbitrary number of `TcpObjectReader`s which reads from all of them simultaneously.
struct MultiReader {
    tx: mpsc::Sender<Option<Message>>,
    // Wrapping these in Mutex and RwLock to have the `add` and `read` methods non mutable.  That
    // in turn is desirable to be able to call the two functions from different coroutines. Note
    // that we don't want to wrap this whole struct in a Mutex/RwLock because we don't want the add
    // function to be blocking.
    rx: Mutex<mpsc::Receiver<Option<Message>>>,
    count: std::sync::RwLock<usize>,
}

impl MultiReader {
    fn new() -> Self {
        let (tx, rx) = mpsc::channel(1);
        Self {
            tx,
            rx: Mutex::new(rx),
            count: std::sync::RwLock::new(0),
        }
    }

    fn add(&self, mut reader: TcpObjectReader) {
        let tx = self.tx.clone();
        *self.count.write().unwrap() += 1;

        task::spawn(async move {
            loop {
                select! {
                    result = reader.read() => {
                        if let Ok(message) = result {
                            tx.send(Some(message)).await.unwrap_or(())
                        } else {
                            tx.send(None).await.unwrap_or(());
                            break;
                        }
                    },
                    _ = tx.closed() => break,
                }
            }
        });
    }

    async fn read(&self) -> Option<Message> {
        loop {
            if *self.count.read().unwrap() == 0 {
                return None;
            }

            match self.rx.lock().await.recv().await {
                Some(Some(message)) => return Some(message),
                Some(None) => {
                    *self.count.write().unwrap() -= 1;
                }
                None => {
                    // This would mean that all senders were closed, but that can't happen because
                    // `self.tx` still exists.
                    unreachable!()
                }
            }
        }
    }
}

/// Wrapper for arbitrary number of `TcpObjectWriter`s which writes to the first available one.
struct MultiWriter {
    // Using Mutexes and RwLocks here because we want the `add` and `write` functions to be const.
    // That will allow us to call them from two different coroutines. Note that we don't want this
    // whole structure to wrap because we don't want the `add` function to be blocking.
    next_id: std::sync::Mutex<usize>,
    writers: std::sync::RwLock<HashMap<usize, Arc<Mutex<TcpObjectWriter>>>>,
}

impl MultiWriter {
    fn new() -> Self {
        Self {
            next_id: std::sync::Mutex::new(0),
            writers: std::sync::RwLock::new(HashMap::new()),
        }
    }

    fn add(&self, writer: TcpObjectWriter) {
        let mut next_id = self.next_id.lock().unwrap();
        let id = *next_id;
        *next_id += 1;
        drop(next_id);

        self.writers
            .write()
            .unwrap()
            .insert(id, Arc::new(Mutex::new(writer)));
    }

    async fn write(&self, message: &Message) -> bool {
        while let Some((id, writer)) = self.pick_writer().await {
            if writer.lock().await.write(message).await.is_ok() {
                return true;
            }

            self.writers.write().unwrap().remove(&id);
        }

        false
    }

    async fn pick_writer(&self) -> Option<(usize, Arc<Mutex<TcpObjectWriter>>)> {
        self.writers
            .read()
            .unwrap()
            .iter()
            .next()
            .map(|(k, v)| (*k, v.clone()))
    }
}

// LinkId is used for when we want to remove a particular link from Links::active, but keep it if
// the link has been replaced with a new one in the mean time. For example, this could happen:
//
// 1. User clones a request_tx from one of the links in Links::active
// 2. User attempts to send to a message using the above request_tx
// 3. In the mean time, the original Link is replaced Links::active with a new one
// 4. The step #2 from above fails and we attempt to remove the link where the request_tx is from,
//    but instead we remove the newly replace link from step #3.
type LinkId = u64;

// Established link between local and remote repositories.
struct Link {
    id: LinkId,
    request_tx: Arc<Mutex<mpsc::Sender<Request>>>,
    response_tx: Arc<Mutex<mpsc::Sender<Response>>>,
}

struct Links {
    active: HashMap<Local<RepositoryId>, Link>,

    // TODO: consider using LruCache instead of HashMap for these, to expire unrequited link
    //       requests.
    pending_outgoing: HashMap<Local<String>, PendingOutgoingLink>,
    pending_incoming: HashMap<Local<String>, PendingIncomingLink>,

    next_link_id: LinkId,
}

impl Links {
    pub fn new() -> Self {
        Self {
            active: HashMap::new(),
            pending_outgoing: HashMap::new(),
            pending_incoming: HashMap::new(),
            next_link_id: 0,
        }
    }

    pub fn insert_active(
        &mut self,
        local_id: Local<RepositoryId>,
        request_tx: mpsc::Sender<Request>,
        response_tx: mpsc::Sender<Response>,
    ) {
        let link_id = self.generate_link_id();

        self.active.insert(
            local_id,
            Link {
                id: link_id,
                request_tx: Arc::new(Mutex::new(request_tx)),
                response_tx: Arc::new(Mutex::new(response_tx)),
            },
        );
    }

    pub fn get_request_link(
        &self,
        local_id: &Local<RepositoryId>,
    ) -> Option<(LinkId, Arc<Mutex<mpsc::Sender<Request>>>)> {
        self.active
            .get(local_id)
            .map(|link| (link.id, link.request_tx.clone()))
    }

    pub fn get_response_link(
        &self,
        local_id: &Local<RepositoryId>,
    ) -> Option<(LinkId, Arc<Mutex<mpsc::Sender<Response>>>)> {
        self.active
            .get(local_id)
            .map(|link| (link.id, link.response_tx.clone()))
    }

    fn destroy_one(&mut self, local_id: &Local<RepositoryId>, link_id: Option<LinkId>) {
        // NOTE: this drops the `request_tx` / `response_tx` senders which causes the
        // corresponding receivers to be closed which terminates the client/server tasks.
        match self.active.entry(*local_id) {
            Entry::Occupied(entry) => {
                if let Some(link_id) = link_id {
                    if entry.get().id == link_id {
                        entry.remove();
                    }
                } else {
                    entry.remove();
                }
            }
            _ => {}
        }
    }

    fn generate_link_id(&mut self) -> LinkId {
        let id = self.next_link_id;
        self.next_link_id += 1;
        id
    }
}

// Pending link initiated by the local repository.
struct PendingOutgoingLink {
    local_id: Local<RepositoryId>,
    index: Index,
}

// Pending link initiated by the remote repository.
struct PendingIncomingLink {
    remote_id: Remote<RepositoryId>,
}
