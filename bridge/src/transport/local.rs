//! Client and Server than run in different processes on the same device.

pub use interprocess::local_socket::{
    tokio::LocalSocketListener, LocalSocketName, ToLocalSocketName,
};

use super::{
    socket::{self, SocketClient},
    Client, Server,
};
use crate::{
    error::Result,
    protocol::{Request, Response},
    state::ServerState,
};
use async_trait::async_trait;
use interprocess::local_socket::tokio::LocalSocketStream;
use std::{io, sync::Arc};
use tokio::task::JoinSet;
use tokio_util::{
    codec::{length_delimited::LengthDelimitedCodec, Framed},
    compat::{Compat, FuturesAsyncReadCompatExt},
};

pub struct LocalServer {
    listener: LocalSocketListener,
}

impl LocalServer {
    pub fn bind<'a>(name: impl ToLocalSocketName<'a>) -> io::Result<Self> {
        let listener = LocalSocketListener::bind(name)?;

        Ok(Self { listener })
    }
}

#[async_trait]
impl Server for LocalServer {
    async fn run(self, state: Arc<ServerState>) {
        let mut connections = JoinSet::new();

        loop {
            match self.listener.accept().await {
                Ok(socket) => {
                    let socket = make_socket(socket);
                    connections.spawn(socket::server_connection::run(socket, state.clone()));
                }
                Err(error) => {
                    tracing::error!(?error, "failed to accept client");
                    break;
                }
            }
        }
    }
}

pub struct LocalClient {
    inner: SocketClient<Socket>,
}

impl LocalClient {
    pub async fn connect<'a>(name: impl ToLocalSocketName<'a>) -> io::Result<Self> {
        let socket = LocalSocketStream::connect(name).await?;
        let socket = make_socket(socket);

        Ok(Self {
            inner: SocketClient::new(socket),
        })
    }
}

#[async_trait(?Send)]
impl Client for LocalClient {
    async fn invoke(&self, request: Request) -> Result<Response> {
        self.inner.invoke(request).await
    }
}

type Socket = Framed<Compat<LocalSocketStream>, LengthDelimitedCodec>;

fn make_socket(inner: LocalSocketStream) -> Socket {
    Framed::new(inner.compat(), LengthDelimitedCodec::new())
}