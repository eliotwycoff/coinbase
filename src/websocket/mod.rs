use crate::common::{authentication::Signer, Error};
use base64::{prelude::BASE64_STANDARD, Engine};
use fastwebsockets::{handshake, FragmentCollector, OpCode, Role, WebSocket};
use http_body_util::Empty;
use hyper::{
    body::Bytes,
    header::{CONNECTION, UPGRADE},
    upgrade::Upgraded,
    Request,
};
use hyper_util::rt::TokioIo;
use std::{
    future::Future,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::net::TcpStream;

pub mod full;

#[derive(Debug)]
pub struct Client {
    signer: Signer,
}

impl Client {
    pub fn new(secret: &str) -> Result<Self, Error> {
        Ok(Self {
            signer: Signer::try_from(secret)?,
        })
    }

    /// Connect to the endpoint and return a websocket connection.
    async fn connect(endpoint: &str) -> Result<FragmentCollector<TokioIo<Upgraded>>, Error> {
        let stream = TcpStream::connect(endpoint).await?; // change to ws direct?
        let request = Request::builder()
            .method("GET")
            .uri(format!("http://{endpoint}"))
            .header("HOST", "ws-feed.exchange.coinbase.com")
            .header(UPGRADE, "websocket")
            .header(CONNECTION, "upgrade")
            .header(
                "Sec-WebSocket-Key",
                fastwebsockets::handshake::generate_key(),
            )
            .header("Sec-WebSocket-Version", "13")
            .body(Empty::<Bytes>::new())?;
        let (ws, _) = handshake::client(&SpawnExecutor, request, stream).await?;

        Ok(FragmentCollector::new(ws))
    }

    async fn subscribe(connection: FragmentCollector<TokioIo<Upgraded>>) -> Result<(), Error> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)?
            .as_millis()
            .to_string();

        todo!()
    }
}

struct SpawnExecutor;

impl<Fut> hyper::rt::Executor<Fut> for SpawnExecutor
where
    Fut: Future + Send + 'static,
    Fut::Output: Send + 'static,
{
    fn execute(&self, fut: Fut) {
        tokio::task::spawn(fut);
    }
}
