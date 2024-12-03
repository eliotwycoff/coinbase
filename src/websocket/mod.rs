use crate::{
    common::{authentication::Signer, types::ProductId, Error},
    websocket::full::Message,
};
use base64::{prelude::BASE64_STANDARD, Engine};
use fastwebsockets::{handshake, FragmentCollector, Frame, OpCode, Payload, Role, WebSocket};
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
    key: String,
    signer: Signer,
    passphrase: String,
}

impl Client {
    pub fn new(key: String, secret: String, passphrase: String) -> Result<Self, Error> {
        Ok(Self {
            key,
            signer: Signer::try_from(secret.as_str())?,
            passphrase,
        })
    }

    /// Connect to the endpoint and return a websocket connection.
    async fn connect_and_subscribe(&self, product_id: ProductId) -> Result<(), Error> {
        // Create the subscription message.
        let product_id: &'static str = product_id.into();
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)?
            .as_millis()
            .to_string();
        let signature =
            self.signer
                .get_cb_access_sign(timestamp.as_str(), "/users/self/verify", "", "GET")?;
        let subscription_message = serde_json::to_string(&serde_json::json!({
            "type": "subscribe",
            "channels": [{ "name": "level3", "product_ids": [product_id] }],
            "signature": signature,
            "key": self.key,
            "passphrase": self.passphrase,
            "timestamp": timestamp,
        }))?;

        // Connect to the endpoint.
        let stream = TcpStream::connect("ws-direct.sandbox.exchange.coinbase.com:443").await?; // change to ws direct?

        // TODO: Upgrade to TLS with tokio-rustls.

        let request = Request::builder()
            .method("GET")
            .uri(format!("https://ws-direct.sandbox.exchange.coinbase.com/"))
            .header("HOST", "ws-direct.sandbox.exchange.coinbase.com:443")
            .header(UPGRADE, "websocket")
            .header(CONNECTION, "upgrade")
            .header(
                "Sec-WebSocket-Key",
                fastwebsockets::handshake::generate_key(),
            )
            .header("Sec-WebSocket-Version", "13")
            .body(Empty::<Bytes>::new())?;
        println!("Creating websocket");
        let (mut ws, _) = handshake::client(&SpawnExecutor, request, stream).await?;

        // Configure the connection.
        println!("Configuring connection");
        ws.set_writev(false);
        ws.set_auto_close(true);
        ws.set_auto_pong(true);

        // Send the subscription message.
        println!("Sending subscription message");
        ws.write_frame(Frame::text(Payload::Borrowed(
            subscription_message.as_bytes(),
        )))
        .await?;

        // Loop through and deserialize the incoming messages.
        loop {
            let frame = ws.read_frame().await?;
            let message = serde_json::from_slice::<Message>(&frame.payload.as_ref())?;

            println!("message => {message:?}");
        }

        Ok(())
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

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn can_receive_messages() {
        dotenvy::from_filename(".env").unwrap();

        // Load the credentials.
        let key = std::env::var("CB_ACCESS_KEY").unwrap();
        let secret = std::env::var("CB_ACCESS_SECRET").unwrap();
        let passphrase = std::env::var("CB_ACCESS_PASSPHRASE").unwrap();

        // Set up the client.
        let client = Client::new(key, secret, passphrase).unwrap();

        // Connect to a pair.
        client
            .connect_and_subscribe(ProductId::KsmUsd)
            .await
            .unwrap();
    }
}
