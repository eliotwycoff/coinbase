use crate::{
    common::{authentication::Signer, types::ProductId, Error},
    websocket::level_three::{Message, Schema},
};
use fastwebsockets::{handshake, Frame, Payload};
use http_body_util::Empty;
use hyper::{
    body::Bytes,
    header::{CONNECTION, UPGRADE},
    Request,
};
use rustls_pki_types::ServerName;
use serde_json::Value;
use std::{
    future::Future,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::net::TcpStream;
use tokio_rustls::{
    rustls::{ClientConfig, RootCertStore},
    TlsConnector,
};

pub mod level_three;

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

    /// Connect to the endpoint and stream order book data.
    pub async fn connect_and_subscribe(&self, product_id: ProductId) -> Result<(), Error> {
        // Create the subscription message.
        let product_id: &'static str = product_id.into();
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)?
            .as_secs()
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
        let tcp_stream = TcpStream::connect("ws-direct.exchange.coinbase.com:443").await?;

        // Upgrade to TLS.
        let mut root_cert_store = RootCertStore::empty();

        root_cert_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

        let tls_config = ClientConfig::builder()
            .with_root_certificates(root_cert_store)
            .with_no_client_auth();
        let tls_connector = TlsConnector::from(Arc::new(tls_config));
        let tls_domain = ServerName::try_from("ws-direct.exchange.coinbase.com")?;
        let tls_stream = tls_connector.connect(tls_domain, tcp_stream).await?;

        // Upgrade to WSS.
        let request = Request::builder()
            .method("GET")
            .uri(format!("https://ws-direct.exchange.coinbase.com/"))
            .header("HOST", "ws-direct.exchange.coinbase.com")
            .header(UPGRADE, "websocket")
            .header(CONNECTION, "upgrade")
            .header(
                "Sec-WebSocket-Key",
                fastwebsockets::handshake::generate_key(),
            )
            .header("Sec-WebSocket-Version", "13")
            .body(Empty::<Bytes>::new())?;
        let (mut ws, _) = handshake::client(&SpawnExecutor, request, tls_stream).await?;

        // Configure the connection.
        ws.set_writev(false);
        ws.set_auto_close(true);
        ws.set_auto_pong(true);

        // Send the subscription message.
        ws.write_frame(Frame::text(Payload::Borrowed(
            subscription_message.as_bytes(),
        )))
        .await?;

        // Deserialize the incoming subscriptions message.
        let frame = match ws.read_frame().await {
            Ok(frame) => frame,
            Err(error) => {
                println!("Error => {error}");

                ws.write_frame(Frame::close_raw(vec![].into())).await?;

                return Err(Error::WebSocket(error));
            }
        };
        let _subscriptions = serde_json::from_slice::<Value>(frame.payload.as_ref())?;

        // Deserialize the incoming schema.
        let frame = match ws.read_frame().await {
            Ok(frame) => frame,
            Err(error) => {
                println!("Error => {error}");

                ws.write_frame(Frame::close_raw(vec![].into())).await?;

                return Err(Error::WebSocket(error));
            }
        };
        let _schema = serde_json::from_slice::<Schema>(frame.payload.as_ref())?;

        // Loop through and deserialize the incoming messages.
        loop {
            let frame = match ws.read_frame().await {
                Ok(frame) => frame,
                Err(error) => {
                    println!("Error => {error}");

                    ws.write_frame(Frame::close_raw(vec![].into())).await?;

                    break;
                }
            };

            println!(
                "{}",
                serde_json::from_slice::<Message>(frame.payload.as_ref()).unwrap()
            );
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
            .connect_and_subscribe(ProductId::BtcUsd)
            .await
            .unwrap();
    }
}
