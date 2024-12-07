use crate::{
    common::{authentication::Signer, types::ProductId, Error},
    websocket::level_three::{Message, Schema},
};
use fastwebsockets::{handshake, Frame, Payload, WebSocket};
use http_body_util::Empty;
use hyper::{
    body::Bytes,
    header::{CONNECTION, UPGRADE},
    upgrade::Upgraded,
    Request,
};
use hyper_util::rt::TokioIo;
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

pub struct Channel {
    subscriptions: Value,
    schema: Schema,
    ws: WebSocket<TokioIo<Upgraded>>,
}

impl Channel {
    pub fn subscriptions(&self) -> &Value {
        &self.subscriptions
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub async fn next(&mut self) -> Result<Message, Error> {
        match self.ws.read_frame().await {
            Ok(frame) => Ok(serde_json::from_slice(frame.payload.as_ref())?),
            Err(error) => {
                self.ws.write_frame(Frame::close_raw(vec![].into())).await?;

                Err(Error::WebSocket(error))
            }
        }
    }
}

#[derive(Debug, Default)]
pub struct ChannelBuilder {
    key: Option<String>,
    signer: Option<Signer>,
    passphrase: Option<String>,
    product_ids: Vec<ProductId>,
    domain: Option<String>,
    port: Option<u16>,
}

impl ChannelBuilder {
    pub fn with_authentication(
        mut self,
        key: String,
        secret: String,
        passphrase: String,
    ) -> Result<Self, Error> {
        self.key = Some(key);
        self.signer = Some(Signer::try_from(secret.as_str())?);
        self.passphrase = Some(passphrase);

        Ok(self)
    }

    pub fn with_product_id(mut self, product_id: ProductId) -> Self {
        self.product_ids.push(product_id);

        self
    }

    pub fn with_endpoint(mut self, domain: impl Into<String>, port: u16) -> Self {
        self.domain = Some(domain.into());
        self.port = Some(port);

        self
    }

    /// Connect to the endpoint and stream order book data.
    pub async fn connect(self) -> Result<Channel, Error> {
        // Create the subscription message.
        let product_ids = self
            .product_ids
            .iter()
            .map(|product_id| product_id.into())
            .collect::<Vec<&'static str>>();
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)?
            .as_secs()
            .to_string();
        let key = self
            .key
            .ok_or_else(|| Error::param_required("authentication key"))?;
        let signature = self
            .signer
            .ok_or_else(|| Error::param_required("authentication secret"))?
            .get_cb_access_sign(timestamp.as_str(), "/users/self/verify", "", "GET")?;
        let passphrase = self
            .passphrase
            .ok_or_else(|| Error::param_required("authentication passphrase"))?;
        let subscription_message = serde_json::to_string(&serde_json::json!({
            "type": "subscribe",
            "channels": [{ "name": "level3", "product_ids": product_ids }],
            "signature": signature,
            "key": key,
            "passphrase": passphrase,
            "timestamp": timestamp,
        }))?;

        // Connect to the endpoint.
        let domain = self
            .domain
            .ok_or_else(|| Error::param_required("endpoint domain"))?;
        let port = self
            .port
            .ok_or_else(|| Error::param_required("endpoint port"))?;
        let tcp_stream = TcpStream::connect(format!("{domain}:{port}")).await?;

        // Upgrade to TLS.
        let mut root_cert_store = RootCertStore::empty();

        root_cert_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

        let tls_config = ClientConfig::builder()
            .with_root_certificates(root_cert_store)
            .with_no_client_auth();
        let tls_connector = TlsConnector::from(Arc::new(tls_config));
        let tls_domain = ServerName::try_from(domain.clone())?;
        let tls_stream = tls_connector.connect(tls_domain, tcp_stream).await?;

        // Upgrade to WSS.
        let request = Request::builder()
            .method("GET")
            .uri(format!("https://{domain}/"))
            .header("HOST", domain.as_str())
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
                ws.write_frame(Frame::close_raw(vec![].into())).await?;

                return Err(Error::WebSocket(error));
            }
        };
        let subscriptions = serde_json::from_slice::<Value>(frame.payload.as_ref())?;

        // Deserialize the incoming schema.
        let frame = match ws.read_frame().await {
            Ok(frame) => frame,
            Err(error) => {
                ws.write_frame(Frame::close_raw(vec![].into())).await?;

                return Err(Error::WebSocket(error));
            }
        };
        let schema = serde_json::from_slice::<Schema>(frame.payload.as_ref())?;

        Ok(Channel {
            subscriptions,
            schema,
            ws,
        })
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
    async fn can_receive_messages() -> Result<(), Box<dyn std::error::Error>> {
        dotenvy::from_filename(".env")?;

        // Load the credentials.
        let key = std::env::var("CB_ACCESS_KEY")?;
        let secret = std::env::var("CB_ACCESS_SECRET")?;
        let passphrase = std::env::var("CB_ACCESS_PASSPHRASE")?;

        // Set up the channel.
        let mut channel = ChannelBuilder::default()
            .with_authentication(key, secret, passphrase)?
            .with_endpoint("ws-direct.exchange.coinbase.com", 443)
            .with_product_id(ProductId::BtcUsd)
            .connect()
            .await?;

        // Receive messages in a loop.
        while let Ok(message) = channel.next().await {
            println!("{message}");
        }

        Ok(())
    }
}
