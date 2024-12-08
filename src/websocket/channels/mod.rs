use crate::common::{authentication::Signer, types::ProductId, Error};
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
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::{
    future::Future,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
    vec::IntoIter,
};
use tokio::{
    net::TcpStream,
    sync::oneshot::{self, error::TryRecvError, Sender},
    task::JoinHandle,
};
use tokio_rustls::{
    rustls::{ClientConfig, RootCertStore},
    TlsConnector,
};

pub mod level_three;

pub trait ChannelType {
    fn channel_type() -> &'static str;
    fn parse_schema() -> bool;
}

pub struct Channel<T>
where
    T: 'static + DeserializeOwned + Send,
{
    ws: WebSocket<TokioIo<Upgraded>>,
    cache: Option<Vec<T>>,
}

impl<T> Channel<T>
where
    T: 'static + DeserializeOwned + Send,
{
    pub async fn next(&mut self) -> Result<T, Error> {
        match self.ws.read_frame().await {
            Ok(frame) => Ok(serde_json::from_slice::<T>(frame.payload.as_ref())?),
            Err(error) => {
                self.close().await?;

                Err(Error::WebSocket(error))
            }
        }
    }

    pub async fn close(&mut self) -> Result<(), Error> {
        self.ws
            .write_frame(Frame::close_raw(vec![].into()))
            .await
            .map_err(|error| error.into())
    }

    pub async fn cache(mut self) -> CachingChannel<T> {
        let (tx, mut rx) = oneshot::channel();
        let join_handle = tokio::spawn(async move {
            loop {
                match rx.try_recv() {
                    Ok(()) => break Ok(self),
                    Err(TryRecvError::Empty) => {
                        let item = self.next().await?;

                        self.cache.get_or_insert_with(|| vec![]).push(item);
                    }
                    Err(TryRecvError::Closed) => break Err(Error::ChannelClosed),
                }
            }
        });

        CachingChannel { tx, join_handle }
    }

    pub fn cached_items(&mut self) -> IntoIter<T> {
        self.cache.take().unwrap_or_else(|| vec![]).into_iter()
    }
}

pub struct CachingChannel<T>
where
    T: 'static + DeserializeOwned + Send,
{
    tx: Sender<()>,
    join_handle: JoinHandle<Result<Channel<T>, Error>>,
}

impl<T> CachingChannel<T>
where
    T: 'static + DeserializeOwned + Send,
{
    pub async fn join(self) -> Result<Channel<T>, Error> {
        // Signal the caching task to stop caching.
        self.tx.send(()).map_err(|_| Error::ChannelClosed)?;

        // Get the channel back from the caching task.
        self.join_handle.await?
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
    pub async fn connect<T>(self) -> Result<Channel<T>, Error>
    where
        T: 'static + ChannelType + DeserializeOwned + Send,
    {
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
            "channels": [{ "name": T::channel_type(), "product_ids": product_ids }],
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
        let _subscriptions = serde_json::from_slice::<Value>(frame.payload.as_ref())?;

        if T::parse_schema() {
            // Deserialize the incoming schema.
            let frame = match ws.read_frame().await {
                Ok(frame) => frame,
                Err(error) => {
                    ws.write_frame(Frame::close_raw(vec![].into())).await?;

                    return Err(Error::WebSocket(error));
                }
            };
            let _schema = serde_json::from_slice::<Value>(frame.payload.as_ref())?;
        }

        Ok(Channel { ws, cache: None })
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
    use crate::websocket::channels::level_three::Message;
    use tokio::time::{sleep, Duration};

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
            .connect::<Message>()
            .await?;

        // Receive messages in a loop.
        while let Ok(message) = channel.next().await {
            println!("{message}");
        }

        Ok(())
    }

    #[tokio::test]
    async fn can_cache_messages() -> Result<(), Box<dyn std::error::Error>> {
        dotenvy::from_filename(".env")?;

        // Load the credentials.
        let key = std::env::var("CB_ACCESS_KEY")?;
        let secret = std::env::var("CB_ACCESS_SECRET")?;
        let passphrase = std::env::var("CB_ACCESS_PASSPHRASE")?;

        // Set up the channel.
        let channel = ChannelBuilder::default()
            .with_authentication(key, secret, passphrase)?
            .with_endpoint("ws-direct.exchange.coinbase.com", 443)
            .with_product_id(ProductId::BtcUsd)
            .connect::<Message>()
            .await?;

        // Cache messages in another task.
        let caching_channel = channel.cache().await;

        // Do something else for a few seconds.
        println!("Caching messages...");

        for _ in 0..12 {
            sleep(Duration::from_millis(250)).await;
            println!(".");
        }

        // Get the channel and its cached messages.
        let mut channel = caching_channel.join().await?;

        // Print all of the cached messages.
        for message in channel.cached_items() {
            println!("{message}");
        }

        // Pull another message off the stream.
        let message = channel.next().await?;

        println!("{message}");

        // Close the channel.
        channel.close().await?;

        println!("Channel closed");

        Ok(())
    }
}
