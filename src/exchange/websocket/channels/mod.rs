use crate::exchange::common::{Error, authentication::Signer, rate_limit::TokenBucket};
use fastwebsockets::{Frame, Payload, WebSocket, handshake};
use http_body_util::Empty;
use hyper::{
    Request,
    body::Bytes,
    header::{CONNECTION, UPGRADE},
    upgrade::Upgraded,
};
use hyper_util::rt::TokioIo;
use rustls_pki_types::ServerName;
use serde::{Deserialize, de::DeserializeOwned};
use serde_json::Value;
use smartstring::{LazyCompact, SmartString};
use std::{
    future::Future,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
    vec::IntoIter,
};
use time::OffsetDateTime;
use tokio::{
    net::TcpStream,
    sync::oneshot::{self, Sender, error::TryRecvError},
    task::JoinHandle,
    time::Duration,
};
use tokio_rustls::{
    TlsConnector,
    rustls::{ClientConfig, RootCertStore},
};
use tracing::{debug, error};

pub mod level_three;

pub trait ChannelType {
    fn channel_type() -> &'static str;
    fn parse_schema() -> bool;
}

pub struct Channel<T>
where
    T: 'static + DeserializeOwned + Send + Clone,
{
    ws: WebSocket<TokioIo<Upgraded>>,
    cache: Option<Vec<T>>,
    token_bucket: TokenBucket,
}

impl<T> Channel<T>
where
    T: 'static + DeserializeOwned + Send + Clone,
{
    /// Send a frame (message) to the host, respecting rate limits.
    async fn write_frame<'f>(&mut self, frame: Frame<'f>) -> Result<(), Error> {
        // Get a permit (token) to send this frame.
        let token = self.token_bucket.get_token().await?;

        // Send the frame and get the result.
        let result = self.ws.write_frame(frame).await;

        // Return the token.
        self.token_bucket.return_token(token).await?;

        // Return the result.
        result.map_err(|error| Error::dependency("websocket", Box::new(error)))
    }

    /// Read a frame (message) from the host, closing the connection on error.
    async fn read_frame<'f>(&'f mut self) -> Result<Frame<'f>, Error> {
        match self.ws.read_frame().await {
            Ok(frame) => Ok(frame),
            Err(error) => {
                self.close().await?;

                Err(Error::from(error))
            }
        }
    }

    /// Read the next `T` message from the websocket.
    ///
    /// This method has a 10-second read timeout. If no frame is received within
    /// that window, the connection is considered dead (likely a silent server-side
    /// disconnect), the websocket is closed, and an error is returned.
    ///
    /// The channel subscribes to both the primary channel (e.g., level3) and the
    /// heartbeat channel. Heartbeat messages arrive every second regardless of
    /// market activity, ensuring the timeout only triggers on actual connection
    /// failures rather than quiet markets. Heartbeat messages are silently filtered
    /// out; only `T` messages are returned to the caller.
    pub async fn next(&mut self) -> Result<T, Error> {
        'message: loop {
            match tokio::time::timeout(Duration::from_secs(10), self.ws.read_frame()).await {
                Ok(Ok(frame)) => match serde_json::from_slice::<T>(frame.payload.as_ref()) {
                    Ok(message) => return Ok(message),
                    Err(message_error) => {
                        match serde_json::from_slice::<Heartbeat>(frame.payload.as_ref()) {
                            Ok(_) => continue 'message,
                            Err(heartbeat_error) => {
                                error!("Failed to deserialize message => {message_error}");
                                error!("Failed to deserialize heartbeat => {heartbeat_error}");

                                if let Ok(payload) = std::str::from_utf8(frame.payload.as_ref()) {
                                    error!("Unknown message payload => {payload}");
                                }

                                return Err(Error::from(heartbeat_error));
                            }
                        }
                    }
                },
                Ok(Err(error)) => {
                    error!("Closing websocket after error => {error}");
                    self.close().await?;

                    return Err(Error::from(error));
                }
                Err(elapsed) => {
                    error!("Closing websocket after timeout");
                    self.close().await?;

                    return Err(Error::dependency("Websocket timed out", Box::new(elapsed)));
                }
            }
        }
    }

    /// Close this WebSocket connection.
    pub async fn close(&mut self) -> Result<(), Error> {
        self.write_frame(Frame::close_raw(vec![].into())).await
    }

    /// Cache incoming `T` messages. Note that this function returns a `CachingChannel`,
    /// i.e. a handle to a `Channel` in caching mode. To stop caching and retrieve
    /// the original `Channel`, call `.join()` on the `CachingChannel`.
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

    /// Get an iterator over all cached `T` messages.
    pub fn cached_items(&mut self) -> IntoIter<T> {
        self.cache.take().unwrap_or_else(|| vec![]).into_iter()
    }

    /// Get a clone of the last cached `T` message.
    pub fn last_cached(&self) -> Option<T> {
        self.cache
            .as_ref()
            .and_then(|cache| cache.last().map(|message| message.clone()))
    }
}

pub struct CachingChannel<T>
where
    T: 'static + DeserializeOwned + Send + Clone,
{
    tx: Sender<()>,
    join_handle: JoinHandle<Result<Channel<T>, Error>>,
}

impl<T> CachingChannel<T>
where
    T: 'static + DeserializeOwned + Send + Clone,
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
    product_ids: Vec<SmartString<LazyCompact>>,
    domain: Option<String>,
    port: Option<u16>,
    token_bucket: Option<TokenBucket>,
    tls_config: Option<Arc<ClientConfig>>,
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

    pub fn with_key(mut self, key: impl Into<String>) -> Self {
        self.key = Some(key.into());

        self
    }

    pub fn with_signer(mut self, signer: Signer) -> Self {
        self.signer = Some(signer);

        self
    }

    pub fn with_passphrase(mut self, passphrase: String) -> Self {
        self.passphrase = Some(passphrase);

        self
    }

    pub fn with_product_id(mut self, product_id: impl Into<SmartString<LazyCompact>>) -> Self {
        self.product_ids.push(product_id.into());

        self
    }

    pub fn with_endpoint(mut self, domain: impl Into<String>, port: u16) -> Self {
        self.domain = Some(domain.into());
        self.port = Some(port);

        self
    }

    pub fn with_token_bucket(mut self, token_bucket: TokenBucket) -> Self {
        self.token_bucket = Some(token_bucket);

        self
    }

    pub fn with_tls_config(mut self, tls_config: Option<Arc<ClientConfig>>) -> Self {
        self.tls_config = tls_config;

        self
    }

    /// Connect to the endpoint and stream order book data.
    pub async fn connect<T>(self) -> Result<Channel<T>, Error>
    where
        T: 'static + ChannelType + DeserializeOwned + Send + Clone,
    {
        debug!("Creating signing key");
        let key = self
            .key
            .ok_or_else(|| Error::unavailable("authentication key"))?;

        debug!("Generating signature");
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)?
            .as_secs()
            .to_string();
        let signature = self
            .signer
            .ok_or_else(|| Error::unavailable("authentication secret"))?
            .get_cb_access_sign(timestamp.as_str(), "/users/self/verify", "", "GET")?;

        debug!("Fetching passphrase");
        let passphrase = self
            .passphrase
            .ok_or_else(|| Error::unavailable("authentication passphrase"))?;

        debug!("Creating subscription message");
        let subscription_message = serde_json::to_string(&serde_json::json!({
            "type": "subscribe",
            "channels": [
                { "name": T::channel_type(), "product_ids": self.product_ids },
                { "name": "heartbeat", "product_ids": self.product_ids },
            ],
            "signature": signature,
            "key": key,
            "passphrase": passphrase,
            "timestamp": timestamp,
        }))?;

        debug!("Fetching endpoint domain");
        let domain = self
            .domain
            .ok_or_else(|| Error::unavailable("endpoint domain"))?;

        debug!("Fetching endpoint port");
        let port = self
            .port
            .ok_or_else(|| Error::unavailable("endpoint port"))?;

        debug!("Establishing TCP stream");
        let tcp_stream = TcpStream::connect(format!("{domain}:{port}")).await?;

        debug!("Getting TLS server name");
        let tls_domain = ServerName::try_from(domain.clone())?;

        debug!("Upgrading to TLS");
        let tls_config = self.tls_config.unwrap_or_else(|| {
            let mut root_cert_store = RootCertStore::empty();

            root_cert_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

            Arc::new(
                ClientConfig::builder()
                    .with_root_certificates(root_cert_store)
                    .with_no_client_auth(),
            )
        });
        let tls_connector = TlsConnector::from(tls_config);
        let tls_stream = tls_connector.connect(tls_domain, tcp_stream).await?;

        debug!("Generating WSS upgrade request");
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

        debug!("Upgrading to WSS");
        let (mut ws, _) = handshake::client(&SpawnExecutor, request, tls_stream).await?;

        ws.set_writev(false);
        ws.set_auto_close(true);
        ws.set_auto_pong(true);

        debug!("Creating WebSocket channel object");
        let mut channel = Channel {
            ws,
            cache: None,
            token_bucket: self
                .token_bucket
                .ok_or_else(|| Error::unavailable("token bucket"))?,
        };

        debug!("Sending subscription message");
        channel
            .write_frame(Frame::text(Payload::Borrowed(
                subscription_message.as_bytes(),
            )))
            .await?;

        debug!("Deserializing subscriptions response");
        let _ = serde_json::from_slice::<Value>(channel.read_frame().await?.payload.as_ref())?;

        if T::parse_schema() {
            debug!("Deserializing schema response");
            let _ = serde_json::from_slice::<Value>(channel.read_frame().await?.payload.as_ref())?;
        }

        Ok(channel)
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

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
pub enum Heartbeat {
    #[serde(rename = "heartbeat")]
    Heartbeat {
        sequence: u64,
        last_trade_id: u64,
        product_id: SmartString<LazyCompact>,
        #[serde(with = "time::serde::iso8601")]
        time: OffsetDateTime,
    },
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{exchange::websocket::channels::level_three::Message, test};
    use tokio::time::{Duration, sleep};

    #[tokio::test]
    async fn can_receive_messages() -> test::Result<()> {
        test::setup()?;

        // Load the credentials.
        let key = std::env::var("CB_ACCESS_KEY")?;
        let secret = std::env::var("CB_ACCESS_SECRET")?;
        let passphrase = std::env::var("CB_ACCESS_PASSPHRASE")?;

        // Set up the channel.
        let mut channel = ChannelBuilder::default()
            .with_authentication(key, secret, passphrase)?
            .with_endpoint("ws-direct.exchange.coinbase.com", 443)
            .with_product_id("BTC-USD")
            .with_token_bucket(TokenBucket::new(1_000, Duration::from_millis(100)))
            .connect::<Message>()
            .await?;

        // Receive messages in a loop.
        while let Ok(message) = channel.next().await {
            println!("{message}");
        }

        Ok(())
    }

    #[tokio::test]
    async fn can_cache_messages() -> test::Result<()> {
        test::setup()?;

        // Load the credentials.
        let key = std::env::var("CB_ACCESS_KEY")?;
        let secret = std::env::var("CB_ACCESS_SECRET")?;
        let passphrase = std::env::var("CB_ACCESS_PASSPHRASE")?;

        // Set up the channel.
        let channel = ChannelBuilder::default()
            .with_authentication(key, secret, passphrase)?
            .with_endpoint("ws-direct.exchange.coinbase.com", 443)
            .with_product_id("BTC-USD")
            .with_token_bucket(TokenBucket::new(1_000, Duration::from_millis(100)))
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
