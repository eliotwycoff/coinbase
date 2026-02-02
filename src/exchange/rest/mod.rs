use crate::exchange::common::{Error, rate_limit::TokenBucket};
use reqwest::{Client as HttpClient, RequestBuilder};
use serde::de::DeserializeOwned;

pub mod products;

pub const DOMAIN: &'static str = "api.exchange.coinbase.com";

#[derive(Debug, Clone)]
pub struct Client {
    http_client: HttpClient,
    token_bucket: TokenBucket,
}

impl Client {
    pub async fn get_response<F, R, T>(&self, f: F) -> Result<T, Error>
    where
        F: Fn(&HttpClient) -> RequestBuilder,
        R: 'static + DeserializeOwned + Into<Result<T, Error>>,
    {
        // Get a permit (token) to send this request.
        let token = self.token_bucket.get_token().await?;

        // Send the request and get the response.
        let response = f(&self.http_client).send().await;

        // Return the token.
        self.token_bucket.return_token(token).await?;

        // Await the response bytes.
        let bytes = response?.bytes().await?;

        // Deserialize the response bytes.
        serde_json::from_slice::<R>(bytes.as_ref())?.into()
    }
}

pub struct ClientBuilder {
    token_bucket: Option<TokenBucket>,
}

impl ClientBuilder {
    pub fn new() -> Self {
        Self { token_bucket: None }
    }

    pub fn with_token_bucket(mut self, token_bucket: TokenBucket) -> Self {
        self.token_bucket = Some(token_bucket);

        self
    }

    pub fn build(self) -> Result<Client, Error> {
        Ok(Client {
            http_client: HttpClient::new(),
            token_bucket: self
                .token_bucket
                .ok_or_else(|| Error::unavailable("token bucket"))?,
        })
    }
}
