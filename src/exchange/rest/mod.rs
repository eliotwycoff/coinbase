use crate::exchange::common::{rate_limit::TokenBucket, Error};
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
    pub async fn get_response<F, T>(&self, f: F) -> Result<T, Error>
    where
        F: Fn(&HttpClient) -> RequestBuilder,
        T: 'static + DeserializeOwned + Send,
    {
        // Get a permit (token) to send this request.
        let token = self.token_bucket.get_token().await?;

        // Send the request and get the response.
        let bytes = f(&self.http_client).send().await?.bytes().await?;

        // Return the token.
        self.token_bucket.return_token(token).await?;

        // Deserialize the response bytes.
        Ok(serde_json::from_slice(bytes.as_ref())?)
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
                .ok_or_else(|| Error::param_required("token bucket"))?,
        })
    }
}
