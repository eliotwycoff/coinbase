use crate::{
    advanced::common::{
        Error,
        authentication::{JwtSigner, Key},
    },
    exchange::common::rate_limit::TokenBucket,
};
use reqwest::{Client as HttpClient, RequestBuilder};
use serde::de::DeserializeOwned;
use std::{sync::Arc, time::Duration};

pub mod accounts;
pub mod fees;
pub mod orders;
pub mod products;

pub const DOMAIN: &'static str = "api.coinbase.com";

#[derive(Clone)]
pub struct Client {
    signer: Arc<JwtSigner>,
    http_client: HttpClient,
    token_bucket: TokenBucket,
}

impl Client {
    pub fn get_jwt(
        &self,
        request_method: &str,
        request_host: &str,
        request_path: &str,
    ) -> Result<String, Error> {
        self.signer
            .get_jwt(request_method, request_host, request_path)
    }

    pub async fn get_response<F, T>(&self, f: F) -> Result<T, Error>
    where
        F: FnOnce(&HttpClient) -> RequestBuilder,
        T: 'static + DeserializeOwned + Send,
    {
        // Get a permit (token) to send this request.
        let token = self
            .token_bucket
            .get_token()
            .await
            .map_err(|error| Error::impossible("semaphore").with_source(Box::new(error)))?;

        // Send the request and get the response.
        let response = f(&self.http_client).send().await;

        // Return the token.
        self.token_bucket
            .return_token(token)
            .await
            .map_err(|error| Error::impossible("semaphore").with_source(Box::new(error)))?;

        // Await the response bytes.
        let bytes = response
            .map_err(|error| Error::domain(Box::new(error)))?
            .bytes()
            .await
            .map_err(|error| Error::domain(Box::new(error)))?;

        // Deserialize the response bytes.
        serde_json::from_slice(bytes.as_ref())
            .map_err(|error| Error::invalid(bytes).with_source(Box::new(error)))
    }
}

pub struct ClientBuilder {
    signer: Option<JwtSigner>,
    token_bucket: Option<TokenBucket>,
}

impl ClientBuilder {
    pub fn new() -> Self {
        Self {
            signer: None,
            token_bucket: None,
        }
    }

    pub fn with_signer(mut self, signer: JwtSigner) -> Self {
        self.signer = Some(signer);

        self
    }

    pub fn with_token_bucket(mut self, token_bucket: TokenBucket) -> Self {
        self.token_bucket = Some(token_bucket);

        self
    }

    pub fn build(self) -> Result<Client, Error> {
        Ok(Client {
            signer: Arc::new(self.signer.map(|signer| Ok(signer)).unwrap_or_else(|| {
                Key::load("osage-cli", "coinbase-advanced.api-key")
                    .map_err(|error| {
                        Error::unavailable("coinbase advanced api key").with_source(Box::new(error))
                    })
                    .and_then(|key| JwtSigner::try_from(key))
            })?),
            http_client: HttpClient::new(),
            token_bucket: self
                .token_bucket
                .unwrap_or_else(|| TokenBucket::new(15, Duration::from_millis(360))),
        })
    }
}
