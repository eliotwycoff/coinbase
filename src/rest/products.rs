use crate::{
    common::{types::ProductId, Error},
    rest::{Client, DOMAIN},
};
use bigdecimal::BigDecimal;
use serde::Deserialize;
use serde_json::Value;
use std::{
    fmt::{Display, Formatter, Result as FmtResult},
    future::Future,
};
use time::OffsetDateTime;
use uuid::Uuid;

pub trait Products {
    fn get_product_book(
        &self,
        product_id: ProductId,
    ) -> impl Future<Output = Result<ProductBook, Error>>;
}

impl Products for Client {
    async fn get_product_book(&self, product_id: ProductId) -> Result<ProductBook, Error> {
        self.get_response(|client| {
            client
                .get(format!("https://{DOMAIN}/products/{product_id}/book"))
                .query(&[("level", "3")])
                .header("Content-Type", "application/json")
                .header("User-Agent", "RustSdk/0.1.0")
        })
        .await
    }
}

#[derive(Debug, Deserialize)]
pub struct ProductBook {
    pub asks: Vec<(BigDecimal, BigDecimal, Uuid)>,
    pub auction: Option<Value>,
    pub auction_mode: bool,
    pub bids: Vec<(BigDecimal, BigDecimal, Uuid)>,
    pub sequence: u64,
    #[serde(with = "time::serde::iso8601")]
    pub time: OffsetDateTime,
}

impl Display for ProductBook {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "ProductBook:\n")?;
        write!(f, " Asks:\n")?;

        for (price, size, order_id) in self.asks.iter() {
            write!(f, "  Price: {price}, Size: {size}, Order ID: {order_id}\n")?;
        }

        write!(f, " Auction: {:?}", self.auction)?;
        write!(f, " Auction Mode: {}", self.auction_mode)?;
        write!(f, " Bids:\n")?;

        for (price, size, order_id) in self.bids.iter() {
            write!(f, "  Price: {price}, Size: {size}, Order ID: {order_id}\n")?;
        }

        write!(f, " Sequence: {}\n", self.sequence)?;
        write!(f, " Time: {}\n", self.time)?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{common::rate_limit::TokenBucket, rest::ClientBuilder};
    use std::time::Duration;

    #[tokio::test]
    async fn can_get_product_book() -> Result<(), Box<dyn std::error::Error>> {
        let client = ClientBuilder::new()
            .with_token_bucket(TokenBucket::new(15, Duration::from_millis(100)))
            .build()?;
        let product_book = client.get_product_book(ProductId::KsmUsd).await?;

        println!("product_book => {product_book}");

        Ok(())
    }
}
