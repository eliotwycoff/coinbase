use crate::exchange::{
    common::{types::Number, Error},
    rest::{Client, DOMAIN},
};
use serde::Deserialize;
use serde_json::Value;
use smartstring::{LazyCompact, SmartString};
use std::{
    fmt::{Display, Formatter, Result as FmtResult},
    future::Future,
};
use time::OffsetDateTime;
use uuid::Uuid;

pub trait Products {
    fn list_trading_pairs(&self) -> impl Future<Output = Result<Vec<Product>, Error>>;
    fn get_single_product(
        &self,
        product_id: impl Display,
    ) -> impl Future<Output = Result<Product, Error>>;
    fn get_product_book(
        &self,
        product_id: impl Display,
    ) -> impl Future<Output = Result<ProductBook, Error>>;
}

impl Products for Client {
    async fn list_trading_pairs(&self) -> Result<Vec<Product>, Error> {
        self.get_response(|client| {
            client
                .get(format!("https://{DOMAIN}/products"))
                .header("Content-Type", "application/json")
                .header("User-Agent", "RustSdk/0.1.0")
        })
        .await
    }

    async fn get_single_product(&self, product_id: impl Display) -> Result<Product, Error> {
        self.get_response(|client| {
            client
                .get(format!("https://{DOMAIN}/products/{product_id}"))
                .header("Content-Type", "application/json")
                .header("User-Agent", "RustSdk/0.1.0")
        })
        .await
    }

    async fn get_product_book(&self, product_id: impl Display) -> Result<ProductBook, Error> {
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
pub struct Product {
    pub auction_mode: bool,
    pub base_currency: SmartString<LazyCompact>,
    pub base_increment: Number,
    pub cancel_only: bool,
    pub display_name: SmartString<LazyCompact>,
    pub fx_stablecoin: bool,
    pub high_bid_limit_percentage: SmartString<LazyCompact>,
    pub id: SmartString<LazyCompact>,
    pub limit_only: bool,
    pub margin_enabled: bool,
    pub max_slippage_percentage: Number,
    pub min_market_funds: Number,
    pub post_only: bool,
    pub quote_currency: SmartString<LazyCompact>,
    pub quote_increment: Number,
    pub status: Status,
    pub status_message: SmartString<LazyCompact>,
    pub trading_disabled: bool,
}

impl Display for Product {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "Product:\n")?;
        write!(f, " Auction Mode: {}\n", self.auction_mode)?;
        write!(f, " Base Currency: {}\n", self.base_currency)?;
        write!(f, " Base Increment: {}\n", self.base_increment)?;
        write!(f, " Cancel Only: {}\n", self.cancel_only)?;
        write!(f, " Display Name: {}\n", self.display_name)?;
        write!(f, " Fx Stablecoin: {}\n", self.fx_stablecoin)?;
        write!(
            f,
            " High Bid Limit Percentage: {}\n",
            self.high_bid_limit_percentage
        )?;
        write!(f, " Id: {}\n", self.id)?;
        write!(f, " Limit Only: {}\n", self.limit_only)?;
        write!(f, " Margin Enabled: {}\n", self.margin_enabled)?;
        write!(
            f,
            " Max Slippage Percentage: {}\n",
            self.max_slippage_percentage
        )?;
        write!(f, " Min Market Funds: {}\n", self.min_market_funds)?;
        write!(f, " Post Only: {}\n", self.post_only)?;
        write!(f, " Quote Currency: {}\n", self.quote_currency)?;
        write!(f, " Quote Increment: {}\n", self.quote_increment)?;
        write!(f, " Status: {:?}\n", self.status)?;
        write!(f, " Status Message: {}\n", self.status_message)?;
        write!(f, " Trading Disabled: {}\n", self.trading_disabled)?;

        Ok(())
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Status {
    Online,
    Offline,
    Internal,
    Delisted,
}

#[derive(Debug, Deserialize)]
pub struct ProductBook {
    pub asks: Vec<(Number, Number, Uuid)>,
    pub auction: Option<Value>,
    pub auction_mode: bool,
    pub bids: Vec<(Number, Number, Uuid)>,
    pub sequence: u64,
    #[serde(with = "time::serde::iso8601")]
    pub time: OffsetDateTime,
}

impl Display for ProductBook {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "ProductBook:\n")?;
        write!(f, " Asks:\n")?;

        for (price, size, order_id) in self.asks.iter().rev() {
            write!(f, "  Price: {price}, Size: {size}, Order ID: {order_id}\n")?;
        }

        write!(f, " Bids:\n")?;

        for (price, size, order_id) in self.bids.iter() {
            write!(f, "  Price: {price}, Size: {size}, Order ID: {order_id}\n")?;
        }

        write!(f, " Auction: {:?}\n", self.auction)?;
        write!(f, " Auction Mode: {}\n", self.auction_mode)?;
        write!(f, " Sequence: {}\n", self.sequence)?;
        write!(f, " Time: {}\n", self.time)?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        exchange::{common::rate_limit::TokenBucket, rest::ClientBuilder},
        test,
    };
    use std::time::Duration;
    use tracing::info;

    #[tokio::test]
    async fn can_list_trading_pairs() -> test::Result<()> {
        test::setup().await?;

        let client = ClientBuilder::new()
            .with_token_bucket(TokenBucket::new(15, Duration::from_millis(100)))
            .build()?;
        let trading_pairs = client.list_trading_pairs().await?;

        println!(
            "{}",
            trading_pairs
                .into_iter()
                .map(|pair| pair.id)
                .collect::<Vec<SmartString<LazyCompact>>>()
                .join(", ")
        );

        Ok(())
    }

    #[tokio::test]
    async fn can_get_single_product() -> test::Result<()> {
        test::setup().await?;

        let client = ClientBuilder::new()
            .with_token_bucket(TokenBucket::new(15, Duration::from_millis(100)))
            .build()?;
        let product = client.get_single_product("KSM-USD").await?;

        info!("{product}");

        Ok(())
    }

    #[tokio::test]
    async fn can_get_product_book() -> test::Result<()> {
        test::setup().await?;

        let client = ClientBuilder::new()
            .with_token_bucket(TokenBucket::new(15, Duration::from_millis(100)))
            .build()?;
        let product_book = client.get_product_book("KSM-USD").await?;

        info!("product_book => {product_book}");

        Ok(())
    }
}
