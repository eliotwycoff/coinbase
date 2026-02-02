use crate::advanced::{
    common::Error,
    rest::{Client, DOMAIN},
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use smartstring::{LazyCompact, SmartString};
use std::fmt::{Display, Formatter, Result as FmtResult};

pub trait Products {
    fn list_products(&self) -> impl Future<Output = Result<ProductList, Error>>;
}

impl Products for Client {
    async fn list_products(&self) -> Result<ProductList, Error> {
        let path = "/api/v3/brokerage/products";
        let token = self.get_jwt("GET", DOMAIN, path)?;

        self.get_response(|client| {
            client
                .get(format!("https://{DOMAIN}{path}"))
                .header("Authorization", format!("Bearer {token}"))
                .header("User-Agent", "RustSdk/0.1.0")
        })
        .await
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ProductList {
    pub products: Vec<Product>,
    pub num_products: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Product {
    pub product_id: SmartString<LazyCompact>,
    pub price: Decimal,
    pub base_increment: Decimal,
    pub quote_increment: Decimal,
    pub quote_min_size: Decimal,
    pub quote_max_size: Decimal,
    pub base_min_size: Decimal,
    pub base_max_size: Decimal,
    pub base_name: SmartString<LazyCompact>,
    pub quote_name: SmartString<LazyCompact>,
    pub is_disabled: bool,
    pub new: bool,
    pub status: SmartString<LazyCompact>,
    pub cancel_only: bool,
    pub limit_only: bool,
    pub post_only: bool,
    pub trading_disabled: bool,
    pub auction_mode: bool,
    pub product_type: SmartString<LazyCompact>,
    pub quote_currency_id: SmartString<LazyCompact>,
    pub base_currency_id: SmartString<LazyCompact>,
    pub alias: SmartString<LazyCompact>,
    pub price_increment: Decimal,
}

impl Display for Product {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "{}", self.product_id)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{advanced::rest::ClientBuilder, test};
    use tracing::info;

    #[tokio::test]
    async fn can_get_product_list() -> test::Result<()> {
        test::setup()?;

        // Create a client and get the product list.
        let client = ClientBuilder::new().build()?;
        let products = client
            .list_products()
            .await?
            .products
            .into_iter()
            .map(|product| product.to_string())
            .collect::<Vec<String>>();

        info!("products => {products:?}");

        Ok(())
    }
}
