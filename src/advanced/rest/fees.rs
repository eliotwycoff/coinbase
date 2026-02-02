use crate::advanced::{
    common::Error,
    rest::{Client, DOMAIN},
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use smartstring::{LazyCompact, SmartString};

pub trait Fees {
    fn get_fee_summary(&self) -> impl Future<Output = Result<FeeSummary, Error>>;
}

impl Fees for Client {
    async fn get_fee_summary(&self) -> Result<FeeSummary, Error> {
        let path = "/api/v3/brokerage/transaction_summary";
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
pub struct FeeSummary {
    pub total_volume: Decimal,
    pub total_fees: Decimal,
    pub fee_tier: FeeTier,
    pub advanced_trade_only_volume: Decimal,
    pub advanced_trade_only_fees: Decimal,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FeeTier {
    pub pricing_tier: SmartString<LazyCompact>,
    pub taker_fee_rate: Decimal,
    pub maker_fee_rate: Decimal,
    pub aop_from: Decimal,
    pub aop_to: Decimal,
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{advanced::rest::ClientBuilder, test};
    use tracing::info;

    #[tokio::test]
    async fn can_get_fee_summary() -> test::Result<()> {
        test::setup()?;

        // Create a client and get the fee summary.
        let client = ClientBuilder::new().build()?;
        let summary = client.get_fee_summary().await?;

        info!("taker fee rate => {:?}", summary.fee_tier.taker_fee_rate);
        info!("maker fee rate => {:?}", summary.fee_tier.maker_fee_rate);

        info!("fee summary => {}", serde_json::to_string_pretty(&summary)?);

        Ok(())
    }
}
