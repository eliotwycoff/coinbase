use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter, Result as FmtResult};
use strum::IntoStaticStr;
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize)]
pub enum CancelReason {
    #[serde(rename = "101")]
    TimeInForce,
    #[serde(rename = "102")]
    SelfTradePrevention,
    #[serde(rename = "103")]
    Admin,
    #[serde(rename = "104")]
    PriceBoundOrderProtection,
    #[serde(rename = "105")]
    InsufficientFunds,
    #[serde(rename = "106")]
    InsufficientLiquidity,
    #[serde(rename = "107")]
    Broker,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OrderType {
    Market,
    Limit,
}

#[derive(Debug, Serialize, Deserialize, IntoStaticStr)]
pub enum ProductId {
    #[serde(rename = "BTC-USD")]
    #[strum(serialize = "BTC-USD")]
    BtcUsd,
    #[serde(rename = "ETH-USD")]
    #[strum(serialize = "ETH-USD")]
    EtcUsd,
    #[serde(rename = "KSM-USD")]
    #[strum(serialize = "KSM-USD")]
    KsmUsd,
}

impl Display for ProductId {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        let ticker: &'static str = self.into();

        write!(f, "{ticker}")
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Reason {
    Filled,
    Canceled,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum UserId<'ws> {
    Taker {
        taker_user_id: &'ws str,
        user_id: &'ws str,
        taker_profile_id: Uuid,
        profile_id: Uuid,
        taker_fee_rate: BigDecimal,
    },
    Maker {
        maker_user_id: &'ws str,
        user_id: &'ws str,
        maker_profile_id: Uuid,
        profile_id: Uuid,
        maker_fee_rate: BigDecimal,
    },
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn can_deserialize_product_id() {
        let json = r#""KSM-USD""#;
        let product_id = serde_json::from_slice::<ProductId>(json.as_bytes()).unwrap();

        println!("product_id => {product_id:?}");

        let sequence = serde_json::from_slice::<u64>(b"1074685508").unwrap();

        println!("sequence => {sequence}");
    }
}
