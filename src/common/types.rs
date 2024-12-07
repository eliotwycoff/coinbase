use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter, Result as FmtResult};
use strum::IntoStaticStr;

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
