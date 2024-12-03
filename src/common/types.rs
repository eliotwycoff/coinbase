use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum ProductId {
    #[serde(rename = "BTC-USD")]
    BtcUsd,
    #[serde(rename = "ETH-USD")]
    EtcUsd,
}
