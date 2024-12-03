use serde::{Deserialize, Serialize};
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
