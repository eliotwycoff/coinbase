use crate::common::types::ProductId;
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum Message<'ws> {
    Received {
        #[serde(with = "time::serde::iso8601")]
        time: OffsetDateTime,
        product_id: ProductId,
        sequence: u64,
        order_id: Uuid,
        size: BigDecimal,
        price: BigDecimal,
        side: Side,
        order_type: OrderType,
        #[serde(rename = "client-oid")]
        client_oid: Option<&'ws str>,
    },
    Open {
        #[serde(with = "time::serde::iso8601")]
        time: OffsetDateTime,
        product_id: ProductId,
        sequence: u64,
        order_id: Uuid,
        price: BigDecimal,
        remaining_size: BigDecimal,
        side: Side,
    },
    Done {
        #[serde(with = "time::serde::iso8601")]
        time: OffsetDateTime,
        product_id: ProductId,
        sequence: u64,
        price: Option<BigDecimal>,
        order_id: Uuid,
        reason: Reason,
        side: Side,
        remaining_size: Option<BigDecimal>,
        cancel_reason: Option<CancelReason>,
    },
    Match {
        trade_id: u64,
        sequence: u64,
        maker_order_id: Uuid,
        taker_order_id: Uuid,
        #[serde(with = "time::serde::iso8601")]
        time: OffsetDateTime,
        product_id: ProductId,
        size: BigDecimal,
        price: BigDecimal,
        side: Side,
        #[serde(flatten)]
        user_id: Option<UserId<'ws>>,
    },
    Change {
        time: OffsetDateTime,
        // TODO: Finish this
    },
    Activate {
        time: OffsetDateTime,
        // TODO: Finish this
    },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OrderType {
    Market,
    Limit,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Reason {
    Filled,
    Canceled,
}

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
    fn can_deserialize_received_message() {
        let json = r#"{"type":"received","time":"2014-11-07T08:19:27.028459Z","product_id":"BTC-USD","sequence":10,"order_id":"d50ec984-77a8-460a-b958-66f114b0de9b","size":"1.34","price":"502.1","side":"buy","order_type":"limit","client-oid":"d50ec974-76a2-454b-66f135b1ea8c"}"#;
        let message: Message = serde_json::from_str(json).unwrap();

        println!("message => {message:?}");
    }
}
