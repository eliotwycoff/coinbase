use crate::{common::types::Number, websocket::channels::ChannelType};
use serde::{
    de::{self, Deserializer, SeqAccess, Visitor},
    Deserialize,
};
use smartstring::{LazyCompact, SmartString};
use std::{
    fmt::{Display, Formatter, Result as FmtResult},
    marker::PhantomData,
};
use time::OffsetDateTime;
use uuid::Uuid;

#[derive(Debug)]
pub enum Message {
    Change {
        product_id: SmartString<LazyCompact>,
        sequence: u64,
        order_id: Uuid,
        price: Number,
        size: Number,
        time: OffsetDateTime,
    },
    Done {
        product_id: SmartString<LazyCompact>,
        sequence: u64,
        order_id: Uuid,
        time: OffsetDateTime,
    },
    Match {
        product_id: SmartString<LazyCompact>,
        sequence: u64,
        maker_order_id: Uuid,
        taker_order_id: Uuid,
        price: Number,
        size: Number,
        time: OffsetDateTime,
    },
    Noop {
        product_id: SmartString<LazyCompact>,
        sequence: u64,
        time: OffsetDateTime,
    },
    Open {
        product_id: SmartString<LazyCompact>,
        sequence: u64,
        order_id: Uuid,
        side: Side,
        price: Number,
        size: Number,
        time: OffsetDateTime,
    },
}

impl ChannelType for Message {
    fn channel_type() -> &'static str {
        "level3"
    }

    fn parse_schema() -> bool {
        true
    }
}

impl Display for Message {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::Change { product_id, sequence, order_id, price, size, time } => write!(f, "[CHANGE] product_id: {product_id}, sequence: {sequence}, order_id: {order_id}, price: {price}, size: {size}, time: {time}"),
            Self::Done { product_id, sequence, order_id, time } => write!(f, "[DONE] product_id: {product_id}, sequence: {sequence}, order_id: {order_id}, time: {time}"),
            Self::Match { product_id, sequence, maker_order_id, taker_order_id, price, size, time } => write!(f, "[MATCH] product_id: {product_id}, sequence: {sequence}, maker_order_id: {maker_order_id}, taker_order_id: {taker_order_id}, price: {price}, size: {size}, time: {time}"),
            Self::Noop { product_id, sequence, time } => write!(f, "[NOOP] product_id: {product_id}, sequence: {sequence}, time: {time}"),
            Self::Open { product_id, sequence, order_id, side, price, size, time } => write!(f, "[OPEN] product_id: {product_id}, sequence: {sequence}, order_id: {order_id}, side: {side:?}, price: {price}, size: {size}, time: {time}"),
        }
    }
}

impl<'de> Deserialize<'de> for Message {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct MessageVisitor;

        impl<'de> Visitor<'de> for MessageVisitor {
            type Value = Message;

            fn expecting(&self, formatter: &mut Formatter) -> FmtResult {
                formatter.write_str("enum Message")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                struct _OffsetDateTime<'de> {
                    value: OffsetDateTime,
                    phantom: PhantomData<Message>,
                    lifetime: PhantomData<&'de ()>,
                }

                impl<'de> Deserialize<'de> for _OffsetDateTime<'de> {
                    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
                    where
                        D: Deserializer<'de>,
                    {
                        Ok(_OffsetDateTime {
                            value: time::serde::iso8601::deserialize(deserializer)?,
                            phantom: PhantomData,
                            lifetime: PhantomData,
                        })
                    }
                }

                struct _U64<'de> {
                    value: u64,
                    phantom: PhantomData<Message>,
                    lifetime: PhantomData<&'de ()>,
                }

                impl<'de> Deserialize<'de> for _U64<'de> {
                    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
                    where
                        D: Deserializer<'de>,
                    {
                        struct _U64Visitor;

                        impl<'de> Visitor<'de> for _U64Visitor {
                            type Value = _U64<'de>;

                            fn expecting(&self, formatter: &mut Formatter) -> FmtResult {
                                formatter.write_str("numeric string")
                            }

                            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
                            where
                                E: de::Error,
                            {
                                match v.parse::<u64>() {
                                    Ok(value) => Ok(_U64 {
                                        value,
                                        phantom: PhantomData,
                                        lifetime: PhantomData,
                                    }),
                                    Err(_) => Err(de::Error::custom("failed to parse u64")),
                                }
                            }
                        }

                        deserializer.deserialize_str(_U64Visitor)
                    }
                }

                match seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(0, &self))?
                {
                    "change" => Ok(Message::Change {
                        product_id: seq
                            .next_element()?
                            .ok_or_else(|| de::Error::invalid_length(1, &self))?,
                        sequence: seq
                            .next_element::<_U64<'de>>()?
                            .ok_or_else(|| de::Error::invalid_length(2, &self))?
                            .value,
                        order_id: seq
                            .next_element()?
                            .ok_or_else(|| de::Error::invalid_length(3, &self))?,
                        price: seq
                            .next_element()?
                            .ok_or_else(|| de::Error::invalid_length(4, &self))?,
                        size: seq
                            .next_element()?
                            .ok_or_else(|| de::Error::invalid_length(5, &self))?,
                        time: seq
                            .next_element::<_OffsetDateTime<'de>>()?
                            .ok_or_else(|| de::Error::invalid_length(6, &self))?
                            .value,
                    }),
                    "done" => Ok(Message::Done {
                        product_id: seq
                            .next_element()?
                            .ok_or_else(|| de::Error::invalid_length(1, &self))?,
                        sequence: seq
                            .next_element::<_U64<'de>>()?
                            .ok_or_else(|| de::Error::invalid_length(2, &self))?
                            .value,
                        order_id: seq
                            .next_element()?
                            .ok_or_else(|| de::Error::invalid_length(3, &self))?,
                        time: seq
                            .next_element::<_OffsetDateTime<'de>>()?
                            .ok_or_else(|| de::Error::invalid_length(4, &self))?
                            .value,
                    }),
                    "match" => Ok(Message::Match {
                        product_id: seq
                            .next_element()?
                            .ok_or_else(|| de::Error::invalid_length(1, &self))?,
                        sequence: seq
                            .next_element::<_U64<'de>>()?
                            .ok_or_else(|| de::Error::invalid_length(2, &self))?
                            .value,
                        maker_order_id: seq
                            .next_element()?
                            .ok_or_else(|| de::Error::invalid_length(3, &self))?,
                        taker_order_id: seq
                            .next_element()?
                            .ok_or_else(|| de::Error::invalid_length(4, &self))?,
                        price: seq
                            .next_element()?
                            .ok_or_else(|| de::Error::invalid_length(5, &self))?,
                        size: seq
                            .next_element()?
                            .ok_or_else(|| de::Error::invalid_length(6, &self))?,
                        time: seq
                            .next_element::<_OffsetDateTime<'de>>()?
                            .ok_or_else(|| de::Error::invalid_length(7, &self))?
                            .value,
                    }),
                    "noop" => Ok(Message::Noop {
                        product_id: seq
                            .next_element()?
                            .ok_or_else(|| de::Error::invalid_length(1, &self))?,
                        sequence: seq
                            .next_element::<_U64<'de>>()?
                            .ok_or_else(|| de::Error::invalid_length(2, &self))?
                            .value,
                        time: seq
                            .next_element::<_OffsetDateTime<'de>>()?
                            .ok_or_else(|| de::Error::invalid_length(3, &self))?
                            .value,
                    }),
                    "open" => Ok(Message::Open {
                        product_id: seq
                            .next_element()?
                            .ok_or_else(|| de::Error::invalid_length(1, &self))?,
                        sequence: seq
                            .next_element::<_U64<'de>>()?
                            .ok_or_else(|| de::Error::invalid_length(2, &self))?
                            .value,
                        order_id: seq
                            .next_element()?
                            .ok_or_else(|| de::Error::invalid_length(3, &self))?,
                        side: seq
                            .next_element()?
                            .ok_or_else(|| de::Error::invalid_length(4, &self))?,
                        price: seq
                            .next_element()?
                            .ok_or_else(|| de::Error::invalid_length(5, &self))?,
                        size: seq
                            .next_element()?
                            .ok_or_else(|| de::Error::invalid_length(6, &self))?,
                        time: seq
                            .next_element::<_OffsetDateTime<'de>>()?
                            .ok_or_else(|| de::Error::invalid_length(7, &self))?
                            .value,
                    }),
                    variant => Err(de::Error::unknown_variant(
                        variant,
                        &["change", "done", "match", "noop", "open"],
                    ))?,
                }
            }
        }

        deserializer.deserialize_seq(MessageVisitor)
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Side {
    Buy,
    Sell,
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn can_deserialize_change_message() {
        let input = r#"["change","KSM-USD","1085439001","5ca12898-a4e0-4da5-83e7-58f6c8b23a08","47.39","466.02","2024-12-07T03:05:26.853178Z"]"#;
        let change_message: Message = serde_json::from_slice(input.as_bytes()).unwrap();

        println!("change_message => {change_message}");
    }

    #[test]
    fn can_deserialize_done_message() {
        let input = r#"["done","KSM-USD","1085439002","c61973b4-64c6-42f5-92ad-0122b6835346","2024-12-07T03:05:26.858722Z"]"#;
        let done_message: Message = serde_json::from_slice(input.as_bytes()).unwrap();

        println!("done_message => {done_message}");
    }

    #[test]
    fn can_deserialize_match_message() {
        let input = r#"["match","KSM-USD","1085550786","f38ca06b-a427-4072-94db-1489294d990b","1b03667a-ada9-45b6-b6bd-7ef8b153c3b5","46.5","4.6203","2024-12-07T03:45:03.660871Z"]"#;
        let match_message: Message = serde_json::from_slice(input.as_bytes()).unwrap();

        println!("match_message => {match_message}");
    }

    #[test]
    fn can_deserialize_noop_message() {
        let input = r#"["noop","KSM-USD","1085550970","2024-12-07T03:45:06.664022Z"]"#;
        let noop_message: Message = serde_json::from_slice(input.as_bytes()).unwrap();

        println!("noop_message => {noop_message}");
    }

    #[test]
    fn can_deserialize_open_message() {
        let input = r#"["open","KSM-USD","1085550965","757aaa18-41e6-4374-9341-769bf32d2c72","sell","46.84","222.7125","2024-12-07T03:45:06.586641Z"]"#;
        let open_message: Message = serde_json::from_slice(input.as_bytes()).unwrap();

        println!("open_message => {open_message}");
    }
}
