use bigdecimal::{BigDecimal, Num};
use serde::{
    de::{self, Deserializer, Visitor},
    Deserialize, Serialize,
};
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

#[derive(Debug)]
pub struct Number {
    value: u64,
    power: usize,
}

impl<'de> Deserialize<'de> for Number {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct NumberVisitor;

        impl<'de> Visitor<'de> for NumberVisitor {
            type Value = Number;

            fn expecting(&self, formatter: &mut Formatter) -> FmtResult {
                formatter.write_str("Number")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                let mut split = v.split(".");
                let lhs = split
                    .next()
                    .ok_or_else(|| de::Error::custom("numeric string is empty"))?
                    .parse::<u64>()
                    .map_err(|_| {
                        de::Error::custom("failed to parse integer component of number")
                    })?;
                let (power, rhs) = match split.next() {
                    Some(str) => (
                        str.len(),
                        str.parse::<u64>().map_err(|_| {
                            de::Error::custom("failed to parse decimal component of number")
                        })?,
                    ),
                    None => (0, 0),
                };
                let scale: u64 = match power {
                    0 => 1,
                    1 => 10,
                    2 => 100,
                    3 => 1_000,
                    4 => 10_000,
                    5 => 100_000,
                    6 => 1_000_000,
                    7 => 10_000_000,
                    8 => 100_000_000,
                    9 => 1_000_000_000,
                    10 => 10_000_000_000,
                    11 => 100_000_000_000,
                    12 => 1_000_000_000_000,
                    _ => return Err(de::Error::custom("power must be in range 0-12")),
                };
                let value = lhs
                    .checked_mul(scale)
                    .ok_or_else(|| de::Error::custom("integer component overflow"))?
                    .checked_add(rhs)
                    .ok_or_else(|| de::Error::custom("number overflow"))?;

                Ok(Number { value, power })
            }
        }

        deserializer.deserialize_str(NumberVisitor)
    }
}

impl Display for Number {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        let mut value = self.value;
        let mut buffer = [0u8; 21];
        let mut n = 0;

        for (i, j) in (0..21).rev().enumerate() {
            if i == self.power && self.power != 0 {
                buffer[j] = b'.';
            } else {
                buffer[j] = (value % 10) as u8 + b'0';
                value /= 10;
            }

            n += 1;

            if value == 0 {
                break;
            }
        }

        let start = 21 - n;
        let repr = unsafe { std::str::from_utf8_unchecked(&buffer[start..]) };

        write!(f, "{repr}")
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

    #[test]
    fn can_deserialize_number_without_decimal() {
        assert!(matches!(
            serde_json::from_slice::<Number>(r#""69""#.as_bytes()).unwrap(),
            Number {
                value: 69,
                power: 0,
            }
        ))
    }

    #[test]
    fn can_deserialize_number_with_decimal() {
        assert!(matches!(
            serde_json::from_slice::<Number>(r#""69.42""#.as_bytes()).unwrap(),
            Number {
                value: 6942,
                power: 2,
            }
        ))
    }

    #[test]
    fn can_display_number_without_decimal() {
        let number = Number {
            value: 69,
            power: 0,
        };

        assert_eq!(format!("{number}"), String::from("69"));
    }

    #[test]
    fn can_display_number_with_decimal() {
        let number = Number {
            value: 6942,
            power: 2,
        };

        assert_eq!(format!("{number}"), String::from("69.42"));
    }
}
