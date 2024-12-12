use crate::common::Error;
use serde::{
    de::{self, Deserializer, Visitor},
    Deserialize,
};
use std::fmt::{Display, Formatter, Result as FmtResult};

#[derive(Debug)]
pub struct Number {
    value: u64,
    decimals: usize,
}

impl Number {
    pub fn normalize(self, decimals: usize) -> Result<u64, Error> {
        let scale: u64 = match decimals - self.decimals {
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
            d if d > 0 => return Err(Error::number("cannot normalize past 12 digits")),
            _ => return Err(Error::number("cannot normalize to fewer decimals")),
        };

        Ok(self
            .value
            .checked_mul(scale)
            .ok_or_else(|| Error::number("normalization overflow"))?)
    }
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
                let (decimals, rhs) = match split.next() {
                    Some(str) => (
                        str.len(),
                        str.parse::<u64>().map_err(|_| {
                            de::Error::custom("failed to parse decimal component of number")
                        })?,
                    ),
                    None => (0, 0),
                };
                let scale: u64 = match decimals {
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
                    _ => return Err(de::Error::custom("decimals must be in range 0-12")),
                };
                let value = lhs
                    .checked_mul(scale)
                    .ok_or_else(|| de::Error::custom("integer component overflow"))?
                    .checked_add(rhs)
                    .ok_or_else(|| de::Error::custom("number overflow"))?;

                Ok(Number { value, decimals })
            }
        }

        deserializer.deserialize_str(NumberVisitor)
    }
}

impl Display for Number {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        let mut value = self.value;
        let mut buffer = [b'0'; 21];
        let mut n = 0;

        for (i, j) in (0..21).rev().enumerate() {
            if i == self.decimals && self.decimals != 0 {
                buffer[j] = b'.';
            } else {
                buffer[j] = (value % 10) as u8 + b'0';
                value /= 10;
            }

            n += 1;

            if value == 0 {
                if i <= self.decimals {
                    let zero_count = self.decimals - i;

                    buffer[j - zero_count] = b'.';
                    n += 1 + zero_count;
                }

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
    fn can_deserialize_number_without_decimal() {
        assert!(matches!(
            serde_json::from_slice::<Number>(r#""69""#.as_bytes()).unwrap(),
            Number {
                value: 69,
                decimals: 0,
            }
        ))
    }

    #[test]
    fn can_deserialize_number_with_decimal() {
        assert!(matches!(
            serde_json::from_slice::<Number>(r#""69.42""#.as_bytes()).unwrap(),
            Number {
                value: 6942,
                decimals: 2,
            }
        ))
    }

    #[test]
    fn can_display_number_without_decimal() {
        let number = Number {
            value: 69,
            decimals: 0,
        };

        assert_eq!(format!("{number}"), String::from("69"));
    }

    #[test]
    fn can_display_number_with_decimal() {
        let number = Number {
            value: 6942,
            decimals: 2,
        };

        assert_eq!(format!("{number}"), String::from("69.42"));
    }

    #[test]
    fn can_display_number_with_leading_zero_decimals() {
        let number = Number {
            value: 6942,
            decimals: 6,
        };

        assert_eq!(format!("{number}"), String::from("0.006942"));
    }

    #[test]
    fn can_normalize_number_without_decimal() {
        let number = Number {
            value: 69,
            decimals: 0,
        };

        assert_eq!(number.normalize(3).unwrap(), 69000);
    }

    #[test]
    fn can_normalize_number_with_decimal() {
        let number = Number {
            value: 6942,
            decimals: 2,
        };

        assert_eq!(number.normalize(3).unwrap(), 69420);
    }
}
