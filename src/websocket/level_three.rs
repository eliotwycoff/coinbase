use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Schema<'ws> {
    #[serde(rename = "type")]
    _type: &'ws str,
    #[serde(rename = "schema")]
    _schema: InnerSchema<'ws>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct InnerSchema<'ws> {
    #[serde(rename = "change", borrow)]
    _change: [&'ws str; 7],
    #[serde(rename = "done")]
    _done: [&'ws str; 5],
    #[serde(rename = "match")]
    _match: [&'ws str; 8],
    #[serde(rename = "noop")]
    _noop: [&'ws str; 4],
    #[serde(rename = "open")]
    _open: [&'ws str; 8],
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn can_deserialize_schema() {
        let json = r#"{"type":"level3","schema":{"change":["type","product_id","sequence","order_id","price","size","time"],"done":["type","product_id","sequence","order_id","time"],"match":["type","product_id","sequence","maker_order_id","taker_order_id","price","size","time"],"noop":["type","product_id","sequence","time"],"open":["type","product_id","sequence","order_id","side","price","size","time"]}}"#;
        let schema: Schema = serde_json::from_str(json).unwrap();

        println!("schema => {schema:?}");
    }
}
