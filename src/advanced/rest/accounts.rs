use crate::advanced::{
    common::Error,
    rest::{Client, DOMAIN},
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use smartstring::{LazyCompact, SmartString};
use uuid::Uuid;

pub trait Accounts {
    fn list_accounts(&self) -> impl Future<Output = Result<AccountList, Error>>;
}

impl Accounts for Client {
    async fn list_accounts(&self) -> Result<AccountList, Error> {
        let path = "/api/v3/brokerage/accounts";
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
pub struct AccountList {
    pub accounts: Vec<Account>,
    pub has_next: bool,
    pub cursor: Uuid,
    pub size: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Account {
    pub uuid: Uuid,
    pub name: SmartString<LazyCompact>,
    pub currency: SmartString<LazyCompact>,
    pub available_balance: Balance,
    pub default: bool,
    pub active: bool,
    pub ready: bool,
    pub hold: Balance,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Balance {
    pub value: Decimal,
    pub currency: SmartString<LazyCompact>,
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{advanced::rest::ClientBuilder, test};
    use tracing::info;

    #[tokio::test]
    async fn can_get_account_list() -> test::Result<()> {
        test::setup()?;

        // Create a client and get the account list.
        let client = ClientBuilder::new().build()?;
        let accounts = client.list_accounts().await?;

        info!("accounts => {}", serde_json::to_string_pretty(&accounts)?);

        Ok(())
    }
}
