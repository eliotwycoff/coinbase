use crate::exchange::common::Error;
use base64::{prelude::BASE64_STANDARD, Engine};
use hmac::{Hmac, Mac};
use sha2::Sha256;

#[derive(Debug)]
pub struct Signer {
    key: Vec<u8>,
}

impl TryFrom<&str> for Signer {
    type Error = Error;

    fn try_from(secret: &str) -> Result<Self, Self::Error> {
        Ok(Self {
            key: BASE64_STANDARD.decode(secret)?,
        })
    }
}

impl Signer {
    pub fn get_cb_access_sign(
        &self,
        cb_access_timestamp: &str,
        request_path: &str,
        body: &str,
        method: &str,
    ) -> Result<String, Error> {
        // Create a SHA 256 HMAC with the key.
        let mut mac: Hmac<Sha256> = Hmac::new_from_slice(self.key.as_slice())?;

        mac.update(cb_access_timestamp.as_bytes());
        mac.update(method.as_bytes());
        mac.update(request_path.as_bytes());
        mac.update(body.as_bytes());

        // Get the digest.
        let digest = mac.finalize().into_bytes();

        // Return the base64-encoded digest.
        Ok(BASE64_STANDARD.encode(digest))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn can_get_cb_access_sign() {
        dotenvy::dotenv().unwrap();

        let cb_access_timestamp = "1732848298232";
        let request_path = "/orders";
        let body = "{}";
        let method = "POST";
        let signer = Signer::try_from("supersecretkeyyy").unwrap();

        assert_eq!(
            signer
                .get_cb_access_sign(cb_access_timestamp, request_path, body, method)
                .unwrap(),
            "jTCLBOBni8IYa563/iL9k1XMTynNKqXrxTuEKoD8tqo="
        );
    }
}
