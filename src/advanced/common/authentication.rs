use crate::advanced::common::Error;
use base64::{Engine, engine::general_purpose::STANDARD};
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use keyring::Entry;
use p256::{SecretKey, ecdsa::SigningKey, pkcs8::EncodePrivateKey};
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::{
    fmt::Write,
    time::{SystemTime, UNIX_EPOCH},
};

/// This JWT signer is used to create JWTs by the Coinbase Advanced `Client`.
pub struct JwtSigner {
    header_kid: String,
    header_typ: String,
    encoding_key: EncodingKey,
}

impl TryFrom<Key> for JwtSigner {
    type Error = Error;

    fn try_from(key: Key) -> Result<Self, Self::Error> {
        // Convert the private key from sec1 to pkcs#8 format.
        let pem = key
            .private_key
            .replace("-----BEGIN EC PRIVATE KEY-----", "")
            .replace("-----END EC PRIVATE KEY-----", "")
            .replace("\n", "")
            .replace("\r", "");
        let pem = STANDARD
            .decode(&pem)
            .map_err(|error| Error::invalid("ec private key").with_source(Box::new(error)))?;
        let secret_key = SecretKey::from_sec1_der(&pem)
            .map_err(|error| Error::invalid("secret key").with_source(Box::new(error)))?;
        let signing_key = SigningKey::from(secret_key);
        let pkcs8_key = signing_key
            .to_pkcs8_der()
            .map_err(|error| Error::invalid("signing key").with_source(Box::new(error)))?;

        // Convert from der to pem.
        let pem_header = "-----BEGIN PRIVATE KEY-----\n";
        let pem_footer = "-----END PRIVATE KEY-----\n";
        let base64_encoded = STANDARD.encode(pkcs8_key.as_bytes());

        // Split into 64-character lines (standard PEM format)
        let pem_body: String = base64_encoded
            .as_bytes()
            .chunks(64)
            .map(|chunk| std::str::from_utf8(chunk).unwrap())
            .collect::<Vec<_>>()
            .join("\n");
        let pem_output = format!("{}{}\n{}", pem_header, pem_body, pem_footer);

        // Create the encoding key and then create the json web token.
        let encoding_key = EncodingKey::from_ec_pem(pem_output.as_bytes())
            .map_err(|error| Error::invalid("pkcs8 key").with_source(Box::new(error)))?;

        Ok(Self {
            header_kid: key.name,
            header_typ: String::from("JWT"),
            encoding_key,
        })
    }
}

impl JwtSigner {
    pub fn get_jwt(
        &self,
        request_method: &str,
        request_host: &str,
        request_path: &str,
    ) -> Result<String, Error> {
        #[derive(Debug, Serialize)]
        struct Claims<'a> {
            sub: &'a str,
            iss: &'static str,
            nbf: u64,
            exp: u64,
            uri: String,
        }

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| {
                Error::impossible("negative system time").with_source(Box::new(error))
            })?
            .as_secs();
        let claims = Claims {
            sub: self.header_kid.as_str(),
            iss: "cdp",
            nbf: now,
            exp: now + 120,
            uri: format!("{request_method} {request_host}{request_path}"),
        };
        let mut rng = rand::rng();
        let mut nonce = String::with_capacity(32);

        for _ in 0..16 {
            write!(&mut nonce, "{:02x}", rng.random_range(0..256)).map_err(|error| {
                Error::impossible("hex write failure").with_source(Box::new(error))
            })?;
        }

        let mut header = Header::new(Algorithm::ES256);

        header.kid = Some(self.header_kid.clone());
        header.typ = Some(self.header_typ.clone());
        header.nonce = Some(nonce);

        let token = jsonwebtoken::encode(&header, &claims, &self.encoding_key)
            .map_err(|error| Error::invalid("jwt params").with_source(Box::new(error)))?;

        Ok(token)
    }
}

/// This is the SEC1-formatted API key that Coinbase Advanced provides.
/// This struct provides the ability to `save` and `load` this key to/from
/// the local keystore, but this key itself must be converted into a
/// `JwtSigner` before it can be used by the Coinbase Advanced `Client`.
#[derive(Debug, Serialize, Deserialize)]
pub struct Key {
    name: String,
    #[serde(rename = "privateKey")]
    private_key: String,
}

impl Key {
    /// Save this key to the local key store with the given `service` and key `name`.
    pub fn save(&self, service: &str, name: &str) -> Result<(), Error> {
        // Serialize `self` to bytes.
        let bytes = serde_json::to_vec(self).map_err(|error| Error::domain(Box::new(error)))?;

        // Save these bytes to the key store.
        Entry::new(service, name)
            .map_err(|error| Error::domain(Box::new(error)))?
            .set_secret(bytes.as_slice())
            .map_err(|error| Error::domain(Box::new(error)))
    }

    /// Load a key from the local key store with the given `service` and key `name`.
    pub fn load(service: &str, name: &str) -> Result<Self, Error> {
        // Load secret bytes from the key store.
        let bytes = Entry::new(service, name)
            .map_err(|error| Error::domain(Box::new(error)))?
            .get_secret()
            .map_err(|error| Error::domain(Box::new(error)))?;

        // Deserialize these bytes into `Self`.
        serde_json::from_slice(bytes.as_slice()).map_err(|error| Error::domain(Box::new(error)))
    }
}
