pub mod authentication;
pub mod types;

#[derive(Debug)]
pub enum Error {
    Base64(base64::DecodeError),
    Hmac(hmac::digest::InvalidLength),
    Io(std::io::Error),
    Http(hyper::http::Error),
    WebSocket(fastwebsockets::WebSocketError),
    SystemTime(std::time::SystemTimeError),
    Json(serde_json::Error),
    DnsName(rustls_pki_types::InvalidDnsNameError),
}

impl From<base64::DecodeError> for Error {
    fn from(error: base64::DecodeError) -> Self {
        Self::Base64(error)
    }
}

impl From<hmac::digest::InvalidLength> for Error {
    fn from(error: hmac::digest::InvalidLength) -> Self {
        Self::Hmac(error)
    }
}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<hyper::http::Error> for Error {
    fn from(error: hyper::http::Error) -> Self {
        Self::Http(error)
    }
}

impl From<fastwebsockets::WebSocketError> for Error {
    fn from(error: fastwebsockets::WebSocketError) -> Self {
        Self::WebSocket(error)
    }
}

impl From<std::time::SystemTimeError> for Error {
    fn from(error: std::time::SystemTimeError) -> Self {
        Self::SystemTime(error)
    }
}

impl From<serde_json::Error> for Error {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error)
    }
}

impl From<rustls_pki_types::InvalidDnsNameError> for Error {
    fn from(error: rustls_pki_types::InvalidDnsNameError) -> Self {
        Self::DnsName(error)
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Base64(error) => write!(f, "Base64 error => {error}"),
            Self::Hmac(error) => write!(f, "Hmac invalid key length => {error}"),
            Self::Io(error) => write!(f, "Io error => {error}"),
            Self::Http(error) => write!(f, "Http error => {error}"),
            Self::WebSocket(error) => write!(f, "WebSocket error => {error}"),
            Self::SystemTime(error) => write!(f, "System time error => {error}"),
            Self::Json(error) => write!(f, "Json error => {error}"),
            Self::DnsName(error) => write!(f, "Dns name error => {error}"),
        }
    }
}
