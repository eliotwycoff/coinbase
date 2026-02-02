pub mod authentication;
pub mod rate_limit;
pub mod types;

#[derive(Debug)]
pub enum Error {
    Api {
        endpoint: &'static str,
        message: String,
    },
    Impossible,
    ChannelClosed,
    PriceDoesNotExist {
        side: crate::exchange::websocket::channels::level_three::Side,
    },
    OrderAlreadyExists,
    OrderDoesNotExist,
    OutOfSequence,
    InsufficientCacheDelay,
    Unavailable(&'static str),
    Math {
        description: &'static str,
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },
    Dependency {
        description: &'static str,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

impl Error {
    pub fn api(endpoint: &'static str, message: impl Into<String>) -> Self {
        Self::Api {
            endpoint,
            message: message.into(),
        }
    }

    pub fn unavailable(name: &'static str) -> Self {
        Self::Unavailable(name)
    }

    pub fn math(
        description: &'static str,
        error: Option<Box<dyn std::error::Error + Send + Sync>>,
    ) -> Self {
        Self::Math {
            description,
            source: error,
        }
    }

    pub fn dependency(
        description: &'static str,
        error: Box<dyn std::error::Error + Send + Sync>,
    ) -> Self {
        Self::Dependency {
            description,
            source: error,
        }
    }
}

impl From<base64::DecodeError> for Error {
    fn from(error: base64::DecodeError) -> Self {
        Self::dependency("Base64 error", Box::new(error))
    }
}

impl From<hmac::digest::InvalidLength> for Error {
    fn from(error: hmac::digest::InvalidLength) -> Self {
        Self::dependency("Hmac invalid key length", Box::new(error))
    }
}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Self::dependency("Io error", Box::new(error))
    }
}

impl From<hyper::http::Error> for Error {
    fn from(error: hyper::http::Error) -> Self {
        Self::dependency("Http error", Box::new(error))
    }
}

impl From<fastwebsockets::WebSocketError> for Error {
    fn from(error: fastwebsockets::WebSocketError) -> Self {
        Self::dependency("Websocket error", Box::new(error))
    }
}

impl From<reqwest::Error> for Error {
    fn from(error: reqwest::Error) -> Self {
        Self::dependency("Reqwest error", Box::new(error))
    }
}

impl From<std::time::SystemTimeError> for Error {
    fn from(error: std::time::SystemTimeError) -> Self {
        Self::dependency("System time error", Box::new(error))
    }
}

impl From<serde_json::Error> for Error {
    fn from(error: serde_json::Error) -> Self {
        Self::dependency("Json error", Box::new(error))
    }
}

impl From<rustls_pki_types::InvalidDnsNameError> for Error {
    fn from(error: rustls_pki_types::InvalidDnsNameError) -> Self {
        Self::dependency("Dns error", Box::new(error))
    }
}

impl From<tokio::task::JoinError> for Error {
    fn from(error: tokio::task::JoinError) -> Self {
        Self::dependency("Tokio error", Box::new(error))
    }
}

impl From<tokio::sync::AcquireError> for Error {
    fn from(error: tokio::sync::AcquireError) -> Self {
        Self::dependency("Semaphore error", Box::new(error))
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Api { endpoint, message } => write!(f, "Api error @ {endpoint} => {message}"),
            Self::Impossible => write!(f, "Impossible"),
            Self::ChannelClosed => write!(f, "Channel closed"),
            Self::PriceDoesNotExist { side } => write!(f, "{side} price does not exist"),
            Self::OrderAlreadyExists => write!(f, "Order already exists"),
            Self::OrderDoesNotExist => write!(f, "Order does not exist"),
            Self::OutOfSequence => write!(f, "Out of sequence"),
            Self::InsufficientCacheDelay => write!(f, "Insufficient cache delay"),
            Self::Unavailable(name) => write!(f, "Unavailable => {name}"),
            Self::Math {
                description,
                source,
            } => write!(f, "Arithmetic overflow: {description} => {source:?}"),
            Self::Dependency {
                description,
                source,
            } => write!(f, "{description} => {source}"),
        }
    }
}

impl std::error::Error for Error {}
