pub mod advanced;
pub mod exchange;

use exchange::common::{Error, authentication::Signer, rate_limit::TokenBucket};
use exchange::rest::{
    Client, ClientBuilder,
    products::{Product, Products},
};
use exchange::websocket::channels::{
    Channel, ChannelBuilder,
    level_three::{Message as LevelThreeMessage, Side},
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use smartstring::{LazyCompact, SmartString};
use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    sync::Arc,
    time::Duration,
};
use time::OffsetDateTime;
use tokio::time::sleep;
use tokio_rustls::rustls::ClientConfig;
use tracing::{debug, trace, warn};
use uuid::Uuid;

use crate::exchange::common::rate_limit::BackOffBucket;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
struct Order {
    id: Uuid,
    size: Decimal,
}

impl From<CompactOrder> for Order {
    fn from(order: CompactOrder) -> Self {
        Self {
            id: order.0.0,
            size: order.0.1.into(),
        }
    }
}

impl Order {
    pub fn new(id: Uuid, size: Decimal) -> Self {
        Self { id, size }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct Orders {
    total_size: Decimal,
    queue: VecDeque<Order>,
}

impl From<Order> for Orders {
    fn from(order: Order) -> Self {
        let total_size = order.size;
        let mut queue = VecDeque::new();

        queue.push_back(order);

        Self { total_size, queue }
    }
}

impl From<CompactOrders> for Orders {
    fn from(orders: CompactOrders) -> Self {
        Self {
            total_size: orders.total_size(),
            queue: orders
                .0
                .into_iter()
                .map(|order| Order::from(order))
                .collect(),
        }
    }
}

impl Orders {
    pub fn reduce_or_delete(&mut self, order_id: Uuid, size: Decimal) -> Result<bool, Error> {
        let index = self
            .queue
            .iter()
            .enumerate()
            .find_map(|(index, order)| (order.id == order_id).then_some(index))
            .ok_or_else(|| Error::OrderDoesNotExist)?;
        let order = self.queue.get_mut(index).ok_or_else(|| Error::Impossible)?;
        let order_size = order
            .size
            .checked_sub(size)
            .ok_or_else(|| Error::math("size exceeds order size", None))?;
        let total_size = self
            .total_size
            .checked_sub(size)
            .ok_or_else(|| Error::math("size exceeds total size", None))?;

        order.size = order_size;
        self.total_size = total_size;

        if order.size.is_zero() {
            self.queue.remove(index).ok_or_else(|| Error::Impossible)?;

            Ok(true) // order was deleted
        } else {
            Ok(false) // order was not deleted
        }
    }

    pub fn insert(&mut self, order: Order) -> Result<(), Error> {
        self.total_size = self
            .total_size
            .checked_add(order.size)
            .ok_or_else(|| Error::math("size overflows total size", None))?;
        self.queue.push_back(order);

        Ok(())
    }

    pub fn delete(&mut self, order_id: Uuid) -> Result<(), Error> {
        let index = self
            .queue
            .iter()
            .enumerate()
            .find_map(|(index, order)| (order.id == order_id).then_some(index))
            .ok_or_else(|| Error::OrderDoesNotExist)?;
        let order = self.queue.remove(index).ok_or_else(|| Error::Impossible)?;
        let total_size = self
            .total_size
            .checked_sub(order.size)
            .ok_or_else(|| Error::math("order size exceeds total size", None))?;

        self.total_size = total_size;

        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct OrderBook {
    // Product data
    pub product: Product,

    // Book data
    best_ask: Decimal,
    best_bid: Decimal,
    asks: BTreeMap<Decimal, Orders>,
    bids: BTreeMap<Decimal, Orders>,
    sequence: u64,
    updated_at: OffsetDateTime,
    index: HashMap<Uuid, (Side, Decimal)>, // price index by order id
}

impl OrderBook {
    fn insert(&mut self, side: Side, price: Decimal, order: Order) -> Result<(), Error> {
        if self.index.insert(order.id, (side, price)).is_some() {
            return Err(Error::OrderAlreadyExists);
        };

        match side {
            Side::Buy => {
                if let Some(orders) = self.bids.get_mut(&price) {
                    orders.insert(order)?;
                } else {
                    self.bids.insert(price, Orders::from(order));
                }

                self.best_bid = (price > self.best_bid)
                    .then(|| price)
                    .unwrap_or(self.best_bid);
            }
            Side::Sell => {
                if let Some(orders) = self.asks.get_mut(&price) {
                    orders.insert(order)?;
                } else {
                    self.asks.insert(price, Orders::from(order));
                }

                self.best_ask = (price < self.best_ask)
                    .then(|| price)
                    .unwrap_or(self.best_ask);
            }
        }

        Ok(())
    }

    fn delete(&mut self, order_id: Uuid) -> Result<Side, Error> {
        let (side, price) = self
            .index
            .remove(&order_id)
            .ok_or_else(|| Error::OrderDoesNotExist)?;

        match side {
            Side::Buy => {
                let orders = self
                    .bids
                    .get_mut(&price)
                    .ok_or_else(|| Error::PriceDoesNotExist { side })?;

                orders.delete(order_id)?;

                if orders.queue.is_empty() {
                    self.bids.remove(&price);
                    self.best_bid = self
                        .bids
                        .last_key_value()
                        .map(|(price, _)| *price)
                        .unwrap_or(Decimal::ZERO);
                }
            }
            Side::Sell => {
                let orders = self
                    .asks
                    .get_mut(&price)
                    .ok_or_else(|| Error::PriceDoesNotExist { side })?;

                orders.delete(order_id)?;

                if orders.queue.is_empty() {
                    self.asks.remove(&price);
                    self.best_ask = self
                        .asks
                        .first_key_value()
                        .map(|(price, _)| *price)
                        .unwrap_or(Decimal::MAX);
                }
            }
        }

        Ok(side)
    }

    pub fn update_with(
        &mut self,
        level_three_message: &LevelThreeMessage,
    ) -> Result<Message, Error> {
        match level_three_message {
            LevelThreeMessage::Open {
                sequence,
                order_id,
                side,
                price,
                size,
                time,
                ..
            } => {
                trace!(seq_b = %self.sequence, seq_m = %sequence, %order_id, %side, "Open");
                if *sequence != self.sequence + 1 {
                    return Err(Error::OutOfSequence);
                }

                self.sequence += 1;
                self.updated_at = *time;
                self.insert(*side, *price, Order::new(*order_id, *size))?;

                let message = Message::Open {
                    sequence: *sequence,
                    time: *time,
                    order_id: *order_id,
                    side: *side,
                    price: *price,
                    size: *size,
                };

                Ok(message)
            }
            LevelThreeMessage::Change {
                sequence,
                order_id,
                price,
                size,
                time,
                ..
            } => {
                trace!(seq_b = %self.sequence, seq_m = %sequence, %order_id, "Change");
                if *sequence != self.sequence + 1 {
                    return Err(Error::OutOfSequence);
                }

                self.sequence += 1;
                self.updated_at = *time;

                let message = Message::Change {
                    sequence: *sequence,
                    time: *time,
                    order_id: *order_id,
                    price: *price,
                    size: *size,
                };
                let (halfbook, old_side, old_price, old_size, old_index) =
                    match self.index.get(&order_id) {
                        Some((old_side, old_price)) => {
                            let halfbook = match old_side {
                                Side::Buy => &mut self.bids,
                                Side::Sell => &mut self.asks,
                            };
                            let (old_index, old_size) = halfbook
                                .get(old_price)
                                .ok_or_else(|| Error::PriceDoesNotExist { side: *old_side })?
                                .queue
                                .iter()
                                .enumerate()
                                .find(|(_, order)| order.id == *order_id)
                                .map(|(index, order)| (index, order.size))
                                .ok_or_else(|| Error::OrderDoesNotExist)?;

                            (halfbook, *old_side, *old_price, old_size, old_index)
                        }
                        None => return Ok(message),
                    };

                if old_price != *price || old_size < *size {
                    // The price changed or the size increased, so delete the old order.
                    let _ = self
                        .index
                        .remove(&order_id)
                        .ok_or_else(|| Error::Impossible)?;
                    let orders = halfbook
                        .get_mut(&old_price)
                        .ok_or_else(|| Error::Impossible)?;
                    let _ = orders
                        .queue
                        .remove(old_index)
                        .ok_or_else(|| Error::Impossible)?;
                    let total_size = orders
                        .total_size
                        .checked_sub(old_size)
                        .ok_or_else(|| Error::math("order size exceeds total size", None))?;

                    orders.total_size = total_size;

                    if orders.queue.is_empty() {
                        match old_side {
                            Side::Buy => {
                                self.bids.remove(&old_price);
                                self.best_bid = self
                                    .bids
                                    .last_key_value()
                                    .map(|(price, _)| *price)
                                    .unwrap_or(Decimal::ZERO);
                            }
                            Side::Sell => {
                                self.asks.remove(&old_price);
                                self.best_ask = self
                                    .asks
                                    .first_key_value()
                                    .map(|(price, _)| *price)
                                    .unwrap_or(Decimal::MAX);
                            }
                        }
                    }

                    // And replace it with (insert) the new order.
                    self.insert(old_side, *price, Order::new(*order_id, *size))?;
                } else {
                    // Only the size decreased, so modify the order in place.
                    let orders = halfbook
                        .get_mut(&old_price)
                        .ok_or_else(|| Error::Impossible)?;
                    let order = orders
                        .queue
                        .get_mut(old_index)
                        .ok_or_else(|| Error::Impossible)?;
                    let total_size = orders
                        .total_size
                        .checked_sub(old_size)
                        .ok_or_else(|| Error::math("old size exceeds total size", None))?
                        .checked_add(*size)
                        .ok_or_else(|| Error::math("new size causes overflow", None))?;

                    order.size = *size;
                    orders.total_size = total_size;
                }

                Ok(message)
            }
            LevelThreeMessage::Match {
                sequence,
                maker_order_id,
                taker_order_id,
                price,
                size,
                time,
                ..
            } => {
                trace!(seq_b = %self.sequence, seq_m = %sequence, %maker_order_id, "Match");
                if *sequence != self.sequence + 1 {
                    return Err(Error::OutOfSequence);
                }

                self.sequence += 1;
                self.updated_at = *time;

                let (side, _) = *self
                    .index
                    .get(&maker_order_id)
                    .ok_or_else(|| Error::OrderDoesNotExist)?;
                let message = Message::Match {
                    sequence: *sequence,
                    time: *time,
                    maker_order_id: *maker_order_id,
                    taker_order_id: *taker_order_id,
                    side,
                    price: *price,
                    size: *size,
                };

                match side {
                    Side::Buy => {
                        let orders = self
                            .bids
                            .get_mut(price)
                            .ok_or_else(|| Error::PriceDoesNotExist { side })?;

                        if orders.reduce_or_delete(*maker_order_id, *size)? {
                            let _ = self.index.remove(&maker_order_id);
                        }

                        if orders.queue.is_empty() {
                            self.bids.remove(price);
                            self.best_bid = self
                                .bids
                                .last_key_value()
                                .map(|(p, _)| *p)
                                .unwrap_or(Decimal::ZERO);
                        }
                    }
                    Side::Sell => {
                        let orders = self
                            .asks
                            .get_mut(price)
                            .ok_or_else(|| Error::PriceDoesNotExist { side })?;

                        if orders.reduce_or_delete(*maker_order_id, *size)? {
                            let _ = self.index.remove(&maker_order_id);
                        }

                        if orders.queue.is_empty() {
                            self.asks.remove(price);
                            self.best_ask = self
                                .asks
                                .first_key_value()
                                .map(|(p, _)| *p)
                                .unwrap_or(Decimal::MAX);
                        }
                    }
                }

                Ok(message)
            }
            LevelThreeMessage::Noop { sequence, time, .. } => {
                trace!(seq_b = %self.sequence, seq_m = %sequence, "Noop");
                if *sequence != self.sequence + 1 {
                    return Err(Error::OutOfSequence);
                }

                self.sequence += 1;
                self.updated_at = *time;

                let message = Message::Noop {
                    sequence: *sequence,
                    time: *time,
                };

                Ok(message)
            }
            LevelThreeMessage::Done {
                sequence,
                order_id,
                time,
                ..
            } => {
                trace!(seq_b = %self.sequence, seq_m = %sequence, %order_id, "Done");
                if *sequence != self.sequence + 1 {
                    return Err(Error::OutOfSequence);
                }

                self.sequence += 1;
                self.updated_at = *time;

                let message = Message::Done {
                    sequence: *sequence,
                    time: *time,
                    order_id: *order_id,
                };

                match self.delete(*order_id) {
                    Ok(_) => Ok(message),
                    Err(Error::OrderDoesNotExist) => Ok(message),
                    Err(error) => Err(error),
                }
            }
        }
    }
}

impl TryFrom<CompactOrderBook> for OrderBook {
    type Error = Error;

    fn try_from(compact_book: CompactOrderBook) -> Result<Self, Self::Error> {
        let mut order_book = Self {
            product: compact_book.product,
            best_ask: Decimal::MAX,
            best_bid: Decimal::ZERO,
            asks: BTreeMap::new(),
            bids: BTreeMap::new(),
            sequence: compact_book.sequence,
            updated_at: compact_book.updated_at,
            index: HashMap::new(),
        };

        for (price, orders) in compact_book.asks {
            for order in orders.0 {
                order_book.insert(Side::Sell, price.into(), Order::from(order))?;
            }
        }

        for (price, orders) in compact_book.bids {
            for order in orders.0 {
                order_book.insert(Side::Buy, price.into(), Order::from(order))?;
            }
        }

        Ok(order_book)
    }
}

pub struct ConnectedOrderBook {
    pub order_book: OrderBook,
    websocket: Channel<LevelThreeMessage>,
}

impl ConnectedOrderBook {
    pub fn sequence(&self) -> u64 {
        self.order_book.sequence
    }

    pub fn updated_at(&self) -> OffsetDateTime {
        self.order_book.updated_at
    }

    pub async fn next_message(&mut self) -> Result<Message, Error> {
        let message = self.websocket.next().await?;

        self.order_book.update_with(&message)
    }

    pub async fn shutdown(&mut self) -> Result<(), Error> {
        self.websocket.close().await
    }

    pub fn to_compact(&self) -> CompactOrderBook {
        CompactOrderBook {
            product: self.order_book.product.clone(),
            asks: self
                .order_book
                .asks
                .clone()
                .into_iter()
                .map(|(price, orders)| (price.into(), CompactOrders::from(orders)))
                .collect(),
            bids: self
                .order_book
                .bids
                .clone()
                .into_iter()
                .map(|(price, orders)| (price.into(), CompactOrders::from(orders)))
                .collect(),
            sequence: self.order_book.sequence,
            updated_at: self.order_book.updated_at,
        }
    }
}

#[derive(Default)]
pub struct OrderBookBuilder {
    key: Option<String>,
    signer: Option<Signer>,
    passphrase: Option<String>,
    product_id: Option<SmartString<LazyCompact>>,
    product: Option<Product>,
    domain: Option<String>,
    port: Option<u16>,
    cache_delay: Option<Duration>,
    rest_client: Option<Client>,
    rest_token_bucket: Option<TokenBucket>,
    book_backoff_bucket: Option<BackOffBucket>,
    websocket_token_bucket: Option<TokenBucket>,
    tls_config: Option<Arc<ClientConfig>>,
}

impl OrderBookBuilder {
    pub fn with_authentication(
        mut self,
        key: String,
        secret: String,
        passphrase: String,
    ) -> Result<Self, Error> {
        self.key = Some(key);
        self.signer = Some(Signer::try_from(secret.as_str())?);
        self.passphrase = Some(passphrase);

        Ok(self)
    }

    pub fn with_key(mut self, key: impl Into<String>) -> Self {
        self.key = Some(key.into());

        self
    }

    pub fn with_signer(mut self, signer: Signer) -> Self {
        self.signer = Some(signer);

        self
    }

    pub fn with_passphrase(mut self, passphrase: String) -> Self {
        self.passphrase = Some(passphrase);

        self
    }

    pub fn with_product_id(mut self, product_id: impl Into<SmartString<LazyCompact>>) -> Self {
        self.product_id = Some(product_id.into());

        self
    }

    pub fn with_product(mut self, product: Product) -> Self {
        self.product = Some(product);

        self
    }

    pub fn with_endpoint(mut self, domain: impl Into<String>, port: u16) -> Self {
        self.domain = Some(domain.into());
        self.port = Some(port);

        self
    }

    pub fn with_cache_delay(mut self, cache_delay_millis: u64) -> Self {
        self.cache_delay = Some(Duration::from_millis(cache_delay_millis));

        self
    }

    pub fn with_rest_client(mut self, rest_client: Client) -> Self {
        self.rest_client = Some(rest_client);

        self
    }

    pub fn with_rest_token_bucket(mut self, token_bucket: TokenBucket) -> Self {
        self.rest_token_bucket = Some(token_bucket);

        self
    }

    pub fn with_book_backoff_bucket(mut self, backoff_bucket: BackOffBucket) -> Self {
        self.book_backoff_bucket = Some(backoff_bucket);

        self
    }

    pub fn with_websocket_token_bucket(mut self, token_bucket: TokenBucket) -> Self {
        self.websocket_token_bucket = Some(token_bucket);

        self
    }

    pub fn with_tls_config(mut self, tls_config: Option<Arc<ClientConfig>>) -> Self {
        self.tls_config = tls_config;

        self
    }

    pub async fn build(self) -> Result<ConnectedOrderBook, Error> {
        debug!("Ensuring all required helper variables are present");
        let key = self
            .key
            .ok_or_else(|| Error::unavailable("authentication key"))?;
        let signer = self
            .signer
            .ok_or_else(|| Error::unavailable("authentication signer"))?;
        let passphrase = self
            .passphrase
            .ok_or_else(|| Error::unavailable("authentication passphrase"))?;
        let domain = self
            .domain
            .unwrap_or_else(|| String::from("ws-direct.exchange.coinbase.com"));
        let port = self.port.unwrap_or(443);
        let product_id = self
            .product_id
            .ok_or_else(|| Error::unavailable("product id"))?;
        let cache_delay = self
            .cache_delay
            .unwrap_or_else(|| Duration::from_millis(5_000));
        let book_backoff_bucket = self.book_backoff_bucket.unwrap_or_else(|| {
            warn!("Using local backoff token bucket -- this is only acceptible in tests!");

            BackOffBucket::new(Duration::from_secs(10), Duration::from_secs(3_600))
        });
        let websocket_token_bucket = self
            .websocket_token_bucket
            .unwrap_or_else(|| TokenBucket::new(1_000, Duration::from_millis(100)));

        debug!("Setting up http client");
        let http_client = match self.rest_client {
            Some(rest_client) => rest_client,
            None => ClientBuilder::new()
                .with_token_bucket(
                    self.rest_token_bucket
                        .unwrap_or_else(|| TokenBucket::new(15, Duration::from_millis(100))),
                )
                .build()?,
        };

        let product = match self.product {
            Some(product) => product,
            None => {
                debug!("Fetching product metadata");
                http_client.get_single_product(product_id.as_str()).await?
            }
        };

        debug!("Establishing websocket channel");
        let channel = ChannelBuilder::default()
            .with_key(key)
            .with_signer(signer)
            .with_passphrase(passphrase)
            .with_endpoint(domain, port)
            .with_product_id(product_id.as_str())
            .with_token_bucket(websocket_token_bucket)
            .with_tls_config(self.tls_config)
            .connect::<LevelThreeMessage>()
            .await?;

        debug!("Caching messages in separate task");
        let caching_channel = channel.cache().await;

        sleep(cache_delay).await;

        debug!("Fetching level-three order book snapshot");
        let book_token = book_backoff_bucket.get_token().await?;
        let product_book = http_client.get_product_book(product_id).await;

        // Return the token before handling a potential error.
        book_backoff_bucket.return_token(book_token).await;

        // Handle the potential error.
        let product_book = product_book?;

        // Setting up order id-price index.
        let mut index = HashMap::with_capacity(16_384);

        // Set up the bids.
        let mut bids = BTreeMap::<Decimal, Orders>::new();

        for (price, size, id) in product_book.bids {
            let order = Order::new(id, size);

            if let Some(orders) = bids.get_mut(&price) {
                orders.insert(order)?;
            } else {
                bids.insert(price, Orders::from(order));
            }

            index.insert(id, (Side::Buy, price));
        }

        // Set up the asks.
        let mut asks = BTreeMap::<Decimal, Orders>::new();

        for (price, size, id) in product_book.asks {
            let order = Order::new(id, size);

            if let Some(orders) = asks.get_mut(&price) {
                orders.insert(order)?;
            } else {
                asks.insert(price, Orders::from(order));
            }

            index.insert(id, (Side::Sell, price));
        }

        debug!("Creating order book");
        let mut order_book = ConnectedOrderBook {
            order_book: OrderBook {
                product,
                best_ask: *asks.keys().next().unwrap_or(&Decimal::ZERO),
                best_bid: *bids.keys().rev().next().unwrap_or(&Decimal::ZERO),
                asks,
                bids,
                sequence: product_book.sequence,
                updated_at: product_book.time,
                index,
            },
            websocket: caching_channel.join().await?,
        };

        debug!("Extracting last cached message");
        let last_cached = order_book
            .websocket
            .last_cached()
            .ok_or_else(|| Error::InsufficientCacheDelay)?;

        // Make sure the last cached message is dated after the order book snapshot.
        if last_cached.sequence() < order_book.order_book.sequence {
            return Err(Error::InsufficientCacheDelay);
        }

        debug!(?last_cached, "Updating order book with cached messages");
        for message in order_book.websocket.cached_items() {
            match order_book.order_book.update_with(&message) {
                Ok(_) => {}
                Err(Error::OutOfSequence) => {}
                Err(error) => return Err(error),
            }
        }

        Ok(order_book)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
struct CompactDecimal(#[serde(with = "rust_decimal::serde::float")] Decimal);

impl From<Decimal> for CompactDecimal {
    fn from(decimal: Decimal) -> Self {
        Self(decimal)
    }
}

impl From<CompactDecimal> for Decimal {
    fn from(decimal: CompactDecimal) -> Self {
        decimal.0
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CompactOrder((Uuid, CompactDecimal));

impl From<Order> for CompactOrder {
    fn from(order: Order) -> Self {
        Self((order.id, order.size.into()))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CompactOrders(VecDeque<CompactOrder>);

impl CompactOrders {
    pub fn total_size(&self) -> Decimal {
        self.0.iter().fold(Decimal::ZERO, |total, order| {
            total.saturating_add(order.0.1.0) // can technically overflow
        })
    }
}

impl From<Orders> for CompactOrders {
    fn from(orders: Orders) -> Self {
        Self(
            orders
                .queue
                .into_iter()
                .map(|order| CompactOrder::from(order))
                .collect(),
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactOrderBook {
    product: Product,
    asks: Vec<(CompactDecimal, CompactOrders)>,
    bids: Vec<(CompactDecimal, CompactOrders)>,
    sequence: u64,
    updated_at: OffsetDateTime,
}

#[derive(Debug, Clone, Copy)]
pub enum Message {
    Open {
        sequence: u64,
        time: OffsetDateTime,
        order_id: Uuid,
        side: Side,
        price: Decimal,
        size: Decimal,
    },
    Change {
        sequence: u64,
        time: OffsetDateTime,
        order_id: Uuid,
        price: Decimal,
        size: Decimal,
    },
    Match {
        sequence: u64,
        time: OffsetDateTime,
        maker_order_id: Uuid,
        taker_order_id: Uuid,
        side: Side,
        price: Decimal,
        size: Decimal,
    },
    Noop {
        sequence: u64,
        time: OffsetDateTime,
    },
    Done {
        sequence: u64,
        time: OffsetDateTime,
        order_id: Uuid,
    },
}

#[derive(Debug, Clone, Copy)]
pub struct Tick {
    pub price: Decimal,
    pub size: Decimal,
    pub side: Side,
    pub time: OffsetDateTime,
    pub sequence: u64,
}

impl Tick {
    pub fn new(
        price: Decimal,
        size: Decimal,
        side: Side,
        time: OffsetDateTime,
        sequence: u64,
    ) -> Self {
        Self {
            price,
            size,
            side,
            time,
            sequence,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::exchange::rest::products::ProductBook;
    use sqlx::{Pool as SqlxPool, Postgres, types::Json};
    use tracing::{error, info};

    pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

    pub fn setup() -> Result<()> {
        dotenvy::from_filename(".env")?;
        telemetry::init_tracing("coinbase-unit-tests", None);

        Ok(())
    }

    #[tokio::test]
    async fn can_get_up_to_date_order_book() -> Result<()> {
        setup()?;

        // Load the credentials.
        let key = std::env::var("CB_ACCESS_KEY")?;
        let secret = std::env::var("CB_ACCESS_SECRET")?;
        let passphrase = std::env::var("CB_ACCESS_PASSPHRASE")?;

        // Set up the order book.
        let order_book = OrderBookBuilder::default()
            .with_authentication(key, secret, passphrase)?
            .with_product_id("KSM-USD")
            .with_cache_delay(10_000)
            .build()
            .await?;

        info!("order_book best_ask => {}", order_book.order_book.best_ask);
        info!("order_book best_bid => {}", order_book.order_book.best_bid);

        Ok(())
    }

    #[tokio::test]
    async fn can_replay_order_book() -> Result<()> {
        setup()?;

        let user = std::env::var("PRICE_ENGINE_DB_ADMIN_USER").unwrap();
        let pass = std::env::var("PRICE_ENGINE_DB_ADMIN_PASS").unwrap();
        let connection_string = format!("postgresql://{user}:{pass}@localhost:15010/engine");
        let sqlx_pool = SqlxPool::connect(connection_string.as_str()).await?;
        let mut sequence = 117314540643i64;
        let end = 117332986589i64;
        let start = sequence as f64;
        let chunk_size = 10_000;
        let product_id = "BTC-USD";
        let mut i = 0;
        let progress_chunk = (end - sequence) / (100 * chunk_size);
        let total = (end - sequence) as f64;

        debug!("Loading starting snapshot with sequence from database");
        let (Json(compact),) = sqlx::query_as::<Postgres, (Json<CompactOrderBook>,)>(
            r#"
                SELECT book
                FROM message
                WHERE product_id = 66
                AND sequence = $1;
            "#,
        )
        .bind(sequence)
        .fetch_one(&sqlx_pool)
        .await?;

        debug!("Populating full starting order book");
        let mut order_book = OrderBook::try_from(compact)?;

        debug!(
            "Starting best bid/ask => {}/{}",
            order_book.bids.last_entry().unwrap().key(),
            order_book.asks.first_entry().unwrap().key(),
        );

        debug!("Loading end state product book from file");
        let product_book = serde_json::from_slice::<ProductBook>(
            std::fs::read("/home/eliot/Documents/dev/osage-arrows/btc-usd-book-117332986589.txt")?
                .as_slice(),
        )?;

        debug!("Setting up end state order book");
        let mut index = HashMap::with_capacity(16_384);
        let mut bids = BTreeMap::<Decimal, Orders>::new();
        let mut asks = BTreeMap::<Decimal, Orders>::new();

        for (price, size, id) in product_book.bids {
            let order = Order::new(id, size);

            if let Some(orders) = bids.get_mut(&price) {
                orders.insert(order)?;
            } else {
                bids.insert(price, Orders::from(order));
            }

            index.insert(id, (Side::Buy, price));
        }

        for (price, size, id) in product_book.asks {
            let order = Order::new(id, size);

            if let Some(orders) = asks.get_mut(&price) {
                orders.insert(order)?;
            } else {
                asks.insert(price, Orders::from(order));
            }

            index.insert(id, (Side::Sell, price));
        }

        let end_order_book = OrderBook {
            product: order_book.product.clone(),
            best_ask: *asks.keys().next().unwrap_or(&Decimal::ZERO),
            best_bid: *bids.keys().rev().next().unwrap_or(&Decimal::ZERO),
            asks,
            bids,
            sequence: product_book.sequence,
            updated_at: product_book.time,
            index,
        };

        #[derive(Debug, Clone, Copy, sqlx::Type)]
        #[sqlx(type_name = "message_variant")]
        pub enum MessageVariant {
            Open,
            Change,
            Tick,
            Noop,
            Done,
        }

        #[derive(Clone, Copy, sqlx::Type)]
        #[sqlx(type_name = "side")]
        pub enum MessageSide {
            Buy,
            Sell,
        }

        impl From<MessageSide> for Side {
            fn from(side: MessageSide) -> Self {
                match side {
                    MessageSide::Buy => Self::Buy,
                    MessageSide::Sell => Self::Sell,
                }
            }
        }

        debug!("Replaying order book");
        while sequence < end {
            if i % progress_chunk == 0 {
                debug!(
                    "Progress => {:.2}%",
                    100f64 * (sequence as f64 - start) / total
                );
            }

            i += 1;

            let upper_bound = match sequence + chunk_size {
                x if x <= end => x,
                _ => end,
            };
            let messages = sqlx::query_as::<
                Postgres,
                (
                    OffsetDateTime,
                    i64,
                    MessageVariant,
                    Option<Uuid>,
                    Option<Uuid>,
                    Option<MessageSide>,
                    Option<Decimal>,
                    Option<Decimal>,
                ),
            >(
                r#"
                    SELECT processed_at, sequence, variant, order_id_one, order_id_two, side, price, size
                    FROM message
                    WHERE product_id = 66
                    AND sequence > $1
                    AND sequence <= $2
                    ORDER BY sequence ASC;
                "#,
            )
            .bind(sequence)
            .bind(upper_bound)
            .fetch_all(&sqlx_pool)
            .await?;

            for (processed_at, sequence, variant, order_id_one, order_id_two, side, price, size) in
                messages.into_iter()
            {
                let message = match variant {
                    MessageVariant::Open => LevelThreeMessage::Open {
                        product_id: product_id.into(),
                        sequence: sequence as u64,
                        time: processed_at,
                        order_id: order_id_one.unwrap(),
                        side: side.unwrap().into(),
                        price: price.unwrap(),
                        size: size.unwrap(),
                    },
                    MessageVariant::Change => LevelThreeMessage::Change {
                        product_id: product_id.into(),
                        sequence: sequence as u64,
                        time: processed_at,
                        order_id: order_id_one.unwrap(),
                        price: price.unwrap(),
                        size: size.unwrap(),
                    },
                    MessageVariant::Tick => LevelThreeMessage::Match {
                        product_id: product_id.into(),
                        sequence: sequence as u64,
                        time: processed_at,
                        maker_order_id: order_id_one.unwrap(),
                        taker_order_id: order_id_two.unwrap(),
                        price: price.unwrap(),
                        size: size.unwrap(),
                    },
                    MessageVariant::Noop => LevelThreeMessage::Noop {
                        product_id: product_id.into(),
                        sequence: sequence as u64,
                        time: processed_at,
                    },
                    MessageVariant::Done => LevelThreeMessage::Done {
                        product_id: product_id.into(),
                        sequence: sequence as u64,
                        time: processed_at,
                        order_id: order_id_one.unwrap(),
                    },
                };

                order_book.update_with(&message)?;
            }

            sequence += chunk_size;
        }

        assert!(order_book.asks == end_order_book.asks);
        assert!(order_book.bids == end_order_book.bids);
        assert!(order_book.best_ask == end_order_book.best_ask);
        assert!(order_book.best_bid == end_order_book.best_bid);
        assert!(order_book.index == end_order_book.index);
        assert!(order_book.sequence == end_order_book.sequence);
        assert!(order_book.updated_at.to_hms_micro() == end_order_book.updated_at.to_hms_micro());
        assert!(order_book.product == end_order_book.product);

        Ok(())
    }

    // ==================== Test Helpers ====================

    fn make_product() -> Product {
        use crate::exchange::rest::products::Status;

        Product {
            auction_mode: false,
            base_currency: "BTC".into(),
            base_increment: Decimal::new(1, 8),
            cancel_only: false,
            display_name: "BTC-USD".into(),
            fx_stablecoin: false,
            high_bid_limit_percentage: "".into(),
            id: "BTC-USD".into(),
            limit_only: false,
            margin_enabled: false,
            max_slippage_percentage: Decimal::ZERO,
            min_market_funds: Decimal::ONE,
            post_only: false,
            quote_currency: "USD".into(),
            quote_increment: Decimal::new(1, 2),
            status: Status::Online,
            status_message: "".into(),
            trading_disabled: false,
        }
    }

    fn make_empty_order_book(sequence: u64) -> OrderBook {
        OrderBook {
            product: make_product(),
            best_ask: Decimal::MAX,
            best_bid: Decimal::ZERO,
            asks: BTreeMap::new(),
            bids: BTreeMap::new(),
            sequence,
            updated_at: OffsetDateTime::now_utc(),
            index: HashMap::new(),
        }
    }

    fn make_order_book_with_orders(
        sequence: u64,
        bids: Vec<(Decimal, Uuid, Decimal)>, // (price, order_id, size)
        asks: Vec<(Decimal, Uuid, Decimal)>,
    ) -> OrderBook {
        let mut index = HashMap::new();
        let mut bids_map: BTreeMap<Decimal, Orders> = BTreeMap::new();
        let mut asks_map: BTreeMap<Decimal, Orders> = BTreeMap::new();

        for (price, order_id, size) in bids {
            index.insert(order_id, (Side::Buy, price));

            let order = Order::new(order_id, size);

            if let Some(orders) = bids_map.get_mut(&price) {
                orders.insert(order).unwrap();
            } else {
                bids_map.insert(price, Orders::from(order));
            }
        }

        for (price, order_id, size) in asks {
            index.insert(order_id, (Side::Sell, price));

            let order = Order::new(order_id, size);

            if let Some(orders) = asks_map.get_mut(&price) {
                orders.insert(order).unwrap();
            } else {
                asks_map.insert(price, Orders::from(order));
            }
        }

        let best_bid = bids_map
            .last_key_value()
            .map(|(p, _)| *p)
            .unwrap_or(Decimal::ZERO);
        let best_ask = asks_map
            .first_key_value()
            .map(|(p, _)| *p)
            .unwrap_or(Decimal::MAX);

        OrderBook {
            product: make_product(),
            best_ask,
            best_bid,
            asks: asks_map,
            bids: bids_map,
            sequence,
            updated_at: OffsetDateTime::now_utc(),
            index,
        }
    }

    // ==================== Open Message Tests ====================

    #[test]
    fn open_adds_bid_and_updates_best_bid() {
        let mut book = make_empty_order_book(1000);
        let order_id = Uuid::new_v4();
        let price = Decimal::new(9900, 2); // 99.00

        // Create an open message.
        let message = LevelThreeMessage::Open {
            product_id: "BTC-USD".into(),
            sequence: 1001,
            order_id,
            side: Side::Buy,
            price,
            size: Decimal::ONE,
            time: OffsetDateTime::now_utc(),
        };

        book.update_with(&message).unwrap();

        assert_eq!(book.best_bid, price);
        assert_eq!(book.sequence, 1001);
        assert!(book.bids.contains_key(&price));
        assert!(book.index.contains_key(&order_id));
    }

    #[test]
    fn open_adds_ask_and_updates_best_ask() {
        let mut book = make_empty_order_book(1000);
        let order_id = Uuid::new_v4();
        let price = Decimal::new(10100, 2); // 101.00

        // Create an open message.
        let message = LevelThreeMessage::Open {
            product_id: "BTC-USD".into(),
            sequence: 1001,
            order_id,
            side: Side::Sell,
            price,
            size: Decimal::ONE,
            time: OffsetDateTime::now_utc(),
        };

        book.update_with(&message).unwrap();

        assert_eq!(book.best_ask, price);
        assert_eq!(book.sequence, 1001);
        assert!(book.asks.contains_key(&price));
        assert!(book.index.contains_key(&order_id));
    }

    #[test]
    fn open_adds_order_to_existing_price_level() {
        let order1 = Uuid::new_v4();
        let order2 = Uuid::new_v4();
        let price = Decimal::new(9900, 2);

        // Create the order book.
        let mut book =
            make_order_book_with_orders(1000, vec![(price, order1, Decimal::ONE)], vec![]);

        // Creat an open message.
        let message = LevelThreeMessage::Open {
            product_id: "BTC-USD".into(),
            sequence: 1001,
            order_id: order2,
            side: Side::Buy,
            price,
            size: Decimal::new(2, 0),
            time: OffsetDateTime::now_utc(),
        };

        book.update_with(&message).unwrap();

        let orders = book.bids.get(&price).unwrap();

        assert_eq!(orders.queue.len(), 2);
        assert_eq!(orders.total_size, Decimal::new(3, 0)); // 1 + 2
    }

    #[test]
    fn open_does_not_update_best_bid_for_worse_price() {
        let order1 = Uuid::new_v4();
        let order2 = Uuid::new_v4();
        let better_price = Decimal::new(9900, 2); // 99.00
        let worse_price = Decimal::new(9800, 2); // 98.00

        // Create the order book.
        let mut book =
            make_order_book_with_orders(1000, vec![(better_price, order1, Decimal::ONE)], vec![]);

        // Create an open message.
        let message = LevelThreeMessage::Open {
            product_id: "BTC-USD".into(),
            sequence: 1001,
            order_id: order2,
            side: Side::Buy,
            price: worse_price,
            size: Decimal::ONE,
            time: OffsetDateTime::now_utc(),
        };

        book.update_with(&message).unwrap();

        assert_eq!(book.best_bid, better_price); // Should remain 99.00
    }

    #[test]
    fn open_out_of_sequence_returns_error() {
        let mut book = make_empty_order_book(1000);

        let message = LevelThreeMessage::Open {
            product_id: "BTC-USD".into(),
            sequence: 1002, // Should be 1001
            order_id: Uuid::new_v4(),
            side: Side::Buy,
            price: Decimal::new(9900, 2),
            size: Decimal::ONE,
            time: OffsetDateTime::now_utc(),
        };

        let result = book.update_with(&message);
        assert!(matches!(result, Err(Error::OutOfSequence)));
    }

    // ==================== Match Message Tests ====================

    #[test]
    fn match_partially_fills_order() {
        let maker_id = Uuid::new_v4();
        let taker_id = Uuid::new_v4();
        let price = Decimal::new(10000, 2);

        let mut book = make_order_book_with_orders(
            1000,
            vec![],
            vec![(price, maker_id, Decimal::new(10, 0))], // 10 units
        );

        let message = LevelThreeMessage::Match {
            product_id: "BTC-USD".into(),
            sequence: 1001,
            maker_order_id: maker_id,
            taker_order_id: taker_id,
            price,
            size: Decimal::new(3, 0), // Fill 3 of 10
            time: OffsetDateTime::now_utc(),
        };

        book.update_with(&message).unwrap();

        let orders = book.asks.get(&price).unwrap();
        assert_eq!(orders.queue.len(), 1);
        assert_eq!(orders.queue[0].size, Decimal::new(7, 0)); // 10 - 3 = 7
        assert!(book.index.contains_key(&maker_id)); // Still in index
    }

    #[test]
    fn match_fully_fills_order_and_removes_from_index() {
        let maker_id = Uuid::new_v4();
        let taker_id = Uuid::new_v4();
        let price = Decimal::new(10000, 2);

        let mut book =
            make_order_book_with_orders(1000, vec![], vec![(price, maker_id, Decimal::ONE)]);

        let message = LevelThreeMessage::Match {
            product_id: "BTC-USD".into(),
            sequence: 1001,
            maker_order_id: maker_id,
            taker_order_id: taker_id,
            price,
            size: Decimal::ONE, // Full fill
            time: OffsetDateTime::now_utc(),
        };

        book.update_with(&message).unwrap();

        assert!(!book.index.contains_key(&maker_id)); // Removed from index
    }

    #[test]
    fn match_updates_best_bid_when_price_level_empties() {
        let maker_id = Uuid::new_v4();
        let other_bid_id = Uuid::new_v4();
        let taker_id = Uuid::new_v4();
        let best_price = Decimal::new(9900, 2); // 99.00
        let worse_price = Decimal::new(9800, 2); // 98.00

        let mut book = make_order_book_with_orders(
            1000,
            vec![
                (best_price, maker_id, Decimal::ONE),
                (worse_price, other_bid_id, Decimal::ONE),
            ],
            vec![],
        );

        assert_eq!(book.best_bid, best_price);

        let message = LevelThreeMessage::Match {
            product_id: "BTC-USD".into(),
            sequence: 1001,
            maker_order_id: maker_id,
            taker_order_id: taker_id,
            price: best_price,
            size: Decimal::ONE,
            time: OffsetDateTime::now_utc(),
        };

        book.update_with(&message).unwrap();

        assert_eq!(book.best_bid, worse_price); // Should fall back to 98.00
        assert!(!book.bids.contains_key(&best_price)); // Price level removed
    }

    #[test]
    fn match_handler_updates_best_ask_when_price_level_empties() {
        let ask_order_id = Uuid::new_v4();
        let bid_order_id = Uuid::new_v4();
        let taker_order_id = Uuid::new_v4();
        let new_bid_order_id = Uuid::new_v4();

        let ask_price = Decimal::new(10000, 2); // 100.00
        let bid_price = Decimal::new(9900, 2); // 99.00
        let new_bid_price = Decimal::new(10050, 2); // 100.50

        let mut order_book = make_order_book_with_orders(
            1000,
            vec![(bid_price, bid_order_id, Decimal::ONE)],
            vec![(ask_price, ask_order_id, Decimal::ONE)],
        );

        // Verify initial state is valid
        assert!(order_book.best_bid < order_book.best_ask);

        // Match fully consumes the ask at 100.00
        let match_message = LevelThreeMessage::Match {
            product_id: "BTC-USD".into(),
            sequence: 1001,
            maker_order_id: ask_order_id,
            taker_order_id,
            price: ask_price,
            size: Decimal::ONE,
            time: OffsetDateTime::now_utc(),
        };

        order_book
            .update_with(&match_message)
            .expect("Match should succeed");

        // After fix: best_ask should be updated to MAX since no asks remain
        assert_eq!(order_book.best_ask, Decimal::MAX);
        assert!(order_book.asks.is_empty());

        // Open adds a new bid at 100.50
        let open_message = LevelThreeMessage::Open {
            product_id: "BTC-USD".into(),
            sequence: 1002,
            order_id: new_bid_order_id,
            side: Side::Buy,
            price: new_bid_price,
            size: Decimal::ONE,
            time: OffsetDateTime::now_utc(),
        };

        order_book
            .update_with(&open_message)
            .expect("Open should succeed");

        // Book should NOT be crossed
        assert!(order_book.best_bid < order_book.best_ask);
        assert_eq!(order_book.best_bid, new_bid_price);
    }

    #[test]
    fn match_out_of_sequence_returns_error() {
        let maker_id = Uuid::new_v4();
        let price = Decimal::new(10000, 2);

        let mut book =
            make_order_book_with_orders(1000, vec![], vec![(price, maker_id, Decimal::ONE)]);

        let message = LevelThreeMessage::Match {
            product_id: "BTC-USD".into(),
            sequence: 999, // Out of sequence
            maker_order_id: maker_id,
            taker_order_id: Uuid::new_v4(),
            price,
            size: Decimal::ONE,
            time: OffsetDateTime::now_utc(),
        };

        let result = book.update_with(&message);
        assert!(matches!(result, Err(Error::OutOfSequence)));
    }

    // ==================== Done Message Tests ====================

    #[test]
    fn done_removes_existing_order() {
        let order_id = Uuid::new_v4();
        let price = Decimal::new(9900, 2);

        let mut book =
            make_order_book_with_orders(1000, vec![(price, order_id, Decimal::ONE)], vec![]);

        let message = LevelThreeMessage::Done {
            product_id: "BTC-USD".into(),
            sequence: 1001,
            order_id,
            time: OffsetDateTime::now_utc(),
        };

        book.update_with(&message).unwrap();

        assert!(!book.index.contains_key(&order_id));
        assert!(book.bids.is_empty());
        assert_eq!(book.best_bid, Decimal::ZERO);
    }

    #[test]
    fn done_for_nonexistent_order_succeeds() {
        let mut book = make_empty_order_book(1000);
        let nonexistent_id = Uuid::new_v4();

        let message = LevelThreeMessage::Done {
            product_id: "BTC-USD".into(),
            sequence: 1001,
            order_id: nonexistent_id,
            time: OffsetDateTime::now_utc(),
        };

        // Should succeed without error (order may have been removed by Match)
        let result = book.update_with(&message);
        assert!(result.is_ok());
        assert_eq!(book.sequence, 1001);
    }

    #[test]
    fn done_updates_best_ask_when_price_level_empties() {
        let order1 = Uuid::new_v4();
        let order2 = Uuid::new_v4();
        let best_price = Decimal::new(10000, 2); // 100.00
        let worse_price = Decimal::new(10100, 2); // 101.00

        let mut book = make_order_book_with_orders(
            1000,
            vec![],
            vec![
                (best_price, order1, Decimal::ONE),
                (worse_price, order2, Decimal::ONE),
            ],
        );

        assert_eq!(book.best_ask, best_price);

        let message = LevelThreeMessage::Done {
            product_id: "BTC-USD".into(),
            sequence: 1001,
            order_id: order1,
            time: OffsetDateTime::now_utc(),
        };

        book.update_with(&message).unwrap();

        assert_eq!(book.best_ask, worse_price); // Should move to 101.00
    }

    #[test]
    fn done_out_of_sequence_returns_error() {
        let mut book = make_empty_order_book(1000);

        let message = LevelThreeMessage::Done {
            product_id: "BTC-USD".into(),
            sequence: 1005, // Should be 1001
            order_id: Uuid::new_v4(),
            time: OffsetDateTime::now_utc(),
        };

        let result = book.update_with(&message);
        assert!(matches!(result, Err(Error::OutOfSequence)));
    }

    // ==================== Change Message Tests ====================

    #[test]
    fn change_decreases_size_in_place() {
        let order_id = Uuid::new_v4();
        let price = Decimal::new(9900, 2);
        let original_size = Decimal::new(10, 0);
        let new_size = Decimal::new(7, 0);

        let mut book =
            make_order_book_with_orders(1000, vec![(price, order_id, original_size)], vec![]);

        let message = LevelThreeMessage::Change {
            product_id: "BTC-USD".into(),
            sequence: 1001,
            order_id,
            price,
            size: new_size,
            time: OffsetDateTime::now_utc(),
        };

        book.update_with(&message).unwrap();

        let orders = book.bids.get(&price).unwrap();
        assert_eq!(orders.queue[0].size, new_size);
        assert_eq!(orders.total_size, new_size);
    }

    #[test]
    fn change_increases_size_reinserts_order() {
        let order_id = Uuid::new_v4();
        let price = Decimal::new(9900, 2);
        let original_size = Decimal::new(5, 0);
        let new_size = Decimal::new(10, 0);

        let mut book =
            make_order_book_with_orders(1000, vec![(price, order_id, original_size)], vec![]);

        let message = LevelThreeMessage::Change {
            product_id: "BTC-USD".into(),
            sequence: 1001,
            order_id,
            price,
            size: new_size,
            time: OffsetDateTime::now_utc(),
        };

        book.update_with(&message).unwrap();

        let orders = book.bids.get(&price).unwrap();
        assert_eq!(orders.queue[0].size, new_size);
        assert!(book.index.contains_key(&order_id));
    }

    #[test]
    fn change_price_reinserts_order_at_new_price() {
        let order_id = Uuid::new_v4();
        let old_price = Decimal::new(9900, 2); // 99.00
        let new_price = Decimal::new(9950, 2); // 99.50
        let size = Decimal::ONE;

        let mut book = make_order_book_with_orders(1000, vec![(old_price, order_id, size)], vec![]);

        assert_eq!(book.best_bid, old_price);

        let message = LevelThreeMessage::Change {
            product_id: "BTC-USD".into(),
            sequence: 1001,
            order_id,
            price: new_price,
            size,
            time: OffsetDateTime::now_utc(),
        };

        book.update_with(&message).unwrap();

        assert!(!book.bids.contains_key(&old_price)); // Old price level gone
        assert!(book.bids.contains_key(&new_price)); // New price level exists
        assert_eq!(book.best_bid, new_price); // Best bid updated
        assert_eq!(book.index.get(&order_id).unwrap().1, new_price); // Index updated
    }

    #[test]
    fn change_for_nonexistent_order_succeeds() {
        let mut book = make_empty_order_book(1000);

        let message = LevelThreeMessage::Change {
            product_id: "BTC-USD".into(),
            sequence: 1001,
            order_id: Uuid::new_v4(), // Doesn't exist
            price: Decimal::new(9900, 2),
            size: Decimal::ONE,
            time: OffsetDateTime::now_utc(),
        };

        // Should succeed (order may have been removed)
        let result = book.update_with(&message);
        assert!(result.is_ok());
        assert_eq!(book.sequence, 1001);
    }

    #[test]
    fn change_updates_best_bid_when_old_price_level_empties() {
        let order_id = Uuid::new_v4();
        let other_bid_id = Uuid::new_v4();
        let best_price = Decimal::new(9900, 2); // 99.00
        let worse_price = Decimal::new(9800, 2); // 98.00
        let new_price = Decimal::new(9700, 2); // 97.00

        let mut book = make_order_book_with_orders(
            1000,
            vec![
                (best_price, order_id, Decimal::ONE),
                (worse_price, other_bid_id, Decimal::ONE),
            ],
            vec![],
        );

        assert_eq!(book.best_bid, best_price);

        // Change moves order from 99.00 to 97.00
        let message = LevelThreeMessage::Change {
            product_id: "BTC-USD".into(),
            sequence: 1001,
            order_id,
            price: new_price,
            size: Decimal::ONE,
            time: OffsetDateTime::now_utc(),
        };

        book.update_with(&message).unwrap();

        // best_bid should now be 98.00 (the other bid)
        assert_eq!(book.best_bid, worse_price);
    }

    #[test]
    fn change_out_of_sequence_returns_error() {
        let order_id = Uuid::new_v4();
        let price = Decimal::new(9900, 2);

        let mut book =
            make_order_book_with_orders(1000, vec![(price, order_id, Decimal::ONE)], vec![]);

        let message = LevelThreeMessage::Change {
            product_id: "BTC-USD".into(),
            sequence: 900, // Out of sequence
            order_id,
            price,
            size: Decimal::ONE,
            time: OffsetDateTime::now_utc(),
        };

        let result = book.update_with(&message);
        assert!(matches!(result, Err(Error::OutOfSequence)));
    }

    // ==================== Noop Message Tests ====================

    #[test]
    fn noop_increments_sequence() {
        let mut book = make_empty_order_book(1000);

        let message = LevelThreeMessage::Noop {
            product_id: "BTC-USD".into(),
            sequence: 1001,
            time: OffsetDateTime::now_utc(),
        };

        book.update_with(&message).unwrap();

        assert_eq!(book.sequence, 1001);
    }

    #[test]
    fn noop_out_of_sequence_returns_error() {
        let mut book = make_empty_order_book(1000);

        let message = LevelThreeMessage::Noop {
            product_id: "BTC-USD".into(),
            sequence: 1003, // Should be 1001
            time: OffsetDateTime::now_utc(),
        };

        let result = book.update_with(&message);
        assert!(matches!(result, Err(Error::OutOfSequence)));
    }

    // ==================== TryFrom<CompactOrderBook> Tests ====================

    #[test]
    fn try_from_compact_reconstructs_order_book() {
        let bid1 = Uuid::new_v4();
        let bid2 = Uuid::new_v4();
        let ask1 = Uuid::new_v4();
        let ask2 = Uuid::new_v4();

        let compact = CompactOrderBook {
            product: make_product(),
            bids: vec![
                (
                    Decimal::new(9900, 2).into(),
                    CompactOrders(VecDeque::from([CompactOrder((
                        bid1,
                        Decimal::new(1, 0).into(),
                    ))])),
                ),
                (
                    Decimal::new(9800, 2).into(),
                    CompactOrders(VecDeque::from([CompactOrder((
                        bid2,
                        Decimal::new(2, 0).into(),
                    ))])),
                ),
            ],
            asks: vec![
                (
                    Decimal::new(10000, 2).into(),
                    CompactOrders(VecDeque::from([CompactOrder((
                        ask1,
                        Decimal::new(3, 0).into(),
                    ))])),
                ),
                (
                    Decimal::new(10100, 2).into(),
                    CompactOrders(VecDeque::from([CompactOrder((
                        ask2,
                        Decimal::new(4, 0).into(),
                    ))])),
                ),
            ],
            sequence: 5000,
            updated_at: OffsetDateTime::now_utc(),
        };

        let book = OrderBook::try_from(compact).unwrap();

        assert_eq!(book.best_bid, Decimal::new(9900, 2));
        assert_eq!(book.best_ask, Decimal::new(10000, 2));
        assert_eq!(book.sequence, 5000);
        assert_eq!(book.bids.len(), 2);
        assert_eq!(book.asks.len(), 2);
        assert!(book.index.contains_key(&bid1));
        assert!(book.index.contains_key(&bid2));
        assert!(book.index.contains_key(&ask1));
        assert!(book.index.contains_key(&ask2));
    }

    #[test]
    fn try_from_compact_handles_empty_book() {
        let compact = CompactOrderBook {
            product: make_product(),
            bids: vec![],
            asks: vec![],
            sequence: 1000,
            updated_at: OffsetDateTime::now_utc(),
        };

        let book = OrderBook::try_from(compact).unwrap();

        assert_eq!(book.best_bid, Decimal::ZERO);
        assert_eq!(book.best_ask, Decimal::MAX);
        assert!(book.bids.is_empty());
        assert!(book.asks.is_empty());
    }

    #[test]
    fn try_from_compact_handles_multiple_orders_at_same_price() {
        let order1 = Uuid::new_v4();
        let order2 = Uuid::new_v4();
        let order3 = Uuid::new_v4();
        let price = Decimal::new(9900, 2);

        let compact = CompactOrderBook {
            product: make_product(),
            bids: vec![(
                price.into(),
                CompactOrders(VecDeque::from([
                    CompactOrder((order1, Decimal::new(1, 0).into())),
                    CompactOrder((order2, Decimal::new(2, 0).into())),
                    CompactOrder((order3, Decimal::new(3, 0).into())),
                ])),
            )],
            asks: vec![],
            sequence: 1000,
            updated_at: OffsetDateTime::now_utc(),
        };

        let book = OrderBook::try_from(compact).unwrap();

        let orders = book.bids.get(&price).unwrap();
        assert_eq!(orders.queue.len(), 3);
        assert_eq!(orders.total_size, Decimal::new(6, 0)); // 1 + 2 + 3
    }

    // ==================== Edge Cases ====================

    #[test]
    fn multiple_orders_at_same_price_fifo_order_preserved() {
        let order1 = Uuid::new_v4();
        let order2 = Uuid::new_v4();
        let order3 = Uuid::new_v4();
        let price = Decimal::new(9900, 2);

        let mut book = make_empty_order_book(1000);

        // Add orders in sequence
        for (seq, order_id) in [(1001, order1), (1002, order2), (1003, order3)] {
            let message = LevelThreeMessage::Open {
                product_id: "BTC-USD".into(),
                sequence: seq,
                order_id,
                side: Side::Buy,
                price,
                size: Decimal::ONE,
                time: OffsetDateTime::now_utc(),
            };

            book.update_with(&message).unwrap();
        }

        let orders = book.bids.get(&price).unwrap();
        assert_eq!(orders.queue[0].id, order1); // First in
        assert_eq!(orders.queue[1].id, order2);
        assert_eq!(orders.queue[2].id, order3); // Last in
    }

    #[test]
    fn match_removes_correct_order_from_queue() {
        let order1 = Uuid::new_v4();
        let order2 = Uuid::new_v4();
        let taker = Uuid::new_v4();
        let price = Decimal::new(10000, 2);

        // Create the order book.
        let mut book = make_empty_order_book(1000);

        // Add two orders at same price
        for (seq, order_id) in [(1001, order1), (1002, order2)] {
            let message = LevelThreeMessage::Open {
                product_id: "BTC-USD".into(),
                sequence: seq,
                order_id,
                side: Side::Sell,
                price,
                size: Decimal::ONE,
                time: OffsetDateTime::now_utc(),
            };
            book.update_with(&message).unwrap();
        }

        // Match against first order
        let message = LevelThreeMessage::Match {
            product_id: "BTC-USD".into(),
            sequence: 1003,
            maker_order_id: order1,
            taker_order_id: taker,
            price,
            size: Decimal::ONE,
            time: OffsetDateTime::now_utc(),
        };
        book.update_with(&message).unwrap();

        let orders = book.asks.get(&price).unwrap();

        assert_eq!(orders.queue.len(), 1);
        assert_eq!(orders.queue[0].id, order2); // order1 removed, order2 remains
    }

    #[test]
    fn book_maintains_sorted_price_levels() {
        let mut book = make_empty_order_book(1000);

        // Add bids at various prices out of order
        let prices = [
            Decimal::new(9950, 2), // 99.50
            Decimal::new(9800, 2), // 98.00
            Decimal::new(9900, 2), // 99.00
            Decimal::new(9700, 2), // 97.00
        ];

        for (i, price) in prices.iter().enumerate() {
            let message = LevelThreeMessage::Open {
                product_id: "BTC-USD".into(),
                sequence: 1001 + i as u64,
                order_id: Uuid::new_v4(),
                side: Side::Buy,
                price: *price,
                size: Decimal::ONE,
                time: OffsetDateTime::now_utc(),
            };
            book.update_with(&message).unwrap();
        }

        // Verify BTreeMap maintains sorted order
        let bid_prices: Vec<_> = book.bids.keys().collect();

        assert_eq!(*bid_prices[0], Decimal::new(9700, 2)); // Lowest
        assert_eq!(*bid_prices[3], Decimal::new(9950, 2)); // Highest

        // best_bid should be highest
        assert_eq!(book.best_bid, Decimal::new(9950, 2));
    }
}
