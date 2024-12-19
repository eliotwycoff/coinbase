pub mod common;
pub mod rest;
pub mod websocket;

use common::{authentication::Signer, rate_limit::TokenBucket, Error};
use rest::{
    products::{Product, Products},
    Client, ClientBuilder,
};
use smartstring::{LazyCompact, SmartString};
use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    time::Duration,
};
use tokio::time::sleep;
use uuid::Uuid;
use websocket::channels::{
    level_three::{Message, Side},
    Channel, ChannelBuilder,
};

pub struct OrderBook {
    // Book data
    product: Product,
    best_ask: u64,
    best_bid: u64,
    asks: BTreeMap<u64, Orders>,
    bids: BTreeMap<u64, Orders>,
    sequence: u64,

    // Price index by order id
    order_price: HashMap<Uuid, u64>,

    // Data connections
    http_client: Client,
    websocket: Channel<Message>,
}

impl OrderBook {
    fn insert(&mut self, side: Side, price: u64, order: Order) {
        self.delete(order.id);
        self.order_price.insert(order.id, price);

        match side {
            Side::Sell => {
                self.asks
                    .entry(price)
                    .and_modify(|orders| orders.insert(order))
                    .or_insert(Orders::from(order));
                self.best_ask = (price < self.best_ask)
                    .then(|| price)
                    .unwrap_or(self.best_ask);
            }
            Side::Buy => {
                self.bids
                    .entry(price)
                    .and_modify(|orders| orders.insert(order))
                    .or_insert(Orders::from(order));
                self.best_bid = (price > self.best_bid)
                    .then(|| price)
                    .unwrap_or(self.best_bid);
            }
        }
    }

    fn delete(&mut self, order_id: Uuid) -> Option<Side> {
        if let Some(price) = self.order_price.remove(&order_id) {
            if price >= self.best_ask {
                if let Some(orders) = self.asks.get_mut(&price) {
                    orders.delete(order_id);

                    if orders.queue.is_empty() {
                        self.asks.remove(&price);
                        self.best_ask = self
                            .asks
                            .first_key_value()
                            .map(|(price, _)| *price)
                            .unwrap_or(u64::MAX);
                    }
                }

                return Some(Side::Sell);
            } else {
                if let Some(orders) = self.bids.get_mut(&price) {
                    orders.delete(order_id);

                    if orders.queue.is_empty() {
                        self.bids.remove(&price);
                        self.best_bid = self
                            .bids
                            .last_key_value()
                            .map(|(price, _)| *price)
                            .unwrap_or(0);
                    }
                }

                return Some(Side::Buy);
            }
        }

        None
    }

    fn update(&mut self, order_id: Uuid, price: u64, size: u64) {
        if let Some(side) = self.delete(order_id) {
            self.insert(side, price, Order::new(order_id, size));
        }
    }

    fn update_with(&mut self, message: Message) -> Result<(), Error> {
        match message {
            Message::Change {
                sequence,
                order_id,
                price,
                size,
                ..
            } => {
                println!("Sequence => {}/{}", self.sequence, sequence);

                if sequence == self.sequence + 1 {
                    let price = price.normalize(self.product.quote_increment.decimals)?;
                    let size = size.normalize(self.product.base_increment.decimals)?;

                    self.update(order_id, price, size);
                    self.sequence += 1;
                } else if sequence > self.sequence + 1 {
                    return Err(Error::number("out of sequence"));
                }
            }
            Message::Done {
                sequence, order_id, ..
            } => {
                println!("Sequence => {}/{}", self.sequence, sequence);

                if sequence == self.sequence + 1 {
                    self.delete(order_id);
                    self.sequence += 1;
                } else if sequence > self.sequence + 1 {
                    return Err(Error::number("out of order"));
                }
            }
            Message::Match {
                sequence,
                maker_order_id,
                price,
                size,
                ..
            } => {
                println!("Sequence => {}/{}", self.sequence, sequence);

                if sequence == self.sequence + 1 {
                    let price = price.normalize(self.product.quote_increment.decimals)?;
                    let size = size.normalize(self.product.base_increment.decimals)?;

                    if price >= self.best_ask {
                        if let Some(orders) = self.asks.get_mut(&price) {
                            orders.reduce_or_delete(maker_order_id, size);
                        }
                    } else {
                        if let Some(orders) = self.bids.get_mut(&price) {
                            orders.reduce_or_delete(maker_order_id, size);
                        }
                    }

                    self.sequence += 1;
                } else if sequence > self.sequence + 1 {
                    return Err(Error::number("out of order"));
                }
            }
            Message::Noop { sequence, .. } => {
                println!("Sequence => {}/{}", self.sequence, sequence);

                if sequence == self.sequence + 1 {
                    self.sequence += 1;
                } else if sequence > self.sequence + 1 {
                    return Err(Error::number("out of sequence"));
                }
            }
            Message::Open {
                sequence,
                order_id,
                side,
                price,
                size,
                ..
            } => {
                println!("Sequence => {}/{}", self.sequence, sequence);

                if sequence == self.sequence + 1 {
                    let price = price.normalize(self.product.quote_increment.decimals)?;
                    let size = size.normalize(self.product.base_increment.decimals)?;

                    self.insert(side, price, Order::new(order_id, size));
                    self.sequence += 1;
                } else if sequence > self.sequence + 1 {
                    return Err(Error::number("out of sequence"));
                }
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
struct Order {
    id: Uuid,
    size: u64,
}

impl Order {
    pub fn new(id: Uuid, size: u64) -> Self {
        Self { id, size }
    }
}

#[derive(Debug)]
struct Orders {
    total_size: u64,
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

impl Orders {
    pub fn reduce_or_delete(&mut self, order_id: Uuid, size: u64) {
        self.queue
            .iter_mut()
            .enumerate()
            .find_map(|(index, order)| {
                if order.id == order_id {
                    if order.size <= size {
                        Some(index) // delete this order in the `inspect` call
                    } else {
                        order.size -= size;
                        self.total_size -= size;

                        None // we modified the order, so no need to delete it
                    }
                } else {
                    None // don't delete this order
                }
            })
            .inspect(|&index| {
                self.queue
                    .remove(index)
                    .inspect(|order| self.total_size -= order.size);
            });
    }

    pub fn insert(&mut self, order: Order) {
        self.total_size += order.size;
        self.queue.push_back(order);
    }

    pub fn delete(&mut self, order_id: Uuid) {
        self.queue
            .iter()
            .enumerate()
            .find_map(|(index, order)| (order.id == order_id).then_some(index))
            .inspect(|&index| {
                self.queue
                    .remove(index)
                    .inspect(|order| self.total_size -= order.size);
            });
    }
}

#[derive(Debug, Default)]
pub struct OrderBookBuilder {
    key: Option<String>,
    signer: Option<Signer>,
    passphrase: Option<String>,
    product_id: Option<SmartString<LazyCompact>>,
    domain: Option<String>,
    port: Option<u16>,
    cache_delay: Option<Duration>,
    rest_token_bucket: Option<TokenBucket>,
    websocket_token_bucket: Option<TokenBucket>,
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

    pub fn with_endpoint(mut self, domain: impl Into<String>, port: u16) -> Self {
        self.domain = Some(domain.into());
        self.port = Some(port);

        self
    }

    pub fn with_cache_delay(mut self, cache_delay_millis: u64) -> Self {
        self.cache_delay = Some(Duration::from_millis(cache_delay_millis));

        self
    }

    pub fn with_rest_token_bucket(mut self, token_bucket: TokenBucket) -> Self {
        self.rest_token_bucket = Some(token_bucket);

        self
    }

    pub fn with_websocket_token_bucket(mut self, token_bucket: TokenBucket) -> Self {
        self.websocket_token_bucket = Some(token_bucket);

        self
    }

    pub async fn build(self) -> Result<OrderBook, Error> {
        // Ensure all required helper variables are present.
        let key = self
            .key
            .ok_or_else(|| Error::param_required("authentication key"))?;
        let signer = self
            .signer
            .ok_or_else(|| Error::param_required("authentication signer"))?;
        let passphrase = self
            .passphrase
            .ok_or_else(|| Error::param_required("authentication passphrase"))?;
        let domain = self
            .domain
            .unwrap_or_else(|| String::from("ws-direct.exchange.coinbase.com"));
        let port = self.port.unwrap_or(443);
        let product_id = self
            .product_id
            .ok_or_else(|| Error::param_required("product id"))?;
        let cache_delay = self
            .cache_delay
            .unwrap_or_else(|| Duration::from_millis(5_000));
        let rest_token_bucket = self
            .rest_token_bucket
            .unwrap_or_else(|| TokenBucket::new(15, Duration::from_millis(100)));
        let websocket_token_bucket = self
            .websocket_token_bucket
            .unwrap_or_else(|| TokenBucket::new(1_000, Duration::from_millis(100)));

        // Set up the http client.
        let http_client = ClientBuilder::new()
            .with_token_bucket(rest_token_bucket)
            .build()?;

        // Get product metadata.
        let product = http_client.get_single_product(product_id.as_str()).await?;

        // Set up the websocket channel.
        let channel = ChannelBuilder::default()
            .with_key(key)
            .with_signer(signer)
            .with_passphrase(passphrase)
            .with_endpoint(domain, port)
            .with_product_id(product_id.as_str())
            .with_token_bucket(websocket_token_bucket)
            .connect::<Message>()
            .await?;

        // Cache messages in another task.
        let caching_channel = channel.cache().await;

        // Let messages accumulate in the cache.
        sleep(cache_delay).await;

        // Download a level-three order book snapshot.
        let product_book = http_client.get_product_book(product_id).await?;

        // Set up the order id-price index.
        let mut order_price = HashMap::with_capacity(16_384);

        // Set up the asks.
        let mut asks = BTreeMap::new();

        for (price, size, id) in product_book.asks {
            let price = price.normalize(product.quote_increment.decimals)?;
            let size = size.normalize(product.base_increment.decimals)?;
            let order = Order::new(id, size);

            asks.entry(price)
                .and_modify(|orders: &mut Orders| orders.insert(order))
                .or_insert_with(|| Orders::from(order));
            order_price.insert(id, price);
        }

        // Set up the bids.
        let mut bids = BTreeMap::new();

        for (price, size, id) in product_book.bids {
            let price = price.normalize(product.quote_increment.decimals)?;
            let size = size.normalize(product.base_increment.decimals)?;
            let order = Order::new(id, size);

            bids.entry(price)
                .and_modify(|orders: &mut Orders| orders.insert(order))
                .or_insert_with(|| Orders::from(order));
            order_price.insert(id, price);
        }

        // Create the order book.
        let mut order_book = OrderBook {
            product,
            best_ask: *asks.keys().next().unwrap_or(&0),
            best_bid: *bids.keys().next().unwrap_or(&0),
            asks,
            bids,
            sequence: product_book.sequence,
            order_price,
            http_client,
            websocket: caching_channel.join().await?,
        };

        // Update the order book with the cached messages.
        for message in order_book.websocket.cached_items() {
            order_book.update_with(message)?;
        }

        Ok(order_book)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn can_get_up_to_date_order_book() -> Result<(), Box<dyn std::error::Error>> {
        dotenvy::from_filename(".env")?;

        // Load the credentials.
        let key = std::env::var("CB_ACCESS_KEY")?;
        let secret = std::env::var("CB_ACCESS_SECRET")?;
        let passphrase = std::env::var("CB_ACCESS_PASSPHRASE")?;

        // Set up the order book.
        let order_book = OrderBookBuilder::default()
            .with_authentication(key, secret, passphrase)?
            .with_product_id("BTC-USD")
            .with_cache_delay(10_000)
            .build()
            .await?;

        // println!("order_book asks => {:?}", order_book.asks);
        // println!("order_book bids => {:?}", order_book.bids);
        println!("order_book best_ask => {}", order_book.best_ask);
        println!("order_book best_bid => {}", order_book.best_bid);

        Ok(())
    }
}
