# Coinbase

This repo contains utilities for connecting to various Coinbase Exchange API endpoints, including its Level 3 WebSocket feed of order book data.

>At present, only Coinbase's institutional API endpoints are supported, so you will need institutional API keys for this demo to work.

## Quickstart

First, generate API keys and populate them in a `.env`. See the `.env.example`.

Then you can test the Level 3 WebSocket feed with 

```shell
cargo test --lib can_receive_messages -- --nocapture
```

Under the hood, this test

1. Creates a secure WebSocket connection with Coinbase.
2. Subscribes to BTC-USD level-three order book data.
3. Moves this WebSocket connection into a separate task (green thread) where messages are temporarily cached.
4. Concurrently fetches the latest full BTC-USD order book snapshot from Coinbase and notes its "sequence number."
5. Retrieves cached messages and ignores those with a sequence number less than that of the order book snapshot.
6. Applies changes to the order book for the remaining cached messages.
7. Continues to listen for new messages and apply changes accordingly.

And it will stream messages like the following to your terminal.

```shell
[CHANGE] product_id: BTC-USD, sequence: 101403138259, order_id: 8aa3a8f9-eadc-4b27-a1d3-185216ff8e24, price: 80828.44, size: 0.29621196, time: 2025-03-12 15:22:01.282268 +00:00:00
[NOOP] product_id: BTC-USD, sequence: 101403138260, time: 2025-03-12 15:22:01.282319 +00:00:00
[OPEN] product_id: BTC-USD, sequence: 101403138261, order_id: 53870396-d5e5-4a31-9939-e84af7534a02, side: Sell, price: 80878.96, size: 0.00912119, time: 2025-03-12 15:22:01.282319 +00:00:00
[DONE] product_id: BTC-USD, sequence: 101403138262, order_id: 9af4d1af-9e27-4826-8fbb-feebc2116916, time: 2025-03-12 15:22:01.28243 +00:00:00
[NOOP] product_id: BTC-USD, sequence: 101403138263, time: 2025-03-12 15:22:01.282463 +00:00:00
[OPEN] product_id: BTC-USD, sequence: 101403138264, order_id: 6eca45ce-a374-4f8a-8d67-223a6fa11ce6, side: Buy, price: 80808.18, size: 0.10047, time: 2025-03-12 15:22:01.282463 +00:00:00
[NOOP] product_id: BTC-USD, sequence: 101403138265, time: 2025-03-12 15:22:01.282803 +00:00:00
[OPEN] product_id: BTC-USD, sequence: 101403138266, order_id: bfa9db69-3ac4-4b56-925a-fb88212e99fa, side: Buy, price: 80807.24, size: 0.02, time: 2025-03-12 15:22:01.282803 +00:00:00
[NOOP] product_id: BTC-USD, sequence: 101403138267, time: 2025-03-12 15:22:01.283121 +00:00:00
[OPEN] product_id: BTC-USD, sequence: 101403138268, order_id: 9eeb5200-7746-4509-a4b2-f8193dda2d89, side: Buy, price: 80799.14, size: 0.1113, time: 2025-03-12 15:22:01.283121 +00:00:00
[NOOP] product_id: BTC-USD, sequence: 101403138269, time: 2025-03-12 15:22:01.283156 +00:00:00
[MATCH] product_id: BTC-USD, sequence: 101403138270, maker_order_id: 2763e5ff-3ca2-4a50-9924-dab92ff62433, taker_order_id: 4e233d49-3a0b-4066-a906-f690671ed2bf, price: 80808.19, size: 0.00796756, time: 2025-03-12 15:22:01.283156 +00:00:00
[DONE] product_id: BTC-USD, sequence: 101403138271, order_id: 4e233d49-3a0b-4066-a906-f690671ed2bf, time: 2025-03-12 15:22:01.283156 +00:00:00
```