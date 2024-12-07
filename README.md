# Coinbase

This repo contains utilities for connecting to various Coinbase Exchange API endpoints, including its Level 3 WebSocket feed of order book data.

## Quickstart

First, generate API keys and populate them in a `.env`. See the `.env.example`.

Then you can test the Level 3 WebSocket feed with 

```shell
cargo test --lib can_receive_messages -- --nocapture
```