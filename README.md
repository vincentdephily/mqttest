# MQTT test server

Mqttest is an [MQTT](http://mqtt.org/) server designed for unittesting clients.

Compared to a standard server like Mosquitto, Mqttest brings a CI-friendly binary, fast RC-free
tests, detailed machine-readable logs, network problem simulations, selectable implementaion quirks,
running the server as a library, etc.

Initial development has been sponsored by [Munic](https://munic.io/).

## Current and planned features

- Simplified CI image
  - [x] No runtime deps
  - [x] Statically-built CLI binary
  - [x] Use as a rust library
    - [x] Decode dump to rust structs
    - [x] Configure and start server
    - [ ] Runtime server reconfiguration and shutdown
    - [ ] Interact with clients using rust channels
  - [x] Automatic open port discovery
- Verbose log file and network dump
  - [x] No need for RC-prone connection of an observer client
  - [x] Json dump of mqtt packet data and metadata (time, connection id...)
  - [x] Global or per-connection dump filename
  - [ ] Flexible payload decoding
- Controllable connection behaviors:
  - [x] Ack delay
  - [x] Close connection after a number of packets or a timeout
  - [ ] Go offline
  - [ ] Bad packet flow (spurious/missing ack/publish)
  - [x] Override session lifetime
  - [x] Reject clients by id or password
  - [ ] Different pid-assignment startegies
  - [ ] Your useful behavior here
  - [x] Different behavior for subsequent connections
- Protocol support
  - [x] MQTT 3.1.1
  - [ ] MQTT 5
  - Warn about or reject use of optional protocol features:
    - [x] Extended client identifiers
    - [x] MQTT3 idioms obsoletted by MQTT5
  - [x] Documentation highlights MQTT implementation gotchas

## Non-features

* Codec-level errors (better to unittest this at the codec library level).
* Wildcard topics (at least for now - not useful ?).
* Database or clustering support (offtopic).
* High performance/scalability (though aim to be small and fast).

## Usage

Install [Rust](http://rust-lang.org/) if necessary.

### Standalone

```shell
# Build and install it
cargo install --path .
# Run it with the default options
mqttest
# See help
mqttest -h
```

### Library

In your `Cargo.toml`:

```toml
[dev-dependencies]
mqttest = "0.1.0"
```

In your unittests:

```rust
// Initialize a logger.
env_logger::init();
// Prepare a server config (see the docs for all options).
let conf = mqttest::Conf::new().ack_delay(Duration::from_millis(100));
// Initialize the server. You'll receive the tcp port and a `Future` to execute the server.
let (port, server) = mqttest::start(conf)?;
// Execute the server future using your chosen runtime.
tokio::spawn(server);

// Run your client(s).
start_my_client(port);
```
