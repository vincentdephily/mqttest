# MQTT test server

Mqttest is an [MQTT](https://mqtt.org/) server designed for unittesting clients.

Compared to a standard server like Mosquitto, Mqttest brings a CI-friendly binary, fast RC-free
tests, detailed machine-readable packet dumps, network problem simulations, selectable implementaion
quirks, running the server as a library, etc.

Initial development has been sponsored by [Munic](https://munic.io/).

## Current and planned features

- Simplified CI image
  - [x] Automatic open port discovery (simplifies parallel test execution)
  - [x] Statically-built single-file CLI binary (no runtime deps or fancy install)
  - [x] Use as a rust library
    - [x] Decode file dumps to rust structs
    - [x] Configure and start server
    - [ ] Runtime server control and client interaction using rust channels
    - [ ] Return dumps and stats after each run
  - [ ] Use as a library from other languages
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
  - [ ] Different pid-assignment strategies
  - [x] Different behavior for subsequent connections
  - [x] Stop server after a number of connections
  - [ ] Stop server after a max runtime
  - [ ] [Your useful behavior here](https://github.com/vincentdephily/mqttest/issues)
- Protocol support
  - [x] MQTT 3.1.1
  - [ ] MQTT 5
  - [ ] IPv6
  - [x] Warn about or reject extended client identifiers
  - [x] Warn about MQTT3 idioms obsoletted by MQTT5
  - [x] Documentation highlights MQTT implementation gotchas

## Non-features

* Codec-level errors (better to unittest this at the codec library level).
* Wildcard topics (at least for now - not useful ?).
* Database or clustering support (offtopic).
* High performance/scalability (though aim to be small and fast).

## Usage

Install [Rust](https://rust-lang.org/) >= 1.39.0 if necessary.

### Standalone binary

```shell
# Build and install the executable
cargo install --path .

# See help
mqttest -h
```

Mqttest starts in just a few milliseconds. You can start a server with a different behaviour for
each of your client unittest. Or you can start a single instance and leave it running while you do
some ad-hoc testing.

### Rust library

In your `Cargo.toml`:

```toml
[dev-dependencies]
# MQTT test server.
mqttest = { version = "0.2.0", default-features = false }
# mqttest needs to be started from a tokio async context.
tokio = "0.2"
# At your discretion, if you want to see server logs.
env_logger = "0.7"
```

In your unittests (see [`test.rs`](src/test.rs) for more detailed examples) :

```rust
/// Boiler-plate to run and log async code from a unittest.
fn block_on<T>(f: impl Future<Output = T>) -> T {
    let _ = env_logger::builder().is_test(true).parse_filters("debug").try_init();
    tokio::runtime::Builder::new().basic_scheduler().enable_all().build().unwrap().block_on(f)
}

/// Example unittest. Bring your own client.
#[test]
fn connect() {
    let conns: Vec<ConnInfo> = block_on(async {
        // Create a server config
        let conf = Conf::new().max_connect(1);
        // Start the server
        let srv = Mqttest::start(conf).await.expect("Failed listen").await;
        // Start your client on the port that the server selected
        client::start(srv.port).await.expect("Client failure");
        // Wait for the server to finish
        srv.fut.await.unwrap()
    });
    // Check run results
    assert_eq!(1, conns.len());
}
```

### Optional features

#### `cli`

Required to build the binary (as opposed to the library). Enabled by default.
