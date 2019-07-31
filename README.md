# MQTT test server

This is an MQTT 3.11 server intended primarily for testing clients.

## Target features

* Automatic open port discovery (plays well with test suites).
* Verbose log file and network dump (no need for RC-prone companion client)
* Controllable client test cases including
  - Ack delay.
  - Go offline, close connection.
  - Bad packet flow (spurious/missing ack/publish).
* Usable as rust lib or CLI.
* No runtime deps (simplifies CI image)

## Non-features

* Codec-level errors (at least for now - better to unittest at the codec library level).
* Wildcard topics (at least for now - not useful ?).
* Payload decoding (at least for now).
* Database or clustering support (offtopic).
* High performance/scalability (though might get there incidentally).

## Usage

Install [rust][http://rust-lang.org/] if necessary.

### Standalone

    # Build and install it
    cargo install
    # Run it with the default options
    mqttest
    # See help
    mqttest -h

### Library

Add ``mqttest`` to your `Cargo.toml` and then:

    // Initialize a logger.
    env_logger::init();
    // Initialize the server. You'll receive the tcp port and a `Future` to execute the server.
    let (port, server) = mqttest::start(1883..2000, Duration::from_millis(1000))?;
    // Execute the server future using your chosen runtime.
    // FIXME: The server currently doesn't have a clean shutdown method, it runs as long as it can.
    tokio::spawn(server);

    // Run your client(s).
    start_my_client(port);


