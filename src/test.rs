//! Mqttest is indirectly but arguably well unittested by the client crates. This file is here as
//! much for documenting usage as for actual unittesting.
// TODO: convert to example

use crate::{test::client::client, *};
use mqttrs::*;
use std::future::Future;
use tokio::{runtime::Builder, spawn};

mod client;

/// Convenience wrapper around tokio::time::timeout
async fn timeout<T>(d: u64, f: impl Future<Output = T>) -> Option<T> {
    tokio::time::timeout(Duration::from_millis(d), f).map(Result::ok).await
}

/// Boiler-plate to run and log async code from a unittest.
fn block_on<T>(f: impl Future<Output = T>) -> T {
    let _ = env_logger::builder().format_timestamp_millis()
                                 .is_test(true)
                                 .parse_filters("debug")
                                 .try_init();
    Builder::new().basic_scheduler().enable_all().build().unwrap().block_on(f)
}

#[test]
fn connect() {
    let conns: Vec<ConnInfo> = block_on(async {
        // Create a server config
        let conf = Conf::new().max_connect(Some(1));
        // Start the server
        let srv = Mqttest::start(conf).await.expect("Failed listen");
        // Start your client on the port that the server selected
        client("mqttest", srv.port, 0).await.expect("client failure");
        // Wait for the server to finish
        srv.report.await.unwrap()
    });
    // Check run results
    assert_eq!(1, conns.len());
}

#[test]
#[ignore] // FIXME fix bug and enable test
fn stop_on_drop() {
    block_on(async {
        // Start a server but drop the Mqttest struct
        let srv = Mqttest::start(Conf::new()).await.expect("Failed listen");
        let port = srv.port;
        drop(srv);

        // Connecting client should fail
        client("mqttest", port, 0).await.expect_err("client failure");
    });
}

#[test]
fn cmd_disconnect() {
    block_on(async {
        // Start the server
        let conf =
            Conf::new().max_connect(Some(1)).event_on(EventKind::Discon).event_on(EventKind::Done);
        let mut srv = Mqttest::start(conf).await.expect("Failed listen");

        // Start long-running client as a separate task, look for start event
        spawn(client("mqttest", srv.port, 1000));

        // Kill the client early, check for quick death
        assert_eq!(None, timeout(50, srv.events.recv()).await);
        srv.commands.send(Command::Disconnect(0)).await.expect("command failed");
        assert_eq!(timeout(50, srv.events.recv()).await, Some(Some(Event::discon(0))));

        // Wait for the server to finish
        assert_eq!(srv.events.recv().await, Some(Event::done()));
    });
}

#[test]
fn cmd_stopserver() {
    block_on(async {
        // Start a never-ending server
        let conf = Conf::new();
        let mut srv = Mqttest::start(conf).await.expect("Failed listen");

        // And kill the server manually
        srv.commands.send(Command::Stop).await.expect("command failed");
        assert_eq!(timeout(50, srv.events.recv()).await, Some(Some(Event::done())));
    });
}

#[test]
fn cmd_sendping() {
    block_on(async {
        // Start the server
        let conf = Conf::new().max_connect(Some(1))
                              .event_on(EventKind::Recv)
                              .event_on(EventKind::Send)
                              .event_on(EventKind::Done);
        let mut srv = Mqttest::start(conf).await.expect("Failed listen");

        // Start client and wait for handshake
        spawn(client("mqttest", srv.port, 200));
        assert!(matches!(srv.events.recv().await, Some(Event::Recv(_, 0, Packet::Connect(_)))));
        assert!(matches!(srv.events.recv().await, Some(Event::Send(_, 0, Packet::Connack(_)))));
        assert!(matches!(srv.events.recv().await, Some(Event::Recv(_, 0, Packet::Publish(_)))));
        assert!(matches!(srv.events.recv().await, Some(Event::Recv(_, 0, Packet::Subscribe(_)))));
        assert!(matches!(srv.events.recv().await, Some(Event::Send(_, 0, Packet::Suback(_)))));

        // Send ping and wait for pong
        srv.commands.send(Command::SendPacket(0, Packet::Pingreq)).await.expect("command failed");
        assert_eq!(srv.events.recv().await, Some(Event::send(0, Packet::Pingreq)));
        assert_eq!(srv.events.recv().await, Some(Event::recv(0, Packet::Pingresp)));

        // Wait for the server to finish
        assert_eq!(srv.events.recv().await, Some(Event::done()));
    });
}
