//! Mqttest is indirectly but arguably well unittested by the client crates. This file is here as
//! much for documenting usage as for actual unittesting.

use crate::{test::client::client, *};
use std::future::Future;
use tokio::{runtime::Builder, spawn};

mod client;

/// Convenience wrapper around tokio::time::timeout
async fn timeout<T>(d: u64, f: impl Future<Output = T>) -> Option<T> {
    tokio::time::timeout(Duration::from_millis(d), f).map(Result::ok).await
}

/// Boiler-plate to run and log async code from a unittest.
fn block_on<T>(f: impl Future<Output = T>) -> T {
    let _ = env_logger::builder().is_test(true).parse_filters("debug").try_init();
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
        let conf = Conf::new().max_connect(Some(1));
        let mut srv = Mqttest::start(conf).await.expect("Failed listen");

        // Start long-running client as a separate task, look for start event
        spawn(client("mqttest", srv.port, 1000));
        assert_eq!(Some(Event::ClientStart(0)), srv.events.recv().await);

        // Kill the client early
        assert_eq!(None, timeout(50, srv.events.recv()).await);
        srv.commands.send(Command::Disconnect(0)).await.expect("command failed");
        assert_eq!(Some(Some(Event::ClientStop(0))), timeout(50, srv.events.recv()).await);

        // Wait for the server to finish
        srv.report.await.unwrap()
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
        assert_eq!(Some(Some(Event::ServerStop)), timeout(50, srv.events.recv()).await);
        srv.report.await.unwrap()
    });
}
