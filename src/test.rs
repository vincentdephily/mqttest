//! Mqttest is indirectly but arguably well unittested by the client crates. This file is here as
//! much for documenting usage as for actual unittesting.
// TODO: convert to example

use crate::{test::client::{client, spawned, Step},
            *};
use assert_matches::*;
use futures::FutureExt;
use mqttrs::*;
use std::future::Future;
use tokio::runtime::Builder;

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
    Builder::new_current_thread().enable_all().build().unwrap().block_on(f)
}

/// Expect server Event
macro_rules! expect_event {
    ($s: ident, $e: pat) => {
        assert_matches!($s.events.recv().await, Some($e));
    };
}

#[test]
fn connect() {
    let stats = block_on(async {
        // Create a server config
        let conf = Conf::new().max_connect(Some(1));
        // Start the server
        let srv = Mqttest::start(conf).await.expect("Failed listen");
        // Start your client on the port that the server selected
        client("mqttest", srv.port, vec![]).await.expect("client failure");
        // Wait for the server to finish
        srv.finish().await
    });
    // Check run results
    assert_eq!(1, stats.conn_count);
}

#[test]
fn stop_on_drop() {
    block_on(async {
        // Start a server but drop the Mqttest struct
        let srv = Mqttest::start(Conf::new()).await.expect("Failed listen");
        let port = srv.port;
        drop(srv);

        // Connecting client should fail
        client("mqttest", port, vec![]).await.expect_err("client failure");
    });
}

#[test]
fn cmd_disconnect() {
    block_on(async {
        // Start the server
        let conf = Conf::new().max_connect(1).event_on(EventKind::Discon);
        let mut srv = Mqttest::start(conf).await.expect("Failed listen");

        // Start long-running client as a separate task, look for start event
        let cli = spawned("mqttest", srv.port, vec![Step::Recv(1000)]);

        // Kill the client early, check for quick death
        assert_eq!(None, timeout(50, srv.events.recv()).await);
        srv.commands.send(Command::Disconnect(0)).expect("command failed");
        assert_eq!(timeout(50, srv.events.recv()).await, Some(Some(Event::discon(0))));

        // Wait for the server to finish
        srv.finish().await;
        cli.await.expect("cli join").expect("cli result");
    });
}

#[test]
fn cmd_stopserver() {
    block_on(async {
        // Start a never-ending server
        let conf = Conf::new();
        let srv = Mqttest::start(conf).await.expect("Failed listen");

        // Kill the server manually and check that it exited quickly
        srv.commands.send(Command::Stop).expect("command failed");
        assert_matches!(timeout(50, srv.finish()).await, Some(_));
    });
}

#[test]
fn cmd_send_ping() {
    block_on(async {
        // Start the server
        let conf = Conf::new().max_connect(1).event_on(EventKind::Recv).event_on(EventKind::Send);
        let mut srv = Mqttest::start(conf).await.expect("Failed listen");

        // Start client and wait for handshake
        let cli = spawned("mqttest", srv.port, vec![Step::Recv(200)]);
        assert_matches!(srv.events.recv().await, Some(Event::Recv(_, 0, Packet::Connect(_))));
        assert_matches!(srv.events.recv().await, Some(Event::Send(_, 0, Packet::Connack(_))));

        // Send ping and wait for pong
        srv.commands.send(Command::SendPacket(0, Packet::Pingreq)).expect("command failed");
        assert_eq!(srv.events.recv().await, Some(Event::send(0, Packet::Pingreq)));
        assert_eq!(srv.events.recv().await, Some(Event::recv(0, Packet::Pingresp)));
        srv.commands.send(Command::SendPacket(0, Packet::Pingreq)).expect("command failed");
        assert_eq!(srv.events.recv().await, Some(Event::send(0, Packet::Pingreq)));
        assert_eq!(srv.events.recv().await, Some(Event::recv(0, Packet::Pingresp)));

        // Wait for the server to finish
        srv.finish().await;
        cli.await.expect("cli join").expect("cli result");
    });
}

fn pubsub(pubqos: QoS, subqos: QoS) {
    block_on(async move {
        // Start the server
        let conf = Conf::new().max_connect(2).event_on(EventKind::Recv).event_on(EventKind::Send);
        let mut srv = Mqttest::start(conf).await.expect("Failed listen");

        // Start subscriber client
        let cli0 = spawned("sub", srv.port, vec![Step::Sub(subqos), Step::Recv(50)]);
        expect_event!(srv, Event::Recv(_, 0, Packet::Connect(_)));
        expect_event!(srv, Event::Send(_, 0, Packet::Connack(_)));
        expect_event!(srv, Event::Recv(_, 0, Packet::Subscribe(_)));
        expect_event!(srv, Event::Send(_, 0, Packet::Suback(_)));

        // Start publisher client
        let cli1 = spawned("pub", srv.port, vec![Step::Pub(pubqos), Step::Recv(50)]);
        expect_event!(srv, Event::Recv(_, 1, Packet::Connect(_)));
        expect_event!(srv, Event::Send(_, 1, Packet::Connack(_)));

        // Receive/send publish and (maybe) puback(s)
        expect_event!(srv, Event::Recv(_, 1, Packet::Publish(_)));
        expect_event!(srv, Event::Send(_, 0, Packet::Publish(_)));
        match pubqos {
            QoS::AtMostOnce => (),
            QoS::AtLeastOnce => {
                expect_event!(srv, Event::Send(_, 1, Packet::Puback(_)));
            },
            QoS::ExactlyOnce => todo!(),
        }
        match subqos {
            QoS::AtMostOnce => (),
            QoS::AtLeastOnce => {
                expect_event!(srv, Event::Recv(_, 0, Packet::Puback(_)));
            },
            QoS::ExactlyOnce => todo!(),
        }

        // Wait for the server to finish
        assert!(srv.finish().await.events.is_empty());
        cli0.await.expect("cli0 join").expect("cli0 result");
        cli1.await.expect("cli1 join").expect("cli1 result");
    });
}

#[test]
fn pub0sub0() {
    pubsub(QoS::AtMostOnce, QoS::AtMostOnce)
}

#[test]
fn pub1sub0() {
    pubsub(QoS::AtLeastOnce, QoS::AtMostOnce)
}

#[test]
fn pub0sub1() {
    pubsub(QoS::AtMostOnce, QoS::AtLeastOnce)
}

#[test]
fn events_full() {
    block_on(async {
        // Start a server with a small result buffer
        let conf = Conf::new().max_connect(1).event_on(EventKind::Send).result_buffer(10);
        let mut srv = Mqttest::start(conf).await.expect("Failed listen");

        // Fill the event channel
        let cli = spawned("mqttest", srv.port, vec![Step::Sub(QoS::AtMostOnce), Step::Recv(150)]);
        assert_matches!(srv.events.recv().await, Some(Event::Send(_, 0, Packet::Connack(_))));
        assert_matches!(srv.events.recv().await, Some(Event::Send(_, 0, Packet::Suback(_))));
        for _ in 0..15 {
            srv.commands.send(Command::SendPacket(0, Packet::Pingreq)).expect("command failed");
        }

        // Expect exactly buffer_size Event::Recv
        for i in 0..=9 {
            assert_matches!(srv.events.recv().await, Some(Event::Send(_, _, _)), "ping {i}");
        }
        assert_matches!(srv.finish().await, Finish{events:e, ..} if e.is_empty());
        cli.await.expect("cli join").expect("cli result");
    })
}
