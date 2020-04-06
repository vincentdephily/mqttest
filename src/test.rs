//! Mqttest is indirectly but arguably well unittested by the client crates. This file is here as
//! much for documenting usage as for actual unittesting.

use super::{Conf, Mqttest, ConnInfo};
use mqttrs::{encode, Connect, Protocol};
use std::{future::Future,
          net::{IpAddr, SocketAddr}};
use tokio::{io::AsyncWriteExt, net::TcpStream, runtime::Builder};

/// Simplistic connect/disconnect client for illustration purpose.
async fn client(port: u16) -> Result<(), Box<dyn std::error::Error>> {
    // Connect to TCP
    let sock = SocketAddr::from((IpAddr::from([127, 0, 0, 1]), port));
    let mut stream = TcpStream::connect(sock).await?;
    // Send MQTT CONNECT
    let mut buf = Vec::new();
    let pkt = Connect { protocol: Protocol::MQTT311,
                        keep_alive: 60,
                        client_id: String::from("test"),
                        clean_session: true,
                        last_will: None,
                        username: None,
                        password: None };
    encode(&pkt.into(), &mut buf)?;
    stream.write_all(&buf).await?;
    // TODO: Receive MQTT CONNACK, subscibe, publish
    // Disconnect (drop the TcpStream)
    Ok(())
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
        client(srv.port).await.expect("client failure");
        // Wait for the server to finish
        srv.fut.await.unwrap()
    });
    // Check run results
    assert_eq!(1, conns.len());
}
