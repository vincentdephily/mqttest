//! Simplistic MQTT client.
//!
//! Just enough functionality to write some mqttest-based tests.

use super::timeout;
use crate::*;
use bytes::BytesMut;
use mqttrs::*;
use std::net::{IpAddr, SocketAddr};
use tokio::net::TcpStream;
use tokio_util::codec::{Decoder, Encoder, Framed};
use futures::{StreamExt, SinkExt};


#[derive(Debug)]
pub enum ClientError {
    Io(std::io::Error),
    Codec(mqttrs::Error),
    Local(String),
}
impl From<std::io::Error> for ClientError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}
impl From<mqttrs::Error> for ClientError {
    fn from(e: mqttrs::Error) -> Self {
        Self::Codec(e)
    }
}
impl From<String> for ClientError {
    fn from(e: String) -> Self {
        Self::Local(e)
    }
}

/// Boiler-plate impl for `tokio_util::codec::Framed`.
struct MqttCodec;
impl Decoder for MqttCodec {
    type Item = Packet;
    type Error = mqttrs::Error;
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        mqttrs::decode(src)
    }
}
impl Encoder<Packet> for MqttCodec {
    type Error = mqttrs::Error;
    fn encode(&mut self, item: Packet, dst: &mut BytesMut) -> Result<(), Self::Error> {
        mqttrs::encode(&item, dst)
    }
}

/// Connect to server, publish "hello", subscribe to "mqttest", handle incoming packets, and stop
/// after `wait` ms of inactivity.
pub async fn client(id: impl std::fmt::Display, port: u16, wait: u64) -> Result<(), ClientError> {
    // Connect to TCP
    debug!("TCP connect");
    let sock = SocketAddr::from((IpAddr::from([127, 0, 0, 1]), port));
    let stream = TcpStream::connect(sock).await?;
    let mut frame = Framed::new(stream, MqttCodec {});

    // Send MQTT CONNECT
    debug!("MQTT handshake");
    let pkt = Connect { protocol: Protocol::MQTT311,
                        keep_alive: 60,
                        client_id: format!("{}", id),
                        clean_session: true,
                        last_will: None,
                        username: None,
                        password: None };
    frame.send(pkt.into()).await?;

    // Wait for and check MQTT CONNACK
    match frame.next().await {
        Some(Ok(Packet::Connack(p))) if p.code == ConnectReturnCode::Accepted => (),
        o => Err(format!("Expected Connack got {:?}", o))?,
    }

    // Publish something
    debug!("MQTT publish");
    let pkt = Publish { dup: false,
                        qospid: QosPid::AtMostOnce,
                        retain: false,
                        topic_name: String::from("mqttest"),
                        payload: "hello".into() };
    frame.send(pkt.into()).await?;

    if wait > 0 {
        // Subscribe
        debug!("MQTT subscribe");
        let pid = Pid::new();
        let topics =
            vec![SubscribeTopic { topic_path: String::from("mqttest"), qos: QoS::AtMostOnce }];
        let pkt = Subscribe { pid, topics };
        frame.send(pkt.into()).await?;
        match frame.next().await {
            Some(Ok(Packet::Suback(p))) if p.pid == pid => (),
            o => Err(format!("Expected Suback got {:?}", o))?,
        }

        // Wait for publish/ping, with a timeout
        while let Some(p) = timeout(wait, frame.next()).await {
            debug!("Treating {:?}", p);
            match p {
                Some(Ok(Packet::Pingreq)) => frame.send(Packet::Pingresp).await?,
                Some(Ok(Packet::Publish(_))) => (),
                None => break,
                o => Err(format!("Unexpected {:?}", o))?,
            }
        }
    }

    // Disconnect (drop the TcpStream)
    debug!("Done");
    Ok(())
}
