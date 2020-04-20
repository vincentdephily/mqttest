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

/// Connect to server, publish "hello", subscribe to "mqttest", wait up to `wait` ms for a plublish
/// message, and disconnect.
pub async fn client(id: impl std::fmt::Display, port: u16, wait: u64) -> Result<(), ClientError> {
    // Connect to TCP
    let sock = SocketAddr::from((IpAddr::from([127, 0, 0, 1]), port));
    let stream = TcpStream::connect(sock).await?;
    let mut fr = Framed::new(stream, MqttCodec {});

    // Send MQTT CONNECT
    let pkt = Connect { protocol: Protocol::MQTT311,
                        keep_alive: 60,
                        client_id: format!("{}", id),
                        clean_session: true,
                        last_will: None,
                        username: None,
                        password: None };
    fr.send(pkt.into()).await?;

    // Wait for and check MQTT CONNACK
    match fr.next().await {
        Some(Ok(Packet::Connack(p))) if p.code == ConnectReturnCode::Accepted => (),
        o => Err(format!("Expected Connack got {:?}", o))?,
    }

    // Publish something
    let pkt = Publish { dup: false,
                        qospid: QosPid::AtMostOnce,
                        retain: false,
                        topic_name: String::from("mqttest"),
                        payload: "hello".into() };
    fr.send(pkt.into()).await?;

    if wait > 0 {
        // Subscribe
        let pid = Pid::new();
        let topics =
            vec![SubscribeTopic { topic_path: String::from("mqttest"), qos: QoS::AtMostOnce }];
        let pkt = Subscribe { pid, topics };
        fr.send(pkt.into()).await?;
        match fr.next().await {
            Some(Ok(Packet::Suback(p))) if p.pid == pid => (),
            o => Err(format!("Expected Suback got {:?}", o))?,
        }

        // Wait for publish, with a timeout
        match timeout(wait, fr.next()).await {
            Some(Some(Ok(Packet::Publish(_)))) => (),
            None => (),
            o => Err(format!("Expected Publish got {:?}", o))?,
        }
    }
    // Disconnect (drop the TcpStream)
    Ok(())
}
