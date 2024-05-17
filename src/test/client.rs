//! Simplistic MQTT client.
//!
//! Just enough functionality to write some mqttest-based tests.

use super::timeout;
use crate::*;
use bytes::BytesMut;
use futures::{SinkExt, StreamExt};
use mqttrs::*;
use std::net::{IpAddr, SocketAddr};
use tokio::{net::TcpStream, spawn, time::sleep};
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
impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        match self {
            Self::Io(e) => write!(f, "{e}"),
            Self::Codec(e) => write!(f, "{e}"),
            Self::Local(e) => write!(f, "{e}"),
        }
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

/// Something the client should do between connection and disconnection
#[derive(Clone, Copy)]
pub enum Step {
    #[allow(dead_code)]
    /// Do nothing for N milliseconds
    Sleep(u64),
    /// Publish "hello" on topic "mqttest"
    Pub(QoS),
    /// Subscribe to topic "mqttest"
    Sub(QoS),
    /// Handle pings and publishes for N milliseconds
    Recv(u64),
}

/// Spawn client and display error
pub fn spawned(id: &'static str,
               port: u16,
               steps: Vec<Step>)
               -> JoinHandle<Result<(), ClientError>> {
    spawn(async move {
        client(id, port, steps).await.map_err(|e| {
                                         error!("id: {e}");
                                         e
                                     })
    })
}

/// Connect to server, go through specified [Step]s, and disconnect
///
/// [Step]: enum.Step.html
pub async fn client(id: &'static str, port: u16, steps: Vec<Step>) -> Result<(), ClientError> {
    // Connect to TCP
    debug!("{id} Connect");
    let sock = SocketAddr::from((IpAddr::from([127, 0, 0, 1]), port));
    let stream = TcpStream::connect(sock).await?;
    let mut frame = Framed::new(stream, MqttCodec {});
    let mut pid = Pid::default();

    // Send MQTT CONNECT
    debug!("{id} Handshake");
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

    for step in steps.iter() {
        match step {
            Step::Sleep(ms) => {
                sleep(Duration::from_millis(*ms)).await;
            },
            Step::Pub(qos) => {
                // Publish something
                let qospid = match qos {
                    QoS::AtMostOnce => QosPid::AtMostOnce,
                    QoS::AtLeastOnce => {
                        pid = pid + 1;
                        QosPid::AtLeastOnce(pid)
                    },
                    QoS::ExactlyOnce => {
                        pid = pid + 1;
                        QosPid::ExactlyOnce(pid)
                    },
                };
                debug!("{id} Publish {qospid:?}");
                let pkt = Publish { dup: false,
                                    qospid: qospid,
                                    retain: false,
                                    topic_name: String::from("mqttest"),
                                    payload: "hello".into() };
                frame.send(pkt.into()).await?;
            },
            Step::Sub(qos) => {
                // Subscribe
                debug!("{id} Subscribe {qos:?} {pid:?}");
                let topics =
                    vec![SubscribeTopic { topic_path: String::from("mqttest"), qos: *qos }];
                let pkt = Subscribe { pid, topics };
                frame.send(pkt.into()).await?;
                match frame.next().await {
                    Some(Ok(Packet::Suback(p))) if p.pid == pid => debug!("{id} Got suback"),
                    o => Err(format!("Expected Suback got {:?}", o))?,
                }
                pid = pid + 1;
            },
            Step::Recv(wait) => {
                debug!("{id} Start receiving");
                // Wait for publish/ping, with a timeout
                while let Some(p) = timeout(*wait, frame.next()).await {
                    debug!("{id} Received {:?}", p);
                    match p {
                        Some(Ok(Packet::Pingreq)) => frame.send(Packet::Pingresp).await?,
                        Some(Ok(Packet::Publish(Publish { qospid, .. }))) => match qospid {
                            QosPid::AtLeastOnce(pid) | QosPid::ExactlyOnce(pid) => {
                                frame.send(Packet::Puback(pid)).await?;
                            },
                            QosPid::AtMostOnce => (),
                        },
                        Some(Ok(Packet::Puback(_))) => (),
                        None => break,
                        o => {
                            error!("Unexpected {:?}", o);
                            Err(format!("Unexpected {:?}", o))?
                        },
                    }
                }
            },
        }
    }

    // Disconnect (drop the TcpStream)
    debug!("{id} Done");
    Ok(())
}
