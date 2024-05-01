use crate::{mqtt::{Packet, QoS, QosPid, SubscribeReturnCodes},
            ConnId};
use log::*;
use serde::{Deserialize, Serialize};
use serde_json::{from_slice, to_string, Value};
use std::{collections::HashMap,
          fs::OpenOptions,
          io::{Error, Write},
          process::{Command, Stdio},
          sync::{Arc, Mutex}};
use time::OffsetDateTime;
use tokio::sync::mpsc::{channel, Sender};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DumpMeta<'a> {
    /// Packet timestamp
    pub ts: OffsetDateTime,
    /// Connection id/counter
    pub con: ConnId,
    /// Packet origin: from (C)lient or from (S)erver.
    pub from: &'a str,
    /// Parsed MQTT packet
    pub pkt: DumpMqtt,
}

pub type DumpPid = u16;

/// Parsed QoS.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum DumpQos {
    AtMostOnce,
    AtLeastOnce,
    ExactlyOnce,
}
impl From<QoS> for DumpQos {
    fn from(q: QoS) -> Self {
        match q {
            QoS::AtMostOnce => Self::AtMostOnce,
            QoS::AtLeastOnce => Self::AtLeastOnce,
            QoS::ExactlyOnce => Self::ExactlyOnce,
        }
    }
}

/// Parsed QoS+PacketIdentifier.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum DumpQosId {
    AtMostOnce,
    AtLeastOnce(DumpPid),
    ExactlyOnce(DumpPid),
}
impl DumpQosId {
    fn from(qp: QosPid) -> Self {
        match qp {
            QosPid::AtMostOnce => Self::AtMostOnce,
            QosPid::AtLeastOnce(i) => Self::AtLeastOnce(i.get()),
            QosPid::ExactlyOnce(i) => Self::ExactlyOnce(i.get()),
        }
    }
}

/// Parsed MQTT connect packet.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DumpConnect {
    /// MQTT version identifier
    pub proto: String,
    /// The client-declared or server-assigned client id
    pub id: String,
    /// "clean" or "restore" for MQTT3
    pub session: String,
}

/// Parsed MQTT connack packet.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DumpConnack {
    pub session: bool,
    pub code: String, //FIXME proper type
}

/// Parsed MQTT publish packet.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DumpPublish {
    pub dup: bool,
    pub qos: DumpQosId,
    pub topic: String,
    pub pl: DumpPayload,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DumpPayload {
    /// Length (in bytes) of the publish payload.
    pub len: usize,
    /// The original payload as an array of bytes.
    pub raw: Vec<u8>,
    /// The decoded payload as a string, if it is valid utf-8.
    pub utf8: Option<String>,
    /// The decoded payload as a json value, if it is valid json.
    pub json: Option<Value>,
    /// Error message from the external decoder, if decoding failed.
    pub err: Option<String>,
}
impl DumpPayload {
    fn new(raw: Vec<u8>, decoder: &Option<String>) -> Self {
        let len = raw.len();
        let dec = match decoder {
            None => Ok(raw.clone()),
            Some(d) => spawn_cmd(&raw, d),
        };
        match dec {
            Ok(d) => {
                let utf8 = String::from_utf8(d.clone()).ok();
                let json = from_slice(&d).ok();
                Self { len, raw, utf8, json, err: None }
            },
            Err(e) => Self { len, raw, utf8: None, json: None, err: Some(e) },
        }
    }
}

/// Run an external command, writing to its stdin and reading from its stdout. Returns an error if
/// the exit status isn't sucessful, stderr isn't empty, or some other error occurs.
// FIXME: Timeout execution.
fn spawn_cmd(raw: &[u8], cmd: &String) -> Result<Vec<u8>, String> {
    let mut child = Command::new(cmd).stdin(Stdio::piped())
                                     .stdout(Stdio::piped())
                                     .stderr(Stdio::piped())
                                     .spawn()
                                     .map_err(|e| format!("Couldn't start {}: {:?}", cmd, e))?;
    child.stdin
         .take()
         .unwrap()
         .write_all(raw)
         .map_err(|e| format!("Couldn't write to {}'s stdin: {:?}", cmd, e))?;

    match child.wait_with_output() {
        Ok(out) if out.status.success() && out.stderr.is_empty() => Ok(out.stdout),
        Ok(out) if !out.stderr.is_empty() => Err(String::from_utf8_lossy(&out.stderr).into_owned()),
        e => Err(format!("unexpected return from {}: {:?}", cmd, e)),
    }
}

/// Parsed MQTT subscribe packet.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DumpSubscribe {
    pub pid: DumpPid,
    pub topics: Vec<DumpSubscribeTopic>,
}

/// Parsed MQTT unsubscribe packet.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DumpUnsubscribe {
    pub pid: DumpPid,
    pub topics: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DumpSubscribeTopic {
    pub topic: String,
    pub qos: DumpQos,
}

/// Parsed MQTT suback packet.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DumpSuback {
    pub pid: DumpPid,
    pub codes: Vec<DumpSubackcode>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum DumpSubackcode {
    AtMostOnce,
    AtLeastOnce,
    ExactlyOnce,
    Failure,
}


/// Parsed MQTT packet.
///
/// We use our own struct and substructs instead of the `mqttrs` ones, so that we can implement json
/// serialisation, and add/remove some fields for readbility/unit-testing.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum DumpMqtt {
    /// The string is the client id.
    Connect(DumpConnect),
    Connack(DumpConnack),
    Publish(DumpPublish),
    Puback(DumpPid),
    Pubrec(DumpPid),
    Pubrel(DumpPid),
    Pubcomp(DumpPid),
    Subscribe(DumpSubscribe),
    Suback(DumpSuback),
    Unsubscribe(DumpUnsubscribe),
    Unsuback(DumpPid),
    Pingreq,
    Pingresp,
    Disconnect,
}
impl DumpMqtt {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Connect(_) => "con",
            Self::Connack(_) => "conack",
            Self::Publish(_) => "pub",
            Self::Puback(_) => "puback",
            Self::Pubrec(_) => "pubrec",
            Self::Pubrel(_) => "pubrel",
            Self::Pubcomp(_) => "pubcomp",
            Self::Subscribe(_) => "sub",
            Self::Suback(_) => "suback",
            Self::Unsubscribe(_) => "unsub",
            Self::Unsuback(_) => "unsuback",
            Self::Pingreq => "pingreq",
            Self::Pingresp => "pingresp",
            Self::Disconnect => "disco",
        }
    }
    fn new(p: &Packet, decode_cmd: &Option<String>) -> Self {
        match p {
            Packet::Connect(p) => {
                Self::Connect(DumpConnect { proto: format!("{:?}", p.protocol),
                                            id: p.client_id.to_string(),
                                            session: String::from(if p.clean_session {
                                                                      "clean"
                                                                  } else {
                                                                      "restore"
                                                                  }) })
            },
            Packet::Connack(p) => Self::Connack(DumpConnack { session: p.session_present,
                                                              code: format!("{:?}", p.code) }),
            Packet::Publish(p) => {
                Self::Publish(DumpPublish { dup: p.dup,
                                            qos: DumpQosId::from(p.qospid),
                                            topic: p.topic_name.clone(),
                                            pl: DumpPayload::new(p.payload.clone(), decode_cmd) })
            },
            Packet::Puback(p) => Self::Puback(p.get()),
            Packet::Pubrec(p) => Self::Pubrec(p.get()),
            Packet::Pubrel(p) => Self::Pubrel(p.get()),
            Packet::Pubcomp(p) => Self::Pubcomp(p.get()),
            Packet::Subscribe(p) => {
                let topics =
                    p.topics
                     .iter()
                     .map(|s| DumpSubscribeTopic { topic: s.topic_path.clone(), qos: s.qos.into() })
                     .collect();
                Self::Subscribe(DumpSubscribe { pid: p.pid.get(), topics })
            },
            Packet::Suback(p) => {
                let codes = p.return_codes
                             .iter()
                             .map(|c| match c {
                                 SubscribeReturnCodes::Success(QoS::AtMostOnce) => {
                                     DumpSubackcode::AtMostOnce
                                 },
                                 SubscribeReturnCodes::Success(QoS::AtLeastOnce) => {
                                     DumpSubackcode::AtLeastOnce
                                 },
                                 SubscribeReturnCodes::Success(QoS::ExactlyOnce) => {
                                     DumpSubackcode::ExactlyOnce
                                 },
                                 SubscribeReturnCodes::Failure => DumpSubackcode::Failure,
                             })
                             .collect();
                Self::Suback(DumpSuback { pid: p.pid.get(), codes })
            },
            Packet::Unsubscribe(p) => {
                Self::Unsubscribe(DumpUnsubscribe { pid: p.pid.get(), topics: p.topics.clone() })
            },
            Packet::Unsuback(p) => Self::Unsuback(p.get()),
            Packet::Pingreq => Self::Pingreq,
            Packet::Pingresp => Self::Pingresp,
            Packet::Disconnect => Self::Disconnect,
        }
    }
}

/// The `Dump` struct manages a global and a local list of dump targets (files).
///
/// The expected usage is to call `Dump::new()` only once and, and to `clone()` the struct
/// afterwards, so that `Dump.reg` refers to the same list program-wide but `Dump.chans` is distinct
/// for every client.
// TODO: support de-registering files.
#[derive(Clone)]
pub(crate) struct Dump {
    reg: Arc<Mutex<HashMap<String, Sender<String>>>>,
    chans: Vec<Sender<String>>,
    decode_cmd: Option<String>,
    prefix: String,
}
impl Dump {
    pub fn new(decode_cmd: &Option<String>, prefix: &str) -> Self {
        Dump { reg: Arc::new(Mutex::new(HashMap::new())),
               chans: vec![],
               decode_cmd: decode_cmd.clone(),
               prefix: prefix.to_owned() }
    }

    /// Register a new file to send dumps to. This spawns an async writer for each file, and makes
    /// sure that a given file is opened only once. Use `dump()` to send data to all the writers
    /// that have been registered with this `Dump`.
    pub fn register(&mut self, name: &str) -> Result<(), Error> {
        let name = format!("{}{}", self.prefix, name);
        let mut reg = self.reg.lock().expect("Aquire Dump.reg");
        let s = match reg.get(&name) {
            None => {
                debug!("Opening dump file {}", name);
                let mut f = OpenOptions::new().append(true).create(true).open(&name)?;
                let (sx, mut rx) = channel::<String>(10);
                reg.insert(name.clone(), sx.clone());
                tokio::spawn(async move {
                    while let Some(s) = rx.recv().await {
                        if let Err(e) = f.write_all(s.as_bytes()) {
                            error!("Writing to {}: {:?}", name, e);
                        }
                    }
                });
                sx
            },
            Some(s) => s.clone(),
        };
        self.chans.push(s);
        Ok(())
    }

    /// Serialize packet/metadata as json and asynchronously write it to the files.
    pub async fn dump<'s>(&'s self, con: ConnId, from: &str, pkt: &Packet) {
        // Build DumpMqtt struct
        let ts = OffsetDateTime::now_utc();
        let pkt = DumpMqtt::new(pkt, &self.decode_cmd);
        let e = to_string(&DumpMeta { ts, con, from, pkt }).expect("Failed to serialize DumpMeta");

        // Send it to all writers
        for c in self.chans.iter() {
            c.clone().send(e.clone()).await.expect("Cannot send to chan");
        }
    }
}
