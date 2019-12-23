use crate::{client::ConnId, mqtt::*};
use futures::{stream::Stream,
              sync::mpsc::{unbounded, UnboundedSender}};
use log::*;
use serde::{Deserialize, Serialize};
use serde_json::{from_slice, to_string, Value};
use std::{collections::HashMap,
          fs::OpenOptions,
          io::Error,
          sync::{Arc, Mutex}};
use tokio::io::Write;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DumpMeta<'a> {
    /// Timestamp formated as a string, fixed-size, iso-8601, UTC
    pub ts: String,
    /// Connection id/counter
    pub con: ConnId,
    /// MQTT Client id
    pub id: &'a str,
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
    /// The payload as an array of bytes.
    pub raw: Vec<u8>,
    /// The payload as a string, if it is valid utf-8.
    pub utf8: Option<String>,
    /// The payload as a json value, if it is valid json.
    pub json: Option<Value>,
}
impl DumpPayload {
    fn new(bytes: Vec<u8>) -> Self {
        Self { len: bytes.len(),
               utf8: String::from_utf8(bytes.clone()).ok(),
               json: from_slice(&bytes).ok(),
               raw: bytes }
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


/// Parsed MQTT packet. We use our own struct and substructs instead of the `mqttrs` ones, so that
/// we can implement json serialisation, and add/remove some fields for readbility/unit-testing.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum DumpMqtt {
    /// The string is the client id.
    Connect(String),
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
    fn new(p: &Packet) -> Self {
        match p {
            Packet::Connect(p) => Self::Connect(p.client_id.clone()),
            Packet::Connack(p) => Self::Connack(DumpConnack { session: p.session_present,
                                                              code: format!("{:?}", p.code) }),
            Packet::Publish(p) => Self::Publish(DumpPublish { dup: p.dup,
                                                              qos: DumpQosId::from(p.qospid),
                                                              topic: p.topic_name.clone(),
                                                              pl: DumpPayload::new(p.payload
                                                                                    .clone()) }),
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
    reg: Arc<Mutex<HashMap<String, UnboundedSender<String>>>>,
    chans: Vec<UnboundedSender<String>>,
}
impl Dump {
    pub fn new() -> Self {
        Dump { reg: Arc::new(Mutex::new(HashMap::new())), chans: vec![] }
    }

    /// Register a new file to send dumps to. This spawns an async writer for each file, and makes
    /// sure that a given file is opened only once. Use `dump()` to send data to all the writers
    /// that have been registered with this `Dump`.
    pub fn register(&mut self, name: &str) -> Result<(), Error> {
        let mut reg = self.reg.lock().expect("Aquire Dump.reg");
        let s = match reg.get(name) {
            None => {
                let mut f = OpenOptions::new().append(true).create(true).open(&name)?;
                let (sx, rx) = unbounded::<String>();
                reg.insert(name.to_owned(), sx.clone());
                let name = name.to_owned();
                tokio::spawn(rx.for_each(move |s| {
                                   f.write_all(s.as_bytes())
                                    .map_err(|e| {
                                        error!("Writing to {}: {:?}", name, e);
                                    })
                               }));
                sx
            },
            Some(s) => s.clone(),
        };
        self.chans.push(s);
        Ok(())
    }

    fn now_str() -> String {
        let t = time::now_utc();
        format!("{}.{:09.09}Z", t.strftime("%FT%T").unwrap(), t.tm_nsec)
    }

    /// Serialize packet/metadata as json and asynchronously write it to the files.
    pub fn dump(&self, conid: ConnId, clientid: &str, dir: &'static str, pkt: &Packet) {
        let e = to_string(&DumpMeta { ts: Dump::now_str(),
                                      con: conid,
                                      id: clientid,
                                      from: dir,
                                      pkt: DumpMqtt::new(pkt) }).unwrap();
        for c in self.chans.iter() {
            c.unbounded_send(e.clone()).unwrap();
        }
    }
}
