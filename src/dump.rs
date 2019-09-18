use crate::{client::ConnId, mqtt::*};
use futures::{stream::Stream,
              sync::mpsc::{unbounded, UnboundedSender}};
use log::*;
use serde::{Deserialize, Serialize};
use serde_json::to_string;
use std::{collections::HashMap,
          fs::OpenOptions,
          io::Error,
          sync::{Arc, Mutex}};
use tokio::io::Write;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DumpMeta<'a> {
    //#[serde(serialize_with = "ser_time", deserialize_with = "de_time")]
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
//fn ser_time<S>(tm: &Tm, s: S) -> Result<S::Ok, S::Error>
//    where S: Serializer
//{
//    s.serialize_str(&format!("{}.{:09.09}Z", tm.strftime("%FT%T").unwrap(), tm.tm_nsec))
//}
//fn de_time<'de, D>(d: D) -> Result<Tm, D::Error>
//    where D: Deserializer<'de>
//{
//    let s = String::deserialize(d)?;
//    time::strptime(&s, "%FT%T.").map_err(serde::de::Error::custom)
//}

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

/// Parsed QoS+PacketIdentifier. Note that the identifier MUST be absent for AtMostOnce and present
/// for AtLeastOnce/ExactlyOnce.
// FIXME: Instead of panicing, return a result and let `DumpMqtt::from(&Packet)` fail cleanly just
// for this client and/or dump a `DumpMqtt::Invalid` variant.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum DumpQosId {
    AtMostOnce,
    AtLeastOnce(DumpPid),
    ExactlyOnce(DumpPid),
}
impl DumpQosId {
    fn from(q: QoS, p: Option<PacketIdentifier>) -> Self {
        match (q, p) {
            (QoS::AtMostOnce, None) => Self::AtMostOnce,
            (QoS::AtLeastOnce, Some(i)) => Self::AtLeastOnce(i.0),
            (QoS::ExactlyOnce, Some(i)) => Self::ExactlyOnce(i.0),
            _ => panic!("Can't have qos {:?} with pid {:?}", q, p),
        }
    }
}

/// Parsed MQTT connack packet.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DumpConnack {
    session: bool,
    code: u8, //FIXME: Proper names
}

/// Parsed MQTT publish packet.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DumpPublish {
    pub dup: bool,
    pub qos: DumpQosId,
    pub topic: String,
    /// The payload as a string, if it is valid utf-8
    pub utf8: Option<String>,
    /// The payload as an array of bytes
    pub bytes: Vec<u8>,
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
    PubComp(DumpPid),
    Subscribe(DumpSubscribe),
    SubAck(DumpSuback),
    UnSubscribe(DumpUnsubscribe),
    UnSubAck(DumpPid),
    PingReq,
    PingResp,
    Disconnect,
}
impl From<&Packet> for DumpMqtt {
    fn from(p: &Packet) -> Self {
        match p {
            Packet::Connect(p) => Self::Connect(p.client_id.clone()),
            Packet::Connack(p) => {
                Self::Connack(DumpConnack { session: p.session_present, code: p.code.to_u8() })
            },
            Packet::Publish(p) => {
                Self::Publish(DumpPublish { dup: p.dup,
                                            qos: DumpQosId::from(p.qos, p.pid),
                                            topic: p.topic_name.clone(),
                                            utf8: String::from_utf8(p.payload.clone()).ok(),
                                            bytes: p.payload.clone() })
            },
            Packet::Puback(p) => Self::Puback(p.0),
            Packet::Pubrec(p) => Self::Pubrec(p.0),
            Packet::Pubrel(p) => Self::Pubrel(p.0),
            Packet::PubComp(p) => Self::PubComp(p.0),
            Packet::Subscribe(p) => {
                let topics =
                    p.topics
                     .iter()
                     .map(|s| DumpSubscribeTopic { topic: s.topic_path.clone(), qos: s.qos.into() })
                     .collect();
                Self::Subscribe(DumpSubscribe { pid: p.pid.0, topics })
            },
            Packet::SubAck(p) => {
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
                Self::SubAck(DumpSuback { pid: p.pid.0, codes })
            },
            Packet::UnSubscribe(p) => {
                Self::UnSubscribe(DumpUnsubscribe { pid: p.pid.0, topics: p.topics.clone() })
            },
            Packet::UnSubAck(p) => Self::UnSubAck(p.0),
            Packet::PingReq => Self::PingReq,
            Packet::PingResp => Self::PingResp,
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
pub struct Dump {
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
                                      pkt: DumpMqtt::from(pkt) }).unwrap();
        for c in self.chans.iter() {
            c.unbounded_send(e.clone()).unwrap();
        }
    }
}
