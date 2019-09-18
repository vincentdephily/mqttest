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

/// Parsed QoS+PacketIdentifier. Note that the identifier MUST be absent for AtMostOnce and present
/// for AtLeastOnce/ExactlyOnce.
// FIXME: Instead of panicing, return a result and let `DumpMqtt::from(&Packet)` fail cleanly just
// for this client and/or dump a `DumpMqtt::Invalid` variant.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum DumpQoS {
    AtMostOnce,
    AtLeastOnce(u16),
    ExactlyOnce(u16),
}
impl DumpQoS {
    fn from(q: QoS, p: Option<PacketIdentifier>) -> Self {
        match (q, p) {
            (QoS::AtMostOnce, None) => Self::AtMostOnce,
            (QoS::AtLeastOnce, Some(i)) => Self::AtLeastOnce(i.0),
            (QoS::ExactlyOnce, Some(i)) => Self::ExactlyOnce(i.0),
            _ => panic!("Can't have qos {:?} with pid {:?}", q, p),
        }
    }
}

/// Parsed MQTT publish packet.
// FIXME: This currently relies on a forked `mqttrs` with serde derives.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DumpPublish {
    pub dup: bool,
    pub qos: DumpQoS,
    pub topic: String,
    /// The payload as a string, if it is valid utf-8
    pub utf8: Option<String>,
    /// The payload as an array of bytes
    pub bytes: Vec<u8>,
}

/// Parsed MQTT packet. We use our own struct instead `mqttrs::Packet` so that we can implement json
/// serialisation, and add/remove some fields for readbility/unit-testing.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum DumpMqtt {
    /// The string is the client id.
    Connect(String),
    Connack,
    Publish(DumpPublish),
    Puback,
    Pubrec,
    Pubrel,
    PubComp,
    Subscribe,
    SubAck,
    UnSubscribe,
    UnSubAck,
    PingReq,
    PingResp,
    Disconnect,
}
impl From<&Packet> for DumpMqtt {
    fn from(p: &Packet) -> Self {
        match p {
            Packet::Connect(c) => Self::Connect(c.client_id.clone()),
            Packet::Connack(_) => Self::Connack,
            Packet::Publish(p) => {
                Self::Publish(DumpPublish { dup: p.dup,
                                            qos: DumpQoS::from(p.qos, p.pid),
                                            topic: p.topic_name.clone(),
                                            utf8: String::from_utf8(p.payload.clone()).ok(),
                                            bytes: p.payload.clone() })
            },
            Packet::Puback(_) => Self::Puback,
            Packet::Pubrec(_) => Self::Pubrec,
            Packet::Pubrel(_) => Self::Pubrel,
            Packet::PubComp(_) => Self::PubComp,
            Packet::Subscribe(_) => Self::Subscribe,
            Packet::SubAck(_) => Self::SubAck,
            Packet::UnSubscribe(_) => Self::UnSubscribe,
            Packet::UnSubAck(_) => Self::UnSubAck,
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
