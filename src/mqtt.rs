//! Wrapper around `mqttrs` crate. Might try to get it merged upstream.

use crate::ConnId;
use bytes::BytesMut;
use log::*;
pub use mqttrs::*;
use tokio_util::codec::{Decoder, Encoder};

/// Decode network bytes into an MQTT packet.
/// The tokio FramedRead API calls this to determine the bounds of a full packet.
pub struct Codec(pub ConnId);
impl Decoder for Codec {
    type Item = Packet;
    type Error = mqttrs::Error;
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        trace!("C{}: dec {:?} -> ...", self.0, src);
        let res = mqttrs::decode(src);
        trace!("C{}: dec ... -> {:?}", self.0, res);
        res
    }
}
impl Encoder<Packet> for Codec {
    type Error = mqttrs::Error;
    fn encode(&mut self, item: Packet, dst: &mut BytesMut) -> Result<(), Self::Error> {
        trace!("C{}: enc {:?} -> ... ", self.0, item);
        let res = mqttrs::encode(&item, dst);
        trace!("C{}: enc ... -> {:?}", self.0, res);
        res
    }
}

pub fn connack(session_present: bool, code: ConnectReturnCode) -> Packet {
    Packet::Connack(Connack { session_present, code })
}

pub fn pingresp() -> Packet {
    Packet::Pingresp
}

pub fn puback(pid: Pid) -> Packet {
    Packet::Puback(pid)
}

pub fn publish(dup: bool,
               qospid: QosPid,
               retain: bool,
               topic_name: String,
               payload: Vec<u8>)
               -> Packet {
    Packet::Publish(Publish { dup, qospid, retain, topic_name, payload })
}

pub fn suback(pid: Pid, return_codes: Vec<SubscribeReturnCodes>) -> Packet {
    Packet::Suback(Suback { pid, return_codes })
}
