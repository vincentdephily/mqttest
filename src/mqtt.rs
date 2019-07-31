/// Wrapper around `mqttrs` crate. Might try to get it merged upstream.
use bytes::BytesMut;
use log::*;
pub use mqttrs::*;
use std::io::Error;
use tokio::codec::{Decoder, Encoder};

//pub type MqttId = u16;
///// equivalent to `mqtt::qos::QualityOfService` but ignores level 0 and lets us derive serde traits.
//#[derive(Debug, Clone, Copy, PartialEq)]
//pub enum MqttQoS {
//    /// Non-acked, at-most-once delivery.
//    Level0,
//    /// Single-acked, at-least-once delivery.
//    Level1(MqttId),
//}
//impl From<MqttQoS> for QoSWithPacketIdentifier {
//    fn from(q: MqttQoS) -> Self {
//        match q {
//            MqttQoS::Level0 => QoSWithPacketIdentifier::new(QualityOfService::Level0, 0),
//            MqttQoS::Level1(i) => QoSWithPacketIdentifier::new(QualityOfService::Level1, i),
//        }
//    }
//}

/// Decode network bytes into an MQTT packet.
/// The tokio FramedRead API calls this to determine the bounds of a full packet.
// TODO: update format to json/protobuf/md30
// FIXME: Emptying and refilling `src` like this is inefficient. Find a clean way to decode the `&mut BytesMut` directly.
pub struct Codec(pub u64);
impl Decoder for Codec {
    type Item = Packet;
    type Error = Error;
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let res = mqttrs::decoder::decode(src);
        trace!("C{}: dec {:?} -> {:?}", self.0, src, res);
        res
    }
}
impl Encoder for Codec {
    type Item = Packet; //TODO: Would be nice to accept Into<Packet>. First need to get an `impl Into<Paket> for Connect...` upstream.
    type Error = Error;
    fn encode(&mut self, item: Self::Item, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let res = mqttrs::encoder::encode(&item, dst);
        trace!("C{}: enc {:?} -> {:?}", self.0, item, dst);
        res
    }
}

pub fn connack(session_present: bool, code: ConnectReturnCode) -> Packet {
    Packet::Connack(Connack { session_present, code })
}

pub fn pingresp() -> Packet {
    Packet::PingResp
}

pub fn puback(pid: PacketIdentifier) -> Packet {
    Packet::Puback(pid)
}

pub fn publish(dup: bool,
               qos: QoS,
               retain: bool,
               topic_name: String,
               pid: Option<PacketIdentifier>,
               payload: Vec<u8>)
               -> Packet {
    Packet::Publish(Publish { dup, qos, retain, topic_name, pid, payload })
}

pub fn suback(pid: PacketIdentifier, return_codes: Vec<SubscribeReturnCodes>) -> Packet {
    Packet::SubAck(Suback { pid, return_codes })
}
