use crate::{dump::*, mqtt::*, pubsub::*, Conf};
use futures::{sink::Wait,
              stream::Stream,
              sync::{mpsc::{unbounded, UnboundedSender},
                     oneshot}};
use log::*;
use std::{collections::BTreeMap,
          io::{Error, ErrorKind},
          sync::{Arc, RwLock},
          time::{Duration, Instant}};
use tokio::{codec::{FramedRead, FramedWrite},
            io::WriteHalf,
            net::TcpStream,
            prelude::*,
            timer::Delay};

/// Connection id for debug and indexing purposes.
pub type ConnId = u64;

/// Allows sending a `Msg` to a `Client`.
#[derive(Clone)]
pub struct Addr(UnboundedSender<Msg>);
impl Addr {
    fn send(&self, pkt: Packet) {
        if let Err(e) = self.0.unbounded_send(Msg::PktOut(pkt)) {
            panic!("Trying to send to disconnected Addr {:?}", e);
        }
    }
}

enum Msg {
    PktIn(Packet),
    PktOut(Packet),
}

/// The `Client` struct follows the actor model. It's owned by one `Future`, that receives `Msg`s
/// and handles them, mutating the struct.
pub(crate) struct Client {
    pub id: ConnId,
    pub name: String,
    pub addr: Addr,
    /// Is the MQTT connection fully established ?
    // FIXME: there's more than two states.
    conn: bool,
    /// Write `Packet`s there, they'll get encoded and sent over the TcpStream.
    //  FIXME switch to async writes,
    writer: Wait<FramedWrite<WriteHalf<TcpStream>, Codec>>,
    /// Dump targets.
    dumps: Dump,
    /// Acks currently pending, and what to do when it times out.
    pend_acks: BTreeMap<PacketIdentifier, oneshot::Sender<()>>,
    /// Pending acks will timeout after that duration.
    ack_timeout: Duration,
    /// Shared list of all the client subscriptions.
    subs: Arc<RwLock<Subs>>,
}
impl Client {
    /// Initializes a new `Client` and moves it into a `Future` that'll handle the whole
    /// connection. It's the caller's responsibility to execute that future.
    pub fn init(id: u64,
                socket: TcpStream,
                subs: Arc<RwLock<Subs>>,
                dumps: Dump,
                conf: &Conf)
                -> impl Future<Item = (), Error = ()> {
        info!("C{}: Connection from {:?}", id, socket);
        let (read, write) = socket.split();
        let (sx, rx) = unbounded::<Msg>();
        let mut client = Client { id,
                                  name: String::from(""),
                                  addr: Addr(sx.clone()),
                                  conn: false,
                                  writer: FramedWrite::new(write, Codec(id)).wait(),
                                  dumps,
                                  pend_acks: BTreeMap::new(),
                                  ack_timeout: conf.ack_timeout,
                                  subs };
        // Initialize json dump target.
        for s in conf.dumps.iter().filter(|s| !s.contains("{i}")) {
            let s = s.replace("{c}", &format!("{}", id));
            match client.dumps.register(&s) {
                Ok(_) => debug!("C{}: Dump to {}", id, s),
                Err(e) => error!("C{}: Cannot dump to {}: {}", id, s, e),
            }
        }
        // This future handles all `Msg`s comming to the client.
        let msg = rx.for_each(move |msg| {
                        match msg {
                            Msg::PktIn(p) => client.handle_pkt_in(p),
                            Msg::PktOut(p) => client.handle_pkt_out(p),
                        }.map_err(move |e| {
                             error!("C{}: msg: {}", id, e);
                         })
                    });
        // This future decodes MQTT packets and forwards them as `Msg`s.
        let fr = FramedRead::new(read, Codec(id));
        let pkt = fr.map_err(move |e| {
                        error!("C{}: pkt: {}", id, e);
                    })
                    .for_each(move |pktin| {
                        sx.unbounded_send(Msg::PktIn(pktin))
                          .expect("Client sending to itself, channel should exist");
                        Ok(())
                    });
        // Resolve this future (aka drop this `Client`) when either the TcpStream or the
        // Receiver<Msg> is closed.
        pkt.join(msg).map(move |_| {
                         info!("C{}: Connection closed", id);
                     })
    }

    /// Receive packets from client.
    fn handle_pkt_in(&mut self, pkt: Packet) -> Result<(), Error> {
        info!("C{}: receive Packet::{:?}", self.id, pkt);
        self.dumps.dump(self.id, &self.name, "C", &pkt);
        match (pkt, self.conn) {
            // Connection
            // FIXME: handle session restore and different return codes
            // FIXME: optionally disallow multiple connections with same client.name
            (Packet::Connect(c), false) => {
                self.conn = true;
                self.name = c.client_id;
                self.addr.send(connack(false, ConnectReturnCode::Accepted));
            },
            // FIXME: Use our own error type, and let this one log as INFO rather than ERROR
            (Packet::Disconnect, true) => {
                self.conn = false;
                return Err(Error::new(ErrorKind::ConnectionReset, "Received Disconnect"));
            },
            // Ping request
            // FIXME: accept ping when not connected ?
            (Packet::PingReq, true) => self.addr.send(pingresp()),
            // Puback: cancel the resend timer if the ack was expected, die otherwise.
            (Packet::Puback(pid), true) => match self.pend_acks.remove(&pid) {
                Some(handle) => {
                    debug!("C{}: Puback {:?} OK", self.id, pid);
                    handle.send(()).expect("Cancel pending ack");
                },
                None => {
                    return Err(Error::new(ErrorKind::InvalidData,
                                          format!("Puback {:?} unexpected", pid)))
                },
            },
            // Publish
            // FIXME: support AtMostOnce see https://github.com/00imvj00/mqttrs/issues/6
            (Packet::Publish(p), true) => {
                if let Some(subs) = self.subs.read().expect("read subs").get(&p.topic_name) {
                    for s in subs.values() {
                        s.addr.send(publish(false,
                                            s.qos,
                                            false,
                                            p.topic_name.clone(),
                                            // FIXME: use proper pid value for that client
                                            match s.qos {
                                                QoS::AtMostOnce => None,
                                                QoS::AtLeastOnce => Some(PacketIdentifier(64)),
                                                QoS::ExactlyOnce => {
                                                    panic!("ExactlyOnce not supported yet")
                                                },
                                            },
                                            p.payload.clone()));
                    }
                }
                match p.qos {
                    QoS::AtMostOnce => (),
                    QoS::AtLeastOnce => self.addr.send(puback(p.pid.unwrap())),
                    QoS::ExactlyOnce => panic!("ExactlyOnce not supported yet"),
                }
            },
            // Subscription request
            // FIXME: support AtMostOnce see https://github.com/00imvj00/mqttrs/issues/6
            (Packet::Subscribe(Subscribe { pid, topics }), true) => {
                let mut subs = self.subs.write().expect("write subs");
                let ret_codes =
                    topics.iter()
                          .map(|SubscribeTopic { topic_path, qos }| {
                              assert_ne!(QoS::ExactlyOnce, *qos, "ExactlyOnce not supported yet");
                              subs.add(topic_path, *qos, self);
                              SubscribeReturnCodes::Success(*qos)
                          })
                          .collect();
                self.addr.send(suback(pid, ret_codes));
            },
            (other, _) => {
                return Err(Error::new(ErrorKind::InvalidData, format!("Unhandled {:?}", other)))
            },
        }
        Ok(())
    }

    /// Send packets to client.
    fn handle_pkt_out(&mut self, pkt: Packet) -> Result<(), Error> {
        info!("C{}: send Packet::{:?}", self.id, pkt);
        self.dumps.dump(self.id, &self.name, "S", &pkt);
        match &pkt {
            Packet::Publish(p) if p.pid.is_some() => {
                // Publish with QoS 1, remember the pid so that we can accept the ack later. If the
                // ack never comes, resend.
                // FIXME: Configurable maximum number of resend attempts.
                let pid = p.pid.unwrap();
                let id = self.id.clone();
                debug!("C{}: waiting for {:?} + {:?}", id, pid, self.pend_acks);
                let p = pkt.clone();
                let addr = self.addr.clone();
                let handle = delay_cancel(self.ack_timeout, move || {
                    warn!("C{}: ack timeout {:?}, resending", id, pid);
                    addr.send(p);
                });
                if let Some(prev) = self.pend_acks.insert(pid, handle) {
                    // Reusing a not-yet-acked pid is a server error, not a client error.
                    error!("C{}: overwriting {:?} {:?}", self.id, pid, prev);
                }
            },
            _ => (),
        }
        self.writer.send(pkt)?;
        self.writer.flush()
    }
}
impl Drop for Client {
    /// If a Client dies, we need to drop all its subscriptions.
    fn drop(&mut self) {
        let mut subs = self.subs.write().expect("write subs");
        subs.del_all(&self);
    }
}

/// Run spawn a future that calls the closure after a delay, and return a cancellation handle.
/// Cancel it using `handle.send(()).unwrap()`.
fn delay_cancel(delay: Duration, act: impl FnOnce() -> () + 'static + Send) -> oneshot::Sender<()> {
    let (sx, rx) = oneshot::channel();
    let f = Delay::new(Instant::now() + delay).map(|_| act());
    tokio::spawn(f.select2(rx).map(|_| ()).map_err(|_| ()));
    sx
}
