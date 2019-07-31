use crate::{mqtt::*, pubsub::*};
use futures::{sink::Wait,
              stream::Stream,
              sync::{mpsc::{unbounded, UnboundedSender},
                     oneshot}};
use log::*;
use std::{collections::HashMap,
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

#[derive(Clone)]
pub struct Addr(UnboundedSender<Msg>);
impl Addr {
    fn send(&self, pkt: Packet) {
        //FIXME: how to behave when channel is closed (mqtt client is disconnected)
        if let Err(e) = self.0.unbounded_send(Msg::PktOut(pkt)) {
            error!("Trying to send to disconnected chan: {:?}", e);
        }
    }
}

pub enum Msg {
    PktIn(Packet),
    PktOut(Packet),
}

pub struct Client {
    pub id: ConnId,
    pub addr: Addr,
    conn: bool,
    fw: Wait<FramedWrite<WriteHalf<TcpStream>, Codec>>,
    /// Acks currently pending, and what to do when it times out.
    pend_acks: HashMap<PacketIdentifier, oneshot::Sender<()>>,
    ack_timeout: Duration,
    subs: Arc<RwLock<Subs>>,
}
impl Client {
    /// Initializes a new `Client` and returns a `Future` that'll handle the whole connection.
    pub fn init(id: u64,
                socket: TcpStream,
                subs: Arc<RwLock<Subs>>,
                ack_timeout: Duration)
                -> impl Future<Item = (), Error = ()> {
        info!("C{}: Connection from {:?}", id, socket);
        let (read, write) = socket.split();
        let fr = FramedRead::new(read, Codec(id));
        let fw = FramedWrite::new(write, Codec(id)).wait(); //FIXME switch to async writes
        let (sx, rx) = unbounded::<Msg>();
        let mut client = Client { id,
                                  addr: Addr(sx.clone()),
                                  conn: false,
                                  fw,
                                  pend_acks: HashMap::new(),
                                  ack_timeout,
                                  subs };

        let msg_fut = rx.for_each(move |msg| {
                            match msg {
                                Msg::PktIn(p) => client.handle_pkt_in(p),
                                Msg::PktOut(p) => client.handle_pkt_out(p),
                            }.map_err(move |e| {
                                 error!("C{}: {}", id, e);
                             })
                        });
        let pkt_fut = fr.map_err(move |e| {
                            error!("C{}: {:?}", id, e);
                        })
                        .for_each(move |pktin| {
                            sx.unbounded_send(Msg::PktIn(pktin))
                              .expect("Client sending to itself, channel should exist");
                            Ok(())
                        });
        pkt_fut.join(msg_fut).map(move |_| {
                                 info!("C{}: Connection closed", id);
                             })
    }

    /// Receive packets from client.
    fn handle_pkt_in(&mut self, pkt: Packet) -> Result<(), Error> {
        info!("C{}: receive Packet::{:?}", self.id, pkt);
        match (pkt, self.conn) {
            // Connection
            // FIXME: handle session restore and different return codes
            (Packet::Connect(_), false) => {
                self.conn = true;
                self.addr.send(connack(false, ConnectReturnCode::Accepted));
            },
            // FIXME: close connection
            (Packet::Disconnect, true) => {
                self.conn = false;
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
                assert_eq!(QoS::AtLeastOnce, p.qos, "Only AtLeastOnce currently supported");
                if let Some(subs) = self.subs.read().expect("read subs").get(&p.topic_name) {
                    for (qos, addr) in subs.values() {
                        addr.send(publish(false,
                                          *qos,
                                          false,
                                          p.topic_name.clone(),
                                          Some(PacketIdentifier(64)),
                                          p.payload.clone()));
                    }
                }
                self.addr.send(puback(p.pid.unwrap()));
            },
            // Subscription request
            // FIXME: support AtMostOnce see https://github.com/00imvj00/mqttrs/issues/6
            (Packet::Subscribe(Subscribe { pid, topics }), true) => {
                let mut subs = self.subs.write().expect("write subs");
                let ret_codes = topics.iter()
                                      .map(|SubscribeTopic { topic_path, qos }| {
                                          assert_eq!(QoS::AtLeastOnce,
                                                     *qos,
                                                     "Only AtLeastOnce currently supported");
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
        self.fw.send(pkt)?;
        self.fw.flush()
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
