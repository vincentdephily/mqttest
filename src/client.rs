use crate::{dump::*, mqtt::*, pubsub::*, session::*, Conf};
use futures::{sink::Wait,
              stream::Stream,
              sync::{mpsc::{unbounded, UnboundedSender},
                     oneshot}};
use log::*;
use rand::{seq::SliceRandom, thread_rng};
use std::{collections::BTreeMap,
          io::{Error, ErrorKind},
          sync::{Arc, Mutex, RwLock},
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
pub struct Addr(UnboundedSender<Msg>, pub(crate) ConnId);
impl Addr {
    pub(crate) fn send(&self, msg: Msg) {
        if let Err(e) = self.0.unbounded_send(msg) {
            warn!("Trying to send to disconnected Addr {:?}", e);
        }
    }
}
impl PartialEq for Addr {
    fn eq(&self, other: &Self) -> bool {
        self.1 == other.1
    }
}
impl Eq for Addr {}
impl std::fmt::Debug for Addr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Addr(_, {})", self.1)
    }
}

pub(crate) enum Msg {
    PktIn(Packet),
    PktOut(Packet),
    Replaced(ConnId, oneshot::Sender<SessionData>),
}

/// Note that we're not deriving Clone. The only place where a new SessionData should be
/// instanciated is Session::open(). This helps making sure that only one SessionData instance
/// exists for a given Client.name.
#[derive(Debug, Default)]
pub(crate) struct SessionData {
    /// Number of connections seen by this session. If it is 0, this is a brand new session.
    cons: usize,
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
    /// Pending acks will timeout after that duration.
    ack_timeout: Duration,
    /// Wether to allow or reject optional behaviours.
    strict: bool,
    /// Client_id must start with this.
    idprefix: String,
    /// Username:password must match this (if `userpass.is_some()`).
    userpass: Option<String>,
    /// Shared list of all the client subscriptions.
    subs: Arc<RwLock<Subs>>,
    /// Shared list of all the client sessions.
    sessions: Arc<Mutex<Sessions>>,
    /// Client session.
    session: Option<SessionData>,
    /// Acks currently pending, and what to do when it times out.
    pend_acks: BTreeMap<PacketIdentifier, oneshot::Sender<()>>,
}
impl Client {
    /// Initializes a new `Client` and moves it into a `Future` that'll handle the whole
    /// connection. It's the caller's responsibility to execute that future.
    pub fn init(id: u64,
                socket: TcpStream,
                subs: Arc<RwLock<Subs>>,
                sessions: Arc<Mutex<Sessions>>,
                dumps: Dump,
                conf: &Conf)
                -> impl Future<Item = (), Error = ()> {
        info!("C{}: Connection from {:?}", id, socket);
        let (read, write) = socket.split();
        let (sx, rx) = unbounded::<Msg>();
        let mut client = Client { id,
                                  name: String::from(""),
                                  addr: Addr(sx.clone(), id),
                                  conn: false,
                                  writer: FramedWrite::new(write, Codec(id)).wait(),
                                  dumps,
                                  ack_timeout: conf.ack_timeout,
                                  strict: conf.strict,
                                  idprefix: conf.idprefix.clone(),
                                  userpass: conf.userpass.clone(),
                                  subs,
                                  sessions,
                                  session: None,
                                  pend_acks: BTreeMap::new() };
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
                            Msg::Replaced(i, c) => client.handle_replaced(i, c),
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
            (Packet::Connect(c), false) => {
                self.conn = true;
                // Set and check client name
                self.name = c.client_id.clone();
                if let Err((code, desc)) = self.check_credentials(&c) {
                    self.addr.send(Msg::PktOut(connack(false, code)));
                    return Err(Error::new(ErrorKind::ConnectionAborted, desc));
                }
                // Load session
                let mut sm = self.sessions.lock().expect("lock sessions");
                let mut sess = sm.open(&self, c.clean_session);
                let isold = sess.cons > 0;
                debug!("C{}: loaded {} session {:?}",
                       self.id,
                       if isold { "old" } else { "new" },
                       sess);
                sess.cons += 1;
                self.session = Some(sess);
                // Send connack
                self.addr.send(Msg::PktOut(connack(isold, ConnectReturnCode::Accepted)));
            },
            // FIXME: Use our own error type, and let this one log as INFO rather than ERROR
            (Packet::Disconnect, true) => {
                self.conn = false;
                return Err(Error::new(ErrorKind::ConnectionAborted, "Disconnect"));
            },
            // Ping request
            // FIXME: accept ping when not connected ?
            (Packet::PingReq, true) => self.addr.send(Msg::PktOut(pingresp())),
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
                        let p =
                            publish(false,
                                    s.qos,
                                    false,
                                    p.topic_name.clone(),
                                    // FIXME: use proper pid value for that client
                                    match s.qos {
                                        QoS::AtMostOnce => None,
                                        QoS::AtLeastOnce => Some(PacketIdentifier(64)),
                                        QoS::ExactlyOnce => panic!("ExactlyOnce not supported yet"),
                                    },
                                    p.payload.clone());
                        s.addr.send(Msg::PktOut(p));
                    }
                }
                match p.qos {
                    QoS::AtMostOnce => (),
                    QoS::AtLeastOnce => self.addr.send(Msg::PktOut(puback(p.pid.unwrap()))),
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
                self.addr.send(Msg::PktOut(suback(pid, ret_codes)));
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
                    addr.send(Msg::PktOut(p));
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

    fn handle_replaced(&mut self,
                       conn: ConnId,
                       chan: oneshot::Sender<SessionData>)
                       -> Result<(), Error> {
        info!("C{}: replaced by connection {}", self.id, conn);
        chan.send(self.session.take().unwrap()).unwrap_or_else(|_| {
                                                   trace!("C{}: C{} didn't wait for the session",
                                                          self.id,
                                                          conn)
                                               });
        Err(Error::new(ErrorKind::ConnectionReset, "Replaced"))
    }

    /// Check client identifier.
    /// http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718031
    fn check_credentials(&mut self,
                         con: &Connect)
                         -> Result<(), (ConnectReturnCode, &'static str)> {
        let allow = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
        if self.name.len() > 23 || !self.name.chars().all(|c| allow.contains(c)) {
            if self.strict {
                return Err((ConnectReturnCode::RefusedIdentifierRejected,
                            "Client_id too long or bad charset [MQTT-3.1.3-8]"));
            }
            warn!("C{}: Servers MAY reject {:?} [MQTT-3.1.3-5/MQTT-3.1.3-6]", self.id, self.name);
        }
        if self.name.is_empty() {
            if !con.clean_session {
                return Err((ConnectReturnCode::RefusedIdentifierRejected,
                            "Empty client_id with session [MQTT-3.1.3-8]"));
            }
            let mut rng = thread_rng();
            for _ in 0..20 {
                self.name.push(*allow.as_bytes().choose(&mut rng).unwrap() as char);
            }
            info!("C{}: Unamed client, assigned random name {:?}", self.id, self.name);
        }
        if con.password.is_some() && con.username.is_none() {
            return Err((ConnectReturnCode::BadUsernamePassword,
                        "Password without a username [MQTT-3.1.2-22]"));
        }
        if let Some(ref req_up) = self.userpass {
            let con_up = format!("{}:{}",
                                 con.username.as_ref().unwrap_or(&String::new()),
                                 con.password.as_ref().unwrap_or(&String::new()));
            if &con_up != req_up {
                return Err((ConnectReturnCode::BadUsernamePassword,
                            "Bad username/password [MQTT-3.1.3.4/3.1.3.5]"));
            }
        }
        if !self.name.starts_with(&self.idprefix) {
            return Err((ConnectReturnCode::NotAuthorized, "Not Authorised [MQTT-5.4.2]"));
        }
        Ok(())
    }
}
impl Drop for Client {
    /// If a Client dies, we need to drop all its subscriptions.
    fn drop(&mut self) {
        let mut subs = self.subs.write().expect("write subs");
        subs.del_all(&self);
        if let Some(sess) = self.session.take() {
            let mut sm = self.sessions.lock().expect("lock sessions");
            sm.close(&self, sess);
        }
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
