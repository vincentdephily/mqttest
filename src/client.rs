use crate::{dump::*, mqtt::*, pubsub::*, session::*, Conf};
use futures::{sink::Wait,
              stream::Stream,
              sync::{mpsc::{unbounded, UnboundedSender},
                     oneshot}};
use log::*;
use rand::{seq::SliceRandom, thread_rng};
use std::{collections::HashMap,
          io::{Error, ErrorKind},
          sync::{Arc, Mutex, RwLock},
          time::{Duration, Instant}};
use tokio::{codec::{FramedRead, FramedWrite},
            io::WriteHalf,
            net::TcpStream,
            prelude::*,
            timer::Delay};


/// Duration longer than program's lifetime (not quite `u64::MAX` so we can add `Instant::now()`).
const FOREVER: Duration = Duration::from_secs(60 * 60 * 24 * 365);

/// Connection id for debug and indexing purposes.
pub type ConnId = u64;

/// Allows sending a `Msg` to a `Client`.
#[derive(Clone)]
pub struct Addr(UnboundedSender<Msg>, pub(crate) ConnId);
impl Addr {
    /// Send `Msg` to `Addr`.
    pub(crate) fn send(&self, msg: Msg) {
        if let Err(e) = self.0.unbounded_send(msg) {
            warn!("Trying to send to disconnected Addr {:?}", e);
        }
    }
    /// Send `Msg` to `Addr` at `Instant`.
    fn send_at(&self, deadline: Instant, msg: Msg) {
        trace!("send_at n{:?} {:?} {:?}", deadline, self, msg);
        let addr = self.clone();
        // Wait until deadline, and send the msg.
        let fut = Delay::new(deadline).map(move |_| addr.send(msg));
        // Spawn the future, ignoring errors.
        tokio::spawn(fut.map(drop).map_err(drop));
    }
    /// Send `Msg` to `Addr` at `Instant`. Returns a handle that will cancel sending if dropped.
    // FIXME: It should be possible to reliably resolve the future as soon as the Receiver is
    // dropped.
    fn send_at_cancel(&self, deadline: Instant, msg: Msg) -> oneshot::Receiver<()> {
        trace!("send_at_cancel {:?} {:?} {:?}", deadline, self, msg);
        let addr = self.clone();
        let (s, r) = oneshot::channel();
        // Wait until deadline, check for cancellation, and send the msg.
        let fut = Delay::new(deadline).map(move |_| {
                                          if !s.is_canceled() {
                                              addr.send(msg)
                                          }
                                      });
        // Spawn the future, ignoring errors.
        tokio::spawn(fut.map(drop).map_err(drop));
        r
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

#[derive(Debug)]
pub(crate) enum Msg {
    PktIn(Packet),
    PktOut(Packet),
    Publish(QoS, Publish),
    CheckQos,
    Replaced(ConnId, oneshot::Sender<SessionData>),
    Disconnect(String),
}

/// Session data. To be restored at connection, and kept up to date during connection.
///
/// Note that we're not deriving Clone. The only place where a new SessionData should be
/// instanciated is Session::open(). This helps making sure that only one SessionData instance
/// exists for a given Client.name.
#[derive(Debug, Default)]
pub(crate) struct SessionData {
    /// Number of connections seen by this session. If it is 0, this is a brand new session.
    cons: usize,
    /// The last pid generated for this client.
    prev_pid: Option<Pid>,
    /// Topics subscribed by this session. Distinct from the global `Subs` store.
    subs: HashMap<String, QoS>,
    /// Pending Qos1 acks. Unacked packets will be resent after a delay.
    /// TODO: Resend immediately at reconnection too.
    qos1: HashMap<Pid, (Instant, Packet)>,
}
impl SessionData {
    /// Return the next pid and store it in self.
    fn next_pid(&mut self) -> Pid {
        let mut pid = match self.prev_pid {
            Some(p) => p + 1,
            None => Pid::new(),
        };
        while self.qos1.contains_key(&pid) {
            pid = pid + 1;
        }
        self.prev_pid = Some(pid);
        pid
    }
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
    /// Protocol-specific ack-timeout config.
    ack_timeouts_conf: (Option<Duration>, Option<Duration>),
    /// Pending acks will timeout after that duration.
    ack_timeout: Duration,
    /// Wait before acking publish and subscribe packets
    // TODO: should be a ring buffer.
    ack_delay: Duration,
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
    /// Handle to future sending the next Msg::CheckQos.
    qos1_check: Option<oneshot::Receiver<()>>,
    /// Disconnect after that many received packets.
    max_pkt: u64,
    /// Count received packets.
    count_pkt: u64,
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
        let max_pkt = conf.max_pkt[id as usize % conf.max_pkt.len()].unwrap_or(std::u64::MAX);
        let mut client = Client { id,
                                  name: String::from(""),
                                  addr: Addr(sx.clone(), id),
                                  conn: false,
                                  writer: FramedWrite::new(write, Codec(id)).wait(),
                                  dumps,
                                  ack_timeouts_conf: conf.ack_timeouts,
                                  ack_timeout: conf.ack_timeouts.0.unwrap_or(FOREVER),
                                  ack_delay: conf.ack_delay,
                                  strict: conf.strict,
                                  idprefix: conf.idprefix.clone(),
                                  userpass: conf.userpass.clone(),
                                  subs,
                                  sessions,
                                  session: None,
                                  qos1_check: None,
                                  max_pkt,
                                  count_pkt: 0 };
        // Setup disconnect timer.
        if let Some(m) = conf.max_time[id as usize % conf.max_time.len()] {
            client.addr.send_at(Instant::now() + m, Msg::Disconnect(format!("max time {:?}", m)))
        }
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
                            Msg::Publish(q, p) => client.handle_publish(q, p),
                            Msg::CheckQos => client.handle_check_qos(Instant::now()),
                            Msg::Replaced(i, c) => client.handle_replaced(i, c),
                            Msg::Disconnect(r) => client.handle_disconnect(r),
                        }.map_err(move |e| {
                             error!("C{}: terminating: {}", id, e);
                         })
                    });
        // This future decodes MQTT packets and forwards them as `Msg`s.
        let fr = FramedRead::new(read, Codec(id));
        let pkt = fr.map_err(move |e| {
                        error!("C{}: invalid packet: {}", id, e);
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
        self.count_pkt += 1;
        match (pkt, self.conn) {
            // Connection
            (Packet::Connect(c), false) => {
                self.conn = true;
                self.ack_timeout = match c.protocol {
                    Protocol::MQTT311 => self.ack_timeouts_conf.0.unwrap_or(FOREVER),
                    Protocol::MQIsdp => self.ack_timeouts_conf.0.unwrap_or(FOREVER),
                };
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
                let mut subs = self.subs.write().expect("write subs");
                for (topic, qos) in sess.subs.iter() {
                    subs.add(&topic, *qos, self.id, self.addr.clone());
                }
                self.session = Some(sess);
                // Handle QoS
                self.addr.send(Msg::CheckQos);
                // Send connack
                self.addr.send(Msg::PktOut(connack(isold, ConnectReturnCode::Accepted)));
            },
            // FIXME: Use our own error type, and let this one log as INFO rather than ERROR
            (Packet::Disconnect, true) => {
                self.conn = false;
                return Err(Error::new(ErrorKind::ConnectionAborted, "Disconnect"));
            },
            // Ping request
            (Packet::Pingreq, true) => self.addr.send(Msg::PktOut(pingresp())),
            // Puback: cancel the resend timer if the ack was expected, die otherwise.
            (Packet::Puback(pid), true) => {
                let sess = self.session.as_mut().expect("unwrap session");
                if sess.qos1.remove(&pid).is_none() {
                    return Err(Error::new(ErrorKind::InvalidData,
                                          format!("Puback {:?} unexpected", pid)));
                }
            },
            // Publish
            (Packet::Publish(p), true) => {
                if let Some(subs) = self.subs.read().expect("read subs").get(&p.topic_name) {
                    for s in subs.values() {
                        s.addr.send(Msg::Publish(s.qos, p.clone()));
                    }
                }
                match p.qospid {
                    QosPid::AtMostOnce => (),
                    QosPid::AtLeastOnce(pid) => {
                        let d = Instant::now() + self.ack_delay;
                        self.addr.send_at(d, Msg::PktOut(puback(pid)));
                    },
                    QosPid::ExactlyOnce(_) => panic!("ExactlyOnce not supported yet"),
                }
            },
            // Subscription request
            (Packet::Subscribe(Subscribe { pid, topics }), true) => {
                let mut subs = self.subs.write().expect("write subs");
                let sess = self.session.as_mut().expect("unwrap session");
                let mut codes = Vec::new();
                for SubscribeTopic { topic_path, qos } in topics {
                    assert_ne!(QoS::ExactlyOnce, qos, "ExactlyOnce not supported yet");
                    subs.add(&topic_path, qos, self.id, self.addr.clone());
                    sess.subs.insert(topic_path.clone(), qos);
                    codes.push(SubscribeReturnCodes::Success(qos));
                }
                let d = Instant::now() + self.ack_delay;
                self.addr.send_at(d, Msg::PktOut(suback(pid, codes)));
            },
            (other, _) => {
                return Err(Error::new(ErrorKind::InvalidData, format!("Unhandled {:?}", other)))
            },
        }
        if self.count_pkt >= self.max_pkt {
            self.addr.send(Msg::Disconnect(format!("max packets {:?}", self.max_pkt)));
        }
        Ok(())
    }

    /// Send packets to client.
    fn handle_pkt_out(&mut self, pkt: Packet) -> Result<(), Error> {
        info!("C{}: send Packet::{:?}", self.id, pkt);
        self.dumps.dump(self.id, &self.name, "S", &pkt);
        self.writer.send(pkt)?;
        self.writer.flush().map_err(|e| e.into())
    }

    fn handle_publish(&mut self, qos: QoS, p: Publish) -> Result<(), Error> {
        let sess = self.session.as_mut().expect("unwrap session");
        let qospid = match qos {
            QoS::AtMostOnce => QosPid::AtMostOnce,
            QoS::AtLeastOnce => QosPid::AtLeastOnce(sess.next_pid()),
            QoS::ExactlyOnce => panic!("ExactlyOnce not supported yet"),
        };
        let pkt = publish(false, qospid, false, p.topic_name, p.payload);
        if let QosPid::AtLeastOnce(pid) = qospid {
            // Publish with QoS 1, remember the pid so that we can accept the ack later. If the
            let deadline = Instant::now() + self.ack_timeout;
            debug!("C{}: waiting for {:?} + {:?}@{:?}", self.id, sess.qos1, pid, deadline);

            // Remember the details so we can aceept the ack or resend the pkt.
            let prev = sess.qos1.insert(pid, (deadline, pkt.clone()));
            assert!(prev.is_none(), "C{}: Server error: reusing {:?} {:?}", self.id, pid, prev);

            // Schedule the next check for timedout acks.
            if self.qos1_check.is_none() && self.ack_timeout < FOREVER {
                self.qos1_check = Some(self.addr.send_at_cancel(deadline, Msg::CheckQos));
            }
        }
        self.handle_pkt_out(pkt)
    }

    /// Go trhough self.session.qos1 and resend any timedout packets.
    fn handle_check_qos(&mut self, reftime: Instant) -> Result<(), Error> {
        let sess = self.session.as_mut().expect("unwrap session");
        trace!("C{}: check Qos acks {:?}", self.id, sess.qos1);
        let id = self.id;
        let addr = self.addr.clone();
        // FIXME: Should be able to just move pkt.
        sess.qos1.retain(|pid, (deadline, pkt)| {
                     if *deadline > reftime {
                         warn!("C{}: Timeout receiving ack {:?}, resending packet", id, pid);
                         addr.send(Msg::PktOut(pkt.clone()));
                         false
                     } else {
                         true
                     }
                 });
        if let Some(deadline) = sess.qos1.values().map(|(d, _)| d).min() {
            self.qos1_check = Some(self.addr.send_at_cancel(*deadline, Msg::CheckQos));
        }
        Ok(())
    }

    fn handle_replaced(&mut self,
                       conn: ConnId,
                       chan: oneshot::Sender<SessionData>)
                       -> Result<(), Error> {
        info!("C{}: replaced by connection {}", self.id, conn);
        self.conn = false;
        chan.send(self.session.take().unwrap()).unwrap_or_else(|_| {
                                                   trace!("C{}: C{} didn't wait for the session",
                                                          self.id,
                                                          conn)
                                               });
        Err(Error::new(ErrorKind::ConnectionReset, "Replaced"))
    }

    fn handle_disconnect(&mut self, reason: String) -> Result<(), Error> {
        info!("C{}: Disconnect by server: {:?}", self.id, reason);
        self.conn = false;
        Err(Error::new(ErrorKind::ConnectionReset, reason))
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
            let con_up = format!("{}:{:?}",
                                 con.username.as_ref().unwrap_or(&String::new()),
                                 con.password.as_ref().unwrap_or(&Vec::new()));
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
