use crate::{dump::*, messages::*, mqtt::*, pubsub::*, session::*, Conf, FOREVER};
use futures::{future::{abortable, AbortHandle},
              lock::Mutex,
              prelude::*};
use log::*;
use rand::{seq::SliceRandom, thread_rng};
use std::{collections::HashMap,
          io::{Error, ErrorKind},
          sync::Arc,
          time::{Duration, Instant}};
use tokio::{net::{tcp::{ReadHalf, WriteHalf},
                  TcpStream},
            spawn,
            sync::{mpsc::{Receiver, Sender},
                   oneshot},
            time::delay_until};
use tokio_util::codec::{FramedRead, FramedWrite};


/// Wrapper around a spawned Future.
///
/// * Dropping the ControledTask aborts the future
/// * `is_done()` can be used to test for completion
struct ControledTask(pub AbortHandle, pub Arc<Mutex<()>>);
impl ControledTask {
    pub fn spawn(fut: impl Future<Output = ()> + Send + 'static) -> Self {
        let m = Arc::new(Mutex::new(()));
        let m2 = m.clone();
        let (f, h) = abortable(async move {
            m2.lock().await;
            fut.await
        });
        spawn(f.map(drop));
        Self(h, m)
    }
    pub fn is_done(&self) -> bool {
        self.1.try_lock().is_some()
    }
}
impl Drop for ControledTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}


/// Allows sending a `ClientEv` to a `Client`.
#[derive(Clone)]
pub struct Addr(Sender<ClientEv>, pub(crate) ConnId);
impl Addr {
    /// Send `ClientEv` to `Addr`.
    pub(crate) async fn send(&self, msg: ClientEv) {
        if let Err(e) = self.0.clone().send(msg).await {
            warn!("Trying to send to disconnected Addr {:?}", e);
        }
    }

    /// Schedule `ClientEv` to be sent to `Addr` at `Instant`.
    ///
    /// The returned `ControledTask` must be kept alive for the send to succeed.
    #[must_use]
    fn send_at(&self, deadline: Instant, msg: ClientEv) -> ControledTask {
        let addr = self.clone();
        ControledTask::spawn(async move {
            trace!("send_at {:?} {:?} {:?}", deadline, addr, msg);
            delay_until(deadline.into()).await;
            addr.send(msg).await;
        })
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

/// The `Client` struct follows the actor model. It's owned by one `Future`, that receives `ClientEv`s
/// and handles them, mutating the struct.
pub(crate) struct Client<'s> {
    pub id: ConnId,
    pub name: String,
    pub addr: Addr,
    /// Is the MQTT connection fully established ?
    // FIXME: there's more than two states.
    conn: bool,
    /// Write `Packet`s there, they'll get encoded and sent over the TcpStream.
    writer: FramedWrite<WriteHalf<'s>, Codec>,
    /// Dump targets.
    dumps: Dump,
    /// Send events to lib user
    event_s: SendEvent,
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
    subs: Arc<Mutex<Subs>>,
    /// Shared list of all the client sessions.
    sessions: Arc<Mutex<Sessions>>,
    /// Client session.
    session: Option<SessionData>,
    /// Override session expiry time.
    pub sess_expire: Option<Duration>,
    /// Handle to future sending the next ClientEv::CheckQos.
    qos1_check: Option<ControledTask>,
    /// Handles to various futures that should abort when the client dies.
    tasks: Vec<ControledTask>,
    /// Disconnect after that many received packets.
    max_pkt: usize,
    /// Delay before max_pkt disconnection.
    max_pkt_delay: Option<Duration>,
    /// Count received packets.
    count_pkt: usize,
}
impl Client<'_> {
    pub(crate) fn spawn(id: ConnId,
                        socket: TcpStream,
                        subs: &Arc<Mutex<Subs>>,
                        sess: &Arc<Mutex<Sessions>>,
                        dumps: &Dump,
                        conf: &Conf,
                        event_s: &SendEvent,
                        cev_s: Sender<ClientEv>,
                        cev_r: Receiver<ClientEv>,
                        mev_s: &Sender<MainEv>) {
        let mut mev_s = mev_s.clone();
        let subs = subs.clone();
        let sess = sess.clone();
        let dumps = dumps.clone();
        let event_s = event_s.clone();
        let conf = conf.clone(); // TODO: use an Arc<Conf>
        spawn(async move {
            Client::start(id, socket, subs, sess, dumps, conf, event_s, cev_s, cev_r).await;
            mev_s.send(MainEv::Finish(id)).await
        });
    }
    /// Start a new `Client` on given socket, using a `Future` (to be executed by the caller) to
    /// represent the whole connection.
    async fn start(id: ConnId,
                   mut socket: TcpStream,
                   subs: Arc<Mutex<Subs>>,
                   sessions: Arc<Mutex<Sessions>>,
                   dumps: Dump,
                   conf: Conf,
                   mut event_s: SendEvent,
                   cev_s: Sender<ClientEv>,
                   cev_r: Receiver<ClientEv>) {
        info!("C{}: Connection from {:?}", id, socket);
        event_s.send(Event::conn(id));
        let (read, write) = socket.split();
        let max_pkt = conf.max_pkt[id as usize % conf.max_pkt.len()].unwrap_or(std::usize::MAX);
        let sess_expire = conf.sess_expire[id as usize % conf.sess_expire.len()];
        let mut client = Client { id,
                                  name: String::from(""),
                                  addr: Addr(cev_s.clone(), id),
                                  conn: false,
                                  writer: FramedWrite::new(write, Codec(id)),
                                  dumps,
                                  event_s,
                                  ack_timeouts_conf: conf.ack_timeouts,
                                  ack_timeout: conf.ack_timeouts.0.unwrap_or(FOREVER),
                                  ack_delay: conf.ack_delay,
                                  strict: conf.strict,
                                  idprefix: conf.idprefix.clone(),
                                  userpass: conf.userpass.clone(),
                                  subs,
                                  sessions,
                                  session: None,
                                  sess_expire,
                                  qos1_check: None,
                                  tasks: vec![],
                                  max_pkt,
                                  max_pkt_delay: conf.max_pkt_delay,
                                  count_pkt: 0 };

        // Setup disconnect timer.
        if let Some(m) = conf.max_time[id as usize % conf.max_time.len()] {
            client.send_in(m, ClientEv::Disconnect(format!("max time {:?}", m)));
        }

        // Initialize json dump target.
        for s in conf.dump_files {
            let s = s.replace("{c}", &format!("{}", id));
            match client.dumps.register(&s) {
                Ok(_) => debug!("C{}: Dump to {}", id, s),
                Err(e) => error!("C{}: Cannot dump to {}: {}", id, s, e),
            }
        }

        // Handle the Tcp and ClientEv streams concurrently.
        let f1 = Self::handle_net(read, cev_s, client.id);
        let f2 = Self::handle_msg(&mut client, cev_r);
        let res = futures::select!(r = f1.fuse() => r, r = f2.fuse() => r);

        // One of the stream ended, cleanup.
        warn!("C{}: Terminating: {:?}", id, res);
        let mut subs = client.subs.lock().await;
        subs.del_all(&client);
        if let Some(sess) = client.session.take() {
            let mut sm = client.sessions.lock().await;
            sm.close(&client, sess);
        }
        client.event_s.send(Event::discon(id))
    }

    /// Send `ClientEv` after a delay, while keeping track of the `ControledTask`s.
    fn send_in(&mut self, delay: Duration, msg: ClientEv) {
        self.tasks.retain(|v| v.is_done());
        self.tasks.push(self.addr.send_at(Instant::now() + delay, msg));
    }

    /// Frame bytes from the socket as Decode MQTT packets, and forwards them as `ClientEv`s.
    async fn handle_net(read: ReadHalf<'_>,
                        mut sx: Sender<ClientEv>,
                        id: ConnId)
                        -> Result<&'static str, Error> {
        let mut frame = FramedRead::new(read, Codec(id));
        while let Some(pkt) = frame.next().await {
            sx.send(ClientEv::PktIn(pkt?)).await.map_err(|e| {
                                                     Error::new(ErrorKind::Other,
                                                               format!("while sending to self: {}",
                                                                       e))
                                                 })?;
        }
        Ok("Connection closed")
    }

    /// Handle `ClientEv`s. This is `Client`'s main event loop.
    async fn handle_msg(client: &mut Client<'_>,
                        mut receiver: Receiver<ClientEv>)
                        -> Result<&'static str, Error> {
        while let Some(msg) = receiver.next().await {
            match msg {
                ClientEv::PktIn(p) => client.handle_pkt_in(p).await?,
                ClientEv::PktOut(p) => client.handle_pkt_out(p).await?,
                ClientEv::Publish(q, p) => client.handle_publish(q, p).await?,
                ClientEv::CheckQos => client.handle_check_qos(Instant::now()).await?,
                ClientEv::Replaced(i, c) => client.handle_replaced(i, c)?,
                ClientEv::Disconnect(r) => client.handle_disconnect(r)?,
            }
        }
        Ok("No more messages")
    }

    /// Receive packets from client.
    async fn handle_pkt_in(&mut self, pkt: Packet) -> Result<(), Error> {
        info!("C{}: receive Packet::{:?}", self.id, pkt);
        self.event_s.send(Event::recv(self.id, pkt.clone()));
        self.dumps.dump(self.id, "C", &pkt).await;
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
                    self.addr.send(ClientEv::PktOut(connack(false, code))).await;
                    return Err(Error::new(ErrorKind::ConnectionAborted, desc));
                }
                // Load session
                let mut sm = self.sessions.lock().await;
                let mut sess = sm.open(&self, c.clean_session).await;
                let isold = sess.cons > 0;
                debug!("C{}: loaded {} session {:?}",
                       self.id,
                       if isold { "old" } else { "new" },
                       sess);
                sess.cons += 1;
                let mut subs = self.subs.lock().await;
                for (topic, qos) in sess.subs.iter() {
                    subs.add(&topic, *qos, self.id, self.addr.clone());
                }
                self.session = Some(sess);
                // Handle QoS
                self.addr.send(ClientEv::CheckQos).await;
                // Send connack
                self.addr.send(ClientEv::PktOut(connack(isold, ConnectReturnCode::Accepted))).await;
            },
            // FIXME: Use our own error type, and let this one log as INFO rather than ERROR
            (Packet::Disconnect, true) => {
                self.conn = false;
                return Err(Error::new(ErrorKind::ConnectionAborted, "Disconnect"));
            },
            // Ping request
            (Packet::Pingreq, true) => self.addr.send(ClientEv::PktOut(pingresp())).await,
            // Ping response
            (Packet::Pingresp, true) => (),
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
                if let Some(subs) = self.subs.lock().await.get(&p.topic_name) {
                    for s in subs.values() {
                        s.addr.send(ClientEv::Publish(s.qos, p.clone())).await;
                    }
                }
                match p.qospid {
                    QosPid::AtMostOnce => (),
                    QosPid::AtLeastOnce(pid) => {
                        self.send_in(self.ack_delay, ClientEv::PktOut(puback(pid)))
                    },
                    QosPid::ExactlyOnce(_) => panic!("ExactlyOnce not supported yet"),
                }
            },
            // Subscription request
            (Packet::Subscribe(Subscribe { pid, topics }), true) => {
                let mut subs = self.subs.lock().await;
                let sess = self.session.as_mut().expect("unwrap session");
                let mut codes = Vec::new();
                for SubscribeTopic { topic_path, qos } in topics {
                    assert_ne!(QoS::ExactlyOnce, qos, "ExactlyOnce not supported yet");
                    subs.add(&topic_path, qos, self.id, self.addr.clone());
                    sess.subs.insert(topic_path.clone(), qos);
                    codes.push(SubscribeReturnCodes::Success(qos));
                }
                drop(subs);
                self.send_in(self.ack_delay, ClientEv::PktOut(suback(pid, codes)));
            },
            (other, _) => {
                return Err(Error::new(ErrorKind::InvalidData, format!("Unhandled {:?}", other)))
            },
        }
        if self.count_pkt >= self.max_pkt {
            let reason = format!("max packets {:?} {:?}", self.max_pkt, self.max_pkt_delay);
            match self.max_pkt_delay {
                Some(d) => self.send_in(d, ClientEv::Disconnect(reason)),
                None => self.addr.send(ClientEv::Disconnect(reason)).await,
            }
        }
        Ok(())
    }

    /// Send packets to client.
    async fn handle_pkt_out(&mut self, pkt: Packet) -> Result<(), Error> {
        info!("C{}: send Packet::{:?}", self.id, pkt);
        self.event_s.send(Event::send(self.id, pkt.clone()));
        self.dumps.dump(self.id, "S", &pkt).await;
        self.writer.send(pkt).await?;
        self.writer.flush().await.map_err(|e| e.into())
    }

    async fn handle_publish(&mut self, qos: QoS, p: Publish) -> Result<(), Error> {
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
                self.qos1_check = Some(self.addr.send_at(deadline, ClientEv::CheckQos));
            }
        }
        self.handle_pkt_out(pkt).await
    }

    /// Go trhough self.session.qos1 and resend any timedout packets.
    async fn handle_check_qos(&mut self, reftime: Instant) -> Result<(), Error> {
        let sess = self.session.as_mut().expect("unwrap session");
        trace!("C{}: check Qos acks {:?}", self.id, sess.qos1);
        let addr = self.addr.clone();
        // FIXME: Should be able to just move pkt.
        for (pid, (deadline, pkt)) in sess.qos1.iter() {
            if *deadline > reftime {
                warn!("C{}: Timeout receiving ack {:?}, resending packet", self.id, pid);
                addr.send(ClientEv::PktOut(pkt.clone())).await
            }
        }
        sess.qos1.retain(|_pid, (deadline, _pkt)| *deadline <= reftime);
        if let Some(deadline) = sess.qos1.values().map(|(d, _)| d).min() {
            self.qos1_check = Some(self.addr.send_at(*deadline, ClientEv::CheckQos));
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
