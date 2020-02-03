use crate::{client::*, dump::*, pubsub::*, session::*};
use futures::{lock::Mutex, prelude::*};
use log::*;
use std::{io::{Error, ErrorKind},
          ops::RangeInclusive,
          sync::Arc,
          time::Duration};
use tokio::{net::TcpListener, spawn};

mod client;
mod dump;
mod mqtt;
mod pubsub;
mod session;

pub use dump::*;

/// Duration longer than program's lifetime (not quite `u64::MAX` so we can add `Instant::now()`).
const FOREVER: Duration = Duration::from_secs(60 * 60 * 24 * 365);
/// Zero Duration, to save on typing.
const ASAP: Duration = Duration::from_secs(0);

#[derive(Debug, Clone)]
pub struct Conf {
    ports: RangeInclusive<u16>,
    ack_timeouts: (Option<Duration>, Option<Duration>),
    ack_delay: Duration,
    dumps: Vec<String>,
    dump_decode: Option<String>,
    strict: bool,
    idprefix: String,
    userpass: Option<String>,
    max_connect: u64,
    max_pkt: Vec<Option<u64>>,
    max_time: Vec<Option<Duration>>,
    /// How long is the session retained after disconnection.
    ///
    /// If None, use client-specified behaviour (clean_session in MQTT3, session expiry in MQTT5).
    sess_expire: Vec<Option<Duration>>,
}
impl Conf {
    pub fn new() -> Self {
        Conf { ports: 1883..=2000,
               dumps: vec![],
               dump_decode: None,
               ack_timeouts: (Some(Duration::from_secs(5)), None),
               ack_delay: ASAP,
               strict: false,
               idprefix: "".into(),
               userpass: None,
               max_connect: std::u64::MAX,
               max_pkt: vec![None],
               max_time: vec![None],
               sess_expire: vec![None] }
    }
    pub fn ports(mut self, ports: RangeInclusive<u16>) -> Self {
        self.ports = ports;
        self
    }
    pub fn dumpfiles(mut self, v: Vec<String>) -> Self {
        self.dumps.extend(v);
        self
    }
    pub fn dump_decode(mut self, s: Option<String>) -> Self {
        self.dump_decode = s;
        self
    }
    pub fn ack_timeouts(mut self, mqtt3: Option<Duration>, mqtt5: Option<Duration>) -> Self {
        self.ack_timeouts = (mqtt3, mqtt5);
        self
    }
    pub fn ack_delay(mut self, d: Duration) -> Self {
        self.ack_delay = d;
        self
    }
    pub fn strict(mut self, strict: bool) -> Self {
        self.strict = strict;
        self
    }
    pub fn idprefix(mut self, s: String) -> Self {
        self.idprefix = s;
        self
    }
    pub fn userpass(mut self, s: Option<String>) -> Self {
        self.userpass = s;
        self
    }
    pub fn max_connect(mut self, c: Option<u64>) -> Self {
        self.max_connect = c.unwrap_or(std::u64::MAX);
        self
    }
    pub fn max_pkt(mut self, d: Vec<Option<u64>>) -> Self {
        self.max_pkt = d;
        self
    }
    pub fn max_time(mut self, d: Vec<Option<Duration>>) -> Self {
        self.max_time = d;
        self
    }
    pub fn sess_expire(mut self, e: Vec<Option<Duration>>) -> Self {
        self.sess_expire = e;
        self
    }
}

async fn listen(ports: &RangeInclusive<u16>) -> Result<(u16, TcpListener), Error> {
    for p in ports.clone().into_iter() {
        match TcpListener::bind(&format!("127.0.0.1:{}", p)).await {
            Ok(l) => return Ok((p, l)),
            Err(e) => trace!("Listen on 127.0.0.1:{}: {}", p, e),
        }
    }
    let s = format!("Listen failed on 127.0.0.1::{:?} (raise log level for details)", ports);
    Err(Error::new(ErrorKind::Other, s))
}

pub struct Mqttest {
    pub conf: Conf,
    pub port: u16,
    listener: TcpListener,
    subs: Arc<Mutex<Subs>>,
    sess: Arc<Mutex<Sessions>>,
    dumps: Dump,
    conn_id: u64,
}
impl Mqttest {
    /// Initialize a server with the given config, and start listening on socket.
    pub async fn start(conf: Conf) -> Result<Mqttest, Error> {
        debug!("Start {:?}", conf);
        let (port, listener) = listen(&conf.ports).await?;
        let subs = Arc::new(Mutex::new(Subs::new()));
        let sess = Arc::new(Mutex::new(Sessions::new()));
        let dumps = Dump::new(&conf.dump_decode);
        Ok(Mqttest { conf, port, listener, subs, sess, dumps, conn_id: 0 })
    }

    /// Accept and process connections.
    pub async fn run(&mut self) {
        let mut jh = Vec::new();
        for s in self.listener.incoming().next().await {
            trace!("New connection {:?}", s);
            match s {
                Ok(socket) => {
                    jh.push(spawn(Client::start(self.conn_id,
                                                socket,
                                                self.subs.clone(),
                                                self.sess.clone(),
                                                self.dumps.clone(),
                                                self.conf.clone())));
                    self.conn_id += 1;
                    if self.conn_id >= self.conf.max_connect {
                        break;
                    }
                },
                Err(e) => error!("Failed to accept socket: {:?}", e),
            };
        }
        // FIXME: Should try_join() inside the loop to avoid growing `jh` too much.
        info!("Accepted {} connections, waiting for them to finish", self.conn_id);
        for h in jh {
            h.await.expect("Client finished abnormally");
        }
    }
}
