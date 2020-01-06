use crate::{client::*, dump::*, pubsub::*, session::*};
use futures::{stream::Stream, Future};
use log::*;
use std::{io::{Error, ErrorKind},
          ops::RangeInclusive,
          sync::{Arc, Mutex, RwLock},
          time::Duration};
use tokio::net::TcpListener;

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

#[derive(Debug)]
pub struct Conf {
    ports: RangeInclusive<u16>,
    ack_timeouts: (Option<Duration>, Option<Duration>),
    ack_delay: Duration,
    dumps: Vec<String>,
    dump_decode: Option<String>,
    strict: bool,
    idprefix: String,
    userpass: Option<String>,
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

fn listen(ports: &RangeInclusive<u16>) -> Result<(u16, TcpListener), Error> {
    for p in ports.clone().into_iter() {
        match TcpListener::bind(&format!("127.0.0.1:{}", p).parse().unwrap()) {
            Ok(l) => return Ok((p, l)),
            Err(e) => trace!("Listen on 127.0.0.1:{}: {}", p, e),
        }
    }
    let s = format!("Listen failed on 127.0.0.1::{:?} (raise log level for details)", ports);
    Err(Error::new(ErrorKind::Other, s))
}

pub fn start(conf: Conf) -> Result<(u16, impl Future<Item = (), Error = ()>), Error> {
    debug!("Start {:?}", conf);
    let (port, listener) = listen(&conf.ports)?;
    info!("Listening on {:?}", port);
    let subs = Arc::new(RwLock::new(Subs::new()));
    let sess = Arc::new(Mutex::new(Sessions::new()));
    let dumps = Dump::new(&conf.dump_decode);
    let mut id = 0;
    let f = listener.incoming()
                    .map_err(|e| error!("Failed to accept socket: {:?}", e))
                    .for_each(move |socket| {
                        tokio::spawn(Client::init(id,
                                                  socket,
                                                  subs.clone(),
                                                  sess.clone(),
                                                  dumps.clone(),
                                                  &conf));
                        id += 1;
                        Ok(())
                    });
    Ok((port, f)) //FIXME: Include a cancellation handle.
}
