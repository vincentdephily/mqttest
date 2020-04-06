//! Using `mqttest` as a crate to unittest your client :
//!  * Start the [tokio] runtime (and optionally a logger).
//!  * Create a [`Conf`] struct with the desired behavior for your test.
//!  * Get an [`Mqttest`] struct by [start()]ing it.
//!  * Connect your client to the server port.
//!  * Wait for your client(s) and/or the server to stop.
//!  * Assert the `Vec<`[ConnInfo]`>` results.
//!
//! You can find practical examples in the [`test.rs`] file.
//!
//! [tokio]: https://tokio.rs/
//! [`Conf`]: struct.Conf.html
//! [`Mqttest`]: struct.Mqttest.html
//! [ConnInfo]: struct.ConnInfo.html
//! [start()]: struct.Mqttest.html#method.start
//! [`test.rs`]: ../src/mqttest/test.rs.html

mod client;
mod dump;
mod mqtt;
mod pubsub;
mod session;
#[cfg(any(test, doc))]
mod test;

use crate::{client::*, dump::*, pubsub::*, session::*};
use futures::{lock::Mutex, prelude::*};
use log::*;
use std::{io::{Error, ErrorKind},
          ops::RangeInclusive,
          sync::Arc,
          time::Duration};
use tokio::{net::TcpListener, spawn, task::JoinHandle};

pub use dump::*;

/// Duration longer than program's lifetime (not quite `u64::MAX` so we can add `Instant::now()`).
const FOREVER: Duration = Duration::from_secs(60 * 60 * 24 * 365);
/// Zero Duration, to save on typing.
const ASAP: Duration = Duration::from_secs(0);

#[derive(Debug, Clone)]
/// Specify server behavior.
///
/// Many methods take a `Vec` of values, where the `Nth mod len()` value is used for the `Nth`
/// connection. For example, `.max_pkt(vec![None,Some(3)])` applies the limit to every second
/// connection.
pub struct Conf {
    ports: RangeInclusive<u16>,
    ack_timeouts: (Option<Duration>, Option<Duration>),
    ack_delay: Duration,
    dumps: Vec<String>,
    dump_prefix: String,
    dump_decode: Option<String>,
    strict: bool,
    idprefix: String,
    userpass: Option<String>,
    max_connect: usize,
    max_pkt: Vec<Option<usize>>,
    max_pkt_delay: Option<Duration>,
    max_time: Vec<Option<Duration>>,
    sess_expire: Vec<Option<Duration>>,
}
impl Conf {
    /// Initialize a default config
    pub fn new() -> Self {
        Conf { ports: 1883..=2000,
               dumps: vec![],
               dump_prefix: String::new(),
               dump_decode: None,
               ack_timeouts: (Some(Duration::from_secs(5)), None),
               ack_delay: ASAP,
               strict: false,
               idprefix: "".into(),
               userpass: None,
               max_connect: std::usize::MAX,
               max_pkt: vec![None],
               max_pkt_delay: None,
               max_time: vec![None],
               sess_expire: vec![None] }
    }
    /// Range of ports to try to listen on, stopping at the first successful one) (defaults to
    /// `1883..=2000`)
    pub fn ports(mut self, ports: RangeInclusive<u16>) -> Self {
        self.ports = ports;
        self
    }
    /// Dump packets to file.
    ///
    /// The filename can contain a `{c}` placeholder that will be replaced by the connection
    /// number. The dump format is json-serialized [`DumpMeta`] struct.
    ///
    /// [`DumpMeta`]: struct.DumpMeta.html
    pub fn dump_files(mut self, v: Vec<String>) -> Self {
        self.dumps.extend(v);
        self
    }
    pub fn dump_prefix(mut self, s: String) -> Self {
        self.dump_prefix = s;
        self
    }
    /// Decode command for publish payload.
    ///
    /// The argument should be a command that reads raw payload from stdin, and writes the
    /// corresponding utf8/json to stdout. If decoding fails, it should output diagnostics to stderr
    /// and exit with a non-zero value.
    pub fn dump_decode(mut self, s: Option<String>) -> Self {
        self.dump_decode = s;
        self
    }
    /// Resend packet during connection if ack takes longer than this (defaults to 5s).
    ///
    /// This only concerns resending during a live connection: resending at connection start (if
    /// session was restored) always happens immediately.
    ///
    /// The second value is for MQTT5 clients. MQTT5 forbids resending during connection, only set
    /// an MQTT5 value for testing purposes. MQTT3 doesn't specify a behaviour, but many
    /// client/servers do resend non-acked packets during connection.
    pub fn ack_timeouts(mut self, mqtt3: Option<Duration>, mqtt5: Option<Duration>) -> Self {
        self.ack_timeouts = (mqtt3, mqtt5);
        self
    }
    /// Delay before sending publish and subscribe acks.
    pub fn ack_delay(mut self, d: Duration) -> Self {
        self.ack_delay = d;
        self
    }
    /// Be stricter about optional MQTT behaviours.
    ///
    /// * [MQTT-3.1.3-5]: Reject client_ids longer than 23 chars or not matching [0-9a-zA-Z].
    /// * [MQTT-3.1.3-6]: Reject empty client_ids.
    ///
    /// [MQTT-3.1.3-5]: http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718031
    /// [MQTT-3.1.3-6]: http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718031
    pub fn strict(mut self, strict: bool) -> Self {
        self.strict = strict;
        self
    }
    /// Reject clients whose client_id does not start with this prefix.
    pub fn idprefix(mut self, s: String) -> Self {
        self.idprefix = s;
        self
    }
    /// Reject clients who didn't suppliy this username:password
    ///
    /// Note that MQTT allows passwords to be binary but we only accept UTF-8.
    // TODO: accept binary passwords
    pub fn userpass(mut self, s: Option<String>) -> Self {
        self.userpass = s;
        self
    }
    /// Only accept up to N connections, and stop the server afterwards.
    pub fn max_connect(mut self, c: Option<usize>) -> Self {
        self.max_connect = c.unwrap_or(std::usize::MAX);
        self
    }
    /// Disconnect the Nth client after receiving that many packets.
    ///
    /// This just closes the TCP stream, without sending an mqtt disconnect packet.
    // TODO: Add an option for clean disconnect
    pub fn max_pkt(mut self, d: Vec<Option<usize>>) -> Self {
        self.max_pkt = d;
        self
    }
    /// Delay before max-pkt disconnection.
    ///
    /// Useful if you want to receive the server response before disconnection.
    pub fn max_pkt_delay(mut self, d: Option<Duration>) -> Self {
        self.max_pkt_delay = d;
        self
    }
    /// Disconnect the Nth client after a certain time.
    ///
    /// This just closes the TCP stream, without sending an mqtt disconnect packet.
    pub fn max_time(mut self, d: Vec<Option<Duration>>) -> Self {
        self.max_time = d;
        self
    }
    /// How long is the Nth session retained after disconnection.
    ///
    /// If None, use client-specified behaviour (clean_session in MQTT3, session expiry in MQTT5).
    pub fn sess_expire(mut self, e: Vec<Option<Duration>>) -> Self {
        self.sess_expire = e;
        self
    }
}

/// Listen on the first available TCP port.
async fn listen(ports: &RangeInclusive<u16>) -> Result<(u16, TcpListener), Error> {
    for p in *ports.start()..=*ports.end() {
        match TcpListener::bind(&format!("127.0.0.1:{}", p)).await {
            Ok(l) => {
                info!("Listening on 127.0.0.1:{}", p);
                return Ok((p, l));
            },
            Err(e) => trace!("Listen on 127.0.0.1:{}: {}", p, e),
        }
    }
    let s = format!("Listen failed on 127.0.0.1::{:?} (raise log level for details)", ports);
    Err(Error::new(ErrorKind::Other, s))
}

/// Handle to a running server.
pub struct Mqttest {
    /// Tcp port that the server is listening on.
    pub port: u16,
    ///
    pub fut: JoinHandle<Vec<ConnInfo>>,
}
impl Mqttest {
    /// Initialize a server with the given config, and start handling connections.
    ///
    /// As soon as this function returns, the server is ready to accept connections. If the server
    /// is configured with a stop condition, you can wait for it using `Mqttest.fut`.
    // TODO: make sure that droping the Mqttest struct terminates all futures
    pub async fn start(conf: Conf) -> Result<Mqttest, Error> {
        debug!("Start {:?}", conf);
        let (port, mut listener) = listen(&conf.ports).await?;

        let fut = spawn(async move {
            let subs = Arc::new(Mutex::new(Subs::new()));
            let sess = Arc::new(Mutex::new(Sessions::new()));
            let dumps = Dump::new(&conf.dump_decode, &conf.dump_prefix);
            let mut conns: Vec<ConnInfo> = Vec::new();
            let mut jh = Vec::new();
            while let Some(s) = listener.incoming().next().await {
                trace!("New connection {:?}", s);
                match s {
                    Ok(socket) => {
                        conns.push(ConnInfo {});
                        jh.push(spawn(Client::start(conns.len() - 1,
                                                    socket,
                                                    subs.clone(),
                                                    sess.clone(),
                                                    dumps.clone(),
                                                    conf.clone())));
                        if conns.len() >= conf.max_connect {
                            break;
                        }
                    },
                    Err(e) => error!("Failed to accept socket: {:?}", e),
                };
            }
            // FIXME: Should try_join() inside the loop to avoid growing `jh` too much.
            info!("Accepted {} connections, waiting for them to finish", conns.len());
            for h in jh {
                h.await.expect("Client finished abnormally");
            }
            conns
        });
        Ok(Mqttest { port, fut })
    }
}

/// Statistics and packet dumps collected about one connection
///
/// Currently just an empty placeholder, but you can still deduce the number of connections from the
/// enclosing `Vec`.
pub struct ConnInfo {}
