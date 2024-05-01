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
//! [`test.rs`]: https://github.com/vincentdephily/mqttest/blob/master/src/test.rs

mod client;
mod dump;
mod messages;
mod mqtt;
mod pubsub;
mod session;
#[cfg(test)]
mod test;

use crate::{client::*, dump::*, messages::*, pubsub::*, session::*};
use futures::lock::Mutex;
use log::*;
use std::{collections::HashMap, io::{Error, ErrorKind}, ops::RangeInclusive, sync::Arc, time::{Duration, Instant}};
use tokio::{net::TcpListener, spawn, sync::mpsc::*, task::JoinHandle};

pub use dump::*;
pub use messages::*;

/// Duration longer than program's lifetime (not quite `u64::MAX` so we can add `Instant::now()`).
const FOREVER: Duration = Duration::from_secs(60 * 60 * 24 * 365);
/// Zero Duration, to save on typing.
const ASAP: Duration = Duration::from_secs(0);

/// Convenience type to save on typing an `Option<Duration>`.
///
/// ```
/// # use mqttest::OptMsDuration;
/// # use std::time::Duration;
/// fn fmt_opt_duration(od: impl Into<OptMsDuration>) -> String {
///     format!("{:?}", od.into().0)
/// }
/// assert_eq!("None", &fmt_opt_duration(None));
/// assert_eq!("Some(0ns)", &fmt_opt_duration(Some(Duration::from_millis(0))));
/// assert_eq!("Some(1s)", &fmt_opt_duration(Duration::from_secs(1)));
/// assert_eq!("Some(2s)", &fmt_opt_duration(2000));
/// ```
pub struct OptMsDuration(pub Option<Duration>);
impl From<Duration> for OptMsDuration {
    fn from(d: Duration) -> Self {
        Self(Some(d))
    }
}
impl From<u64> for OptMsDuration {
    fn from(u: u64) -> Self {
        Self(Some(Duration::from_millis(u)))
    }
}
impl From<Option<Duration>> for OptMsDuration {
    fn from(od: Option<Duration>) -> Self {
        Self(od)
    }
}

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
    dump_files: Vec<String>,
    dump_prefix: String,
    dump_decode: Option<String>,
    strict: bool,
    idprefix: String,
    userpass: Option<String>,
    max_connect: usize,
    max_runtime: Option<Duration>,
    max_pkt: Vec<Option<usize>>,
    max_pkt_delay: Option<Duration>,
    max_time: Vec<Option<Duration>>,
    sess_expire: Vec<Option<Duration>>,
    event_filter: HashMap<EventKind, bool>,
    event_default: bool,
    result_buffer: usize,
}
impl Conf {
    /// Initialize a default config
    pub fn new() -> Self {
        Conf { ports: 1883..=2000,
               dump_files: vec![],
               dump_prefix: String::new(),
               dump_decode: None,
               ack_timeouts: (Some(Duration::from_secs(5)), None),
               ack_delay: ASAP,
               strict: false,
               idprefix: "".into(),
               userpass: None,
               max_connect: std::usize::MAX,
               max_runtime: None,
               max_pkt: vec![None],
               max_pkt_delay: None,
               max_time: vec![None],
               sess_expire: vec![None],
               event_filter: HashMap::new(),
               event_default: true,
               result_buffer: 1000 }
    }
    /// Range of ports to try to listen on, stopping at the first successful one) (defaults to
    /// `1883..=2000`)
    pub fn ports(mut self, ports: RangeInclusive<u16>) -> Self {
        self.ports = ports;
        self
    }
    /// Dump packets to files.
    ///
    /// The filename can contain a `{c}` placeholder that will be replaced by the connection
    /// number. The dump format is json-serialized [`DumpMeta`] struct.
    ///
    /// [`DumpMeta`]: struct.DumpMeta.html
    // FIXME: PathBuf
    pub fn dump_files(mut self, vs: Vec<impl Into<String>>) -> Self {
        self.dump_files = vs.into_iter().map(|s| s.into()).collect();
        self
    }
    // FIXME: PathBuf
    pub fn dump_prefix(mut self, s: impl Into<String>) -> Self {
        self.dump_prefix = s.into();
        self
    }
    /// Decode command for publish payload.
    ///
    /// The argument should be a command that reads raw payload from stdin, and writes the
    /// corresponding utf8/json to stdout. If decoding fails, it should output diagnostics to stderr
    /// and exit with a non-zero value.
    pub fn dump_decode(mut self, s: impl Into<Option<String>>) -> Self {
        self.dump_decode = s.into();
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
    pub fn ack_timeouts(mut self,
                        mqtt3: impl Into<OptMsDuration>,
                        mqtt5: impl Into<OptMsDuration>)
                        -> Self {
        self.ack_timeouts = (mqtt3.into().0, mqtt5.into().0);
        self
    }
    /// Delay before sending publish and subscribe acks.
    pub fn ack_delay(mut self, d: impl Into<OptMsDuration>) -> Self {
        self.ack_delay = d.into().0.unwrap_or(Duration::from_secs(0));
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
    pub fn idprefix(mut self, s: impl Into<String>) -> Self {
        self.idprefix = s.into();
        self
    }
    /// Reject clients who didn't suppliy this username:password
    ///
    /// Note that MQTT allows passwords to be binary but we only accept UTF-8.
    // TODO: accept binary passwords
    pub fn userpass(mut self, s: impl Into<Option<String>>) -> Self {
        self.userpass = s.into();
        self
    }
    /// Only accept up to N connections, and stop the server after established connections close.
    pub fn max_connect(mut self, c: impl Into<Option<usize>>) -> Self {
        self.max_connect = c.into().unwrap_or(std::usize::MAX);
        self
    }
    /// Stop the server and any existing connections after a certain time.
    ///
    /// This just closes the TCP stream, without sending an mqtt disconnect packet.
    pub fn max_runtime(mut self, d: impl Into<OptMsDuration>) -> Self {
        self.max_runtime = d.into().0;
        self
    }
    /// Disconnect the Nth client after receiving that many packets.
    ///
    /// This just closes the TCP stream, without sending an mqtt disconnect packet.
    // TODO: Add an option for clean disconnect
    pub fn max_pkt(mut self, vou: Vec<impl Into<Option<usize>>>) -> Self {
        self.max_pkt = vou.into_iter().map(|ou| ou.into()).collect();
        self
    }
    /// Delay before max_pkt disconnection.
    ///
    /// Useful if you want to receive the server response before disconnection.
    pub fn max_pkt_delay(mut self, d: impl Into<OptMsDuration>) -> Self {
        self.max_pkt_delay = d.into().0;
        self
    }
    /// Disconnect the Nth client after a certain connection time.
    ///
    /// This just closes the TCP stream, without sending an mqtt disconnect packet.
    pub fn max_time(mut self, vod: Vec<impl Into<OptMsDuration>>) -> Self {
        self.max_time = vod.into_iter().map(|od| od.into().0).collect();
        self
    }
    /// How long is the Nth session retained after disconnection.
    ///
    /// If None, use client-specified behaviour (clean_session in MQTT3, session expiry in MQTT5).
    pub fn sess_expire(mut self, vod: Vec<impl Into<OptMsDuration>>) -> Self {
        self.sess_expire = vod.into_iter().map(|od| od.into().0).collect();
        self
    }
    /// Filter server-sent events: set the default to *ignore* and add an exception to *send* this [`EventKind`].
    ///
    /// [`EventKind`]: enum.EventKind.html
    pub fn event_on(mut self, k: EventKind) -> Self {
        self.event_filter.insert(k, true);
        self.event_default = false;
        self
    }
    /// Filter server-sent events: set the default to *send* and add an exception to *ignore* this [`EventKind`].
    ///
    /// [`EventKind`]: enum.EventKind.html
    pub fn event_off(mut self, k: EventKind) -> Self {
        self.event_filter.insert(k, false);
        self.event_default = true;
        self
    }
    /// Filter server-sent events: set the default to *send* (`true`) or *ignore* (`false`) and remove exceptions.
    ///
    /// [`EventKind`]: enum.EventKind.html
    pub fn event_reset(mut self, def: bool) -> Self {
        self.event_filter = HashMap::new();
        self.event_default = def;
        self
    }
    /// Limit the number of event and stats that will be buffered by the server (defult 1000)
    ///
    /// This limits avoids unbounded memory use if the server receives a lot of packets or
    /// connections. To avoid reaching the limit (a warning will be logged), either raise the limit
    /// here or consume [`Event`] while the server is running.
    ///
    /// [`Event`]: enum.Event.html
    pub fn result_buffer(mut self, c: usize) -> Self {
        self.result_buffer = c;
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

/// Checked send to client task.
async fn send_client(clients: &mut HashMap<usize, Sender<ClientEv>>, idx: usize, msg: ClientEv) {
    trace!("Sending {:?} to {}", msg, idx);
    if match clients.get_mut(&idx) {
        Some(chan) => chan.send(msg).await.is_err(),
        None => true,
    } {
        warn!("Can't send msg to {}", idx)
    }
}

/// Handle to a running server.
pub struct Mqttest {
    /// Tcp port that the server is listening on.
    pub port: u16,
    /// Send a [`Command`] to the running server.
    ///
    /// [`Command`]: enum.Command.html
    pub commands: UnboundedSender<Command>,
    /// Receive [`Event`]s from the running server.
    ///
    /// By default, all [`EventKind`]s are sent. See [`event_on()`]/[`event_off()`]/[`event_reset()`] to configure.
    ///
    /// [`EventKind`]: enum.EventKind.html
    /// [`event_on()`]: struct.Conf.html#method.event_on
    /// [`event_off()`]: struct.Conf.html#method.event_off
    /// [`event_reset()`]: struct.Conf.html#method.event_reset
    /// [`Event`]: enum.Event.html
    pub events: Receiver<Event>,
    /// Handle to the main task
    done: JoinHandle<Finish>,
    /// RAII struct to send a stop command the server
    #[allow(dead_code)]
    sendondrop: SendOnDrop,
}
impl Mqttest {
    /// Initialize a server with the given config, and start handling connections.
    ///
    /// As soon as this function returns, the server is ready to accept connections. If the server
    /// is configured with a stop condition, you can wait for it using `Mqttest.fut`.
    // FIXME: droping the Mqttest struct should terminate all futures
    pub async fn start(conf: Conf) -> Result<Mqttest, Error> {
        debug!("Start {:?}", conf);

        // Basic init
        let (port, listener) = listen(&conf.ports).await?;
        let (cmd_s, mut cmd_r) = unbounded_channel();
        let (event_s, event_r) = channel(conf.result_buffer);
        let event_s = SendEvent::new(event_s, conf.event_filter.clone(), conf.event_default);
        let (mev_s, mut mev_r) = channel(10);
        let max_connect = conf.max_connect;
        let subs = Arc::new(Mutex::new(Subs::new()));
        let sess = Arc::new(Mutex::new(Sessions::new()));
        let dumps = Dump::new(&conf.dump_decode, &conf.dump_prefix);
        let mut id = 0;
        let mut clients = HashMap::new();
        let start_time = Instant::now();

        // Task to accept new connections and forward them to the main event loop
        let mev_s2 = mev_s.clone();
        spawn(async move {
            let mut con = 0;
            while max_connect > con {
                let res = listener.accept().await.map(|(s,_)|s);
                con += 1;
                if mev_s2.send(MainEv::Accept(res)).await.is_err() {
                    trace!("Main task finished, stopping accept task");
                    break;
                }
            }
            info!("Accepted {} connections, waiting for them to finish", max_connect);
        });

        // Task to receive external commands and forward them to the main event loop
        let mev_s3 = mev_s.clone();
        spawn(async move {
            while let Some(c) = cmd_r.recv().await {
                if mev_s3.send(MainEv::Cmd(c)).await.is_err() {
                    trace!("Main task finished, stopping cmd task");
                    break;
                }
            }
            trace!("cmd task finished");
        });

        // Task to kill the server
        if let Some(d) = conf.max_runtime {
            let mev_s4 = mev_s.clone();
            spawn(async move {
                tokio::time::sleep(d).await;
                let _ = mev_s4.send(MainEv::Cmd(Command::Stop)).await;
            });
        }
        let sendondrop = SendOnDrop(mev_s.clone());

        // Main event loop task
        let done = spawn(async move {
            while let Some(ev) = mev_r.recv().await {
                info!("{:?}", ev);
                match ev {
                    MainEv::Accept(Ok(socket)) => {
                        let (cev_s, cev_r) = channel::<ClientEv>(10);
                        clients.insert(id, cev_s.clone());
                        Client::spawn(id, socket, &subs, &sess, &dumps, &conf, &event_s, cev_s,
                                      cev_r, &mev_s);
                        id += 1;
                    },
                    MainEv::Accept(Err(e)) => error!("Failed to accept socket: {:?}", e),
                    MainEv::Cmd(Command::Disconnect(i)) => {
                        send_client(&mut clients,
                                    i,
                                    ClientEv::Disconnect(String::from("Cmd::Disconnect"))).await
                    },
                    MainEv::Cmd(Command::SendPacket(i, p)) => {
                        send_client(&mut clients, i, ClientEv::PktOut(p)).await
                    },
                    MainEv::Cmd(Command::Stop) => {
                        for cev_s in clients.values_mut() {
                            let _ = cev_s.send(ClientEv::Disconnect(String::from("Cmd::Stop")));
                        }
                        break;
                    },
                    MainEv::Finish(idx) => {
                        clients.remove(&idx).expect("Trying to remove non-existent client");
                        if id >= max_connect && clients.is_empty() {
                            break;
                        }
                    },
                }
            }
            trace!("main task finished");
            Finish { elapsed: Instant::now() - start_time, conn_count: id, events: vec![] }
        });

        // Server is ready
        Ok(Mqttest { port, commands: cmd_s, events: event_r, done, sendondrop })
    }

    /// Wait for the server to finish, and retrieve the final stats
    pub async fn finish(mut self) -> Finish {
        let mut stats = self.done.await.expect("failed join");
        while let Ok(e) = self.events.try_recv() {
            stats.events.push(e);
        }
        stats
    }
}

/// Send `MainEv::Cmd(Command::Stop)` when this gets dropped.
struct SendOnDrop(Sender<MainEv>);
impl Drop for SendOnDrop {
    fn drop(&mut self) {
        let _ = self.0.try_send(MainEv::Cmd(Command::Stop));
    }
}

#[derive(Debug)]
/// Statistics and events collected during the server run
// TODO: global and per-conn packet/bytes
pub struct Finish {
    /// Run duration
    pub elapsed: Duration,
    /// Remaining events (that weren't manually received)
    pub events: Vec<Event>,
    /// Total number of connections
    pub conn_count: usize,
}
