use env_logger::builder;
use log::*;
use mqttest::{Conf, Mqttest};
use std::time::Duration;
use structopt::{clap::AppSettings::*, StructOpt};

#[derive(Debug)]
struct OptDuration(Option<Duration>);
impl std::str::FromStr for OptDuration {
    type Err = std::num::ParseIntError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match &s[0..1] {
            "-" => Ok(Self(None)),
            _ => Ok(Self(Some(Duration::from_millis(s.parse()?)))),
        }
    }
}
#[derive(Debug)]
struct OptU64(Option<u64>);
impl std::str::FromStr for OptU64 {
    type Err = std::num::ParseIntError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match &s[0..1] {
            "-" => Ok(Self(None)),
            _ => Ok(Self(Some(s.parse()?))),
        }
    }
}

#[derive(StructOpt, Debug)]
#[structopt(name = "mqttest", help_message = "Prints help information. Use --help for more details.", global_settings = &[ColoredHelp, DeriveDisplayOrder, UnifiedHelpMessage])]
struct Opt {
    /// Increase log level (info -> debug -> trace). Shorthand for "--log debug" or "--log trace".
    #[structopt(short = "v", parse(from_occurrences))]
    verbose: usize,
    /// Detailed log level. See https://docs.rs/env_logger/0.6.2/env_logger/index.html.
    #[structopt(long = "log", default_value = "")]
    log: String,
    /// Ports to listen on (will only listen on the first successful one).
    #[structopt(short = "p",
                value_name = "PORT",
                use_delimiter = true,
                value_delimiter = "-",
                max_values = 2,
                default_value = "1883-2000")]
    ports: Vec<u16>,
    /// Resend packet during connection if ack takes longer than this.
    ///
    /// This only concerns resending during a live connection: resending at connection start (if
    /// session was restored) always happens immediately. Use "-" to disable resending.
    ///
    /// The second value is for MQTT5 clients. MQTT5 forbids resending during connection, only set
    /// an MQTT5 value for testing purposes. MQTT3 doesn't specify a behaviour, but many
    /// client/servers do resend non-acked packets during connection.
    #[structopt(long = "ack-timeouts",
                value_name = "ms",
                default_value = "5000,-",
                use_delimiter = true,
                min_values = 1,
                max_values = 2)]
    ack_timeouts: Vec<OptDuration>,
    /// Wait before sending publish and subscribe acks.
    #[structopt(long = "ack-delay", value_name = "ms", default_value = "0")]
    ack_delay: u64,
    /// Dump packets to file.
    ///
    /// The filename can contain a `{c}` placeholder that will be replaced by the connection
    /// number. The dump format is json and corresponds to `pub struct mqttest::DumpMeta` in the
    /// rust lib.
    #[structopt(long = "dump", value_name = "DUMP")]
    dumps: Vec<String>,
    /// Decode command for publish payload.
    ///
    /// The argument should be a command that reads raw payload from stdin, and writes the
    /// corresponding utf8/json to stdout. If decoding fails, it should output diagnostics to stderr
    /// and exit with a non-zero value.
    #[structopt(long = "dump-decode", value_name = "CMD")]
    dump_decode: Option<String>,
    /// Be stricter about optional behaviours.
    ///
    /// [MQTT-3.1.3-5]: Reject client_ids longer than 23 chars or not matching [0-9a-zA-Z].
    /// [MQTT-3.1.3-6]: Reject empty client_ids.
    #[structopt(long = "strict")]
    strict: bool,
    /// Reject clients whose client_id does not start with this prefix
    #[structopt(long = "idprefix", default_value = "")]
    idprefix: String,
    /// Reject clients who didn't suppliy this username:password
    ///
    /// Note that MQTT allows passwords to be binary but we only accept UTF-8.
    #[structopt(long = "userpass")]
    userpass: Option<String>,
    /// Only accept up to N connections, and stop the server afterwards.
    #[structopt(long = "max-connect", short = "c", value_name = "count", default_value = "-")]
    max_connect: OptU64,
    /// Disconnect the client after receiving that many packets.
    ///
    /// Use "-" for no disconnect. Multiple values apply to subsequent connections.
    /// This just closes the TCP stream, without sending an mqtt disconnect packet.
    #[structopt(long = "max-pkt", value_name = "count", default_value = "-", use_delimiter = true)]
    max_pkt: Vec<OptU64>,
    /// Disconnect the client after a certain time.
    ///
    /// Use "-" for no disconnect. Multiple values apply to subsequent connections.
    /// This just closes the TCP stream, without sending an mqtt disconnect packet.
    #[structopt(long = "max-time", value_name = "ms", default_value = "-", use_delimiter = true)]
    max_time: Vec<OptDuration>,
    /// How long to retain the session after disconnection.
    ///
    /// Use "-" to use the client-specified value: CONNECT.clean_session (MQTT3) or
    /// CONNECT.session_expiry (MQTT5). Multiple values apply to subsequent connections.
    #[structopt(long = "session-expire",
                value_name = "ms",
                default_value = "-",
                use_delimiter = true)]
    sess_expire: Vec<OptDuration>,
    //    /// Warn/Error if client reuses an MQTT id from the previous N packets.
    //    #[structopt(long = "oldid",
    //                value_name = "W/E",
    //                use_delimiter = true,
    //                value_delimiter = "/",
    //                max_values = 2,
    //                default_value = "10/1")]
    //    oldid: Vec<usize>,
    //    /// Send more acks than expected
    //    #[structopt(long = "ackdup", value_name = "N", use_delimiter = true, default_value = "0")]
    //    ackduplicate: Vec<usize>,
}

// FIXME: Define exit codes
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opt = Opt::from_args();
    builder().filter_level(match opt.verbose {
                               0 => LevelFilter::Info,
                               1 => LevelFilter::Debug,
                               _ => LevelFilter::Trace,
                           })
             .format_timestamp_micros()
             .parse_filters(&opt.log)
             .init();
    trace!("Cli {:?}", opt);
    let conf = Conf::new().ports(opt.ports[0]..=opt.ports[opt.ports.len() - 1])
                          .dumpfiles(opt.dumps)
                          .dump_decode(opt.dump_decode)
                          .ack_timeouts(opt.ack_timeouts[0].0,
                                        opt.ack_timeouts.get(1).unwrap_or(&OptDuration(None)).0)
                          .ack_delay(Duration::from_millis(opt.ack_delay))
                          .strict(opt.strict)
                          .idprefix(opt.idprefix)
                          .userpass(opt.userpass)
                          .max_connect(opt.max_connect.0)
                          .max_pkt(opt.max_pkt.into_iter().map(|d| d.0).collect())
                          .max_time(opt.max_time.into_iter().map(|d| d.0).collect())
                          .sess_expire(opt.sess_expire.into_iter().map(|d| d.0).collect());
    let mut server = Mqttest::start(conf).await?;
    info!("Listening on {:?}", server.port);
    server.run().await;
    info!("done");
    Ok(())
}
