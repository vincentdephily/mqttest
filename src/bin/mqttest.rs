use env_logger::builder;
use log::*;
use mqttest::{start, Conf};
use std::time::Duration;
use structopt::{clap::AppSettings::*, StructOpt};

#[derive(Debug)]
struct OptDuration(Option<Duration>);
impl std::str::FromStr for OptDuration {
    type Err = std::num::ParseIntError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match &s[0..1] {
            "-" => Ok(OptDuration(None)),
            _ => Ok(OptDuration(Some(Duration::from_millis(s.parse()?)))),
        }
    }
}

#[derive(StructOpt, Debug)]
#[structopt(name = "mqttest", global_settings = &[ColoredHelp, DeriveDisplayOrder, UnifiedHelpMessage])]
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
    ///// Resend QoS1/2 packet during connection if ack takes too long.
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
    /// Disconnect the client after receiving that many packets.
    ///
    /// This just closes the TCP stream, without sending an mqtt disconnect packet.
    #[structopt(long = "max-pkt", value_name = "count", default_value = "1000000")]
    max_pkt: usize,
    /// Disconnect the client after that many milliseconds.
    ///
    /// This just closes the TCP stream, without sending an mqtt disconnect packet.
    #[structopt(long = "max-time", value_name = "ms", default_value = "3600000")]
    max_time: u64,
    //    /// Warn/Error if client reuses an MQTT id from the previous N packets.
    //    #[structopt(long = "oldid",
    //                value_name = "W/E",
    //                use_delimiter = true,
    //                value_delimiter = "/",
    //                max_values = 2,
    //                default_value = "10/1")]
    //    oldid: Vec<usize>,
    //    /// Wait before acking
    //    #[structopt(long = "ackdelay",
    //                value_name = "ms",
    //                use_delimiter = true,
    //                default_value = "0",
    //                parse(try_from_str = "from_ms"))]
    //    ackdelay: Vec<Duration>,
    //    /// Send more acks than expected
    //    #[structopt(long = "ackdup", value_name = "N", use_delimiter = true, default_value = "0")]
    //    ackduplicate: Vec<usize>,
}

// FIXME: Define exit codes
fn main() {
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
    match start(Conf::new().ports(opt.ports[0]..=opt.ports[opt.ports.len() - 1])
                           .dumpfiles(opt.dumps)
                           .ack_timeouts(opt.ack_timeouts[0].0,
                                         opt.ack_timeouts.get(1).unwrap_or(&OptDuration(None)).0)
                           .ack_delay(Duration::from_millis(opt.ack_delay))
                           .strict(opt.strict)
                           .idprefix(opt.idprefix)
                           .userpass(opt.userpass)
                           .max_pkt(opt.max_pkt)
                           .max_time(Duration::from_millis(opt.max_time)))
    {
        Ok((_port, server)) => {
            tokio::run(server);
            info!("done");
        },
        Err(e) => error!("Couldn't initialize server: {}", e),
    }
}
