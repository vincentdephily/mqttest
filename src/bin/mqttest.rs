use env_logger::builder;
use log::*;
use mqttest::{start, Conf};
use structopt::{clap::AppSettings::*, StructOpt};


#[derive(StructOpt)]
#[structopt(name = "mqttest",
            global_settings = &[ColoredHelp, DeriveDisplayOrder, InferSubcommands, DisableHelpSubcommand, VersionlessSubcommands])]
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
    /// How long to wait for QoS1/2 acks.
    #[structopt(long = "ack_timeout", value_name = "ms", default_value = "1000")]
    ack_timeout: u64,
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
             .default_format_timestamp_nanos(true)
             .parse_filters(&opt.log)
             .init();
    match start(Conf::new().ports(opt.ports[0]..=opt.ports[opt.ports.len() - 1])
                           .dumpfiles(opt.dumps)
                           .ack_timeout(opt.ack_timeout)
                           .strict(opt.strict)
                           .idprefix(opt.idprefix)
                           .userpass(opt.userpass))
    {
        Ok((_port, server)) => {
            tokio::run(server);
            info!("done");
        },
        Err(e) => error!("Couldn't initialize server: {}", e),
    }
}
