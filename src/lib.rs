use crate::{client::*, pubsub::*};
use futures::{Future, stream::Stream};
use log::*;
use std::{io::{Error, ErrorKind},
          ops::Range,
          sync::{Arc, RwLock},
          time::Duration};
use tokio::net::TcpListener;

mod client;
mod mqtt;
mod pubsub;

fn listen(ports: Range<u16>) -> Result<(u16, TcpListener), Error> {
    for p in ports.clone() {
        match TcpListener::bind(&format!("127.0.0.1:{}", p).parse().unwrap()) {
            Ok(l) => return Ok((p, l)),
            Err(e) => trace!("Listen on 127.0.0.1:{}: {}", p, e),
        }
    }
    let s = format!("Listen failed on 127.0.0.1::{:?} (raise log level for more info)", ports);
    Err(Error::new(ErrorKind::Other, s))
}

pub fn start(ports: Range<u16>,
             ack_timeout: Duration)
             -> Result<(u16, impl Future<Item = (), Error = ()>), Error> {
    let (port, listener) = listen(ports)?;
    info!("Listening on {:?}", port);
    let subs = Arc::new(RwLock::new(Subs::new()));
    let mut id = 0;
    let f = listener.incoming()
                    .map_err(|e| error!("Failed to accept socket: {:?}", e))
                    .for_each(move |socket| {
                        tokio::spawn(Client::init(id, socket, subs.clone(), ack_timeout));
                        id += 1;
                        Ok(())
                    });
    Ok((port, f)) //FIXME: Include a cancellation handle.
}
