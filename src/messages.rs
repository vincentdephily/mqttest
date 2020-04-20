//! This module contains definition of messages used internally between the main server loop and the
//! client tasks, as well as in the public API between the server struct and the caller.

use crate::{mqtt::*, client::SessionData};
use log::*;
use tokio::{net::TcpStream,sync::oneshot,
            sync::mpsc::{error::TrySendError, Sender}};

/// Id of a client connection, for debug and indexing purposes.
///
/// This monotonically-increasing integer can be seen in logs, dumps and events, and can be used in
/// commands.
pub type ConnId = usize;

/// Commands that can be sent to a running server.
// TODO code docs
#[derive(Debug)]
pub enum Command {
    //SendBytes(ConnId, &[u8]),
    //SendPacket(ConnId, mqttrs::Packet),
    //EnableAck(ConnId),
    //DisableAck(ConnId),
    /// Disconnect the given client
    Disconnect(ConnId),
    //PauseListen,
    //ResumeListen,
    /// Stop the whole server
    Stop,
}

/// Events sent by the server to the caller.
// TODO code docs
#[derive(Debug, PartialEq)]
pub enum Event {
    /// New TCP connection from a client
    ClientStart(ConnId),
    /// Client connection closed
    ClientStop(ConnId),
    //Packet(DumpMeta),
    /// Whole server stopped
    ServerStop,
}

/// Logging and short-circuiting wrapper for `Option<Sender<T>>`.
///
/// * warn!() when the channel is full
/// * debug!() once and never send again if there is no Receiver
// TODO: Use std::intrinsics::type_name
pub(crate) fn option_send<T>(sender: &mut Option<Sender<T>>, event: T, typ: &str) where T: std::fmt::Debug {
    if let Some(s) = sender {
        match s.try_send(event) {
            Err(TrySendError::Closed(_)) => {
                debug!("No receiver for {} messages", typ);
                *sender = None;
            },
            Err(TrySendError::Full(e)) => {
                warn!("{} receiver too slow, dropping {:?}", typ, e)
            },
            Ok(()) => (),
        }
    }
}
/// Events for the client task.
#[derive(Debug)]
pub(crate) enum ClientEv {
    PktIn(Packet),
    PktOut(Packet),
    Publish(QoS, Publish),
    CheckQos,
    Replaced(ConnId, oneshot::Sender<SessionData>),
    Disconnect(String),
}

/// Events for the main server loop.
#[derive(Debug)]
pub(crate) enum MainEv {
    Accept(Result<TcpStream, std::io::Error>),
    Finish(usize),
    Cmd(Command),
}

