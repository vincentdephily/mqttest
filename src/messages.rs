//! This module contains definition of messages used internally between the main server loop and the
//! client tasks, as well as in the public API between the server struct and the caller.

use crate::{client::SessionData, mqtt::*};
use log::*;
use std::{collections::HashMap, time::Instant};
use tokio::{net::TcpStream,
            sync::{mpsc::{error::TrySendError, Sender},
                   oneshot}};

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
    SendPacket(ConnId, Packet),
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
///
/// Server-sent events contain the creation timestamp. But to simplify test asserts, two `Event`s
/// will still compare equal if either timestamp is `None`.
///
/// ```
/// # use std::time::Instant;
/// # use mqttest::Event;
/// let t = Instant::now();
/// // Actually identical
/// assert_eq!(Event::Conn(None, 42), Event::conn(42));
/// assert_eq!(Event::conn(42).at(t), Event::conn(42).at(t));
/// // One None timestamp
/// assert_eq!(Event::conn(42).now(), Event::conn(42));
/// // Two different timestamps
/// assert_ne!(Event::conn(42).now(), Event::conn(42).now());
/// ```
#[derive(Debug)]
pub enum Event {
    /// New TCP connection from a client
    Conn(Option<Instant>, ConnId),
    /// Client connection closed
    Discon(Option<Instant>, ConnId),
    /// Client received [`Packet`]
    ///
    /// [`Packet`]: ../mqttrs/struct.Packet.html
    Recv(Option<Instant>, ConnId, Packet),
    /// Sent [`Packet`] to Client
    ///
    /// [`Packet`]: ../mqttrs/struct.Packet.html
    Send(Option<Instant>, ConnId, Packet),
    /// Whole server stopped
    Done(Option<Instant>),
}
impl Event {
    pub fn now(self) -> Self {
        self.at(Instant::now())
    }
    pub fn at(self, t: Instant) -> Self {
        match self {
            Self::Conn(_, i) => Self::Conn(Some(t), i),
            Self::Discon(_, i) => Self::Discon(Some(t), i),
            Self::Recv(_, i, p) => Self::Recv(Some(t), i, p),
            Self::Send(_, i, p) => Self::Send(Some(t), i, p),
            Self::Done(_) => Self::Done(Some(t)),
        }
    }
    pub fn conn(i: ConnId) -> Self {
        Self::Conn(None, i)
    }
    pub fn discon(i: ConnId) -> Self {
        Self::Discon(None, i)
    }
    pub fn recv(i: ConnId, p: Packet) -> Self {
        Self::Recv(None, i, p)
    }
    pub fn send(i: ConnId, p: Packet) -> Self {
        Self::Send(None, i, p)
    }
    pub fn done() -> Self {
        Self::Done(None)
    }
    pub fn kind(&self) -> EventKind {
        match self {
            Self::Conn(..) => EventKind::Conn,
            Self::Discon(..) => EventKind::Discon,
            Self::Recv(..) => EventKind::Recv,
            Self::Send(..) => EventKind::Send,
            Self::Done(..) => EventKind::Done,
        }
    }
}
impl PartialEq<Event> for Event {
    /// Timestamps (first field) compares equal if any of them is `None`.
    fn eq(&self, e: &Event) -> bool {
        let t =
            |t1: &Option<Instant>, t2: &Option<Instant>| t1.is_none() || t2.is_none() || t1 == t2;
        match (self, e) {
            (Self::Conn(t1, i1), Self::Conn(t2, i2)) if i1 == i2 => t(t1, t2),
            (Self::Discon(t1, i1), Self::Discon(t2, i2)) if i1 == i2 => t(t1, t2),
            (Self::Recv(t1, i1, p1), Self::Recv(t2, i2, p2)) if i1 == i2 && p1 == p2 => t(t1, t2),
            (Self::Send(t1, i1, p1), Self::Send(t2, i2, p2)) if i1 == i2 && p1 == p2 => t(t1, t2),
            (Self::Done(t1), Self::Done(t2)) => t(t1, t2),
            _ => false,
        }
    }
}

/// Matches an [`Event`]
///
/// [`Event`]: enum.Event.html
// TODO: Split Recv and Sent into per-packet types
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub enum EventKind {
    Conn,
    Discon,
    Recv,
    Send,
    Done,
}

/// Logging/filtering/short-circuiting wrapper for `Option<Sender<Event>>`.
///
/// * Filter events by kind before sending
/// * Set the timestamp
/// * warn!() when the channel is full
/// * debug!() once and never send again if there is no Receiver
#[derive(Clone)]
pub(crate) struct SendEvent {
    chan: Option<Sender<Event>>,
    filt: HashMap<EventKind, bool>,
    def: bool,
}
impl SendEvent {
    pub(crate) fn new(chan: Sender<Event>, filt: HashMap<EventKind, bool>, def: bool) -> Self {
        SendEvent { chan: Some(chan), filt, def }
    }
    pub(crate) fn send(&mut self, event: Event) {
        if let Some(ref mut s) = self.chan {
            if *self.filt.get(&event.kind()).unwrap_or(&self.def) {
                match s.try_send(event.now()) {
                    Err(TrySendError::Closed(_)) => {
                        debug!("No receiver for Event messages");
                        self.chan = None;
                    },
                    Err(TrySendError::Full(e)) => {
                        warn!("Event receiver too slow, dropping {:?}", e)
                    },
                    Ok(()) => (),
                }
            }
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
