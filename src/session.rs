//! MQTT sessions have multiple purposes:
//!  * Detect reconnection of the same client (previous connection must be teared down).
//!  * Remember subscriptions.
//!  * Follow up on pending QoS exchanges.
//!
//! An MQTT 3.1.1 client can specify clean_session=true at connection, meaning that any previous
//! session content (from a clean_session=false connection) should be reset, and that the session
//! should not be stored at the end of connection. We still need to store them for the connection's
//! duration, to enable concurrent connection prevention.
//!
//! Note that connect.clean_session=true's "I don't want to use sessions" is different from
//! connack.session_present=false's "I've lost the previous session data, let's start a new
//! one". There no surefire way for the server or client to check that the other peer has the same
//! session state, and both peers should probably just assume this is the case, and handle failures
//! graciously. This is doable for QoS, impossible for subscriptions. In other words, MQTT 3.1.1's
//! session negociation is bad and should not be blindly relied on.
//!
//! The situation is better with MQTT 5, which splits the clean_session connection parameter into
//! separate start_clean and session_expire ones, with much less surprising and useful
//! meanings. MQTT still doesn't protect against old sessions being restored (say the server stores
//! them in an eventually-consistent db, or the client reboots into an old state).


use crate::{messages::*,client::*, ASAP, FOREVER};
use log::*;
use std::{collections::HashMap,
          time::{Duration, Instant}};
use tokio::sync::oneshot;

pub(crate) struct Sessions(HashMap<String, Session>);
impl Sessions {
    pub fn new() -> Self {
        Self(HashMap::new())
    }
    /// Return new/live/closed SessionData, and mark it a "live", pointing to the calling client.
    pub async fn open(&mut self, client: &Client<'_>, clean_session: bool) -> SessionData {
        trace!("C{}: load session {} {:?}", client.id, client.name, self.0.keys());
        // Load previous session from the store (if it exist and hasn't expired).
        let prev_session = match self.0.remove(&client.name) {
            Some(s) => s.aquire(client).await,
            None => None,
        };
        // Make the session point to the calling client.
        self.0
            .insert(client.name.clone(), Session::new(client, &client.sess_expire, clean_session));
        // Return the previous session (if it exist and hasn't expired and the client didn't opt
        // out), or a new one.
        match prev_session {
            Some(_) if !clean_session => prev_session.unwrap(),
            _ => SessionData::default(),
        }
    }
    /// Close or delete a session, making sure that `client` was the session's owner to begin with.
    pub fn close(&mut self, client: &Client, session: SessionData) {
        match self.0.get(&client.name) {
            None => panic!("C{}: Closing non-existing session", client.id),
            Some(Session::Closed(_, _)) => panic!("C{}: Closing already-closed session", client.id),
            Some(Session::Live(addr, d)) => {
                assert_eq!(*addr, client.addr, "C{}: closing another client's session", client.id);
                if *d > ASAP {
                    debug!("C{}: close session {} {:?}, expire in {:?}",
                           client.id, client.name, session, *d);
                    let expire = Instant::now() + *d;
                    self.0.insert(client.name.clone(), Session::Closed(session, expire));
                } else {
                    debug!("C{}: remove session {}", client.id, client.name);
                    self.0.remove(&client.name);
                }
            },
        }
    }
}

/// Session expiration corresponds to !CONNECT.clean_session in MQTT3.1.1, and
/// CONNECT.session_expiry in MQTT5.
pub(crate) enum Session {
    /// Session is currently owned by live client, and will expire some time after closing.
    Live(Addr, Duration),
    /// Session is currently not used, and will expire at specified date.
    Closed(SessionData, Instant),
}
impl Session {
    fn new(client: &Client, expire: &Option<Duration>, clean_session: bool) -> Self {
        Self::Live(client.addr.clone(), match expire {
            None if clean_session => ASAP,
            None => FOREVER,
            Some(d) => *d,
        })
    }
    /// Obtain the SessionData from the Session enum or from the live Client, and return it if it
    /// hasn't expired.
    async fn aquire(self, client: &Client<'_>) -> Option<SessionData> {
        match self {
            Self::Live(addr, d) => {
                debug!("C{}: aquiring session from C{} {:?}", client.id, addr.1, d);
                let (snd, rcv) = oneshot::channel();
                addr.send(ClientEv::Replaced(client.id, snd)).await;
                if d > ASAP {
                    Some(rcv.await.unwrap())
                } else {
                    None
                }
            },
            Self::Closed(data, date) => {
                if date > Instant::now() {
                    debug!("C{}: reopening session", client.id);
                    Some(data)
                } else {
                    debug!("C{}: droping closed session", client.id);
                    None
                }
            },
        }
    }
}
