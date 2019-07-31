use crate::{client::*, mqtt::*};
use log::*;
use std::collections::HashMap;

pub struct Subs(HashMap<String, HashMap<ConnId, (QoS, Addr)>>);
impl Subs {
    pub fn new() -> Self {
        Subs(HashMap::new())
    }
    pub fn get(&self, s: &str) -> Option<&HashMap<ConnId, (QoS, Addr)>> {
        self.0.get(s)
    }
    /// Register subscription. Returns `true` if it is new.
    pub fn add(&mut self, topic: &String, qos: QoS, client: &Client) -> bool {
        debug!("Connection {} subscribes to {:?} {:?}", client.id, topic, qos);
        self.0
            .entry(topic.clone())
            .or_default()
            .insert(client.id, (qos, client.addr.clone()))
            .is_none()
    }
    //fn del(&mut self, topic: &String, client: &Client) {}
    //fn del_all(&mut self, client: &Client) {}
}
