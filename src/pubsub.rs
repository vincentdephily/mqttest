use crate::{client::*, mqtt::*};
use log::*;
use std::collections::HashMap;

pub(crate) struct Subs(HashMap<String, HashMap<ConnId, Sub>>);
impl Subs {
    pub fn new() -> Self {
        Subs(HashMap::new())
    }
    pub fn get(&self, s: &str) -> Option<&HashMap<ConnId, Sub>> {
        self.0.get(s)
    }
    /// Register subscription. Returns `true` if it is new.
    pub fn add(&mut self, topic: &String, qos: QoS, id: ConnId, addr: Addr) -> bool {
        debug!("C{}: subscribe to {:?} {:?}", id, topic, qos);
        self.0.entry(topic.clone()).or_default().insert(id, Sub { qos, addr }).is_none()
    }
    //fn del(&mut self, topic: &String, client: &Client) {}
    pub fn del_all(&mut self, client: &Client) {
        debug!("C{}: unsubscribe all", client.id);
        self.0.values_mut().for_each(|h| {
                               h.remove(&client.id);
                           });
        self.0.retain(|_s, h| !h.is_empty());
    }
}

pub struct Sub {
    pub qos: QoS,
    pub addr: Addr,
}
