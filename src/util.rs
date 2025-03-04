use std::{
    cmp,
    sync::Arc,
    time::{Duration, Instant},
};

use ckb_gen_types::packed::RelayMessage;
use rand::{Rng, SeedableRng};
use tokio::task::JoinHandle;

use crate::{
    edge::NodeEdge,
    node::{NodeConfig, SimulatedNode},
};

pub enum Event {
    TimeUsage {
        time_usage: usize,
        #[allow(unused)]
        description: String,
    },
    DataUsage {
        bytes_usage: usize,
        #[allow(unused)]
        description: String,
    },
}

#[derive(Clone)]
pub struct SimulatedTransaction {
    pub tx: ckb_gen_types::packed::TransactionView,
}

pub struct MessageTransportSimulator {
    nodes: Vec<Arc<SimulatedNode>>,
    event_sender: tokio::sync::mpsc::UnboundedSender<Event>,
    pub event_receiver: tokio::sync::mpsc::UnboundedReceiver<Event>,
}

impl MessageTransportSimulator {
    pub fn clone_nodes(&self) -> Vec<Arc<SimulatedNode>> {
        self.nodes.clone()
    }
    pub fn get_node_count(&self) -> usize {
        self.nodes.len()
    }
    pub fn get_event_sender(&self) -> tokio::sync::mpsc::UnboundedSender<Event> {
        self.event_sender.clone()
    }
    pub fn new(node_count: usize, erlay_salt_seed: u64, node_config: NodeConfig) -> Self {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let mut rng = rand_chacha::ChaCha20Rng::seed_from_u64(erlay_salt_seed);
        Self {
            nodes: (0..node_count)
                .map(|i| {
                    Arc::new(SimulatedNode::new(
                        i,
                        tx.clone(),
                        rng.random(),
                        node_config.clone(),
                    ))
                })
                .collect(),
            event_receiver: rx,
            event_sender: tx,
        }
    }
    pub fn start_workers(&self) -> Vec<(JoinHandle<()>, tokio::sync::watch::Sender<bool>)> {
        self.nodes
            .iter()
            .map(|x| x.clone().start_all_workers())
            .collect()
    }
    pub fn add_edge(&mut self, i: usize, j: usize, len: usize) {
        self.nodes[j].add_edge(NodeEdge {
            distance: len,
            to_node: Arc::downgrade(&self.nodes[i]),
            sender: self.nodes[i].get_message_sender(),
            event_sender: self.get_event_sender(),
        });
        self.nodes[i].add_edge(NodeEdge {
            distance: len,
            to_node: Arc::downgrade(&self.nodes[j]),
            sender: self.nodes[j].get_message_sender(),
            event_sender: self.get_event_sender(),
        });
        log::info!("Add edge {:03} <-> {:03}, length = {}", i, j, len);
    }
}

pub struct MessagePack {
    pub message: RelayMessage,
    pub source_node_index: usize,
}

#[derive(Eq, PartialEq, Clone)]
pub struct UnknownTxHashPriority {
    pub request_time: Instant,
    pub peer_indexes: Vec<usize>,
    pub requested: bool,
}

impl UnknownTxHashPriority {
    pub fn should_request(&self, now: Instant) -> bool {
        self.next_request_at() < now
    }

    pub fn next_request_at(&self) -> Instant {
        if self.requested {
            self.request_time + Duration::from_millis(100)
        } else {
            self.request_time
        }
    }

    pub fn next_request_peer(&mut self) -> Option<usize> {
        if self.requested {
            if self.peer_indexes.len() > 1 {
                self.request_time = Instant::now();
                self.peer_indexes.swap_remove(0);
                self.peer_indexes.first().cloned()
            } else {
                None
            }
        } else {
            self.requested = true;
            self.peer_indexes.first().cloned()
        }
    }

    pub fn push_peer(&mut self, peer_index: usize) {
        self.peer_indexes.push(peer_index);
    }
    #[allow(unused)]
    pub fn requesting_peer(&self) -> Option<usize> {
        if self.requested {
            self.peer_indexes.first().cloned()
        } else {
            None
        }
    }
}

impl Ord for UnknownTxHashPriority {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.next_request_at()
            .cmp(&other.next_request_at())
            .reverse()
    }
}

impl PartialOrd for UnknownTxHashPriority {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}
