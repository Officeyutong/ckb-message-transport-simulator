use std::sync::Arc;

use ckb_gen_types::packed::RelayMessage;
use tokio::task::JoinHandle;

use crate::node::SimulatedNode;

pub struct TimeUsageEvent {
    pub time_usage: usize,
    pub description: String,
}

#[derive(Clone)]
pub struct SimulatedTransaction {
    pub tx: ckb_gen_types::packed::TransactionView,
}

pub struct MessageTransportSimulator {
    pub nodes: Vec<Arc<SimulatedNode>>,
    event_sender: tokio::sync::mpsc::Sender<TimeUsageEvent>,
    pub event_receiver: tokio::sync::mpsc::Receiver<TimeUsageEvent>,
}

impl MessageTransportSimulator {
    pub fn get_event_sender(&self) -> tokio::sync::mpsc::Sender<TimeUsageEvent> {
        self.event_sender.clone()
    }
    pub fn new() -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(100);
        Self {
            nodes: vec![],
            event_receiver: rx,
            event_sender: tx,
        }
    }
    pub fn start_workers(&self) -> Vec<(JoinHandle<()>, tokio::sync::mpsc::Sender<()>)> {
        self.nodes
            .iter()
            .map(|x| x.clone().start_worker())
            .collect()
    }
}

pub struct MessagePack {
    pub message: RelayMessage,
    pub source_node_index: usize,
}
