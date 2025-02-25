use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::Duration,
};

use crate::{
    edge::NodeEdge,
    util::{MessagePack, SimulatedTransaction, TimeUsageEvent},
};
use ckb_gen_types::{
    packed::{Byte32, RelayMessage, RelayTransactionHashes},
    prelude::{Entity, PackVec},
};
use ckb_gen_types::{
    packed::{GetRelayTransactions, RelayTransaction},
    prelude::{Pack, Reader},
};
use ckb_gen_types::{
    packed::{RelayMessageUnionReader, RelayTransactionVec, RelayTransactions},
    prelude::Builder,
};
use rand::seq::SliceRandom;
use tokio::task::JoinHandle;

pub struct SimulatedNode {
    node_name: String,
    connected_nodes: std::sync::Mutex<Vec<NodeEdge>>,
    tx_pool: tokio::sync::RwLock<HashMap<Byte32, SimulatedTransaction>>,
    event_sender: tokio::sync::mpsc::Sender<TimeUsageEvent>,
    unknown_tx_hashes: tokio::sync::RwLock<HashSet<Byte32>>,
    message_sender: tokio::sync::mpsc::UnboundedSender<MessagePack>,
    message_receiver: Mutex<Option<tokio::sync::mpsc::UnboundedReceiver<MessagePack>>>,
    edge_index_map: std::sync::Mutex<HashMap<usize, usize>>,
    node_index: usize,
}

impl SimulatedNode {
    pub fn get_node_index(&self) -> usize {
        self.node_index
    }
    fn emit_event(&self, time_usage: usize, description: String) {
        self.event_sender
            .try_send(TimeUsageEvent {
                time_usage,
                description,
            })
            .unwrap();
    }
    pub fn new(
        node_name: impl AsRef<str>,
        node_index: usize,
        event_sender: tokio::sync::mpsc::Sender<TimeUsageEvent>,
    ) -> Self {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        Self {
            node_name: node_name.as_ref().to_string(),
            connected_nodes: Default::default(),
            tx_pool: Default::default(),
            event_sender,
            unknown_tx_hashes: Default::default(),
            message_receiver: Mutex::new(Some(rx)),
            message_sender: tx,
            edge_index_map: Default::default(),
            node_index,
        }
    }
    pub fn add_edge(&self, edge: NodeEdge) {
        let mut nodes = self.connected_nodes.lock().unwrap();
        let opposite_index = edge.to_node.upgrade().unwrap().node_index;

        nodes.push(edge);
        self.edge_index_map
            .lock()
            .unwrap()
            .insert(nodes.len() - 1, opposite_index);
    }
    pub fn start_worker(
        self: Arc<SimulatedNode>,
    ) -> (JoinHandle<()>, tokio::sync::mpsc::Sender<()>) {
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let mut message_rx = self.message_receiver.lock().unwrap().take().unwrap();

        let handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = rx.recv() => {
                        log::info!("Node {} exiting..",self.node_name);
                        break;
                    }
                    _ = tokio::time::sleep(Duration::from_millis(300)) => {
                        log::info!("Node {} flooding..",self.node_name);
                        self.flood_broadcast_hashes().await;

                    }
                    _ = tokio::time::sleep(Duration::from_millis(300)) => {
                        log::info!("Node {} requesting missing txs..",self.node_name);
                        self.ask_for_missing_txs().await;
                    }
                    Some(msg) = message_rx.recv() => {
                        self.accept_message(msg.message,msg.source_node_index).await;
                    }
                }
            }
        });
        (handle, tx)
    }

    // Send GetRelayTransactions to connected peers, so we can retrive missing transactions
    // Encode and transport time overhead are accounted when sending messages
    // While decoding overhead are counted when receiving messages
    pub async fn ask_for_missing_txs(self: &Arc<Self>) {
        let unknown_txs = self.unknown_tx_hashes.read().await;
        if unknown_txs.is_empty() {
            log::info!("No need to ask for missing txs");
            return;
        }
        let mut peers = self.connected_nodes.lock().unwrap().clone();
        peers.shuffle(&mut rand::rng());
        let tx_per_peer = unknown_txs.len().div_ceil(peers.len());
        // Chunk missing transactions to every connected node
        for chunk in unknown_txs.iter().collect::<Vec<_>>().chunks(tx_per_peer) {
            let curr_peer = peers.pop().unwrap();
            let content = GetRelayTransactions::new_builder()
                .tx_hashes(
                    chunk
                        .iter()
                        .map(|x| (*x).clone())
                        .collect::<Vec<_>>()
                        .pack(),
                )
                .build();
            let message = RelayMessage::new_builder().set(content).build();
            curr_peer
                .send_message_through_edge(message, self.node_index)
                .unwrap();
        }
    }

    // Accept a message. Call this function will add the time usage of decoding message to total_time
    async fn accept_message(
        self: &Arc<SimulatedNode>,
        msg: RelayMessage,
        source_node_index: usize,
    ) {
        self.emit_event(
            1000 * msg.as_slice().len(),
            format!("Decoding RelayMessage at {}", self.node_name),
        );
        match msg.as_reader().to_enum() {
            RelayMessageUnionReader::RelayTransactionHashes(reader) => {
                // Receiving RelayTransactionHashes, record unknown tx hashes
                log::info!("Node {} handling RelayTransactionHashes", self.node_name);
                for hash in reader.tx_hashes().iter() {
                    if !self
                        .unknown_tx_hashes
                        .read()
                        .await
                        .contains(&hash.to_entity())
                    {}
                }
            }
            RelayMessageUnionReader::GetRelayTransactions(reader) => {
                // Receiving GetRelayTransactions, construct a RelayTransactions and send it back
                log::info!("Node {} handling GetRelayTransactions", self.node_name);
                let mut to_send_tx = vec![];
                let tx_pool = self.tx_pool.write().await;

                for hash in reader.to_entity().tx_hashes().into_iter() {
                    if let Some(v) = tx_pool.get(&hash) {
                        to_send_tx.push(
                            RelayTransaction::new_builder()
                                .cycles((0_u64).pack())
                                .transaction(v.tx.data())
                                .build(),
                        );
                    }
                }
                let message = RelayMessage::new_builder()
                    .set(
                        RelayTransactions::new_builder()
                            .transactions(
                                RelayTransactionVec::new_builder().set(to_send_tx).build(),
                            )
                            .build(),
                    )
                    .build();

                if let Some(to_edge) = self.find_node_edge(source_node_index) {
                    to_edge
                        .send_message_through_edge(message, self.node_index)
                        .unwrap();
                }
            }
            _ => {}
        }
    }
    // Send RelayTransactionHashes to all of connected peers, will add the time usage of encoding message and network transport
    pub async fn flood_broadcast_hashes(self: &Arc<SimulatedNode>) {
        let hashes = self
            .tx_pool
            .read()
            .await
            .keys()
            .map(|x| x.clone())
            .collect::<Vec<_>>();
        if hashes.is_empty() {
            return;
        }
        let connected_nodes = self.connected_nodes.lock().unwrap().clone();
        for peer in connected_nodes.iter() {
            let content = RelayTransactionHashes::new_builder()
                .tx_hashes(hashes.clone().pack())
                .build();
            let message = RelayMessage::new_builder().set(content).build();
            peer.send_message_through_edge(message, self.node_index)
                .unwrap();
        }
    }
    pub fn get_message_sender(&self) -> tokio::sync::mpsc::UnboundedSender<MessagePack> {
        self.message_sender.clone()
    }
    fn find_node_edge(&self, node_index: usize) -> Option<NodeEdge> {
        let edge_index_map = self.edge_index_map.lock().unwrap();
        let connected_nodes = self.connected_nodes.lock().unwrap();

        connected_nodes
            .get(*edge_index_map.get(&node_index)?)
            .cloned()
    }
}
