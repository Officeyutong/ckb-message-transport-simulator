use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use crate::{
    edge::NodeEdge,
    util::{Event, MessagePack, SimulatedTransaction, UnknownTxHashPriority},
};
use ckb_gen_types::{
    packed::{Byte32, RelayMessage, RelayTransactionHashes, TransactionViewBuilder},
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
use keyed_priority_queue::KeyedPriorityQueue;
use tokio::task::JoinHandle;

pub struct SimulatedNode {
    connected_nodes: std::sync::Mutex<Vec<NodeEdge>>,
    tx_pool: tokio::sync::RwLock<HashMap<Byte32, SimulatedTransaction>>,
    event_sender: tokio::sync::mpsc::UnboundedSender<Event>,
    unknown_tx_hashes: tokio::sync::RwLock<KeyedPriorityQueue<Byte32, UnknownTxHashPriority>>,

    message_sender: tokio::sync::mpsc::UnboundedSender<MessagePack>,
    message_receiver: Mutex<Option<tokio::sync::mpsc::UnboundedReceiver<MessagePack>>>,

    edge_index_map: std::sync::Mutex<HashMap<usize, usize>>,
    node_index: usize,
}

impl SimulatedNode {
    pub fn get_tx_pool_size(&self) -> usize {
        self.tx_pool.blocking_read().len()
    }
    pub fn add_transaction(&self, tx: SimulatedTransaction) {
        let mut tx_pool = self.tx_pool.blocking_write();
        tx_pool.insert(tx.tx.hash(), tx);
    }
    pub fn get_node_name(&self) -> String {
        format!("node-{:03}", self.node_index)
    }
    pub fn get_node_index(&self) -> usize {
        self.node_index
    }
    fn emit_time_usage_event(&self, time_usage: usize, description: String) {
        self.event_sender
            .send(Event::TimeUsage {
                time_usage,
                description,
            })
            .unwrap();
    }
    pub fn new(node_index: usize, event_sender: tokio::sync::mpsc::UnboundedSender<Event>) -> Self {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        Self {
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
            .insert(opposite_index, nodes.len() - 1);
    }
    pub fn start_worker(
        self: Arc<SimulatedNode>,
    ) -> (JoinHandle<()>, tokio::sync::mpsc::Sender<()>) {
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let mut message_rx = self.message_receiver.lock().unwrap().take().unwrap();

        let handle = tokio::spawn(async move {
            loop {
                let new_self = self.clone();
                tokio::select! {
                    _ = rx.recv() => {
                        log::info!("Node {} exiting..",self.get_node_name());
                        break;
                    }
                    _ = tokio::time::sleep(Duration::from_millis(100)) => {
                        log::info!("Node {} flooding..",self.get_node_name());
                        tokio::spawn(async move {    new_self.flood_broadcast_hashes().await});

                    }
                    _ = tokio::time::sleep(Duration::from_millis(100)) => {
                        log::info!("Node {} requesting missing txs..",self.get_node_name());
                       tokio::spawn(async move {new_self.ask_for_missing_txs().await});
                    }
                    Some(msg) = message_rx.recv() => {
                    tokio::spawn(async move {   new_self.accept_message(msg.message,msg.source_node_index).await});
                    }
                }
            }
        });
        (handle, tx)
    }

    fn pop_ask_for_txs(
        &self,
        unknown_tx_hashes: &mut KeyedPriorityQueue<Byte32, UnknownTxHashPriority>,
    ) -> HashMap<usize, Vec<Byte32>> {
        let mut result: HashMap<usize, Vec<Byte32>> = HashMap::new();
        let now = Instant::now();

        if !unknown_tx_hashes
            .peek()
            .map(|(_tx_hash, priority)| priority.should_request(now))
            .unwrap_or_default()
        {
            return result;
        }

        while let Some((tx_hash, mut priority)) = unknown_tx_hashes.pop() {
            if priority.should_request(now) {
                if let Some(peer_index) = priority.next_request_peer() {
                    result
                        .entry(peer_index)
                        .and_modify(|hashes| hashes.push(tx_hash.clone()))
                        .or_insert_with(|| vec![tx_hash.clone()]);
                    unknown_tx_hashes.push(tx_hash, priority);
                }
            } else {
                unknown_tx_hashes.push(tx_hash, priority);
                break;
            }
        }
        result
    }

    // Send GetRelayTransactions to connected peers, so we can retrive missing transactions
    // Encode and transport time overhead are accounted when sending messages
    // While decoding overhead are counted when receiving messages
    pub async fn ask_for_missing_txs(self: &Arc<Self>) {
        log::debug!(
            "Node {} entering ask_for_missing_txs..",
            self.get_node_name()
        );
        let mut unknown_txs = self.unknown_tx_hashes.write().await;
        if unknown_txs.is_empty() {
            log::debug!("No need to ask for missing txs");
            return;
        } else {
            log::debug!(
                "Node {} unknown_txs size = {}",
                self.get_node_name(),
                unknown_txs.len()
            );
        }
        let request_list = self.pop_ask_for_txs(&mut *unknown_txs);
        log::debug!(
            "Node {} request list {:?}",
            self.get_node_name(),
            request_list
        );
        for (peer, txs) in request_list {
            let content = GetRelayTransactions::new_builder()
                .tx_hashes(txs.clone().pack())
                .build();
            let message = RelayMessage::new_builder().set(content).build();
            if let Some(curr_peer) = self.find_node_edge(peer) {
                log::debug!(
                    "{}: requesting missing txs {:?} from {}",
                    self.get_node_name(),
                    txs,
                    curr_peer.to_node.upgrade().unwrap().get_node_name(),
                );
                curr_peer
                    .send_message_through_edge(message, self.node_index)
                    .unwrap();
            } else {
                log::warn!(
                    "Node {} trying to send GetRelayTransactions to an unknown peer: {}",
                    self.get_node_name(),
                    peer
                );
            }
        }
    }

    // Accept a message. Call this function will add the time usage of decoding message to total_time
    async fn accept_message(
        self: &Arc<SimulatedNode>,
        msg: RelayMessage,
        source_node_index: usize,
    ) {
        self.emit_time_usage_event(
            1000 * msg.as_slice().len(),
            format!("Decoding RelayMessage at {}", self.get_node_name()),
        );
        match msg.as_reader().to_enum() {
            RelayMessageUnionReader::RelayTransactionHashes(reader) => {
                // Receiving RelayTransactionHashes, record unknown tx hashes
                log::debug!(
                    "Node {} handling RelayTransactionHashes (length = {})",
                    self.get_node_name(),
                    reader.tx_hashes().len()
                );
                let mut inserted_count = 0;
                let tx_pool = self.tx_pool.read().await;
                let mut unknown_tx = self.unknown_tx_hashes.write().await;
                for hash in reader.tx_hashes().iter() {
                    if tx_pool.contains_key(&hash.to_entity()) {
                        continue;
                    }
                    match unknown_tx.entry(hash.to_entity()) {
                        keyed_priority_queue::Entry::Occupied(entry) => {
                            let mut priority = entry.get_priority().clone();
                            priority.push_peer(source_node_index);
                            entry.set_priority(priority);
                        }
                        keyed_priority_queue::Entry::Vacant(entry) => {
                            entry.set_priority(UnknownTxHashPriority {
                                request_time: Instant::now(),
                                requested: false,
                                peer_indexes: vec![source_node_index],
                            });

                            inserted_count += 1;
                        }
                    }
                }
                log::debug!(
                    "Node {} added {} unknown txs",
                    self.get_node_name(),
                    inserted_count
                );
            }
            RelayMessageUnionReader::GetRelayTransactions(reader) => {
                // Receiving GetRelayTransactions, construct a RelayTransactions and send it back
                log::debug!(
                    "Node {} handling GetRelayTransactions",
                    self.get_node_name()
                );
                let to_send_tx = {
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
                    to_send_tx
                };
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
                    log::debug!(
                        "Node {} sending {:?}(RelayTransactions) to {}",
                        self.get_node_name(),
                        message,
                        to_edge.to_node.upgrade().unwrap().get_node_name()
                    );
                    to_edge
                        .send_message_through_edge(message, self.node_index)
                        .unwrap();
                } else {
                    log::warn!(
                        "Node {} can't find edge to {}",
                        self.get_node_name(),
                        source_node_index
                    );
                }
            }
            RelayMessageUnionReader::RelayTransactions(reader) => {
                log::debug!("Node {} handling RelayTransactions", self.get_node_name());
                let mut added_count = 0;
                let mut tx_pool = self.tx_pool.write().await;
                let mut unknown_tx_hashes = self.unknown_tx_hashes.write().await;

                for tx in reader.to_entity().transactions().into_iter() {
                    let tx_hash = tx.transaction().calc_tx_hash();
                    unknown_tx_hashes.remove(&tx_hash);
                    tx_pool.entry(tx_hash).or_insert_with(|| {
                        added_count += 1;
                        SimulatedTransaction {
                            tx: TransactionViewBuilder::default()
                                .data(tx.transaction())
                                .build(),
                        }
                    });
                }
                log::info!(
                    "Node {} received {} new txs",
                    self.get_node_name(),
                    added_count
                );
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
            log::info!(
                "Node {} won't flood, since it doesn't have any transactions",
                self.get_node_name()
            );
            return;
        }

        log::info!(
            "Node {} has {} known txs, {} unknown txs, flooding..",
            self.get_node_name(),
            self.tx_pool.read().await.len(),
            self.unknown_tx_hashes.read().await.len(),
        );

        log::debug!(
            "Node {} has {} known txs: {:?}, {} unknown txs: {:?}, flooding..",
            self.get_node_name(),
            self.tx_pool.read().await.len(),
            self.tx_pool.read().await.keys().collect::<Vec<_>>(),
            self.unknown_tx_hashes.read().await.len(),
            self.unknown_tx_hashes
                .read()
                .await
                .iter()
                .map(|x| x.0)
                .collect::<Vec<_>>(),
        );
        let connected_nodes = self.connected_nodes.lock().unwrap().clone();
        for peer in connected_nodes.iter() {
            let content = RelayTransactionHashes::new_builder()
                .tx_hashes(hashes.clone().pack())
                .build();
            let message = RelayMessage::new_builder().set(content).build();
            peer.send_message_through_edge(message, self.node_index)
                .unwrap();
            log::debug!(
                "Node {} flooded {} hashes to {}",
                self.get_node_name(),
                hashes.len(),
                peer.to_node.upgrade().unwrap().get_node_name()
            );
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
