use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use crate::{
    edge::NodeEdge,
    erlay::{ErlaySessionForRequester, ErlaySessionForResponder},
    util::{Event, MessagePack, SimulatedTransaction, UnknownTxHashPriority},
};
use ckb_gen_types::{
    packed::{
        Byte32, RelayMessage, RelayMessageUnion, RelayTransactionHashes, RelayTransactionSalt,
        TransactionViewBuilder,
    },
    prelude::{Entity, PackVec, Unpack},
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
use minisketch_rs::Minisketch;
use tokio::task::JoinHandle;

#[derive(Default, Clone)]
pub struct NodeConfig {
    pub enable_hash_flood: bool,
    pub enable_erlay: bool,
}
pub struct SimulatedNode {
    connected_nodes: std::sync::Mutex<Vec<NodeEdge>>,
    tx_pool: tokio::sync::RwLock<HashMap<Byte32, SimulatedTransaction>>,
    event_sender: tokio::sync::mpsc::UnboundedSender<Event>,
    unknown_tx_hashes: tokio::sync::RwLock<KeyedPriorityQueue<Byte32, UnknownTxHashPriority>>,

    message_sender: tokio::sync::mpsc::UnboundedSender<MessagePack>,
    message_receiver: Mutex<Option<tokio::sync::mpsc::UnboundedReceiver<MessagePack>>>,

    edge_index_map: std::sync::Mutex<HashMap<usize, usize>>,
    node_index: usize,

    erlay_short_id_salt: u64,
    salts_of_other_nodes: tokio::sync::RwLock<HashMap<usize, u64>>,

    erlay_requester_sessions: tokio::sync::RwLock<HashMap<usize, ErlaySessionForRequester>>,
    erlay_responder_sessions: tokio::sync::RwLock<HashMap<usize, ErlaySessionForResponder>>,

    config: NodeConfig,
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
    pub fn new(
        node_index: usize,
        event_sender: tokio::sync::mpsc::UnboundedSender<Event>,
        erlay_short_id_salt: u64,
        config: NodeConfig,
    ) -> Self {
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
            erlay_short_id_salt,
            salts_of_other_nodes: Default::default(),
            erlay_requester_sessions: Default::default(),
            erlay_responder_sessions: Default::default(),
            config,
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
    fn broadcast_erlay_salt(&self) {
        log::debug!(
            "Node {} broadcasting erlay salt to all nodes",
            self.get_node_index()
        );
        let conected_nodes = self.connected_nodes.lock().unwrap().clone();
        let message = RelayMessage::new_builder()
            .set(
                RelayTransactionSalt::new_builder()
                    .salt(self.erlay_short_id_salt.pack())
                    .build(),
            )
            .build();
        for node in conected_nodes.into_iter() {
            node.send_message_through_edge(message.clone(), self.get_node_index())
                .unwrap();
        }
    }
    pub fn start_worker(
        self: Arc<SimulatedNode>,
    ) -> (JoinHandle<()>, tokio::sync::mpsc::Sender<()>) {
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let mut message_rx = self.message_receiver.lock().unwrap().take().unwrap();
        self.broadcast_erlay_salt();
        let handle = tokio::spawn(async move {
            loop {
                let new_self = self.clone();
                tokio::select! {
                    _ = rx.recv() => {
                        log::info!("Node {} exiting..",self.get_node_name());
                        break;
                    }
                    _ = tokio::time::sleep(Duration::from_millis(100)) => {
                        if new_self.config.enable_hash_flood {
                            log::info!("Node {} flooding..",self.get_node_name());
                            tokio::spawn(async move {    new_self.flood_broadcast_hashes().await});
                        }

                    }
                    _ = tokio::time::sleep(Duration::from_millis(100)) => {
                        log::info!("Node {} requesting missing txs..",self.get_node_name());
                        tokio::spawn(async move {new_self.ask_for_missing_txs().await});
                    }
                    Some(msg) = message_rx.recv() => {
                        tokio::spawn(async move {   new_self.accept_message(msg.message,msg.source_node_index).await});
                    }
                    _ = tokio::time::sleep(Duration::from_secs(5)) => {
                        if new_self.config.enable_erlay {
                            tokio::task::spawn_blocking(move || new_self.broadcast_erlay_salt());
                        }
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
            RelayMessageUnionReader::RelayTransactionSalt(reader) => {
                log::debug!(
                    "Node {} handling RelayTransactionSalt",
                    self.get_node_index()
                );
                let salt: u64 = reader.salt().to_entity().unpack();
                log::debug!(
                    "Node {} recorded salt {} from node {}",
                    self.get_node_index(),
                    salt,
                    source_node_index
                );
                self.salts_of_other_nodes
                    .write()
                    .await
                    .insert(source_node_index, salt);
            }
            RelayMessageUnionReader::GetRelayTransactionSketch(reader) => {
                log::debug!(
                    "Node {} handling GetRelayTransactionSketch",
                    self.get_node_index()
                );
                let mut responder_session = self.erlay_responder_sessions.write().await;
                let session = match responder_session.entry(source_node_index) {
                    std::collections::hash_map::Entry::Occupied(_) => {
                        log::warn!(
                            "Node {}: There is already a responder session from source node {}",
                            self.get_node_index(),
                            source_node_index
                        );
                        return;
                    }
                    std::collections::hash_map::Entry::Vacant(vacant_entry) => {
                        vacant_entry.insert(ErlaySessionForResponder::new(
                            self.erlay_short_id_salt,
                            *self
                                .salts_of_other_nodes
                                .read()
                                .await
                                .get(&source_node_index)
                                .unwrap(),
                            self.tx_pool.read().await.clone(),
                        ))
                    }
                };
                let local_set_size = session.snapshot().len() as u64;
                let remote_set_size: u64 = reader.set_size().unpack();
                let u64_q: u64 = reader.q().unpack();
                let f64_q: f64 = f64::from_le_bytes(u64_q.to_le_bytes());
                // |remote_set_size-local_set_size|+q*min(remote_set_size,local_set_size)+1
                let expected_set_size = ((local_set_size.abs_diff(remote_set_size) as f64)
                    + f64_q * (remote_set_size.min(local_set_size) as f64)
                    + 1f64) as usize;
                log::debug!(
                    "Node {} expected set size {}",
                    self.get_node_index(),
                    expected_set_size
                );
                let mut sketch = Minisketch::try_new(32, 0, expected_set_size).unwrap();
                for key in session.iter_short_ids() {
                    sketch.add(*key as u64);
                }
                let sketch_buf = {
                    let mut buf = vec![0u8; sketch.serialized_size()];
                    sketch.serialize(&mut buf).unwrap();
                    buf
                };
                // sketch.serialize(buf)
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
