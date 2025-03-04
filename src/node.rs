use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use crate::{
    edge::NodeEdge,
    erlay::{
        ErlayRequesterStage, ErlayResponderStage, ErlaySessionForRequester,
        ErlaySessionForResponder,
    },
    util::{Event, MessagePack, SimulatedTransaction, UnknownTxHashPriority},
};
use anyhow::{anyhow, bail, Context};
use ckb_gen_types::{
    packed::{
        Byte32, GetRelayTransactionExtendedSketchBuilder, GetRelayTransactionSketchBuilder,
        RelayMessage, RelayMessageBuilder, RelayTransactionHashes, RelayTransactionSalt,
        RelayTransactionSketchBuilder, RelayTransactionSketchResultBuilder, TransactionViewBuilder,
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
    erlay_q: tokio::sync::Mutex<f64>,

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
            erlay_q: tokio::sync::Mutex::new(0f64),
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
        log::info!(
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
    pub fn start_all_workers(
        self: Arc<SimulatedNode>,
    ) -> (JoinHandle<()>, tokio::sync::watch::Sender<bool>) {
        let (tx, rx) = tokio::sync::watch::channel(false);
        let mut message_rx = self.message_receiver.lock().unwrap().take().unwrap();
        if self.config.enable_erlay {
            self.broadcast_erlay_salt();
        }
        let transaction_asker = {
            let new_self = self.clone();
            let mut new_rx = rx.clone();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = new_rx.changed() => {
                            log::info!("Node {}-transaction asker exiting..",new_self.get_node_name());
                            break;
                        }
                        _ = tokio::time::sleep(Duration::from_millis(300)) => {
                            log::info!("Node {} asking for missing txs..",new_self.get_node_name());
                            new_self.ask_for_missing_txs().await;
                        }
                    }
                }
            })
        };
        let flooding_worker = if self.config.enable_hash_flood {
            let new_self = self.clone();
            let mut new_rx = rx.clone();
            Some(tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = new_rx.changed() => {
                            log::info!("Node {}-flood worker exiting..",new_self.get_node_name());
                            break;
                        }
                        _ = tokio::time::sleep(Duration::from_millis(100)) => {
                            log::info!("Node {} flooding..",new_self.get_node_name());
                            new_self.flood_broadcast_hashes(None).await;
                        }
                    }
                }
            }))
        } else {
            None
        };
        let message_scceeptor = {
            let new_self = self.clone();
            let mut new_rx = rx.clone();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = new_rx.changed() => {
                            log::info!("Node {}-message acceptor exiting..",new_self.get_node_name());
                            break;
                        }
                        Some(msg) = message_rx.recv() => {
                            new_self.accept_message(msg.message,msg.source_node_index).await.unwrap();
                        }
                    }
                }
            })
        };
        let erlay_salt_broadcastor = if self.config.enable_erlay {
            let new_self = self.clone();
            let mut new_rx = rx.clone();
            Some(tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = new_rx.changed() => {
                            log::info!("Node {}-erlay salt broadcastor acceptor exiting..",new_self.get_node_name());
                            break;
                        }

                        _ = tokio::time::sleep(Duration::from_secs(5)) => {
                            new_self.broadcast_erlay_salt();
                        }
                    }
                }
            }))
        } else {
            None
        };
        let erlay_session_maker = if self.config.enable_erlay {
            let new_self = self.clone();
            let mut new_rx = rx.clone();
            Some(tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = new_rx.changed() => {
                            log::info!("Node {}-erlay session maker acceptor exiting..",new_self.get_node_name());
                            break;
                        }

                        _ = tokio::time::sleep(Duration::from_secs(2)) => {
                            new_self.start_erlay_session_with_all_connected_nodes().await
                        }
                    }
                }
            }))
        } else {
            None
        };
        let handle = tokio::spawn(async move {
            if let Some(v) = flooding_worker {
                v.await.unwrap();
            }
            message_scceeptor.await.unwrap();
            if let Some(v) = erlay_salt_broadcastor {
                v.await.unwrap();
            }
            if let Some(v) = erlay_session_maker {
                v.await.unwrap();
            }
            transaction_asker.await.unwrap();
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
            log::info!(
                "Node {}: no need to ask for missing txs",
                self.get_node_index()
            );
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
        log::info!(
            "Node {} requesting {} missing txs from {} nodes..",
            self.get_node_name(),
            unknown_txs.len(),
            request_list.len()
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
    ) -> anyhow::Result<()> {
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
                self.send_relay_transaction_to(
                    source_node_index,
                    reader.tx_hashes().to_entity().into_iter().collect(),
                )
                .await?;
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
                        bail!("Invalid state switch");
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
                let message = RelayMessageBuilder::default()
                    .set(
                        RelayTransactionSketchBuilder::default()
                            .short_id_sketch(sketch_buf.pack())
                            .sketch_size((expected_set_size as u32).pack())
                            .build(),
                    )
                    .build();
                if let Some(remote_node) = self.find_node_edge(source_node_index) {
                    remote_node
                        .send_message_through_edge(message, self.get_node_index())
                        .unwrap();
                    log::debug!(
                        "Node {} sending RelayTransactionSketch to {}",
                        self.get_node_index(),
                        source_node_index
                    );
                    session
                        .switch_to_basic_sketch_sended(expected_set_size)
                        .unwrap();
                } else {
                    log::error!("Bad source node: {}", source_node_index);
                }
            }
            RelayMessageUnionReader::GetRelayTransactionExtendedSketch(_) => {
                log::debug!(
                    "Node {} handling GetRelayTransactionExtendedSketch",
                    self.get_node_index()
                );
                let mut responder_session = self.erlay_responder_sessions.write().await;
                let session = match responder_session.entry(source_node_index) {
                    std::collections::hash_map::Entry::Occupied(occupied_entry) => {
                        occupied_entry.into_mut()
                    }
                    std::collections::hash_map::Entry::Vacant(_) => {
                        log::warn!("Node {} received GetRelayTransactionExtendedSketch from {} but there is no session",self.get_node_index(),source_node_index);
                        bail!("Invalid state switch");
                    }
                };
                // Generate an extended sketch
                let initial_size = match session.current_stage() {
                    ErlayResponderStage::BasicSketchSended { sketch_size } => *sketch_size,
                    _ => bail!("Unexpected stage"),
                };
                log::debug!(
                    "Node {} constructing extended sketch of size {}",
                    self.get_node_index(),
                    initial_size * 2
                );
                let mut sketch = Minisketch::try_new(32, 0, initial_size * 2).unwrap();
                for key in session.iter_short_ids() {
                    sketch.add(*key as u64);
                }
                let sketch_buf = {
                    let mut buf = vec![0u8; sketch.serialized_size()];
                    sketch.serialize(&mut buf).unwrap();
                    buf
                };
                let message = RelayMessageBuilder::default()
                    .set(
                        RelayTransactionSketchBuilder::default()
                            .short_id_sketch(sketch_buf.pack())
                            .sketch_size(((initial_size * 2) as u32).pack())
                            .build(),
                    )
                    .build();
                log::debug!(
                    "Node {} sending extended sketch of size {} bytes, message size {} bytes",
                    self.get_node_index(),
                    sketch_buf.len(),
                    message.as_slice().len()
                );
                if let Some(edge) = self.find_node_edge(source_node_index) {
                    edge.send_message_through_edge(message, self.get_node_index()).with_context(||anyhow!("Node {}: Unable to send RelayTransactionSketch for extended to source node {}",self.get_node_index(),source_node_index))?;
                    session.switch_to_extended_sketch_sended().unwrap();
                } else {
                    bail!("Source edge not found");
                }
            }
            RelayMessageUnionReader::RelayTransactionSketchResult(reader) => {
                log::debug!(
                    "Node {} handling RelayTransactionSketchResult",
                    self.get_node_index()
                );
                let mut sessions = self.erlay_responder_sessions.write().await;
                let session_entry = match sessions.entry(source_node_index) {
                    std::collections::hash_map::Entry::Occupied(occupied_entry) => occupied_entry,
                    std::collections::hash_map::Entry::Vacant(_) => {
                        log::error!("Node {} received RelayTransactionSketchResult, but the remote node {} is not in a session",self.get_node_index(),source_node_index);
                        bail!("Invalid session");
                    }
                };
                if reader.success().unpack() {
                    log::debug!(
                        "Node {} got an successful set recover from {}, with {} missing txs",
                        self.get_node_index(),
                        source_node_index,
                        reader.missing_short_ids().len()
                    );

                    // Requester says it recovered successfully, so we can send missing txs to it

                    let to_send_hashes = reader
                        .missing_short_ids()
                        .to_entity()
                        .into_iter()
                        .map(|item| session_entry.get().short_id_mapper().get(&item.unpack()))
                        .map(|x| x.cloned().unwrap())
                        .collect::<Vec<_>>();
                    log::debug!(
                        "Node {} replied to RelayTransactionSketchResult from {}: sended {} txs",
                        self.get_node_index(),
                        source_node_index,
                        to_send_hashes.len()
                    );
                    self.send_relay_transaction_to(source_node_index, to_send_hashes)
                        .await
                        .with_context(|| anyhow!("Unable to send relay transaction"))?;
                    log::debug!(
                        "Node {}: erlay session {} (responder) and {} (requester) ended",
                        self.get_node_index(),
                        self.get_node_index(),
                        source_node_index
                    );
                    session_entry.remove();
                } else {
                    log::debug!(
                        "Node {} got an failed set recover from {}",
                        self.get_node_index(),
                        source_node_index
                    );
                    self.flood_broadcast_hashes(Some(vec![source_node_index]))
                        .await;
                    session_entry.remove();
                    log::debug!(
                        "Node {} flooding missing hashes to {}, erlay session terminated",
                        self.get_node_index(),
                        source_node_index
                    );
                }
            }
            RelayMessageUnionReader::RelayTransactionSketch(reader) => {
                log::debug!(
                    "Node {} handling RelayTransactionSketch",
                    self.get_node_index()
                );
                let mut sessions = self.erlay_requester_sessions.write().await;
                let mut session = match sessions.entry(source_node_index) {
                    std::collections::hash_map::Entry::Occupied(occupied_entry) => occupied_entry,
                    std::collections::hash_map::Entry::Vacant(_) => {
                        log::error!("Node {} received RelayTransactionSketch from {}, but not in a session with it",self.get_node_index(),source_node_index);
                        bail!("Invalid session");
                    }
                };
                // Handling of RelayTransactionSketch will always produce a message to be sended
                let message_to_send = match session.get().current_stage() {
                    ErlayRequesterStage::Initialized => unreachable!(),
                    s @ (ErlayRequesterStage::WaitingForSketch
                    | ErlayRequesterStage::WaitingForExtendedSketch) => {
                        let sketch_size: u32 = reader.sketch_size().unpack();
                        let mut decoded_set = vec![0u64; sketch_size as usize];
                        let decode_result = {
                            let mut remote_sketch = {
                                let mut sketch =
                                    Minisketch::try_new(32, 0, sketch_size as usize).unwrap();
                                sketch.deserialize(&Unpack::<Vec<u8>>::unpack(
                                    &reader.short_id_sketch().to_entity(),
                                ));
                                sketch
                            };
                            let local_sketch =
                                session.get().create_self_sketch(sketch_size as usize);
                            remote_sketch
                                .merge(&local_sketch)
                                .expect("Failed to merge sketch");
                            remote_sketch.decode(&mut decoded_set)
                        };
                        match decode_result {
                            Ok(diff_size) => {
                                log::info!(
                                    "Node {} decode sketch from {} successfully, decoded size {}, session terminated",
                                    self.get_node_index(),
                                    source_node_index,
                                    diff_size
                                );
                                let local_set_size = session.get().snapshot_set_size() as f64;
                                let remote_set_size =
                                    Unpack::<u32>::unpack(&reader.set_size()) as f64;
                                let new_q = (diff_size as f64
                                    - (local_set_size - remote_set_size).abs())
                                    / (local_set_size.min(remote_set_size));
                                *self.erlay_q.lock().await = new_q;

                                session.remove();
                                RelayMessageBuilder::default()
                                    .set(
                                        RelayTransactionSketchResultBuilder::default()
                                            .success((true).pack())
                                            .missing_short_ids(
                                                decoded_set[0..diff_size]
                                                    .iter()
                                                    .map(|x| *x as u32)
                                                    .collect::<Vec<_>>()
                                                    .pack(),
                                            )
                                            .build(),
                                    )
                                    .build()
                            }
                            Err(err) => {
                                if matches!(s, ErlayRequesterStage::WaitingForSketch) {
                                    log::info!(
                                        "Node {} unable to decode merged set with initial \
                                size {}, requesting extended sketch..: {}",
                                        self.get_node_index(),
                                        sketch_size,
                                        err
                                    );
                                    session
                                        .get_mut()
                                        .switch_to_waiting_for_extended_sketch()
                                        .unwrap();
                                    RelayMessageBuilder::default()
                                        .set(
                                            GetRelayTransactionExtendedSketchBuilder::default()
                                                .build(),
                                        )
                                        .build()
                                } else {
                                    log::warn!(
                                        "Node {} unable to recover an extended sketch,\
                                     sending result of failure to {}. Session terminated.",
                                        self.get_node_index(),
                                        source_node_index
                                    );
                                    session.remove();
                                    RelayMessageBuilder::default()
                                        .set(
                                            RelayTransactionSketchResultBuilder::default()
                                                .success((false).pack())
                                                .build(),
                                        )
                                        .build()
                                }
                            }
                        }
                    }
                };
                if let Some(edge) = self.find_node_edge(source_node_index) {
                    edge.send_message_through_edge(message_to_send, self.get_node_index())
                        .unwrap();
                } else {
                    log::warn!(
                        "Node {}: Failed to send response for RelayTransactionSketch to {}",
                        self.get_node_index(),
                        source_node_index
                    );
                }
            }
            _ => {}
        }
        return Ok(());
    }
    async fn start_erlay_session_with_all_connected_nodes(&self) {
        log::info!(
            "Node {} starting erlay session with all nodes",
            self.get_node_index()
        );
        let node_ids = self
            .connected_nodes
            .lock()
            .unwrap()
            .iter()
            .map(|x| x.to_node.upgrade().unwrap().get_node_index())
            .collect::<Vec<_>>();
        for node in node_ids.into_iter() {
            self.start_erlay_session_to(node).await;
        }
    }
    async fn start_erlay_session_to(&self, to_node: usize) {
        log::info!(
            "Node {} trying to start erlay session with {}",
            self.get_node_index(),
            to_node
        );

        let mut sessions = self.erlay_requester_sessions.write().await;
        let entry = match sessions.entry(to_node) {
            std::collections::hash_map::Entry::Occupied(_) => {
                log::info!(
                    "Node {} unable to start erlay session with {}, already in session",
                    self.get_node_index(),
                    to_node
                );
                return;
            }
            std::collections::hash_map::Entry::Vacant(vacant_entry) => vacant_entry,
        };
        let remote_salt = *self
            .salts_of_other_nodes
            .read()
            .await
            .get(&to_node)
            .unwrap();
        let snapshot = self.tx_pool.read().await.clone();
        let q = *self.erlay_q.lock().await;
        let mut session =
            ErlaySessionForRequester::new(self.erlay_short_id_salt, remote_salt, snapshot);
        let message = RelayMessageBuilder::default()
            .set(
                GetRelayTransactionSketchBuilder::default()
                    .q(u64::from_le_bytes(q.to_le_bytes()).pack())
                    .set_size((session.snapshot_set_size() as u64).pack())
                    .build(),
            )
            .build();
        if let Some(to_edge) = self.find_node_edge(to_node) {
            to_edge
                .send_message_through_edge(message, self.get_node_index())
                .unwrap();
            log::info!(
                "Node {} sended GetRelayTransactionSketch to {}, q = {}, set_size = {}",
                self.get_node_index(),
                to_node,
                q,
                session.snapshot_set_size()
            );
            session.switch_to_waiting_for_sketch().unwrap();
            entry.insert(session);
        } else {
            log::warn!(
                "Node {} unable to find edge to {}",
                self.get_node_index(),
                to_node
            );
        }
    }
    async fn send_relay_transaction_to(
        &self,
        to_node: usize,
        hashes: Vec<Byte32>,
    ) -> anyhow::Result<()> {
        let to_send_tx = {
            let mut to_send_tx = vec![];
            let tx_pool = self.tx_pool.write().await;

            for hash in hashes.into_iter() {
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

        log::debug!(
            "Node {} sending relay transactions to {}, containing {} transactions",
            self.get_node_index(),
            to_node,
            to_send_tx.len()
        );
        let message = RelayMessage::new_builder()
            .set(
                RelayTransactions::new_builder()
                    .transactions(RelayTransactionVec::new_builder().set(to_send_tx).build())
                    .build(),
            )
            .build();

        if let Some(to_edge) = self.find_node_edge(to_node) {
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
                to_node
            );
        }

        Ok(())
    }
    // Send RelayTransactionHashes to all of connected peers, will add the time usage of encoding message and network transport
    pub async fn flood_broadcast_hashes(self: &Arc<SimulatedNode>, node_set: Option<Vec<usize>>) {
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
        let node_limit_set = node_set.map(|x| x.into_iter().collect::<HashSet<usize>>());

        for peer in connected_nodes.iter() {
            if let Some(ref limiter) = node_limit_set {
                let peer_index = peer.to_node.upgrade().unwrap().get_node_index();

                if !limiter.contains(&peer_index) {
                    log::debug!(
                        "Node {}: Ignoring flooding hashes to {}, limited",
                        self.get_node_index(),
                        peer_index
                    );
                    continue;
                }
            }
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
