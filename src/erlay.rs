use std::collections::{hash_map::Keys, HashMap};

use anyhow::bail;
use ckb_gen_types::{packed::Byte32, prelude::Unpack};
use const_siphasher::sip::SipHasher24;
use minisketch_rs::Minisketch;
use sha2::{Digest, Sha256};

use crate::util::SimulatedTransaction;

const RELAY_TAG: &'static str = "Tx Relay Salting";

pub struct ErlaySession {
    // short_id_hasher: ShortIdHasher,
    snapshot: HashMap<Byte32, SimulatedTransaction>,
    short_id_mapper: HashMap<u32, Byte32>,
}
impl ErlaySession {
    pub fn new(salt1: u64, salt2: u64, snapshot: HashMap<Byte32, SimulatedTransaction>) -> Self {
        let hasher = ShortIdHasher::new(salt1, salt2);

        let mut short_id_mapper = HashMap::new();
        for key in snapshot.keys() {
            short_id_mapper.insert(hasher.hash(&Unpack::<[u8; 32]>::unpack(key)), key.clone());
        }
        Self {
            // short_id_hasher: hasher,
            short_id_mapper,
            snapshot,
        }
    }
}
pub enum ErlayRequesterStage {
    Initialized,
    WaitingForSketch,
    WaitingForExtendedSketch,
}

pub struct ErlaySessionForRequester {
    session: ErlaySession,
    stage: ErlayRequesterStage,
}

impl ErlaySessionForRequester {
    pub fn snapshot_set_size(&self) -> usize {
        self.session.snapshot.len()
    }
    pub fn switch_to_waiting_for_sketch(&mut self) -> anyhow::Result<()> {
        if !matches!(self.stage, ErlayRequesterStage::Initialized) {
            bail!("Invalid switch");
        }
        self.stage = ErlayRequesterStage::WaitingForSketch;
        Ok(())
    }
    pub fn switch_to_waiting_for_extended_sketch(&mut self) -> anyhow::Result<()> {
        if !matches!(self.stage, ErlayRequesterStage::WaitingForSketch) {
            bail!("Invalid switch");
        }
        self.stage = ErlayRequesterStage::WaitingForExtendedSketch;
        Ok(())
    }

    pub fn current_stage(&self) -> &ErlayRequesterStage {
        &self.stage
    }
    pub fn new(salt1: u64, salt2: u64, snapshot: HashMap<Byte32, SimulatedTransaction>) -> Self {
        Self {
            session: ErlaySession::new(salt1, salt2, snapshot),
            stage: ErlayRequesterStage::Initialized,
        }
    }
    pub fn create_self_sketch(&self, sketch_size: usize) -> Minisketch {
        let mut sketch = Minisketch::try_new(32, 0, sketch_size).unwrap();
        for item in self.session.short_id_mapper.keys() {
            sketch.add(*item as u64);
        }
        sketch
    }
}

pub enum ErlayResponderStage {
    Initialized,
    BasicSketchSended { sketch_size: usize },
    ExtendedSketchSended,
}
pub struct ErlaySessionForResponder {
    session: ErlaySession,
    stage: ErlayResponderStage,
}

impl<'a> ErlaySessionForResponder {
    pub fn short_id_mapper(&self) -> &HashMap<u32, Byte32> {
        &self.session.short_id_mapper
    }
    pub fn iter_short_ids(&'a self) -> Keys<'a, u32, Byte32> {
        self.session.short_id_mapper.keys()
    }
    pub fn new(salt1: u64, salt2: u64, snapshot: HashMap<Byte32, SimulatedTransaction>) -> Self {
        Self {
            session: ErlaySession::new(salt1, salt2, snapshot),
            stage: ErlayResponderStage::Initialized,
        }
    }
    pub fn current_stage(&self) -> &ErlayResponderStage {
        &self.stage
    }
    pub fn switch_to_basic_sketch_sended(&mut self, sketch_size: usize) -> anyhow::Result<()> {
        if !matches!(self.stage, ErlayResponderStage::Initialized) {
            bail!("Trying to switch illegal path");
        }
        self.stage = ErlayResponderStage::BasicSketchSended { sketch_size };
        Ok(())
    }
    pub fn switch_to_extended_sketch_sended(&mut self) -> anyhow::Result<()> {
        if !matches!(self.stage, ErlayResponderStage::BasicSketchSended { .. }) {
            bail!("Trying to switch illegal path");
        }
        self.stage = ErlayResponderStage::ExtendedSketchSended;
        Ok(())
    }
    pub fn snapshot(&self) -> &HashMap<Byte32, SimulatedTransaction> {
        &self.session.snapshot
    }
}

pub struct ShortIdHasher {
    hasher: SipHasher24,
}

impl ShortIdHasher {
    pub fn new(salt1: u64, salt2: u64) -> Self {
        let min_salt = salt1.min(salt2);
        let max_salt = salt1.max(salt2);
        let mut hasher = Sha256::new();
        hasher.update(RELAY_TAG.as_bytes());
        hasher.update(min_salt.to_le_bytes());
        hasher.update(max_salt.to_le_bytes());
        let h = hasher.finalize();

        let k0 = u64::from_le_bytes(h[0..8].try_into().unwrap());
        let k1 = u64::from_le_bytes(h[8..16].try_into().unwrap());
        Self {
            hasher: SipHasher24::new_with_keys(k0, k1),
        }
    }
    pub fn hash(&self, data: &[u8]) -> u32 {
        let h = self.hasher.hash(data);
        ((h % 0xffffffff) + 1) as u32
    }
}
