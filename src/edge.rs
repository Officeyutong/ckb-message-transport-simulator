use std::sync::Weak;

use ckb_gen_types::packed::RelayMessage;

use crate::{
    node::SimulatedNode,
    util::{MessagePack, TimeUsageEvent},
};
use ckb_gen_types::prelude::Entity;
#[derive(Clone)]
pub struct NodeEdge {
    pub distance: usize,
    pub to_node: Weak<SimulatedNode>,
    pub sender: tokio::sync::mpsc::UnboundedSender<MessagePack>,
    pub event_sender: tokio::sync::mpsc::UnboundedSender<TimeUsageEvent>,
}

impl NodeEdge {
    fn emit_event(&self, time_usage: usize, description: String) {
        self.event_sender
            .send(TimeUsageEvent {
                time_usage,
                description,
            })
            .ok();
    }

    pub fn send_message_through_edge(
        &self,
        msg: RelayMessage,
        source_index: usize,
    ) -> anyhow::Result<()> {
        let to_node_idx = self.to_node.upgrade().unwrap().get_node_index();
        self.emit_event(
            1000 * msg.as_slice().len(),
            format!(
                "Encoding message overhead from {:03} to {:03}",
                source_index, to_node_idx
            ),
        );
        let message_length = msg.as_slice().len();

        let time_usage = self.distance * 1000_000 / 10 + message_length * 1000;
        if let Ok(_) = self.sender.send(MessagePack {
            message: msg,
            source_node_index: source_index,
        }) {
            self.emit_event(
                time_usage,
                format!(
                    "Message transport overhead from {} to {}",
                    source_index, to_node_idx
                ),
            );
        } else {
            log::warn!("Found a dead peer connected to {}", source_index);
        }

        Ok(())
    }
}
