use std::{collections::HashSet, sync::Arc};

use edge::NodeEdge;
use node::SimulatedNode;
use rand::Rng;
use util::MessageTransportSimulator;

mod edge;
mod node;
mod util;
fn main() -> anyhow::Result<()> {
    flexi_logger::Logger::try_with_env_or_str("info")
        .unwrap()
        .log_to_stdout()
        .start()
        .unwrap();

    let mut sim = MessageTransportSimulator::new();
    for i in 0..100 {
        sim.nodes.push(Arc::new(SimulatedNode::new(
            format!("node-{}", i),
            i,
            sim.get_event_sender(),
        )));
    }

    let push_edge = |i: usize, j: usize, len: usize| {
        sim.nodes[j].add_edge(NodeEdge {
            distance: len,
            to_node: Arc::downgrade(&sim.nodes[i]),
            sender: sim.nodes[i].get_message_sender(),
            event_sender: sim.get_event_sender(),
        });
        sim.nodes[i].add_edge(NodeEdge {
            distance: len,
            to_node: Arc::downgrade(&sim.nodes[j]),
            sender: sim.nodes[j].get_message_sender(),
            event_sender: sim.get_event_sender(),
        });
        log::info!("Add edge {:03} <-> {:03}, length = {}", i, j, len);
    };
    let mut added_edges = HashSet::<(usize, usize)>::new();
    // Each node connects to a node with a smaller index, so the node network can form a tree
    let mut rng = rand::rng();

    for i in 1..100 {
        let to_idx = rng.random_range(0..i);
        let rand_len: usize = rng.random_range(100..1000);
        push_edge(i, to_idx, rand_len);
        added_edges.insert((to_idx, i));
    }
    // Add some random edges..
    while added_edges.len() < 500 {
        let a = rng.random_range(0..sim.nodes.len() - 1);
        let b = rng.random_range(a + 1..sim.nodes.len());
        let rand_len: usize = rng.random_range(100..1000);
        if !added_edges.contains(&(a, b)) {
            push_edge(a, b, rand_len);
            added_edges.insert((a, b));
        } else {
            continue;
        }
    }

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    let _guard = runtime.enter();

    // Start workers
    let handles_and_senders = sim.start_workers();

    let events = runtime.block_on(async move {
        let mut evts = vec![];
        loop {
            tokio::select! {
                Some(evt) = sim.event_receiver.recv() => {
                    evts.push(evt);
                }
                Ok(_) = tokio::signal::ctrl_c() => {
                    log::info!("Ctrl+C pressed, Exiting..");
                    break;
                }
            }
        }
        for (_, sender) in handles_and_senders.iter() {
            sender.send(()).await.unwrap();
        }
        for (handle, _) in handles_and_senders.into_iter() {
            handle.await.unwrap();
        }
        evts
    });
    log::info!("Collected {} events", events.len());
    Ok(())
}
