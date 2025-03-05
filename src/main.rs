use std::collections::HashSet;
use std::sync::atomic::AtomicBool;
use std::time::{Duration, Instant};

use ckb_gen_types::packed::{
    Byte, Byte32, Byte32Builder, CellInputBuilder, CellOutputBuilder, OutPointBuilder,
    RawTransactionBuilder, TransactionBuilder, TransactionViewBuilder,
};
use ckb_gen_types::prelude::{Builder, Pack, PackVec};
use rand::seq::IndexedRandom;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha12Rng;
use util::{MessageTransportSimulator, SimulatedTransaction};

mod edge;
mod erlay;
mod node;
mod util;

const NODE_COUNT: usize = 10;
const TX_COUNT: usize = 1000;
const EDGE_COUNT: usize = 30;

const GRAPH_SEED: u64 = 0xDEADBEEF;
const TX_SEED: u64 = 0xCAFEBABE;
const ERLAY_SALT_SEED: u64 = 0x12345678;
fn rand_byte32(rng: &mut impl Rng) -> Byte32 {
    let mut rand_data: [Byte; 32] = Default::default();
    for item in rand_data.iter_mut() {
        *item = Byte::new(rng.random());
    }
    Byte32Builder::default().set(rand_data).build()
}

fn create_random_transaction(rng: &mut impl Rng) -> SimulatedTransaction {
    let rand_data = (0..rng.random_range(0..500usize))
        .map(|_| rng.random())
        .collect::<Vec<u8>>();
    let raw_transaction = RawTransactionBuilder::default()
        .outputs(
            vec![CellOutputBuilder::default()
                .capacity((100u64).pack())
                .build()]
            .pack(),
        )
        .outputs_data(vec![rand_data.pack()].pack())
        .inputs(
            vec![CellInputBuilder::default()
                .previous_output(
                    OutPointBuilder::default()
                        .index((0u32).pack())
                        .tx_hash(rand_byte32(rng))
                        .build(),
                )
                .build()]
            .pack(),
        )
        .version((1u32).pack())
        .build();
    let tx = TransactionBuilder::default().raw(raw_transaction).build();

    let data = TransactionViewBuilder::default()
        .hash(tx.calc_tx_hash())
        .data(tx)
        .build();

    SimulatedTransaction { tx: data }
}

static SHOULD_EXIT: AtomicBool = AtomicBool::new(false);

const TRY_TIMES: usize = 10;

fn main() -> anyhow::Result<()> {
    flexi_logger::Logger::try_with_env_or_str("info")
        .unwrap()
        .log_to_stdout()
        .start()
        .unwrap();
    let mut result = vec![];
    for _ in 0..TRY_TIMES {
        let mut sim = MessageTransportSimulator::new(
            NODE_COUNT,
            ERLAY_SALT_SEED,
            node::NodeConfig {
                enable_hash_flood: false,
                enable_erlay: true,
                enable_real_time_simulation: true,
            },
        );

        {
            let mut added_edges = HashSet::<(usize, usize)>::new();
            // Each node connects to a node with a smaller index, so the node network can form a tree
            let mut rng = ChaCha12Rng::seed_from_u64(GRAPH_SEED);
            for i in 1..NODE_COUNT {
                let to_idx = rng.random_range(0..i);
                let rand_len: usize = rng.random_range(100..1000);
                sim.add_edge(i, to_idx, rand_len);
                added_edges.insert((to_idx, i));
            }
            // Add some random edges..
            while added_edges.len() < EDGE_COUNT {
                let a = rng.random_range(0..sim.get_node_count() - 1);
                let b = rng.random_range(a + 1..sim.get_node_count());
                let rand_len: usize = rng.random_range(100..1000);
                if !added_edges.contains(&(a, b)) {
                    sim.add_edge(a, b, rand_len);
                    added_edges.insert((a, b));
                } else {
                    continue;
                }
            }
        }

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(48)
            .build()?;

        let _guard = runtime.enter();
        let start_time = Instant::now();
        // Start workers
        let handles_and_senders = sim.start_workers();

        let mut monitor_rx = {
            let (monitor_tx, monitor_rx) = tokio::sync::mpsc::channel(1);

            let nodes = sim.clone_nodes();
            tokio::task::spawn_blocking(move || {
                while !nodes.iter().all(|x| x.get_tx_pool_size() == TX_COUNT) {
                    std::thread::sleep(Duration::from_millis(300));
                    if SHOULD_EXIT.load(std::sync::atomic::Ordering::SeqCst) {
                        return;
                    }
                }
                log::info!("Check passed..");
                monitor_tx.try_send(()).unwrap();
            });

            monitor_rx
        };

        // Setup some transactions..
        let tx_creator = {
            let nodes = sim.clone_nodes();
            std::thread::spawn(move || {
                let mut rng = rand_chacha::ChaCha12Rng::seed_from_u64(TX_SEED);
                for _ in 0..TX_COUNT {
                    let tx = create_random_transaction(&mut rng);
                    for selected_node in nodes.choose_multiple(&mut rng, 3) {
                        selected_node.add_transaction(tx.clone());
                    }

                    if SHOULD_EXIT.load(std::sync::atomic::Ordering::SeqCst) {
                        return;
                    }
                    std::thread::sleep(Duration::from_millis(1));
                }
                log::info!("Transaction generating done");
            })
        };
        let events = runtime.block_on(async move {
            let mut evts = vec![];
            loop {
                tokio::select! {
                    _ = monitor_rx.recv() => {
                        log::info!("All nodes satisfied, exiting..");
                        break;
                    }
                    Some(evt) = sim.event_receiver.recv() => {
                        evts.push(evt);
                    }
                    Ok(_) = tokio::signal::ctrl_c() => {
                        log::info!("Ctrl+C pressed, exiting..");
                        SHOULD_EXIT.store(true,std::sync::atomic::Ordering::SeqCst);
                        break;
                    }
                }
            }
            for (_, sender) in handles_and_senders.iter() {
                sender.send(true).unwrap();
            }
            for (handle, _) in handles_and_senders.into_iter() {
                handle.await.unwrap();
            }
            evts
        });

        tx_creator.join().unwrap();
        if SHOULD_EXIT.load(std::sync::atomic::Ordering::SeqCst) {
            break;
        }
        let curr_result = events
            .iter()
            .map(|x| match x {
                util::Event::TimeUsage { time_usage, .. } => (*time_usage, 0),
                util::Event::DataUsage { bytes_usage, .. } => (0, *bytes_usage),
            })
            .fold((0, 0), |x, y| (x.0 + y.0, x.1 + y.1));

        let time_consumed = start_time.elapsed().as_nanos();
        log::info!(
            "Collected {} events, (time usage, data usage, real_time_usage): {:?}",
            events.len(),
            curr_result
        );

        result.push((curr_result.0, curr_result.1, time_consumed));
    }
    let (total_time, total_data, total_real_time) = result
        .iter()
        .fold((0, 0, 0), |x, y| (x.0 + y.0, x.1 + y.1, x.2 + y.2));
    println!("Average time usage: {}", total_time / result.len());
    println!("Average data usage: {}", total_data / result.len());
    println!(
        "Average real time usage: {}",
        total_real_time / result.len() as u128
    );

    Ok(())
}
