mod config;
mod node;

use crate::config::Export as _;
use crate::config::{Committee, Secret};
use crate::node::Node;
use clap::{Parser, Subcommand};
use consensus::Committee as ConsensusCommittee;
use env_logger::Env;
use futures::future::join_all;
use log::error;
use std::fs;
use tokio::task::JoinHandle;
use transport::cli::TransportArgs;

#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
struct Cli {
    /// Turn debugging information on.
    #[clap(short, long, action = clap::ArgAction::Count)]
    verbose: u8,
    /// The command to execute.
    #[clap(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Generate a new keypair.
    Keys {
        /// The file where to print the new key pair.
        #[clap(short, long, value_parser, value_name = "FILE")]
        filename: String,
    },
    /// Run a single node.
    Run {
        /// Transport-level configuration (P2P).
        #[clap(flatten)]
        transport: TransportArgs,
        /// The file containing the node keys.
        #[clap(short, long, value_parser, value_name = "FILE")]
        keys: String,
        /// The file containing committee information.
        #[clap(short, long, value_parser, value_name = "FILE")]
        committee: String,
        /// Optional file containing the node parameters.
        #[clap(short, long, value_parser, value_name = "FILE")]
        parameters: Option<String>,
        /// The path where to create the data store.
        #[clap(short, long, value_parser, value_name = "PATH")]
        store: String,
    },
    /// Deploy a local testbed with the specified number of nodes.
    Deploy {
        #[clap(short, long, value_parser = clap::value_parser!(u16).range(4..))]
        nodes: u16,
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let log_level = match cli.verbose {
        0 => "error",
        1 => "warn",
        2 => "info",
        3 => "debug",
        _ => "trace",
    };
    let mut logger = env_logger::Builder::from_env(Env::default().default_filter_or(log_level));
    #[cfg(feature = "benchmark")]
    logger.format_timestamp_millis();
    logger.init();

    match cli.command {
        Command::Keys { filename } => {
            if let Err(e) = Node::print_key_file(&filename) {
                error!("{}", e);
            }
        }
        Command::Run {
            transport,
            keys,
            committee,
            parameters,
            store,
        } => match Node::new(transport, &committee, &keys, &store, parameters).await {
            Ok(mut node) => {
                tokio::spawn(async move {
                    node.analyze_block().await;
                })
                .await
                .expect("Failed to analyze committed blocks");
            }
            Err(e) => error!("{}", e),
        },
        Command::Deploy { nodes } => match deploy_testbed(nodes) {
            Ok(handles) => {
                let _ = join_all(handles).await;
            }
            Err(e) => error!("Failed to deploy testbed: {}", e),
        },
    }
}

fn deploy_testbed(nodes: u16) -> Result<Vec<JoinHandle<()>>, Box<dyn std::error::Error>> {

    let _ = fs::create_dir_all("testnet");

    let keys: Vec<_> = (0..nodes).map(|_| Secret::new()).collect();

    if keys.is_empty() {
        return Err("No keys generated".into());
    }

    // Simply use the first key as the leader.
    let leader = keys[0].name;
    let epoch = 1;

    let consensus_committee = ConsensusCommittee::new(
        keys.iter()
            .map(|key| {
                let name = key.name;
                let stake = 1;
                (name, stake)
            })
            .collect(),
        leader,
        epoch,
    );
    let committee_file = "testnet/committee.json";
    let _ = fs::remove_file(committee_file);
    Committee {
        consensus: consensus_committee,
    }
    .write(committee_file)?;

    // Generate transport-level configuration.
    let transports = TransportArgs::testing_args(nodes);

    // Write the key files and spawn all nodes.
    keys.iter()
        .enumerate()
        .map(|(i, keypair)| {
            let key_file = format!("testnet/node_{}.json", i);
            let _ = fs::remove_file(&key_file);
            keypair.write(&key_file)?;

            let store_path = format!("testnet/db_{}", i);
            let _ = fs::remove_dir_all(&store_path);

            let transport = transports[i].clone();

            Ok(tokio::spawn(async move {
                match Node::new(transport, committee_file, &key_file, &store_path, None).await {
                    Ok(mut node) => {
                        // Sink the commit channel.
                        while node.commit.recv().await.is_some() {}
                    }
                    Err(e) => error!("{}", e),
                }
            }))
        })
        .collect::<Result<_, Box<dyn std::error::Error>>>()
}
