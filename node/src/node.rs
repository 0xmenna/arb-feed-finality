use crate::config::Export as _;
use crate::config::{Committee, ConfigError, Parameters, Secret};
use async_trait::async_trait;
use bytes::Bytes;
use consensus::{Block, Consensus, ConsensusReceiverHandler};
use crypto::SignatureService;
use log::info;
use store::Store;
use tokio::sync::mpsc::{channel, Receiver};
use transport::cli::TransportArgs;
use transport::{
    protocol::{BLOCK_PROPOSALS_TOPIC, BLOCK_VOTES_TOPIC},
    MessageHandler, P2PTransport, TopicHandler,
};

/// The default channel capacity for this module.
pub const CHANNEL_CAPACITY: usize = 1_000;

pub const CONSENSUS_TOPICS: [&str; 2] = [BLOCK_PROPOSALS_TOPIC, BLOCK_VOTES_TOPIC];

#[derive(Clone)]
struct MessageTopicHandler {
    consensus: ConsensusReceiverHandler,
}

#[async_trait]
impl TopicHandler for MessageTopicHandler {
    async fn dispatch(
        &self,
        topic: &str,
        message: Bytes,
    ) -> Result<(), Box<dyn core::error::Error>> {
        if CONSENSUS_TOPICS.contains(&topic) {
            return self.consensus.dispatch(message).await;
        }
        panic!("Unsupported topic: {}", topic);
    }

    fn topics(&self) -> Vec<&'static str> {
        CONSENSUS_TOPICS
            .iter()
            .map(|topic| *topic)
            .collect::<Vec<&'static str>>()
    }
}

pub struct Node {
    pub commit: Receiver<Block>,
}

impl Node {
    pub async fn new(
        transport: TransportArgs,
        committee_file: &str,
        key_file: &str,
        store_path: &str,
        parameters: Option<String>,
    ) -> Result<Self, ConfigError> {
        let (tx_commit, rx_commit) = channel(CHANNEL_CAPACITY);
        let (tx_publisher, rx_publisher) = channel(CHANNEL_CAPACITY);

        // Read the committee and secret key from file.
        let committee = Committee::read(committee_file)?;
        let secret = Secret::read(key_file)?;
        let name = secret.name;
        let secret_key = secret.secret;

        // Load default parameters if none are specified.
        // TODO: add custom parameters.
        let _parameters = match parameters {
            Some(filename) => Parameters::read(&filename)?,
            None => Parameters::default(),
        };

        // Make the data store.
        let store = Store::new(store_path).expect("Failed to create store");

        // Run the signature service.
        let signature_service = SignatureService::new(secret_key);

        // Run the consensus core.
        let consensus_handler = Consensus::spawn(
            name,
            committee.consensus,
            // parameters.consensus, // TODO: we will surely need other parameters for consensus
            signature_service,
            store,
            tx_publisher,
            tx_commit,
        );

        // Run the p2p transport.
        P2PTransport::spawn(
            transport,
            MessageTopicHandler {
                consensus: consensus_handler,
            },
            rx_publisher,
        );

        info!("Node {} successfully booted", name);
        Ok(Self { commit: rx_commit })
    }

    pub fn print_key_file(filename: &str) -> Result<(), ConfigError> {
        Secret::new().write(filename)
    }

    pub async fn analyze_block(&mut self) {
        while let Some(_block) = self.commit.recv().await {
            // This is where we can further process committed block.
        }
    }
}
