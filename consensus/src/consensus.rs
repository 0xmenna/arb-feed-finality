use crate::config::Committee;
use crate::core::Core;
use crate::error::ConsensusError;
use crate::messages::{Block, Vote};
use crate::proposer::Proposer;
use async_trait::async_trait;
use bytes::Bytes;
use codec::{Decode, Encode};
use crypto::{PublicKey, SignatureService};
use evm::BigInt;
use feed::batch_maker::{BatchMakerResult, MiniBatchFeed};
use feed::source::BroadcastFeedMessage;
use log::info;
use std::error::Error;
use std::time::Duration;
use store::Store;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::oneshot;
use transport::{MessageHandler, Publisher};

/// The default channel capacity for each channel of the consensus.
pub const CHANNEL_CAPACITY: usize = 1_000;

/// The consensus view.
#[derive(Hash, Debug, Encode, Decode, Default, Ord, PartialOrd, Eq, PartialEq, Clone, Copy)]
pub struct View {
    pub checkpoint: u64,
    pub round: u64,
}

impl std::fmt::Display for View {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "View {{ checkpoint: {}, round: {} }}",
            self.checkpoint, self.round
        )
    }
}

impl View {
    pub fn new(checkpoint: u64, round: u64) -> Self {
        Self { checkpoint, round }
    }

    pub fn is_next_of(&self, prev: &Self) -> bool {
        (self.checkpoint == prev.checkpoint && self.round == prev.round + 1)
            || (self.checkpoint == prev.checkpoint + 1 && self.round == 0)
    }
}

pub type ParentRound = BigInt;

#[derive(Encode, Decode, Debug)]
pub enum ConsensusMessage {
    Propose(Block),
    Vote(Vote),
}

pub struct Consensus;

impl Consensus {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        signature_service: SignatureService,
        store: Store,
        // TODO: the following three channels handle them directly in the core.
        // rx_feed: Receiver<BroadcastFeedMessage>,
        // tx_mini_batch: Sender<MiniBatchFeed>,
        // rx_batch_maker: Receiver<BatchMakerResult>,
        network_publisher: Publisher,
        tx_commit: Sender<Block>,
        proposal_min_interval: Duration,
    ) -> (oneshot::Sender<()>, ConsensusReceiverHandler) {
        let (tx_consensus, rx_consensus) = channel(CHANNEL_CAPACITY);
        let (tx_loopback, rx_loopback) = channel(CHANNEL_CAPACITY);
        let (tx_proposer, rx_proposer) = channel(CHANNEL_CAPACITY);

        // Spawn the consensus core.
        let tx_transport_ready = Core::spawn(
            name,
            committee.clone(),
            signature_service.clone(),
            store.clone(),
            /* rx_message */ rx_consensus,
            rx_loopback,
            tx_proposer,
            tx_commit,
            network_publisher.clone(),
            proposal_min_interval,
        );

        if name == committee.leader {
            // Spawn the block proposer.
            Proposer::spawn(
                name,
                committee.clone(),
                signature_service,
                /* rx_message */ rx_proposer,
                tx_loopback,
                network_publisher,
            );
        }

        (
            tx_transport_ready,
            // Return the network handler.
            ConsensusReceiverHandler { tx_consensus },
        )
    }
}

/// Defines how the network receiver handles incoming messages.
#[derive(Clone)]
pub struct ConsensusReceiverHandler {
    tx_consensus: Sender<ConsensusMessage>,
}

#[async_trait]
impl MessageHandler for ConsensusReceiverHandler {
    async fn dispatch(&self, message: Bytes) -> Result<(), Box<dyn Error>> {
        // Decode the message
        let message =
            ConsensusMessage::decode(&mut &message[..]).map_err(ConsensusError::CodecError)?;
        self.tx_consensus
            .send(message)
            .await
            .expect("Failed to send consensus message");

        Ok(())
    }
}
