use crate::config::Committee;
use crate::core::Core;
use crate::error::ConsensusError;
use crate::helper::Helper;
use crate::leader::{self};
use crate::messages::{Block, Vote};
use crate::proposer::Proposer;
use async_trait::async_trait;
use bytes::Bytes;
use codec::{Decode, Encode};
use crypto::{Digest, PublicKey, SignatureService};
use evm::BigInt;
use feed::batch_maker::{BatchMakerResult, MiniBatchFeed};
use feed::source::BroadcastFeedMessage;
use log::info;
use std::error::Error;
use store::Store;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use transport::{MessageHandler, Publisher};

#[cfg(test)]
#[path = "tests/consensus_tests.rs"]
pub mod consensus_tests;

/// The default channel capacity for each channel of the consensus.
pub const CHANNEL_CAPACITY: usize = 1_000;

/// The consensus checkpoint number.
pub type Checkpoint = u64;

/// The consensus round number.
pub type Round = u64;

/// The consensus parent chain round number.
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
        rx_feed: Receiver<BroadcastFeedMessage>,
        tx_mini_batch: Sender<MiniBatchFeed>,
        rx_batch_maker: Receiver<BatchMakerResult>,
        tx_network_publisher: Publisher,
        tx_commit: Sender<Block>,
    ) -> ConsensusReceiverHandler {
        let (tx_consensus, rx_consensus) = channel(CHANNEL_CAPACITY);
        let (tx_loopback, rx_loopback) = channel(CHANNEL_CAPACITY);
        let (tx_proposer, rx_proposer) = channel(CHANNEL_CAPACITY);
        let (tx_helper, rx_helper) = channel(CHANNEL_CAPACITY);

        info!(
            "Node {} registered a new network handler for consensus messages",
            name
        );

        // Make the leader election module.
        let leader = leader::get_leader_from_parent_chain();

        // Spawn the consensus core.
        Core::spawn(
            name,
            committee.clone(),
            signature_service.clone(),
            store.clone(),
            leader,
            /* rx_message */ rx_consensus,
            rx_loopback,
            tx_proposer,
            tx_commit,
            tx_network_publisher,
        );

        if name == leader {
            // Spawn the block proposer.
            Proposer::spawn(
                name,
                committee.clone(),
                signature_service,
                /* rx_message */ rx_proposer,
                tx_loopback,
            );
        }

        // Spawn the helper module.
        Helper::spawn(committee, store, /* rx_requests */ rx_helper);

        // Return the network handler.
        ConsensusReceiverHandler {
            tx_consensus,
            tx_helper,
        }
    }
}

/// Defines how the network receiver handles incoming primary messages.
#[derive(Clone)]
struct ConsensusReceiverHandler {
    tx_consensus: Sender<ConsensusMessage>,
    tx_helper: Sender<(Digest, PublicKey)>,
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
