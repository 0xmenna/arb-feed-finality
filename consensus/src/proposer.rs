use crate::config::Committee;
use crate::consensus::{ConsensusMessage, ParentRound, View};
use crate::messages::{Block, QC};
use bytes::Bytes;
use codec::Encode;
use crypto::{Digest, PublicKey, SignatureService};
use log::{debug, info};
use std::collections::HashSet;
use tokio::sync::mpsc::{Receiver, Sender};
use transport::protocol::BLOCK_PROPOSALS_TOPIC;
use transport::Publisher;

#[derive(Debug)]
pub struct MakeProposal {
    pub view: View,
    pub parent_round: ParentRound,
    pub last_feed_sequence_number: u64,
    pub batch_poster_digest: Digest,
    pub feed_merkle_root: Digest,
    pub qc: QC,
}

pub struct Proposer {
    name: PublicKey,
    committee: Committee,
    signature_service: SignatureService,
    rx_message: Receiver<MakeProposal>,
    tx_loopback: Sender<Block>,
    buffer: HashSet<Digest>,
    network_publisher: Publisher,
}

impl Proposer {
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        signature_service: SignatureService,
        rx_message: Receiver<MakeProposal>,
        tx_loopback: Sender<Block>,
        tx_publisher: Publisher,
    ) {
        tokio::spawn(async move {
            Self {
                name,
                committee,
                signature_service,
                rx_message,
                buffer: HashSet::new(),
                tx_loopback,
                network_publisher: tx_publisher,
            }
            .run()
            .await;
        });
    }

    async fn make_block(&mut self, proposal: MakeProposal) {
        // Generate a new block.
        let block = Block::new(
            proposal.qc,
            proposal.view,
            proposal.parent_round,
            proposal.last_feed_sequence_number,
            proposal.batch_poster_digest,
            proposal.feed_merkle_root,
            self.signature_service.clone(),
        )
        .await;

        info!("Created {}", block);

        // Broadcast our new block.
        debug!("Broadcasting {:?}", block);
        let message = ConsensusMessage::Propose(block.clone()).encode();

        self.network_publisher
            .send((BLOCK_PROPOSALS_TOPIC, Bytes::from(message)))
            .await
            .expect("Failed to send block");

        // Send our block to the core for processing.
        self.tx_loopback
            .send(block)
            .await
            .expect("Failed to send block");

        // TODO: Be sure in this function or another calling module to wait to make proposal until there is no quorum of the before block.
    }

    async fn run(&mut self) {
        loop {
            if let Some(proposal) = self.rx_message.recv().await {
                self.make_block(proposal).await;
            }
        }
    }
}
