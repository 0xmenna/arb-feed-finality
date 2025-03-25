use crate::config::{Committee, Stake};
use crate::consensus::{Checkpoint, ConsensusMessage, ParentRound, Round};
use crate::messages::{Block, QC};
use bytes::Bytes;
use codec::Encode;
use crypto::{Digest, PublicKey, SignatureService};
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt as _;
use log::{debug, info};
use std::collections::HashSet;
use tokio::sync::mpsc::{Receiver, Sender};
use transport::protocol::BLOCK_PROPOSALS_TOPIC;
use transport::{CancelHandler, Publisher};

#[derive(Debug)]
pub enum ProposerMessage {
    Make(Round, QC),
    Cleanup(Vec<Digest>),
}

pub struct Proposer {
    name: PublicKey,
    committee: Committee,
    signature_service: SignatureService,
    rx_message: Receiver<ProposerMessage>,
    tx_loopback: Sender<Block>,
    buffer: HashSet<Digest>,
    network_publisher: Publisher,
}

impl Proposer {
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        signature_service: SignatureService,
        rx_message: Receiver<ProposerMessage>,
        tx_loopback: Sender<Block>,
        tx_publisher: Publisher,
    ) {
        tokio::spawn(async move {
            Self {
                name,
                committee,
                signature_service,
                rx_message,
                tx_loopback,
                buffer: HashSet::new(),
                network_publisher: tx_publisher,
            }
            .run()
            .await;
        });
    }

    /// Helper function. It waits for a future to complete and then delivers a value.
    async fn waiter(wait_for: CancelHandler, deliver: Stake) -> Stake {
        let _ = wait_for.await;
        deliver
    }

    async fn make_block(
        &mut self,
        checkpoint: Checkpoint,
        round: Round,
        parent_round: ParentRound,
        last_feed_sequence_number: u64,
        batch_poster_digest: Digest,
        feed_merkle_root: Digest,
        qc: QC,
    ) {
        // Generate a new block.
        let block = Block::new(
            qc,
            checkpoint,
            round,
            parent_round,
            last_feed_sequence_number,
            batch_poster_digest,
            feed_merkle_root,
            self.signature_service.clone(),
        )
        .await;

        info!("Created {}", block);

        // #[cfg(feature = "benchmark")]
        // for x in &block.payload {
        //     // NOTE: This log entry is used to compute performance.
        //     info!("Created {} -> {:?}", block, x);
        // }

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

        // Control system: Wait for 2f+1 nodes to acknowledge our block before continuing.
        let mut wait_for_quorum: FuturesUnordered<_> = names
            .into_iter()
            .zip(handles.into_iter())
            .map(|(name, handler)| {
                let stake = self.committee.stake(&name);
                Self::waiter(handler, stake)
            })
            .collect();

        let mut total_stake = self.committee.stake(&self.name);
        while let Some(stake) = wait_for_quorum.next().await {
            total_stake += stake;
            if total_stake >= self.committee.quorum_threshold() {
                break;
            }
        }
    }

    async fn run(&mut self) {
        loop {
            tokio::select! {
                Some(digest) = self.rx_mempool.recv() => {
                    //if self.buffer.len() < 155 {
                        self.buffer.insert(digest);
                    //}
                },
                Some(message) = self.rx_message.recv() => match message {
                    ProposerMessage::Make(round, qc, tc) => self.make_block(round, qc, tc).await,
                    ProposerMessage::Cleanup(digests) => {
                        for x in &digests {
                            self.buffer.remove(x);
                        }
                    }
                }
            }
        }
    }
}
