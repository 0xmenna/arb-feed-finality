use crate::aggregator::Aggregator;
use crate::config::Committee;
use crate::consensus::{ConsensusMessage, View};
use crate::error::{ConsensusError, ConsensusResult};
use crate::messages::{Block, Vote, QC};
use crate::proposer::MakeProposal;
use async_recursion::async_recursion;
use bytes::Bytes;
use codec::Encode;
use crypto::{Digest, Hash};
use crypto::{PublicKey, SignatureService};
use evm::BigInt;
use log::{debug, error, info, warn};
use std::cmp::max;
use std::time::Duration;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::oneshot;
use tokio::time::{sleep, Instant};
use transport::protocol::BLOCK_VOTES_TOPIC;
use transport::Publisher;

pub struct Core {
    name: PublicKey,
    committee: Committee,
    store: Store,
    signature_service: SignatureService,
    rx_message: Receiver<ConsensusMessage>,
    rx_loopback: Receiver<Block>,
    tx_proposer: Sender<MakeProposal>,
    tx_commit: Sender<Block>,
    last_voted_view: View,
    last_committed_view: View,
    committing_block: Block,
    last_msg_sequence_number: u64,
    high_qc: QC,
    aggregator: Aggregator,
    network_publisher: Publisher,
    last_proposal_time: Instant,
    proposal_min_interval: Duration,
}

impl Core {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        signature_service: SignatureService,
        store: Store,
        rx_message: Receiver<ConsensusMessage>,
        rx_loopback: Receiver<Block>,
        tx_proposer: Sender<MakeProposal>,
        tx_commit: Sender<Block>,
        network_publisher: Publisher,
        proposal_min_interval: Duration,
    ) -> oneshot::Sender<()> {
        // Consensus core is being notified when the network is ready.
        let (ready_tx, ready_rx) = oneshot::channel();

        tokio::spawn(async move {
            Self {
                name,
                committee: committee.clone(),
                signature_service,
                store,
                rx_message,
                rx_loopback,
                tx_proposer,
                tx_commit,
                last_voted_view: View::new(0, 0),
                last_committed_view: View::new(0, 0),
                last_msg_sequence_number: 0,
                committing_block: Block::genesis(),
                high_qc: QC::genesis(),
                aggregator: Aggregator::new(committee),
                network_publisher,
                proposal_min_interval,
                last_proposal_time: Instant::now(),
            }
            .run(ready_rx)
            .await
        });

        ready_tx
    }

    async fn store_block(&mut self, block: &Block) {
        // We only store one block at a time, since older blocks are not useful for the feed finality protocol.
        let key = b"cblock".to_vec();
        let value = block.encode();
        self.store.write(key, value).await;
    }

    fn increase_last_voted_view(&mut self, target: View) {
        self.last_voted_view = max(self.last_voted_view, target);
    }

    fn increase_msg_seq_num(&mut self, target: u64) {
        self.last_msg_sequence_number = max(self.last_msg_sequence_number, target);
    }

    async fn try_make_vote(&mut self, block: &Block) -> Option<Vote> {
        // Check if we can vote for this block.
        let safety_rule_1 = block.view > self.last_voted_view;
        let safety_rule_2 = block.view.is_next_of(&block.qc.view);
        let safety_rule_3 = block.last_feed_sequence_number > self.last_msg_sequence_number;

        if !(safety_rule_1 && safety_rule_2 && safety_rule_3) {
            return None;
        }

        // TODO: Implement the feed logic to then check block corrispondence.

        // Ensure we won't vote for contradicting blocks.
        self.increase_last_voted_view(block.view);
        self.increase_msg_seq_num(block.last_feed_sequence_number);
        // TODO [issue #15]: Write to storage preferred_round and last_voted_round.
        Some(Vote::new(block, self.name, self.signature_service.clone()).await)
    }

    async fn commit(&mut self, block: &Block) -> ConsensusResult<()> {
        let committing = self.committing_block.clone();

        if committing.view == block.qc.view && committing.digest() == block.qc.hash {
            self.store_block(&committing).await;
            self.last_committed_view = committing.view;

            info!("Committed {}", committing);
            if let Err(e) = self.tx_commit.send(committing).await {
                warn!("Failed to send block through the commit channel: {}", e);
            }
        }

        self.committing_block = block.clone();

        Ok(())
    }

    fn update_high_qc(&mut self, qc: &QC) {
        if qc.view > self.high_qc.view {
            self.high_qc = qc.clone();
        }
        self.aggregator.cleanup(&qc.view);
    }

    #[async_recursion]
    async fn handle_vote(&mut self, vote: &Vote) -> ConsensusResult<()> {
        debug!("Processing {:?}", vote);
        if vote.view <= self.high_qc.view {
            return Ok(());
        }

        // Ensure the vote is well formed.
        vote.verify(&self.committee)?;

        // Add the new vote to our aggregator and see if we have a quorum.
        if let Some(qc) = self.aggregator.add_vote(vote.clone())? {
            debug!("Assembled {:?}", qc);

            // Process the QC.
            self.update_high_qc(&qc);

            // Make a new block if we are the next leader.
            if self.name == self.committee.leader {
                // This is just to avoid flooding the network if quorum is reached too fast.
                let since_last = Instant::now().duration_since(self.last_proposal_time);
                if since_last < self.proposal_min_interval {
                    let wait = self.proposal_min_interval - since_last;
                    debug!("Waiting {:?} before next proposal", wait);
                    sleep(wait).await;
                }
                self.generate_proposal().await;
                self.last_proposal_time = Instant::now();
            }
        }
        Ok(())
    }

    #[async_recursion]
    async fn generate_proposal(&mut self) {
        // TODO: Might implement here logic of feed construction

        let mut checkpoint = self.high_qc.view.checkpoint;
        let mut round = self.high_qc.view.round;
        let is_new_checkpoint = false; // this must be set appropriately based on batch logic
        if is_new_checkpoint {
            checkpoint += 1;
            round = 0;
        } else {
            round += 1;
        }
        let view = View::new(checkpoint, round);

        // TODO: This will have to be based on the batch logic
        let new_msg_sequence_number = self.last_msg_sequence_number + 1;

        // TODO: This must be based on the batch logic
        let proposal = MakeProposal {
            view,
            parent_round: BigInt::default(),
            last_feed_sequence_number: new_msg_sequence_number,
            batch_poster_digest: Digest::default(),
            feed_merkle_root: Digest::default(),
            qc: self.high_qc.clone(),
        };

        self.tx_proposer
            .send(proposal)
            .await
            .expect("Failed to send message to proposer");
    }

    #[async_recursion]
    async fn process_block(&mut self, block: &Block) -> ConsensusResult<()> {
        debug!("Processing {:?}", block);

        // See if we can vote for this block.
        if let Some(vote) = self.try_make_vote(block).await {
            debug!("Created {:?}", vote);

            if self.name == self.committee.leader {
                self.handle_vote(&vote).await?;
            } else {
                debug!("Sending {:?} to {}", vote, self.committee.leader);
                let message = ConsensusMessage::Vote(vote).encode();
                self.network_publisher
                    .send((BLOCK_VOTES_TOPIC, Bytes::from(message)))
                    .await
                    .expect("Failed to send vote");
            }

            self.commit(&block).await?;
        }

        Ok(())
    }

    async fn handle_proposal(&mut self, block: &Block) -> ConsensusResult<()> {
        // Check the block is correctly formed.
        block.verify(&self.committee)?;

        // Evaluate to update the high QC.
        self.update_high_qc(&block.qc);

        // All check pass, we can process this block.
        self.process_block(block).await
    }

    pub async fn run(&mut self, ready_rx: oneshot::Receiver<()>) {
        // Wait for the network to be ready.
        ready_rx.await.expect("Failed to receive ready signal");

        if self.name == self.committee.leader {
            // Upon booting, generate the very first block (only if we are the leader).
            self.generate_proposal().await;
        }

        // This is the main loop: it processes incoming blocks and votes,
        // and receive timeout notifications from our Timeout Manager.
        loop {
            let result = tokio::select! {
                Some(message) = self.rx_message.recv() => match message {
                    ConsensusMessage::Propose(block) => self.handle_proposal(&block).await,
                    ConsensusMessage::Vote(vote) => self.handle_vote(&vote).await,
                    _ => panic!("Unexpected protocol message")
                },
                Some(block) = self.rx_loopback.recv() => self.process_block(&block).await,
            };
            match result {
                Ok(()) => (),
                Err(ConsensusError::StoreError(e)) => error!("{}", e),
                Err(ConsensusError::CodecError(e)) => error!("Store corrupted. {}", e),
                Err(e) => warn!("{}", e),
            }
        }
    }
}
