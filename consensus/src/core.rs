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
use feed::batch_maker::{BatchMakerResult, MiniBatchFeed};
use feed::source::BroadcastFeedMessage;
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
    rx_l2_msg: Receiver<BroadcastFeedMessage>,
    tx_mini_batch: Sender<MiniBatchFeed>,
    max_mini_batch_size: usize,
    rx_batch_res: Receiver<BatchMakerResult>,
    tx_commit_batch: Sender<Digest>,
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
        rx_l2_msg: Receiver<BroadcastFeedMessage>,
        tx_mini_batch: Sender<MiniBatchFeed>,
        max_mini_batch_size: usize,
        rx_batch_res: Receiver<BatchMakerResult>,
        tx_commit_batch: Sender<Digest>,
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
                rx_l2_msg,
                tx_mini_batch,
                max_mini_batch_size,
                rx_batch_res,
                tx_commit_batch,
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

        if self.name != self.committee.leader {
            // Verify the remaining safety rules
            // In this prototype, we assume the node is in sync within the ongoing batch.
            // In production, if the node is not in sync, it should retrieve the missing feeds to reconstruct the correct batch.

            // Extract the feed messages from the node feed source until the block's last feed sequence number.

            let mut feed_messages = Vec::new();

            // In production based systems, if the feed source is malicious, it could wait indefinitely or consistently process wrong messages. Implement a timeout fallback path to avoid this and ask other nodes for the missing feeds.
            let mut parent_round = 0;
            let mut total_size = 0;
            while let Some(item) = self.rx_l2_msg.recv().await {
                let size = item.message.message.l2_msg.size();
                total_size += size;
                if total_size > self.max_mini_batch_size {
                    warn!(
                        "Mini batch size exceeded: {} bytes, max allowed is {} bytes",
                        total_size, self.max_mini_batch_size
                    );
                    return None;
                }

                let msg_seq_num = item.sequence_number;
                let l1_round = item.message.message.header.block_number;
                feed_messages.push(item);

                if msg_seq_num == block.last_feed_sequence_number {
                    // We have reached the last feed sequence number included in this block.
                    parent_round = l1_round;
                    break;
                }
            }

            // 2. Send the mini batch to the batch maker.
            assert!(!feed_messages.is_empty(), "Mini batch should not be empty");

            let mini_batch = MiniBatchFeed {
                messages: feed_messages,
                round: parent_round,
            };

            self.tx_mini_batch
                .send(mini_batch)
                .await
                .expect("Failed to send mini batch");

            // 3. Wait for the batch maker to process the mini batch and get its result.
            let batch_res = self
                .rx_batch_res
                .recv()
                .await
                .expect("Failed to receive batch result");

            let checkpoint = self.high_qc.view.checkpoint;
            let round = self.high_qc.view.round;

            let (round, checkpoint, digest) = match batch_res {
                BatchMakerResult::Ongoing(batch) => {
                    let round = round + 1;
                    let checkpoint = checkpoint;

                    (round, checkpoint, batch.digest)
                }
                BatchMakerResult::New(batch) => {
                    // Reset round for new batch
                    let round = 0;
                    let checkpoint = checkpoint + 1;

                    (round, checkpoint, batch.digest)
                }
            };

            let safety_rule_4 = block.view.checkpoint == checkpoint;
            let safety_rule_5 = block.view.round == round;
            let safety_rule_6 = block.batch_poster_digest == digest;

            if !(safety_rule_4 && safety_rule_5 && safety_rule_6) {
                return None;
            }
        }

        // Ensure we won't vote for contradicting blocks.
        self.increase_last_voted_view(block.view);
        self.increase_msg_seq_num(block.last_feed_sequence_number);

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
                // Wait until some mini batch is not sent.
                while !self.send_mini_batch().await {}
                self.generate_proposal().await;
                self.last_proposal_time = Instant::now();
            }
        }
        Ok(())
    }

    pub async fn send_mini_batch(&mut self) -> bool {
        let mut feed_messages = Vec::new();
        let mut total_size = 0;

        let mut sent = false;
        while let Ok(item) = self.rx_l2_msg.try_recv() {
            let msg_size = item.message.message.l2_msg.size();

            if msg_size > self.max_mini_batch_size {
                warn!(
                    "Received oversized message of size {} bytes, max allowed is {} bytes",
                    msg_size, self.max_mini_batch_size
                );
                continue;
            }

            if total_size + msg_size > self.max_mini_batch_size && !feed_messages.is_empty() {
                self.flush_mini_batch(&mut feed_messages).await;
                total_size = 0;

                sent = true;
            }

            total_size += msg_size;
            feed_messages.push(item);
        }

        if !feed_messages.is_empty() {
            self.flush_mini_batch(&mut feed_messages).await;
            sent = true;
        }

        sent
    }

    async fn flush_mini_batch(&self, feed_messages: &mut Vec<BroadcastFeedMessage>) {
        // Use the last message's block number as the round for the mini batch.
        let parent_round = feed_messages
            .last()
            .expect("Mini batch is expected to have at least one message")
            .message
            .message
            .header
            .block_number;

        let batch = MiniBatchFeed {
            messages: std::mem::take(feed_messages),
            round: parent_round,
        };

        self.tx_mini_batch
            .send(batch)
            .await
            .expect("Failed to send mini batch");
    }

    #[async_recursion]
    async fn generate_proposal(&mut self) {
        let checkpoint = self.high_qc.view.checkpoint;
        let round = self.high_qc.view.round;

        let batch_res = self
            .rx_batch_res
            .recv()
            .await
            .expect("Failed to receive batch result");

        let (checkpoint, round, batch) = match batch_res {
            BatchMakerResult::Ongoing(batch) => {
                debug!("Batch is ongoing: {:?}", batch);
                let round = round + 1;
                let checkpoint = checkpoint;

                (checkpoint, round, batch)
            }
            BatchMakerResult::New(batch) => {
                debug!("New batch created: {:?}", batch);
                // Reset round for new batch
                let round = 0;
                let checkpoint = checkpoint + 1;

                (checkpoint, round, batch)
            }
        };

        let view = View::new(checkpoint, round);

        // TODO: This must be based on the batch logic
        let proposal = MakeProposal {
            view,
            // As of now this is not used
            parent_round: BigInt::default(),
            latest_msg_seq_num: batch.latest_seq_num,
            batch_poster_digest: batch.digest,
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
            // Upon booting, send mini batch to the batch maker and generate the very first block (only if we are the leader).

            // Wait until some mini batch is not sent.
            while !self.send_mini_batch().await {}
            // Generate the first proposal.
            self.generate_proposal().await;
        }

        // This is the main loop: it processes incoming blocks and votes,
        // and receive timeout notifications from our Timeout Manager.
        loop {
            let result = tokio::select! {
                Some(message) = self.rx_message.recv() => match message {
                    ConsensusMessage::Propose(block) => self.handle_proposal(&block).await,
                    ConsensusMessage::Vote(vote) => self.handle_vote(&vote).await
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
