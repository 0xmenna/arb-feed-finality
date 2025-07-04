use std::io::Write;

use crate::{
    processor::{BatchPosterMetadata, BatchPosterPosition, BATCH_POSTER_META_KEY},
    source::{BroadcastFeedMessage, MessageWithMetadata},
};
use brotli::CompressorWriter;
use codec::Encode;
use crypto::{keccak, merkle::MerkleTree, Digest};
use evm::{abi, Address};
use log::{debug, info};
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};

const BATCH_SEGMENT_KIND_L2_MESSAGE: u8 = 0;
const BATCH_SEGMENT_KIND_DELAYED_MESSAGES: u8 = 2;
const BATCH_SEGMENT_KIND_ADVANCE_TIMESTAMP: u8 = 3;
const BATCH_SEGMENT_KIND_ADVANCE_L1_BLOCK_NUMBER: u8 = 4;

const MAX_DECOMPRESSED_LEN: usize = 1024 * 1024 * 16; // 16 MiB
const MAX_SEGMENTS_PER_SEQUENCER_MESSAGE: usize = 100 * 1024;

const BROTLI_MESSAGE_HEADER_BYTE: u8 = 0;

type Round = u64;

/// Whether to close the batch or not and the digest of the resulting batch
#[derive(Debug, Clone)]
pub struct BatchMakerResult {
    pub closed_batch_digest: Option<Digest>,
    pub merkle_root: Digest,
    pub latest_seq_num: u64,
    pub size: usize,
}

pub struct MiniBatchFeed {
    pub round: Round,
    pub messages: Vec<BroadcastFeedMessage>,
}

struct OngoingBatch {
    /// The opening round of the ongoing building batch
    opening_round: Round,
    /// The building batch
    batch: BuildingBatch,
    /// The size of the batch in bytes (based on the added messages)
    size: usize,
    /// The merkle tree of the batch
    merkle_tree: MerkleTree,
}

impl OngoingBatch {
    pub fn new(position: BatchPosterPosition) -> Self {
        Self {
            opening_round: 0,
            batch: BuildingBatch::new(
                position.msg_count,
                position.msg_count,
                position.delayed_msg_count,
            ),
            size: 0,
            merkle_tree: MerkleTree::new(),
        }
    }

    fn opening_round(&self) -> Round {
        self.opening_round
    }

    fn commit_tree(&mut self) -> Option<Digest> {
        self.merkle_tree.commit()
    }

    pub fn add_message(&mut self, msg: &BroadcastFeedMessage) {
        self.batch
            .segments
            .add_message(&msg.message)
            .expect("Message must fit into batch segments");

        self.size += msg.message.message.l2_msg.size();

        let ins = self.merkle_tree.insert(msg.encode());
        debug_assert!(ins.is_ok(), "The merkle tree is not expected to overflow");

        self.batch.msg_count = self.batch.msg_count.saturating_add(1);
    }

    pub fn is_opened(&self) -> bool {
        !(self.opening_round == 0)
    }

    pub fn open(&mut self, round: Round) {
        self.opening_round = round;
    }

    pub fn close_batch(
        self,
        position: &BatchPosterPosition,
    ) -> Result<(Digest, BatchPosterMsg), anyhow::Error> {
        // Build the batch digest
        let after_delayed_msg = self.batch.segments.delayed_msg();
        let msg = self.batch.segments.close()?;

        let msg = msg.unwrap_or_default();

        let batchposter_msg = BatchPosterMsg {
            sequence_number: position.next_seq_number,
            msg: msg.clone(),
            after_delayed_msg,
            gas_refunder: Address::zero(), // TODO: think about this later
            prev_msg_count: position.msg_count,
            new_msg_count: self.batch.msg_count,
        };

        let batchposter_digest = keccak::hash(batchposter_msg.encode());

        Ok((batchposter_digest, batchposter_msg))
    }
}

pub struct SealBatch {
    /// The batch digest
    digest: Digest,
    /// The batchposter message
    msg: BatchPosterMsg,
    /// The batch metadata
    metadata: BatchPosterMetadata,
}

impl SealBatch {
    pub fn new(digest: Digest, msg: BatchPosterMsg, metadata: BatchPosterMetadata) -> Self {
        Self {
            digest,
            msg,
            metadata,
        }
    }
}

pub struct BatchMaker {
    /// Max size of feed messages within a single batch
    max_size: usize,
    /// The maximum number of rounds before closeing the batch
    max_rounds: Round,
    /// The initial position of the batch poster
    batchposter_pos: BatchPosterPosition,
    /// The channel that receives a mini batch containing feed messages
    rx_mini_batch: Receiver<MiniBatchFeed>,
    /// The data store
    store: Store,
    /// A sender that outputs the batch maker result
    tx_result: Sender<BatchMakerResult>,
    /// The channel that receives the commit batch command with the batchposter digest
    rx_commit_batch: Receiver<Digest>,
}

impl BatchMaker {
    pub fn spawn(
        max_size: usize,
        max_rounds: Round,
        batchposter_pos: BatchPosterPosition,
        rx_mini_batch: Receiver<MiniBatchFeed>,
        store: Store,
        tx_result: Sender<BatchMakerResult>,
        rx_commit_batch: Receiver<Digest>,
    ) {
        tokio::spawn(async move {
            Self {
                max_size,
                max_rounds,
                batchposter_pos,
                rx_mini_batch,
                store,
                tx_result,
                rx_commit_batch,
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        debug!(
            "BatchMaker starting from batch position: {:?}",
            self.batchposter_pos
        );

        // Initialize the ongoing batch using the current batch poster position.
        let mut ongoing_batch = OngoingBatch::new(self.batchposter_pos);
        // Track the latest processed message sequence number.
        let mut latest_seq_num = 0;
        // Flag indicating if the current batch must be sealed.
        let mut seal_batch = false;
        // Holds the sealed batch ready to be committed.
        let mut committed_batch = None;

        loop {
            tokio::select! {
                // Process incoming mini batches.
                Some(mini_batch) = self.rx_mini_batch.recv() => {
                    // Compute the total size of the mini batch.
                    let mini_batch_size: usize = mini_batch.messages.iter()
                        .map(|msg| msg.message.message.l2_msg.size())
                        .sum();

                    assert!(mini_batch_size <= self.max_size, "The mini batch is not expected to exceed the maximum size");

                    // Check if adding this mini batch would exceed the ongoing batch size.
                    let new_batch_size = ongoing_batch.size + mini_batch_size;
                    let exceeds_size = new_batch_size > self.max_size;

                    // Open the ongoing batch if it isn’t already opened.
                    if !ongoing_batch.is_opened() {
                        ongoing_batch.open(mini_batch.round);
                    }
                    debug_assert!(mini_batch.round >= ongoing_batch.opening_round());

                    // Check if the number of rounds exceeds the maximum allowed.
                    let rounds_elapsed = mini_batch.round - ongoing_batch.opening_round();
                    let exceeds_rounds = rounds_elapsed > self.max_rounds;

                    // Mark the batch for sealing if either condition is met.
                    seal_batch = exceeds_size || exceeds_rounds;

                    // If the batch should be sealed, close it and return the closed batch digest. Then, prepare for a new batch.
                    let maybe_digest = if seal_batch {
                        let (digest, batchposter_data) = ongoing_batch
                            .close_batch(&self.batchposter_pos)
                            .expect("Batch is expected to be closed");

                        // Update the batch poster position for the next batch.
                        self.batchposter_pos = BatchPosterPosition {
                            msg_count: batchposter_data.new_msg_count,
                            delayed_msg_count: batchposter_data.after_delayed_msg,
                            next_seq_number: self.batchposter_pos.next_seq_number.saturating_add(1),
                        };

                        let metadata = BatchPosterMetadata {
                            position: self.batchposter_pos.clone(),
                            last_committed_msg_seq_number: latest_seq_num,
                        };

                        committed_batch = Some(SealBatch::new(digest, batchposter_data, metadata));

                        // Reinitialize the ongoing batch, starting it with the mini batch's round.
                        ongoing_batch = OngoingBatch::new(self.batchposter_pos);
                        ongoing_batch.open(mini_batch.round);

                        Some(digest)
                    } else {
                        None
                    };

                    // Process each message in the mini batch.
                    for msg in mini_batch.messages.iter() {
                        debug!(
                            "BatchMaker processing message with sequence number: {}",
                            msg.sequence_number
                        );
                        ongoing_batch.add_message(&msg);
                        latest_seq_num = msg.sequence_number;
                    }

                    let res = BatchMakerResult {
                        closed_batch_digest: maybe_digest,
                        merkle_root: ongoing_batch.commit_tree().expect("Root is expected to be computed"),
                        latest_seq_num,
                        size: ongoing_batch.size,
                    };

                    self.tx_result.send(res).await.expect("Failed to send batch result");
                },
                // Process commit commands.
                Some(commit_digest) = self.rx_commit_batch.recv() => {
                    let batch = committed_batch.expect("No batch to commit");
                    debug_assert_eq!(commit_digest, batch.digest.clone());

                    self.store.write(batch.digest.to_vec(), batch.msg.encode()).await;
                    self.store.write(BATCH_POSTER_META_KEY.to_vec(), batch.metadata.encode()).await;

                    committed_batch = None;

                    // TODO: Delete old committed batches
                },
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct BatchPosterMsg {
    pub sequence_number: u64,
    pub msg: Vec<u8>,
    pub after_delayed_msg: u64,
    pub gas_refunder: Address,
    pub prev_msg_count: u64,
    pub new_msg_count: u64,
}

impl Encode for BatchPosterMsg {
    fn encode(&self) -> Vec<u8> {
        // Create ABI tokens for each field
        let tokens = vec![
            abi::Token::Uint(self.sequence_number.into()),
            abi::Token::Bytes(self.msg.clone()),
            abi::Token::Uint(self.after_delayed_msg.into()),
            abi::Token::Address(self.gas_refunder),
            abi::Token::Uint(self.prev_msg_count.into()),
            abi::Token::Uint(self.new_msg_count.into()),
        ];

        // Encode the tokens into a byte array
        abi::encode(&tokens)
    }
}

pub struct BuildingBatch {
    /// Instance that handles the segments of the batch
    pub segments: BatchSegments,
    /// Message count at the start of the batch.
    pub start_msg_count: u64,
    /// Current message count of the batch.
    pub msg_count: u64,
}

impl BuildingBatch {
    pub fn new(start_msg_count: u64, msg_count: u64, first_delayed: u64) -> Self {
        let segments = BatchSegments::new(first_delayed);

        Self {
            segments,
            start_msg_count,
            msg_count,
        }
    }

    pub fn size(&self) -> usize {
        self.segments.total_uncompressed_size
    }
}

const COMPRESSION_LEVEL: u32 = 11;
const LGWIN: u32 = 22;
pub const MAX_SIZE: usize = 90000;

pub struct BatchSegments {
    compressed_writer: CompressorWriter<Vec<u8>>,
    raw_segments: Vec<Vec<u8>>,
    timestamp: u64,
    block_num: u64,
    delayed_msg: u64,
    new_uncompressed_size: usize,
    total_uncompressed_size: usize,
    last_compressed_size: usize,
    trailing_headers: usize,
}

impl BatchSegments {
    pub fn new(first_delayed: u64) -> Self {
        let compressed_writer =
            CompressorWriter::new(Vec::new(), MAX_SIZE * 2, COMPRESSION_LEVEL, LGWIN);

        Self {
            compressed_writer,
            raw_segments: Vec::new(),
            delayed_msg: first_delayed,
            timestamp: 0,
            block_num: 0,
            new_uncompressed_size: 0,
            total_uncompressed_size: 0,
            last_compressed_size: 0,
            trailing_headers: 0,
        }
    }
}

impl BatchSegments {
    fn test_for_overflow(&mut self, is_header: bool) -> anyhow::Result<bool> {
        if self.total_uncompressed_size > MAX_DECOMPRESSED_LEN {
            return Ok(true);
        }

        if self.raw_segments.len() >= MAX_SEGMENTS_PER_SEQUENCER_MESSAGE {
            return Ok(true);
        }

        if (self.last_compressed_size + self.new_uncompressed_size) < MAX_SIZE {
            return Ok(false);
        }

        if is_header || self.raw_segments.len() == self.trailing_headers {
            return Ok(false);
        }

        self.compressed_writer.flush()?;
        self.last_compressed_size = self.compressed_writer.get_ref().len();
        self.new_uncompressed_size = 0;

        if self.last_compressed_size >= MAX_SIZE {
            return Ok(true);
        }
        Ok(false)
    }

    fn add_segment_to_compressed(&mut self, segment: Vec<u8>) -> anyhow::Result<()> {
        let encoded = evm::rlp::encode(&segment);
        let len_written = self.compressed_writer.write(&encoded)?;
        self.new_uncompressed_size += len_written;
        self.total_uncompressed_size += len_written;

        Ok(())
    }

    pub fn add_segment(&mut self, segment: Vec<u8>, is_header: bool) -> anyhow::Result<()> {
        self.add_segment_to_compressed(segment.clone())?;

        let overflow = self.test_for_overflow(is_header)?;
        if overflow {
            // current implementation just returns an error if it overflows
            return Err(anyhow::anyhow!("Batch segments overflowed"));

            // self.close()?;
            // return Ok(());
        }

        self.raw_segments.push(segment);
        if is_header {
            self.trailing_headers += 1;
        } else {
            self.trailing_headers = 0;
        }

        Ok(())
    }

    fn prepare_int_segment(&self, val: u64, segment_header: u8) -> Vec<u8> {
        let mut segment = vec![segment_header];
        segment.extend(evm::rlp::encode(&val));
        segment
    }

    fn maybe_add_diff_segment(
        &mut self,
        base: u64,
        new_val: u64,
        segment_header: u8,
    ) -> anyhow::Result<()> {
        if new_val == base {
            return Ok(());
        }
        let diff = new_val - base;
        let segment = self.prepare_int_segment(diff, segment_header);
        self.add_segment(segment, true)?;

        Ok(())
    }

    fn add_delayed_message(&mut self) -> anyhow::Result<()> {
        let segment = vec![BATCH_SEGMENT_KIND_DELAYED_MESSAGES];
        self.add_segment(segment, false)?;
        self.delayed_msg += 1;

        Ok(())
    }

    pub fn add_message(&mut self, msg: &MessageWithMetadata) -> anyhow::Result<()> {
        if msg.delayed_messages_read > self.delayed_msg {
            if msg.delayed_messages_read != self.delayed_msg + 1 {
                return Err(anyhow::anyhow!(
                    "Attempted to add delayed message {} after {}",
                    msg.delayed_messages_read,
                    self.delayed_msg
                ));
            }
            return self.add_delayed_message();
        }

        self.maybe_add_diff_segment(
            self.timestamp,
            msg.message.header.timestamp,
            BATCH_SEGMENT_KIND_ADVANCE_TIMESTAMP,
        )?;

        self.timestamp = msg.message.header.timestamp;

        self.maybe_add_diff_segment(
            self.block_num,
            msg.message.header.block_number,
            BATCH_SEGMENT_KIND_ADVANCE_L1_BLOCK_NUMBER,
        )?;

        self.block_num = msg.message.header.block_number;

        self.add_l2_msg(msg.message.l2_msg.as_ref())
    }

    fn add_l2_msg(&mut self, l2msg: &[u8]) -> anyhow::Result<()> {
        let mut segment = vec![BATCH_SEGMENT_KIND_L2_MESSAGE];
        segment.extend_from_slice(l2msg);
        self.add_segment(segment, false)
    }

    pub fn delayed_msg(&self) -> u64 {
        self.delayed_msg
    }

    pub fn close(mut self) -> anyhow::Result<Option<Vec<u8>>> {
        // Remove trailing headers
        let len = self.raw_segments.len();
        self.raw_segments
            .truncate(len.saturating_sub(self.trailing_headers));
        self.trailing_headers = 0;

        self.compressed_writer =
            CompressorWriter::new(Vec::new(), MAX_SIZE * 2, COMPRESSION_LEVEL, LGWIN);
        self.new_uncompressed_size = 0;
        self.total_uncompressed_size = 0;

        for segment in self.raw_segments.clone() {
            self.add_segment_to_compressed(segment)?;
        }

        if self.total_uncompressed_size > MAX_DECOMPRESSED_LEN {
            return Err(anyhow::anyhow!(
                "Batch size exceeds maximum decompressed length"
            ));
        }
        if self.raw_segments.len() >= MAX_SEGMENTS_PER_SEQUENCER_MESSAGE {
            return Err(anyhow::anyhow!(
                "Number of raw segments exceeds maximum allowed"
            ));
        }

        if self.raw_segments.is_empty() {
            return Ok(None);
        }

        self.compressed_writer.flush()?;
        let compressed_bytes = self.compressed_writer.into_inner();
        let mut full_msg = vec![BROTLI_MESSAGE_HEADER_BYTE];
        full_msg.extend_from_slice(&compressed_bytes);
        Ok(Some(full_msg))
    }
}
