use std::io::Write;

use crate::{
    processor::{BatchPosterMetadata, BatchPosterPosition, BATCH_POSTER_META_KEY},
    source::{BroadcastFeedMessage, MessageWithMetadata},
};
use brotli::CompressorWriter;
use codec::Encode;
use crypto::{keccak, Digest};
use evm::{abi, Address, BigInt};
use log::{debug, warn};
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};

const BATCH_SEGMENT_KIND_L2_MESSAGE: u8 = 0;
const BATCH_SEGMENT_KIND_DELAYED_MESSAGES: u8 = 2;
const BATCH_SEGMENT_KIND_ADVANCE_TIMESTAMP: u8 = 3;
const BATCH_SEGMENT_KIND_ADVANCE_L1_BLOCK_NUMBER: u8 = 4;

const MAX_DECOMPRESSED_LEN: usize = 1024 * 1024 * 16; // 16 MiB
const MAX_SEGMENTS_PER_SEQUENCER_MESSAGE: usize = 100 * 1024;

const BROTLI_MESSAGE_HEADER_BYTE: u8 = 0;

type Round = BigInt;

/// Whether to close the batch or not and the digest of the resulting batch
pub enum BatchMakerResult {
    /// The batch is ongoing and has not yet been closed. It cointains the batch digest
    Ongoing(BuildingBatchDigest),
    /// A new batch with the new associated digest
    New(BuildingBatchDigest),
    /// An error occured while processing the batch
    Error(anyhow::Error),
}

pub struct MiniBatchFeed {
    round: Round,
    messages: Vec<BroadcastFeedMessage>,
}

pub type BuildingBatchDigest = Digest;

struct OngoingBatch {
    /// The opening round of the ongoing building batch
    opening_round: Round,
    /// The size of the batch (size of total messages in bytes)
    size: usize,
    /// The building batch
    batch: BuildingBatch,
    /// Batch poster message of the batch with its digest
    batchposter_msg: Option<(Digest, BatchPosterMsg)>,
}

impl OngoingBatch {
    pub fn new(position: BatchPosterPosition) -> Self {
        Self {
            opening_round: Round::zero(),
            size: 0,
            batch: BuildingBatch::new(
                position.msg_count,
                position.msg_count,
                position.delayed_msg_count,
            ),
            batchposter_msg: None,
        }
    }

    fn opening_round(&self) -> Round {
        self.opening_round
    }

    fn size(&self) -> usize {
        self.size
    }

    pub fn add_message(&mut self, msg: &BroadcastFeedMessage) {
        self.batch
            .segments
            .add_message(&msg.message)
            .expect("Message must fit into batch segments");

        self.batch.msg_count = self.batch.msg_count.saturating_add(1);
    }

    pub fn build_batchposter_digest(
        mut self,
        position: &BatchPosterPosition,
    ) -> Result<(Self, Digest), anyhow::Error> {
        // Build the batch digest
        let after_delayed_msg = self.batch.segments.delayed_msg();
        let (cloned_segments, msg) = self.batch.segments.close()?;
        self.batch.segments = cloned_segments;

        let msg = msg.unwrap_or_default();

        let batchposter_msg = BatchPosterMsg {
            sequence_number: position.next_seq_number,
            msg,
            after_delayed_msg,
            gas_refunder: Address::zero(), // TODO: think about this later
            prev_msg_count: position.msg_count,
            new_msg_count: self.batch.msg_count,
        };

        let batchposter_digest = keccak::hash(batchposter_msg.encode());

        self.batchposter_msg = Some((batchposter_digest.clone(), batchposter_msg));

        Ok((self, batchposter_digest))
    }

    pub fn is_opened(&self) -> bool {
        !self.opening_round.is_zero()
    }

    pub fn open(&mut self, round: Round) {
        self.opening_round = round;
    }

    pub fn close_batch(self) -> Result<(BuildingBatch, Digest, BatchPosterMsg), anyhow::Error> {
        let (digest, batchposter) = self.batchposter_msg.ok_or_else(|| {
            anyhow::anyhow!("Batchposter message must be present when closing the batch")
        })?;

        Ok((self.batch, digest, batchposter))
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
                    let mini_batch_size = mini_batch.messages.iter()
                        .map(|msg| msg.message.message.l2_msg.size())
                        .sum();

                    // If the mini batch alone exceeds the maximum allowed size, error out.
                    if mini_batch_size > self.max_size {
                        warn!("Mini batch cannot be handled because it exceeds the batch maximum size");
                        self.tx_result.send(
                            BatchMakerResult::Error(anyhow::anyhow!("Mini batch exceeds maximum size"))
                        ).await.expect("Failed to send error result");
                        continue;
                    }

                    // Check if adding this mini batch would exceed the ongoing batch size.
                    let new_batch_size = ongoing_batch.size() + mini_batch_size;
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

                    // If the batch should be sealed, close it and prepare for a new batch.
                    if seal_batch {
                        let (batch, digest, batchposter_data) = ongoing_batch
                            .close_batch()
                            .expect("Batch is expected to be closed");
                        debug_assert!(committed_batch.is_none());

                        // Update the batch poster position for the next batch.
                        self.batchposter_pos = BatchPosterPosition {
                            msg_count: batch.msg_count,
                            delayed_msg_count: batch.segments.delayed_msg(),
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
                        // Start the new batch with the size of the current mini batch.
                        ongoing_batch.size = mini_batch_size;
                    }

                    // Process each message in the mini batch.
                    for msg in mini_batch.messages.iter() {
                        debug!(
                            "BatchMaker processing message with sequence number: {}",
                            msg.sequence_number
                        );
                        ongoing_batch.add_message(&msg);
                        latest_seq_num = msg.sequence_number;
                    }

                    // Build the batchposter digest from the ongoing batch.
                    let (batch, digest) = ongoing_batch
                        .build_batchposter_digest(&self.batchposter_pos)
                        .expect("Failed to close batch segments");
                    ongoing_batch = batch;

                    // Send either a new or ongoing batch result based on whether the batch was sealed.
                    let result = if seal_batch {
                        BatchMakerResult::New(digest)
                    } else {
                        BatchMakerResult::Ongoing(digest)
                    };
                    self.tx_result.send(result).await.expect("Failed to send batch result");
                },
                // Process commit commands.
                Some(commit_digest) = self.rx_commit_batch.recv() => {
                    let batch = committed_batch.expect("No batch to commit");
                    debug_assert_eq!(commit_digest, batch.digest.clone());

                    self.store.write(batch.digest.to_vec(), batch.msg.encode()).await;
                    self.store.write(BATCH_POSTER_META_KEY.to_vec(), batch.metadata.encode()).await;

                    committed_batch = None;

                    // TODO: Delete old committed batches
                }
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

    pub fn close(mut self) -> anyhow::Result<(Self, Option<Vec<u8>>)> {
        let cloned = Self {
            compressed_writer: self.compressed_writer,
            raw_segments: self.raw_segments.clone(),
            delayed_msg: self.delayed_msg,
            timestamp: self.timestamp,
            block_num: self.block_num,
            new_uncompressed_size: self.new_uncompressed_size,
            total_uncompressed_size: self.total_uncompressed_size,
            last_compressed_size: self.last_compressed_size,
            trailing_headers: self.trailing_headers,
        };

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
            return Ok((cloned, None));
        }

        self.compressed_writer.flush()?;
        let compressed_bytes = self.compressed_writer.into_inner();
        let mut full_msg: Vec<u8> = vec![BROTLI_MESSAGE_HEADER_BYTE];
        full_msg.extend_from_slice(&compressed_bytes);
        Ok((cloned, Some(full_msg)))
    }
}
