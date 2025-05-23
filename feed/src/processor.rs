use crate::source::BroadcastFeedMessage;
use crate::source::BroadcastMessage;
use codec::{Decode, Encode};
use log::debug;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::oneshot;

pub const FEED_KEY_PREFIX: &[u8] = b"feed_";

pub const BATCH_POSTER_META_KEY: &[u8] = b"bp_meta";

#[derive(Debug, Clone, Encode, Decode, Default)]
pub struct BatchPosterMetadata {
    /// The batch poster postion
    pub position: BatchPosterPosition,
    /// The sequence number of the last committed message within the sealed batch
    pub last_committed_msg_seq_number: u64,
}

impl BatchPosterMetadata {
    pub fn new(position: BatchPosterPosition, last_msg_seq_number: u64) -> Self {
        Self {
            position,
            last_committed_msg_seq_number: last_msg_seq_number,
        }
    }
}

#[derive(Debug, Clone, Encode, Decode, Copy)]
pub struct BatchPosterPosition {
    /// Number of messages processed by the batch maker at the begginning of the batch
    pub msg_count: u64,
    /// Number of delayed messages processed by the batch maker at the begginning of the batch
    pub delayed_msg_count: u64,
    /// The batch sequece number
    pub next_seq_number: u64,
}

impl BatchPosterPosition {
    pub fn new(msg_count: u64, delayed_msg_count: u64, next_seq_number: u64) -> Self {
        Self {
            msg_count,
            delayed_msg_count,
            next_seq_number,
        }
    }
}

impl Default for BatchPosterPosition {
    fn default() -> Self {
        Self {
            msg_count: 1,
            delayed_msg_count: 1,
            next_seq_number: 1,
        }
    }
}

/// Process incoming feeds.
pub struct Processor;

impl Processor {
    pub fn spawn(
        // The persistent storage.
        mut store: Store,
        // Input channel to receive feeds.
        mut rx_feed: Receiver<BroadcastMessage>,
        // Output channel to send out feed messages.
        tx_msg: Sender<BroadcastFeedMessage>,
        // A one shot channel that sends the batch poster position
        tx_batchposter_pos: oneshot::Sender<BatchPosterPosition>,
    ) {
        tokio::spawn(async move {
            // TODO: The design implementation should be:
            // 1. Read the latest committed block on ethereum and the associated batch poster position from the store.
            // 2. Read the same data from ethereum, in case the store is far behind and confront the two.
            // 3. Extract from the block of the latest committed batch the last committed sequence number message.
            // 4. Extract the message sequence number of the incoming feed (to know the range of missing messages - from the (last committed message + 1) to the incoming feed).
            // 5. Sync using a syncronizer (that leverages the DHT) the missing mesages within the extracted range.
            // 6. Send the messages from the (last committed message + 1) onwards to the batch maker.
            // (The assumption is that the batch poster position read is actually the previous of the ongoing one, but in rare cases it could be an older one, so you must manage a scenario in which this happens: todo)

            // Current implementation is simpler:
            // Just read the last batch poster position from the store and the last committed message sequence number.
            // The bigger assumption here is that the node is in sync

            // The sequence number of the last committed message of the last sealed batch
            let batchposter_meta = store
                .read(BATCH_POSTER_META_KEY.to_vec())
                .await
                .expect("Failed to read from store")
                .map(|bytes| {
                    BatchPosterMetadata::decode(&mut bytes.as_slice())
                        .expect("Failed to decode batch poster metadata")
                })
                .unwrap_or_default();
            // Send the batch poster position
            tx_batchposter_pos
                .send(batchposter_meta.position)
                .expect("Failed to send batch poster position");

            while let Some(feed) = rx_feed.recv().await {
                for msg in feed.messages {
                    if msg.sequence_number <= batchposter_meta.last_committed_msg_seq_number {
                        debug!(
                            "Skipping message with sequence number: {}",
                            msg.sequence_number
                        );
                        continue;
                    }
                    // Send the feed message
                    tx_msg.send(msg).await.expect("Failed to send feed message");
                }
            }
        });
    }
}

fn build_feed_key(msg_seq_num: u64) -> Vec<u8> {
    // key prefix
    let mut key = FEED_KEY_PREFIX.to_vec();
    key.extend_from_slice(&msg_seq_num.encode());

    key
}
