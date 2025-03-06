use crate::source::BroadcastFeedMessage;
use crate::source::BroadcastMessage;
use bincode::de;
use codec::{Decode, Encode};
use log::debug;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::oneshot;

const FEED_KEY_PREFIX: &[u8] = b"feed";

pub const BATCH_POSTER_META_KEY: &[u8] = b"bp_meta";

#[derive(Debug, Clone, Encode, Decode, Default)]
pub struct BatchPosterMetadata {
    /// The batch poster postion
    pub position: BatchPosterPosition,
    /// The sequence number of the last message within the sealed batch
    pub last_msg_seq_number: u64,
}

impl BatchPosterMetadata {
    pub fn new(position: BatchPosterPosition, last_msg_seq_number: u64) -> Self {
        Self {
            position,
            last_msg_seq_number,
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

/// Stores feeds .
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
            // The sequence number of the last message of the last sealed batch
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
                    if msg.sequence_number <= batchposter_meta.last_msg_seq_number {
                        debug!(
                            "Skipping message with sequence number: {}",
                            msg.sequence_number
                        );
                        continue;
                    }
                    // Check wheter the message is already in the store.
                    // If no message is found, write the message to the store.
                    // If a message is found, use the found message rather than the proposed message.
                    let msg_key = build_feed_key(msg.sequence_number);
                    let maybe_msg = store
                        .read(msg_key)
                        .await
                        .expect("Failed to read from store");
                    let msg = if let None = maybe_msg {
                        store.write(msg_key, msg.encode()).await;

                        msg.encode()
                    } else {
                        maybe_msg.unwrap()
                    };

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
