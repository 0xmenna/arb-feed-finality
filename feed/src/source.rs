use anyhow::Result;
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use codec::{Decode, Encode};
use evm::{Address, BigInt, H256};
use futures::{FutureExt, StreamExt};
use log::{debug, warn};
use serde::Deserialize;
use std::time::Duration;
use tokio::{
    sync::mpsc::Sender,
    time::{sleep, Interval},
};
use tokio_tungstenite::connect_async;
use tungstenite::Message;

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BroadcastMessage {
    pub version: u32,
    pub messages: Vec<BroadcastFeedMessage>,
    pub confirmed_sequence_number_message: Option<ConfirmedSequenceNumberMessage>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfirmedSequenceNumberMessage {
    pub sequence_number: u64,
}

#[derive(Encode, Decode, Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BroadcastFeedMessage {
    pub sequence_number: u64,
    pub message: MessageWithMetadata,
    pub signature: Option<Signature>,
}

#[derive(Encode, Decode, Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MessageWithMetadata {
    pub message: L1IncomingMessage,
    pub delayed_messages_read: u64,
}

#[derive(Encode, Decode, Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct L1IncomingMessage {
    pub header: L1IncomingMessageHeader,
    pub l2_msg: L2Message,
    pub batch_gas_cost: Option<u64>,
}

#[derive(Encode, Decode, Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct L1IncomingMessageHeader {
    pub kind: u8,
    pub sender: Address,
    pub block_number: u64,
    pub timestamp: u64,
    pub request_id: Option<H256>,
    pub base_fee_l1: Option<BigInt>,
}

type L2Message = Base64Bytes;

type Signature = Base64Bytes;

#[derive(Encode, Decode, Debug, Clone)]
pub struct Base64Bytes(Vec<u8>);

impl Base64Bytes {
    pub fn size(&self) -> usize {
        self.0.len()
    }
}

impl From<Vec<u8>> for Base64Bytes {
    fn from(val: Vec<u8>) -> Self {
        Self(val)
    }
}

impl AsRef<[u8]> for Base64Bytes {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl<'de> Deserialize<'de> for Base64Bytes {
    fn deserialize<D>(deserializer: D) -> Result<Base64Bytes, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let base64_str = String::deserialize(deserializer)?;

        let l2_msg = BASE64
            .decode(&base64_str)
            .map_err(|_| serde::de::Error::custom("Base64 L2Msg has invalid format"))?;

        Ok(Base64Bytes(l2_msg))
    }
}

pub struct FeedSource {
    feed_url: String,
    tx: Sender<BroadcastMessage>,
    poll_interval: Interval,
}

impl FeedSource {
    /// Spawns a new feed source that connects to the given feed URL and sends messages to the given channel.
    pub fn spawn(feed_url: String, tx: Sender<BroadcastMessage>, poll_interval: Interval) {
        tokio::spawn(async move {
            Self {
                feed_url,
                tx,
                poll_interval,
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        let mut retry_attempt = 0;

        loop {
            let connection = self.connect_and_listen().await;
            if let Err(err) = connection {
                warn!("Error in feed connection: {:?}", err);
                retry_attempt += 1;
                let backoff = Self::calculate_backoff(retry_attempt);
                warn!(
                    "Retrying connection in {} seconds (attempt {})",
                    backoff.as_secs(),
                    retry_attempt
                );
                sleep(backoff).await;
            }
        }
    }

    async fn connect_and_listen(&mut self) -> Result<()> {
        // Establish WebSocket connection to read the sequencer's feed
        let (stream, _) = connect_async(&self.feed_url).await?;

        log::info!("Successfully connected to the real-time feed");

        let (_, mut read) = stream.split();

        loop {
            self.poll_interval.tick().await;

            if let Some(msg_res) = read.next().now_or_never().unwrap_or_default() {
                match Self::process_message(msg_res).await {
                    Ok(broadcast_msg) => {
                        log::debug!(
                            "Received broadcast messages starting from sequence number: {}",
                            broadcast_msg.messages[0].sequence_number
                        );
                        self.tx
                            .send(broadcast_msg)
                            .await
                            .expect("Failed to send feed");
                    }
                    Err(err) => debug!("Feed processing message error: {:?}", err),
                }
            }
        }
    }

    async fn process_message(
        message_result: Result<Message, tokio_tungstenite::tungstenite::Error>,
    ) -> Result<BroadcastMessage, String> {
        let msg = message_result.map_err(|e| format!("Cannot read message from feed: {:?}", e))?;

        let json_str = msg
            .to_text()
            .map_err(|e| format!("Cannot convert message to text: {:?}", e))?;

        serde_json::from_str(json_str).map_err(|e| format!("Cannot deserialize message: {:?}", e))
    }

    fn calculate_backoff(attempt: u32) -> Duration {
        let base_delay = 2u64; // 2 seconds
        let max_delay = 60u64; // 1 minute
        let delay = base_delay.saturating_pow(attempt).min(max_delay);
        Duration::from_secs(delay)
    }
}
