use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::mpsc::Sender;
use transport::MessageHandler;

pub type Feed = Vec<u8>;

/// Defines how the network receiver handles incoming transactions.
#[derive(Clone)]
struct TxReceiverHandler {
    tx_batch_maker: Sender<Feed>,
}

#[async_trait]
impl MessageHandler for TxReceiverHandler {
    async fn dispatch(&self, message: Bytes) -> Result<(), Box<dyn core::error::Error>> {
        // Send the transaction to the batch maker.
        self.tx_batch_maker
            .send(message.to_vec())
            .await
            .expect("Failed to send feed");

        // Give the change to schedule other tasks.
        tokio::task::yield_now().await;
        Ok(())
    }
}

pub struct FeedSyncronizer {
    feed: Vec<u8>,
    rx_feed: u8,
}
