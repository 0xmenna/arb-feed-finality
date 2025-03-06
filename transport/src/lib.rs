use std::{
    collections::HashMap,
    fmt::{Display, Formatter},
};

use async_trait::async_trait;
use behaviour::{
    base::{BaseBehaviour, BaseBehaviourEvent},
    wrapped::Wrapped,
};
use builder::P2PTransportBuilder;
use cli::TransportArgs;
use futures::StreamExt;
use libp2p::{
    bytes::Bytes,
    noise,
    swarm::{DialError, SwarmEvent},
    Swarm, TransportError,
};
use log::{debug, warn};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{channel, Receiver, Sender};

pub mod behaviour;
pub mod builder;
pub mod chain_client;
pub mod cli;
pub mod protocol;
pub mod utils;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Libp2p transport creation failed: {0}")]
    Transport(String),
    #[error("Listening failed: {0:?}")]
    Listen(#[from] TransportError<std::io::Error>),
    #[error("Dialing failed: {0:?}")]
    Dial(#[from] DialError),
    #[error("Decoding message failed: {0}")]
    Decode(String),
    // #[error("{0}")]
    // Contract(#[from] sqd_contract_client::ClientError),
}

impl From<noise::Error> for Error {
    fn from(e: noise::Error) -> Self {
        Self::Transport(e.to_string())
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::Transport(e.to_string())
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct AgentInfo {
    pub name: &'static str,
    pub version: &'static str,
}

impl Display for AgentInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.name, self.version)
    }
}

#[macro_export]
macro_rules! get_agent_info {
    () => {
        AgentInfo {
            name: env!("CARGO_PKG_NAME"),
            version: env!("CARGO_PKG_VERSION"),
        }
    };
}

#[async_trait]
pub trait MessageHandler: Clone + Send + Sync + 'static {
    /// Defines how to handle an incoming message. A typical usage is to define a `MessageHandler` with a
    /// number of `Sender<T>` channels. Then implement `dispatch` to deserialize incoming messages and
    /// forward them through the appropriate delivery channel. Then `writer` can be used to send back
    /// responses or acknowledgements to the sender machine (see unit tests for examples).
    async fn dispatch(&self, message: Bytes) -> Result<(), Box<dyn core::error::Error>>;
}

/// The p2p swarm
pub type NodeSwarm = Swarm<Wrapped<BaseBehaviour>>;

/// It publishes messages to a specific topic
pub type Publisher = Sender<(&'static str, Bytes)>;

/// It registers a new handler for incoming messages from a subscribed topic
pub type RegisterHandler<Handler> = Sender<(&'static str, Handler)>;



pub struct P2PTransport<Handler: MessageHandler> {
    /// The swarm on top of which the node operates
    swarm: NodeSwarm,
    /// Received handlers from other tasks
    rx_handlers: Receiver<(&'static str, Handler)>,
    /// Recieved messages for publishing them to a topic
    rx_publishers: Receiver<(&'static str, Bytes)>,
    /// Message handlers for a task that subscribed to a topic
    handlers: HashMap<String, Handler>,
    /// Known topics
    topics: HashMap<String, ()>,
}

impl<Handler: MessageHandler> P2PTransport<Handler> {
    pub async fn spawn(args: TransportArgs) -> (Publisher, RegisterHandler<Handler>) {
        let agent_info = get_agent_info!();
        let builder = P2PTransportBuilder::from_cli(args, agent_info)
            .await
            .expect("Invalid transport arguments");
        // Create a default Swarm.
        let swarm = builder
            .build_default_swarm()
            .expect("Cannot build swarm for invalid arguments");

        // Create channels for receiver handlers and publishers
        let (tx_handlers, rx_handlers) = channel(100);
        let (tx_publishers, rx_publishers) = channel(1_000);

        tokio::spawn(async move {
            Self {
                swarm,
                rx_handlers,
                rx_publishers,
                handlers: HashMap::new(),
                topics: HashMap::new(),
            }
            .run()
            .await
        });

        (tx_publishers, tx_handlers)
    }

    pub async fn run(&mut self) {
        loop {
            tokio::select! {
                // Handle incoming handler registration
                Some((topic, handler)) = self.rx_handlers.recv() => {
                    if self.topics.get(topic).is_none() {
                        // Topic is not known so subscribe to topic, add to known topics and add handler
                        self.swarm.behaviour_mut().subscribe(topic);
                        self.topics.insert(topic.to_string(), ());
                        self.handlers.insert(topic.to_string(), handler);
                    } else if self.handlers.get(topic).is_none() {
                        // Topic is known, just add the handler
                        self.handlers.insert(topic.to_string(), handler);
                    } else {
                        debug!("Handler of topic {} is already registered", topic);
                    }
                },

                // Publish message to given topic
                Some((topic, data)) = self.rx_publishers.recv() => {
                    if self.topics.get(topic).is_none() {
                        // Subscribe to topic and add to known topics
                        self.swarm.behaviour_mut().subscribe(topic);
                        self.topics.insert(topic.to_string(), ());
                    }
                    self.swarm.behaviour_mut().publish_message(topic, data);
                },

                // Handle swarm events
                event = self.swarm.select_next_some() => {
                    match event {
                        SwarmEvent::Behaviour(BaseBehaviourEvent::Gossipsub(msg)) => {
                            let topic = msg.topic;

                            if let Some(handler) = self.handlers.get(topic) {
                                if let Err(e) = handler.dispatch(msg.message.into()).await {
                                    warn!("{}", e);
                                }
                            }
                        },

                        other => {
                            debug!("Other swarm event: {:?}", other);
                        }
                    }
                }
            }
        }
    }
}
