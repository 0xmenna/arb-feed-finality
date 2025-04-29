use futures::future::ready;
use libp2p::{futures::Stream, PeerId};
use std::{
    collections::HashSet,
    fs::File,
    io::{BufRead, BufReader},
    pin::Pin,
    time::Duration,
};
use tokio_stream::{wrappers::IntervalStream, StreamExt};

use crate::cli::TESTING_PEERS_FILE;

// A dummy implementation of a chain client, just to provide the authority list.

pub type ClientError = ();

pub type AuthorityPeers = HashSet<PeerId>;

pub type NodeStream =
    Pin<Box<dyn Stream<Item = Result<AuthorityPeers, ClientError>> + Send + 'static>>;

pub struct ContractClient {
    testnet: bool,
}

impl Default for ContractClient {
    fn default() -> Self {
        Self { testnet: false }
    }
}

impl ContractClient {
    pub fn testing_client() -> Self {
        Self { testnet: true }
    }

    pub fn network_nodes_stream(&self, interval: Duration) -> NodeStream {
        let authority_peers = if self.testnet {
            get_testing_peers
        } else {
            get_authority_peers
        };
        Box::pin(
            IntervalStream::new(tokio::time::interval(interval))
                .then(move |_| ready(Ok(authority_peers()))),
        )
    }
}

fn get_testing_peers() -> AuthorityPeers {
    let file: File = File::open(TESTING_PEERS_FILE).expect("Failed to open authority file");
    let reader = BufReader::new(file);
    let mut set = HashSet::new();

    for line in reader.lines() {
        let line = line.unwrap();
        if let Ok(peer_id) = line.parse::<PeerId>() {
            set.insert(peer_id);
        }
    }

    set
}

fn get_authority_peers() -> AuthorityPeers {
    todo!()
}
