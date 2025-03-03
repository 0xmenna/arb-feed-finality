use futures::future::ready;
use libp2p::{futures::Stream, PeerId};
use std::{collections::HashSet, pin::Pin, str::FromStr, time::Duration};
use tokio_stream::{wrappers::IntervalStream, StreamExt};

// A dummy implementation of a chain client, just to provide the authority list.

pub type ClientError = ();

pub type AuthorityPeers = HashSet<PeerId>;

pub type NodeStream =
    Pin<Box<dyn Stream<Item = Result<AuthorityPeers, ClientError>> + Send + 'static>>;

pub struct ContractClient;

impl Default for ContractClient {
    fn default() -> Self {
        Self
    }
}

impl ContractClient {
    pub fn network_nodes_stream(&self, interval: Duration) -> NodeStream {
        Box::pin(
            IntervalStream::new(tokio::time::interval(interval))
                .then(move |_| ready(Ok(get_authority_peers()))),
        )
    }
}

// Dummy implementation for demonstration purposes.
fn get_authority_peers() -> AuthorityPeers {
    // Just add a single random peer
    let mut authorities = HashSet::new();
    let peer1 = PeerId::from_str("12D3KooWQ9kBn1y89W1ELUDAvfKcnwJASMTFZsYsh2a84yrjMHqy").unwrap();
    let peer2 = PeerId::from_str("12D3KooWKATkQFnM5jKPLzfmzZiz7obCX9q2NeWszaEqqyLWK8Dv").unwrap();
    let peer3 = PeerId::from_str("12D3KooWCr7f1QXPuegmvmk3ZGa7SAqkhgJLRRnchMPixTtxp5fM").unwrap();
    authorities.insert(peer1);
    authorities.insert(peer2);
    authorities.insert(peer3);

    authorities
}
