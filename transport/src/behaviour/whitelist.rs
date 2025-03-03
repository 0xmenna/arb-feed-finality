use std::{
    collections::HashSet,
    task::{Context, Poll},
    time::Duration,
};

use futures::StreamExt;
use libp2p::{
    allow_block_list::{self, AllowedPeers},
    swarm::ToSwarm,
    PeerId,
};
use serde::{Deserialize, Serialize};

use super::wrapped::{BehaviourWrapper, TToSwarm};
use crate::chain_client::{AuthorityPeers, ClientError, ContractClient, NodeStream};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct WhitelistConfig {
    pub nodes_update_interval: Duration,
}

impl WhitelistConfig {
    pub fn new(nodes_update_interval: Duration) -> Self {
        Self {
            nodes_update_interval,
        }
    }
}

impl Default for WhitelistConfig {
    fn default() -> Self {
        Self::new(Duration::from_secs(60))
    }
}

pub struct WhitelistBehavior {
    allow: allow_block_list::Behaviour<AllowedPeers>,
    active_nodes_stream: NodeStream,
    registered_nodes: HashSet<PeerId>,
}

impl WhitelistBehavior {
    pub fn new(contract_client: ContractClient, config: WhitelistConfig) -> Self {
        let active_nodes_stream =
            contract_client.network_nodes_stream(config.nodes_update_interval);
        Self {
            allow: Default::default(),
            active_nodes_stream,
            registered_nodes: Default::default(),
        }
    }

    pub fn allow_peer(&mut self, peer_id: PeerId) {
        log::debug!("Allowing peer {peer_id}");
        self.allow.allow_peer(peer_id);
    }

    pub fn disallow_peer(&mut self, peer_id: PeerId) {
        log::debug!("Disallowing peer {peer_id}");
        self.allow.disallow_peer(peer_id);
    }

    fn on_nodes_update(
        &mut self,
        result: Result<AuthorityPeers, ClientError>,
    ) -> Option<AuthorityPeers> {
        let nodes = result
            .map_err(|e| log::warn!("Error retrieving registered nodes from chain: {e:?}"))
            .ok()?;

        if nodes == self.registered_nodes {
            log::debug!("Registered nodes set unchanged.");
            return None;
        }
        log::info!("Updating registered nodes");
        // Disallow nodes which are no longer registered
        for peer_id in self.registered_nodes.difference(&nodes) {
            log::debug!("Blocking peer {peer_id}");
            self.allow.disallow_peer(*peer_id);
        }
        // Allow newly registered nodes
        for peer_id in nodes.difference(&self.registered_nodes) {
            log::debug!("Allowing peer {peer_id}");
            self.allow.allow_peer(*peer_id);
        }
        self.registered_nodes = nodes.clone();
        Some(nodes)
    }
}

impl BehaviourWrapper for WhitelistBehavior {
    type Inner = allow_block_list::Behaviour<AllowedPeers>;
    type Event = AuthorityPeers;

    fn inner(&mut self) -> &mut Self::Inner {
        &mut self.allow
    }

    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<impl IntoIterator<Item = TToSwarm<Self>>> {
        match self.active_nodes_stream.poll_next_unpin(cx) {
            Poll::Ready(Some(res)) => {
                Poll::Ready(self.on_nodes_update(res).map(ToSwarm::GenerateEvent))
            }
            Poll::Pending => Poll::Pending,
            _ => unreachable!(), // infinite stream
        }
    }
}
