use clap::ValueEnum;
use libp2p::StreamProtocol;

pub const BLOCKS_TOPIC: &str = "/iceberg/blocks/1.0.0";

pub const ID_PROTOCOL: &str = "/iceberg/1.0.0";

pub const MAX_PUBSUB_MSG_SIZE: usize = 65536;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Default)]
#[clap(rename_all = "kebab_case")]
pub enum Network {
    Testnet,
    #[default]
    Mainnet,
}

pub const KNOWN_TOPICS: [&'static str; 1] = [BLOCKS_TOPIC];

pub const fn dht_protocol(network: Network) -> StreamProtocol {
    match network {
        Network::Testnet => StreamProtocol::new("/iceberg/dht/testnet/1.0.0"),
        Network::Mainnet => StreamProtocol::new("/iceberg/dht/mainnet/1.0.0"),
    }
}
