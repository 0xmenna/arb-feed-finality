use crypto::{CryptoError, Digest, PublicKey};
use store::StoreError;
use thiserror::Error;

use crate::consensus::View;

#[macro_export]
macro_rules! bail {
    ($e:expr) => {
        return Err($e);
    };
}

#[macro_export(local_inner_macros)]
macro_rules! ensure {
    ($cond:expr, $e:expr) => {
        if !($cond) {
            bail!($e);
        }
    };
}

pub type ConsensusResult<T> = Result<T, ConsensusError>;

#[derive(Error, Debug)]
pub enum ConsensusError {
    #[error("Network error: {0}")]
    NetworkError(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    CodecError(#[from] codec::Error),

    #[error("Store error: {0}")]
    StoreError(#[from] StoreError),

    #[error("Node {0} is not in the committee")]
    NotInCommittee(PublicKey),

    #[error("Invalid signature")]
    InvalidSignature(#[from] CryptoError),

    #[error("Received more than one vote from {0}")]
    AuthorityReuse(PublicKey),

    #[error("Received vote from unknown authority {0}")]
    UnknownAuthority(PublicKey),

    #[error("Received QC without a quorum")]
    QCRequiresQuorum,

    #[error("Received TC without a quorum")]
    TCRequiresQuorum,

    #[error("Malformed block {0}")]
    MalformedBlock(Digest),

    #[error("Received block {digest} from leader {leader} at view {view}")]
    WrongLeader {
        digest: Digest,
        leader: PublicKey,
        view: View,
    },

    #[error("Invalid payload")]
    InvalidPayload,
}
