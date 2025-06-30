use crate::config::Committee;
use crate::consensus::{ParentRound, View};
use crate::error::{ConsensusError, ConsensusResult};
use base64::write;
use codec::{Decode, Encode};
use crypto::{keccak, Digest, Hash, PublicKey, Signature, SignatureService};
use std::collections::HashSet;
use std::fmt;

#[derive(Encode, Decode, Default, Clone)]
pub struct Block {
    pub view: View,
    pub qc: QC,
    pub parent_round: ParentRound,
    pub last_feed_sequence_number: u64,
    pub batch_poster_digest: Digest,
    pub feed_merkle_root: Digest,
    pub signature: Signature,
}

impl Block {
    pub async fn new(
        qc: QC,
        view: View,
        parent_round: ParentRound,
        last_feed_sequence_number: u64,
        batch_poster_digest: Digest,
        feed_merkle_root: Digest,
        mut signature_service: SignatureService,
    ) -> Self {
        let block = Self {
            view,
            qc,
            parent_round,
            last_feed_sequence_number,
            batch_poster_digest,
            feed_merkle_root,
            signature: Signature::default(),
        };
        let signature = signature_service.request_signature(block.digest()).await;
        Self { signature, ..block }
    }

    pub fn genesis() -> Self {
        Block::default()
    }

    pub fn parent(&self) -> &Digest {
        &self.qc.hash
    }

    pub fn verify(&self, committee: &Committee) -> ConsensusResult<()> {
        // Ensure the authority has voting rights.
        let voting_rights = committee.stake(&committee.leader);
        debug_assert!(voting_rights > 0);

        // Check the signature.
        self.signature.verify(&self.digest(), &committee.leader)?;

        // Check the embedded QC.
        if self.qc != QC::genesis() {
            self.qc.verify(committee)?;
        }

        Ok(())
    }
}

#[derive(Encode, Decode, Clone)]
pub struct BlockPreImage {
    pub view: View,
    pub qc: QC,
    pub parent_round: ParentRound,
    pub last_feed_sequence_number: u64,
    pub batch_poster_digest: Digest,
    pub feed_merkle_root: Digest,
}

impl From<&Block> for BlockPreImage {
    fn from(block: &Block) -> Self {
        Self {
            view: block.view,
            qc: block.qc.clone(),
            parent_round: block.parent_round,
            last_feed_sequence_number: block.last_feed_sequence_number,
            batch_poster_digest: block.batch_poster_digest.clone(),
            feed_merkle_root: block.feed_merkle_root.clone(),
        }
    }
}

impl Hash for Block {
    fn digest(&self) -> Digest {
        let preimage = BlockPreImage::from(self).encode();

        keccak::hash(&preimage)
    }
}

impl fmt::Debug for Block {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(
            f,
            "{}: B({:?}, {:?}, {:?}, {}, {}, {})",
            self.digest(),
            self.view,
            self.qc,
            self.parent_round,
            self.last_feed_sequence_number,
            self.batch_poster_digest,
            self.feed_merkle_root,
        )
    }
}

impl fmt::Display for Block {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "")?;
        writeln!(f, "  🔢 View: {}", self.view)?;
        writeln!(f, "  🗳️ QC: {}", self.qc)?;
        writeln!(f, "  🧬 Parent Round: {}", self.parent_round)?;
        writeln!(
            f,
            "  📈 Last Feed Sequence #: {}",
            self.last_feed_sequence_number
        )?;
        writeln!(f, "  🖋️ Batch Poster Digest: {}", self.batch_poster_digest)?;
        writeln!(f, "  🌿 Feed Merkle Root: {}", self.feed_merkle_root)
    }
}

#[derive(Clone, Encode, Decode)]
pub struct Vote {
    pub hash: Digest,
    pub view: View,
    pub author: PublicKey,
    pub signature: Signature,
}

impl Vote {
    pub async fn new(
        block: &Block,
        author: PublicKey,
        mut signature_service: SignatureService,
    ) -> Self {
        let vote = Self {
            hash: block.digest(),
            view: block.view,
            author,
            signature: Signature::default(),
        };
        let signature = signature_service.request_signature(vote.digest()).await;
        Self { signature, ..vote }
    }

    pub fn verify(&self, committee: &Committee) -> ConsensusResult<()> {
        // Ensure the authority has voting rights.
        ensure!(
            committee.stake(&self.author) > 0,
            ConsensusError::UnknownAuthority(self.author)
        );

        // Check the signature.
        self.signature.verify(&self.digest(), &self.author)?;
        Ok(())
    }
}

impl Hash for Vote {
    fn digest(&self) -> Digest {
        let mut preimage = Vec::new();
        preimage.extend_from_slice(&self.hash.to_vec());
        preimage.extend_from_slice(&self.view.encode());

        keccak::hash(&preimage)
    }
}

impl fmt::Debug for Vote {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "V({}, {:?}, {})", self.author, self.view, self.hash)
    }
}

#[derive(Clone, Encode, Decode, Default)]
pub struct QC {
    pub hash: Digest,
    pub view: View,
    pub votes: Vec<(PublicKey, Signature)>,
}

impl QC {
    pub fn genesis() -> Self {
        QC::default()
    }

    pub fn verify(&self, committee: &Committee) -> ConsensusResult<()> {
        // Ensure the QC has a quorum.
        let mut weight = 0;
        let mut used = HashSet::new();
        for (name, _) in self.votes.iter() {
            ensure!(!used.contains(name), ConsensusError::AuthorityReuse(*name));
            let voting_rights = committee.stake(name);
            ensure!(voting_rights > 0, ConsensusError::UnknownAuthority(*name));
            used.insert(*name);
            weight += voting_rights;
        }
        ensure!(
            weight >= committee.quorum_threshold(),
            ConsensusError::QCRequiresQuorum
        );

        // Check the signatures.
        Signature::verify_batch(&self.digest(), &self.votes).map_err(ConsensusError::from)
    }
}

impl Hash for QC {
    fn digest(&self) -> Digest {
        let mut preimage = Vec::new();
        preimage.extend_from_slice(&self.hash.to_vec());
        preimage.extend_from_slice(&self.view.encode());

        keccak::hash(&preimage)
    }
}

impl fmt::Debug for QC {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "QC({}, {:?})", self.hash, self.view)
    }
}

impl fmt::Display for QC {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "")?;
        writeln!(f, "       🔑 Hash: {}", self.hash)?;
        writeln!(f, "       🔢 View: {}", self.view)?;
        writeln!(f, "       ✍️  Votes [{}] ", self.votes.len())?;
        for (i, (pk, _)) in self.votes.iter().enumerate() {
            writeln!(f, "           {}. 🧾 Voter: {}", i + 1, pk)?;
        }
        Ok(())
    }
}

impl PartialEq for QC {
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash && self.view == other.view
    }
}
