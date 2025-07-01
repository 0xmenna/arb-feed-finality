use crate::Digest;
use rs_merkle::Hasher as MerkleHasher;
use tiny_keccak::Hasher;

pub fn hash<T: AsRef<[u8]>>(data: T) -> Digest {
    let mut hasher = tiny_keccak::Keccak::v256();
    let mut h = [0u8; 32];
    hasher.update(data.as_ref());
    hasher.finalize(&mut h);
    Digest(h)
}

#[derive(Clone)]
pub struct Keccak;

impl MerkleHasher for Keccak {
    type Hash = Digest;

    fn hash(data: &[u8]) -> Self::Hash {
        crate::keccak::hash(data)
    }
}
