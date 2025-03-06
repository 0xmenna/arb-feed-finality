use crate::Digest;
use tiny_keccak::Hasher;

pub fn hash<T: AsRef<[u8]>>(data: T) -> Digest {
    let mut hasher = tiny_keccak::Keccak::v256();
    let mut h = [0u8; 32];
    hasher.update(data.as_ref());
    hasher.finalize(&mut h);
    Digest(h)
}
