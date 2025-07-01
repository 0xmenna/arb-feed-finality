use crate::{keccak::Keccak as KeccakHasher, Digest};
use rs_merkle::{Hasher, MerkleTree as InnerMerkleTree};
use std::collections::HashMap;

pub type IndexValue = (usize, Vec<u8>);

pub enum Error {
    TooManyLeaves,
}

pub struct MerkleTree {
    leaf_values: HashMap<Digest, IndexValue>,
    size: usize,
    inner: InnerMerkleTree<KeccakHasher>,
}

impl MerkleTree {
    pub fn new() -> Self {
        MerkleTree {
            leaf_values: HashMap::new(),
            size: 0,
            inner: InnerMerkleTree::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.size
    }

    pub fn inner(&self) -> &InnerMerkleTree<KeccakHasher> {
        &self.inner
    }

    pub fn values(&self) -> Vec<Vec<u8>> {
        self.leaf_values
            .values()
            .map(|(_, value)| value.clone())
            .collect()
    }

    pub fn from_leaf_values(val: Vec<Vec<u8>>) -> Self {
        let mut map = HashMap::new();
        let mut leaves: Vec<Digest> = val
            .iter()
            .enumerate()
            .map(|(idx, val)| {
                let digest = KeccakHasher::hash(val.as_slice());
                map.insert(digest, (idx, val.clone()));
                digest
            })
            .collect();

        let mut inner = InnerMerkleTree::new();
        inner.append(&mut leaves);

        MerkleTree {
            leaf_values: map,
            size: val.len(),
            inner,
        }
    }

    pub fn insert(&mut self, value: Vec<u8>) -> Result<(), Error> {
        if self.size >= usize::MAX {
            return Err(Error::TooManyLeaves);
        }

        let digest = KeccakHasher::hash(value.as_slice());
        self.leaf_values.insert(digest, (self.size, value));
        self.inner.insert(digest);
        self.size += 1;

        Ok(())
    }

    pub fn append(&mut self, values: Vec<Vec<u8>>) -> Result<(), Error> {
        if self.size.checked_add(values.len()).is_none() {
            return Err(Error::TooManyLeaves);
        }

        let mut digests: Vec<Digest> = values
            .iter()
            .enumerate()
            .map(|(idx, val)| {
                let idx = self.size + idx;
                let digest = KeccakHasher::hash(val.as_slice());
                self.leaf_values.insert(digest, (idx, val.clone()));
                digest
            })
            .collect();

        self.inner.append(&mut digests);
        self.size += values.len();

        Ok(())
    }

    pub fn commit(&mut self) -> Option<Digest> {
        self.inner.commit();

        self.inner.root()
    }

    pub fn root(&self) -> Option<Digest> {
        self.inner.root()
    }

    pub fn root_hex(&self) -> Option<String> {
        self.inner.root_hex()
    }

    pub fn get(&self, digest: &Digest) -> Option<&Vec<u8>> {
        self.leaf_values.get(digest).map(|(_, value)| value)
    }

    pub fn proof(&self, digest: &Digest) -> Option<Vec<u8>> {
        let idx_to_prove = self.leaf_values.get(digest).map(|(idx, _)| *idx);

        if let Some(idx) = idx_to_prove {
            let merkle_proof = self.inner.proof(&vec![idx]);
            Some(merkle_proof.to_bytes())
        } else {
            None
        }
    }
}
