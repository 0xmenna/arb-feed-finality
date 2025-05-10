use codec::{Decode, Encode};
pub use ethers::{
    abi,
    types::{serde_helpers::deserialize_number, Address, H256, U256},
    utils::rlp,
};
use serde::Deserialize;

#[derive(Debug, Encode, Decode, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Copy)]
pub struct BigInt(pub U256);

impl std::ops::Add for BigInt {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        Self(self.0 + other.0)
    }
}

impl std::ops::Sub for BigInt {
    type Output = Self;

    fn sub(self, other: Self) -> Self {
        Self(self.0 - other.0)
    }
}

impl BigInt {
    pub fn saturating_sub(&self, other: Self) -> Self {
        Self(self.0.saturating_sub(other.0))
    }

    pub fn saturating_add(&self, other: Self) -> Self {
        Self(self.0.saturating_add(other.0))
    }

    pub fn is_zero(&self) -> bool {
        self.0.is_zero()
    }

    pub fn zero() -> Self {
        Self(U256::zero())
    }
}

impl<'de> Deserialize<'de> for BigInt {
    fn deserialize<D>(deserializer: D) -> Result<BigInt, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let n = deserialize_number(deserializer)?;
        Ok(BigInt::from(n))
    }
}

impl AsRef<U256> for BigInt {
    fn as_ref(&self) -> &U256 {
        &self.0
    }
}

impl From<U256> for BigInt {
    fn from(val: U256) -> Self {
        Self(val)
    }
}
