use codec::{Decode, Encode};
pub use ethers::{
    abi,
    types::{serde_helpers::deserialize_number, Address, H256, U256},
    utils::rlp,
};
use serde::Deserialize;

#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Copy)]
pub struct BigInt(pub U256);

impl Encode for BigInt {
    fn encode(&self) -> Vec<u8> {
        self.0 .0.encode()
    }
}

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

impl Decode for BigInt {
    fn decode<I: codec::Input>(input: &mut I) -> std::result::Result<Self, codec::Error> {
        let mut u256 = [0u64; 4];
        let decode = &Vec::<u64>::decode(input)?;
        if decode.len() != 4 {
            return Err(codec::Error::from("Invalid BigInt length"));
        }
        u256.copy_from_slice(&decode[..]);

        Ok(Self(U256(u256)))
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
