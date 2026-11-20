//! What names a chunk.

use snafu::Snafu;
use std::{fmt, str::FromStr};

/// Bytes in a 256-bit digest.
const BITS256: usize = 32;
/// Bytes in a 512-bit digest.
const BITS512: usize = 64;

/// The digest of a chunk's uncompressed contents, which is both its key and its
/// integrity check.
///
/// One variant per digest width: BLAKE3, the default `hashAlgorithm`, is 256 bits, and
/// BLAKE2b-512 is 512. Built from a digest it cannot be wrong; parsed from text, as the
/// collector does from a listed key, it is refused unless it is one. Either way it
/// renders as lowercase hex, so one digest has exactly one key.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Checksum {
    /// A 256-bit digest.
    Bits256([u8; BITS256]),
    /// A 512-bit digest.
    Bits512([u8; BITS512]),
}

/// Text that is not a checksum.
#[allow(missing_docs)]
#[derive(Debug, Snafu, PartialEq, Eq)]
pub enum ChecksumError {
    #[snafu(display(
        "a checksum is {} or {} hex digits, got {len}",
        BITS256 * 2,
        BITS512 * 2
    ))]
    Length { len: usize },
    #[snafu(display("a checksum is lowercase hex, got '{text}'"))]
    Digits { text: String },
}

impl Checksum {
    /// The digest itself.
    pub fn digest(&self) -> &[u8] {
        match self {
            Self::Bits256(digest) => digest,
            Self::Bits512(digest) => digest,
        }
    }
}

impl From<[u8; BITS256]> for Checksum {
    fn from(digest: [u8; BITS256]) -> Self {
        Self::Bits256(digest)
    }
}

impl From<[u8; BITS512]> for Checksum {
    fn from(digest: [u8; BITS512]) -> Self {
        Self::Bits512(digest)
    }
}

impl FromStr for Checksum {
    type Err = ChecksumError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        match text.len() {
            len if len == BITS256 * 2 => decode(text).map(Self::Bits256),
            len if len == BITS512 * 2 => decode(text).map(Self::Bits512),
            len => LengthSnafu { len }.fail(),
        }
    }
}

/// Lowercase hex into a digest of the width the caller asked for.
fn decode<const N: usize>(text: &str) -> Result<[u8; N], ChecksumError> {
    let nibble = |digit: u8| match digit {
        b'0'..=b'9' => Some(digit - b'0'),
        b'a'..=b'f' => Some(digit - b'a' + 10),
        _ => None,
    };
    let mut digest = [0; N];
    for (byte, pair) in digest.iter_mut().zip(text.as_bytes().chunks_exact(2)) {
        let (Some(high), Some(low)) = (nibble(pair[0]), nibble(pair[1])) else {
            return DigitsSnafu { text }.fail();
        };
        *byte = high << 4 | low;
    }
    Ok(digest)
}

impl fmt::Display for Checksum {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.digest()
            .iter()
            .try_for_each(|byte| write!(f, "{byte:02x}"))
    }
}

impl fmt::Debug for Checksum {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Checksum({self})")
    }
}
