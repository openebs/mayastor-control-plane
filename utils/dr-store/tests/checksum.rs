//! What may name a chunk.

use dr_store::{Checksum, ChecksumError};

const BLAKE3: &str = "ab12cd34ef567890ab12cd34ef567890ab12cd34ef567890ab12cd34ef567890";

#[test]
fn a_digest_is_a_checksum() {
    let checksum = Checksum::from([0xab; 32]);
    assert_eq!(checksum.to_string(), "ab".repeat(32));
    let wide = Checksum::from([0x0f; 64]);
    assert_eq!(wide.to_string(), "0f".repeat(64));

    // Its text parses back to it, so a key the collector lists names the same chunk.
    assert_eq!(checksum.to_string().parse::<Checksum>(), Ok(checksum));
    assert_eq!(wide.to_string().parse::<Checksum>(), Ok(wide));
    assert_eq!(BLAKE3.parse::<Checksum>().unwrap().to_string(), BLAKE3);
}

#[test]
fn text_that_is_not_one_is_refused() {
    let parse = |text: &str| text.parse::<Checksum>().unwrap_err();

    // Only the two digest widths: anything shorter could not be sharded.
    assert_eq!(parse(""), ChecksumError::Length { len: 0 });
    assert_eq!(parse("ab12"), ChecksumError::Length { len: 4 });
    assert_eq!(parse(&BLAKE3[1..]), ChecksumError::Length { len: 63 });
    assert_eq!(
        parse(&format!("{BLAKE3}0")),
        ChecksumError::Length { len: 65 }
    );

    // Lowercase hex only, so one digest has one key. Non-ASCII is refused rather than
    // cut mid-character.
    let upper = BLAKE3.to_uppercase();
    assert!(matches!(parse(&upper), ChecksumError::Digits { .. }));
    let not_hex = BLAKE3.replace('a', "g");
    assert!(matches!(parse(&not_hex), ChecksumError::Digits { .. }));
    let wide = format!("é{}", &BLAKE3[2..]);
    assert!(matches!(parse(&wide), ChecksumError::Digits { .. }));
}
