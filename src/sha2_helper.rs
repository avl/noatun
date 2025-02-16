use sha2::digest::FixedOutput;
use sha2::Digest;
use sha2::Sha256;
use crate::MessageId;

pub fn sha2(bytes: &[u8]) -> [u8; 16] {
    let mut hasher = Sha256::new();
    // write input message
    hasher.update(bytes);

    // read hash digest and consume hasher
    hasher.finalize_fixed()[0..16].try_into().unwrap()
}
pub fn sha2_message(message_id: MessageId, bytes: &[u8]) -> [u8; 16] {
    let mut hasher = Sha256::new();
    // write input message
    hasher.update(message_id.as_bytes());
    hasher.update(bytes);

    // read hash digest and consume hasher
    hasher.finalize_fixed()[0..16].try_into().unwrap()
}
