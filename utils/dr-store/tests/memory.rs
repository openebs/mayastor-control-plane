//! The verbs against the in-memory store.
//!
//! The same suite `tests/s3.rs` runs, so the fake is held to the real store's answers
//! rather than to a second set of assertions that agree with it by eye. Fast, and no
//! container, which is what everything built on `Store` gets to test against.
//!
//! What it cannot answer for is whether a store *evaluates* a conditional delete: this
//! one compares the tag itself. Only the real store settles that.

mod common;

use dr_store::Store;

#[tokio::test]
async fn memory() {
    common::suite(&Store::memory(common::layout())).await;
}
