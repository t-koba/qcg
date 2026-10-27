//! Synchronous access to opaque secret bytes in the operating-system keyring.
pub use keyring::{Entry, Error};
pub fn open(service: &str, account: &str) -> Result<Entry, Error> {
    Entry::new(service, account)
}
