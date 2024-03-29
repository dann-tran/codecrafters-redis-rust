pub(crate) mod command;
pub(crate) mod resp;
pub mod server;
pub(crate) mod utils;

pub(crate) trait ToBytes {
    fn to_bytes(&self) -> Vec<u8>;
}
