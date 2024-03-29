use std::time::{SystemTime, UNIX_EPOCH};

pub(crate) fn get_current_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis()
}
