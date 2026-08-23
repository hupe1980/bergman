//! The maintenance operations themselves.
//!
//! Each operation is a free function taking a loaded table, a resolved policy,
//! and an observer. None of them holds state between runs: a crashed run leaves
//! only files nothing references, and re-running replans from the table's
//! current snapshot. That is what "crash-only" means here, and it is why there
//! is no journal, no lock file, and nothing to repair after a `kill -9`.

pub mod expire;
pub mod orphans;
pub mod reachability;
pub mod store;

use std::time::Duration;

/// How many times a commit is retried before the operation gives up for this
/// cycle.
///
/// Small on purpose. Maintenance is a background tenant: a table being written
/// hard will keep winning, and the right response is to come back next cycle
/// rather than to spend the cycle losing repeatedly. A table that conflicts
/// every time is reported as busy, which is information — a silent retry loop
/// is not.
pub const MAX_COMMIT_ATTEMPTS: usize = 3;

/// Base delay between commit attempts.
pub const RETRY_BASE_DELAY: Duration = Duration::from_millis(250);

/// The delay before attempt `attempt` (0-based), with exponential growth.
///
/// Deterministic rather than jittered, because Bergman's own concurrency is
/// bounded per table: two attempts on the *same* table are already serialised,
/// so there is no thundering herd of its own making to spread out. Jitter would
/// only make the tests non-reproducible.
pub fn retry_delay(attempt: usize) -> Duration {
    RETRY_BASE_DELAY * 2u32.saturating_pow(attempt as u32)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_delay_grows_exponentially() {
        assert_eq!(retry_delay(0), Duration::from_millis(250));
        assert_eq!(retry_delay(1), Duration::from_millis(500));
        assert_eq!(retry_delay(2), Duration::from_millis(1000));
    }
}
