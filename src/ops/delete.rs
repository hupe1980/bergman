//! One deleter, one safety model.
//!
//! Two operations remove files: snapshot expiration deletes what its commit
//! made unreachable, and orphan removal deletes what no retained metadata ever
//! referenced. They decide *what* to delete by completely different reasoning —
//! that is the whole of §5.3 and §5.4 — but once a path is on a kill list the
//! rules are identical, and they are here:
//!
//! - **The blast radius is capped**, by `limits.max_deletes_per_run`. However
//!   wrong the reasoning that produced the list turns out to be, one operation
//!   removes at most that many and says loudly that it withheld the rest. What
//!   is withheld is reclaimed by the next scan.
//! - **The list is announced before the first deletion**, so a crash halfway
//!   through leaves a record of what was about to go. The ceiling is applied
//!   *before* the announcement: a record naming files that were never going to
//!   be touched is a misleading record.
//! - **Deletion is concurrent but bounded.** A million objects one round trip
//!   at a time takes hours; ten thousand at once is how a shared bucket starts
//!   throttling everybody.
//! - **One undeletable file does not stop the rest.** The metadata is already
//!   correct before anything is deleted, so a failure leaves a leak rather than
//!   a corruption.
//!
//! One module rather than two, because two would drift — and the half that
//! drifted would be the half nobody was looking at.

use async_trait::async_trait;
use futures::stream::{self, StreamExt};

use crate::error::Result;
use crate::obs::{MaintenanceObserver, OperationContext};

/// How many deletions are in flight at once.
pub const DELETE_CONCURRENCY: usize = 32;

/// Something that can remove an object by path.
///
/// Narrower than either transport it is implemented for, because deletion is
/// the only thing this module does and a wider trait would let the two
/// operations diverge again by reaching for something else.
#[async_trait]
pub trait FileDeleter: Send + Sync {
    /// Remove one object.
    async fn delete(&self, path: &str) -> Result<()>;
}

/// Expiration deletes through the table's own `FileIO`, which is already
/// configured with whatever credentials the catalog vended for it.
#[async_trait]
impl FileDeleter for iceberg::io::FileIO {
    async fn delete(&self, path: &str) -> Result<()> {
        iceberg::io::FileIO::delete(self, path)
            .await
            .map_err(|e| crate::error::Error::Storage(Box::new(e)))
    }
}

// Orphan removal deletes through its listing client, because the paths came
// from that listing and nothing else has seen them. No impl is needed here:
// `ObjectStore` has `FileDeleter` as a supertrait, so `&dyn ObjectStore`
// upcasts to `&dyn FileDeleter` directly.

/// What one deletion pass did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Deletion {
    /// How many objects were removed.
    pub deleted: usize,
    /// How many could not be, and remain as a leak.
    pub failed: usize,
    /// How many the per-run ceiling left for the next pass.
    pub withheld: usize,
}

/// Apply the blast-radius ceiling, returning how many were withheld.
///
/// Separate from [`announce_and_delete`] so it happens *before* the audit record is
/// written: the record must name what will actually be deleted.
pub fn withhold_beyond(doomed: &mut Vec<String>, ceiling: usize) -> usize {
    if doomed.len() <= ceiling {
        return 0;
    }
    let withheld = doomed.len() - ceiling;
    doomed.truncate(ceiling);
    withheld
}

/// Announce a kill list and delete it.
///
/// The announcement is [`MaintenanceObserver::deleting_files`], which the audit
/// sink writes and flushes before returning — so the record exists on disk
/// before the first object is removed.
pub async fn announce_and_delete(
    deleter: &dyn FileDeleter,
    observer: &dyn MaintenanceObserver,
    ctx: OperationContext<'_>,
    doomed: &[String],
) -> Deletion {
    if doomed.is_empty() {
        return Deletion::default();
    }

    observer.deleting_files(ctx, doomed).await;

    let results: Vec<bool> = stream::iter(doomed)
        .map(|path| async move {
            match deleter.delete(path).await {
                Ok(()) => true,
                Err(e) => {
                    tracing::warn!(
                        table = %ctx.table,
                        file = %path,
                        error = %e,
                        "file could not be deleted; it remains as a leak"
                    );
                    false
                }
            }
        })
        .buffer_unordered(DELETE_CONCURRENCY)
        .collect()
        .await;

    let deleted = results.iter().filter(|ok| **ok).count();
    Deletion {
        deleted,
        failed: results.len() - deleted,
        withheld: 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_list_within_the_ceiling_is_untouched() {
        let mut doomed = vec!["a".to_string(), "b".to_string()];
        assert_eq!(withhold_beyond(&mut doomed, 10), 0);
        assert_eq!(doomed.len(), 2);
    }

    #[test]
    fn a_list_over_the_ceiling_is_truncated_and_the_rest_reported() {
        // Never silent: a pass that quietly stopped at the ceiling would report
        // "everything deleted" while leaving most of the garbage in place —
        // and, far worse, would hide that something produced far more deletions
        // than a healthy table ever does.
        let mut doomed: Vec<String> = (0..10).map(|i| i.to_string()).collect();
        assert_eq!(withhold_beyond(&mut doomed, 4), 6);
        assert_eq!(doomed, vec!["0", "1", "2", "3"]);
    }

    #[test]
    fn a_list_exactly_at_the_ceiling_withholds_nothing() {
        let mut doomed: Vec<String> = (0..4).map(|i| i.to_string()).collect();
        assert_eq!(withhold_beyond(&mut doomed, 4), 0);
        assert_eq!(doomed.len(), 4);
    }
}
