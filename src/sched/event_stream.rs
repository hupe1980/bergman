//! Reacting to table changes instead of polling for them.
//!
//! A cron cadence is a guess: too slow and a streaming table stays fragmented
//! for an hour, too fast and a quiet catalog is rescanned for nothing.
//! `Lakekeeper` made the point by scheduling its own maintenance after commits
//! rather than on a timer.
//!
//! # Bergman does not speak to a broker
//!
//! `Lakekeeper` emits `CloudEvents` to NATS or Kafka. Bergman deliberately does not
//! carry a client for either: pulling a message broker into a maintenance
//! engine imports exactly the operational footprint the project exists to
//! avoid, and it would make the *default* deployment — a static binary and a
//! catalog — carry a dependency almost nobody uses.
//!
//! So this is a channel, not a consumer. Whatever already receives events in
//! your deployment calls [`Events::notify`]; a subscriber you write, a bridge,
//! or the binary's own `CloudEvents` endpoint. Bergman owns the trigger and the
//! debounce; the transport stays yours.

use std::collections::HashSet;
use std::time::Duration;

use tokio::sync::mpsc;

use crate::policy::TableRef;

/// How many pending notifications are held before senders are told to drop
/// them.
///
/// A busy streaming table can commit far faster than a maintenance cycle runs,
/// and the *n*-th notification for one table says nothing the first did not.
/// Dropping is therefore correct rather than merely acceptable: the debounce
/// would have collapsed them anyway.
const CHANNEL_DEPTH: usize = 1024;

/// Sends table-changed notifications to a daemon.
///
/// Cloneable and cheap; hand one to every subscriber you run.
#[derive(Debug, Clone)]
pub struct Events {
    tx: mpsc::Sender<TableRef>,
}

/// Receives them.
#[derive(Debug)]
pub struct EventStream {
    rx: mpsc::Receiver<TableRef>,
    debounce: Duration,
}

/// Create a connected pair.
///
/// `debounce` is how long the daemon keeps collecting after the first
/// notification before acting. A streaming writer commits every few seconds,
/// and a cycle per commit would be maintenance thrashing rather than
/// maintenance.
pub fn channel(debounce: Duration) -> (Events, EventStream) {
    let (tx, rx) = mpsc::channel(CHANNEL_DEPTH);
    (Events { tx }, EventStream { rx, debounce })
}

impl Events {
    /// Note that a table changed.
    ///
    /// Never blocks and never fails: a full queue means a cycle is already
    /// pending for more tables than it can be told about, and the notification
    /// is redundant. Returns whether it was accepted, for callers that want to
    /// count drops.
    pub fn notify(&self, table: TableRef) -> bool {
        match self.tx.try_send(table) {
            Ok(()) => true,
            Err(mpsc::error::TrySendError::Full(table)) => {
                tracing::debug!(%table, "event queue full; notification dropped");
                false
            }
            Err(mpsc::error::TrySendError::Closed(table)) => {
                tracing::debug!(%table, "no daemon is listening; notification dropped");
                false
            }
        }
    }
}

impl EventStream {
    /// Wait for a change, then collect everything that arrives during the
    /// debounce window.
    ///
    /// Returns `None` when every sender is gone.
    pub async fn next_batch(&mut self) -> Option<Vec<TableRef>> {
        // Block until something happens. Waking on a timer to find an empty
        // queue is the polling this exists to replace.
        let first = self.rx.recv().await?;

        let mut batch = HashSet::new();
        batch.insert(first);

        // Then drain for the debounce window. A writer committing every two
        // seconds produces one cycle rather than thirty.
        let deadline = tokio::time::Instant::now() + self.debounce;
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                break;
            }
            match tokio::time::timeout(remaining, self.rx.recv()).await {
                Ok(Some(table)) => {
                    batch.insert(table);
                }
                // Senders gone, or the window closed. Either way this batch is
                // complete — the tables already collected are still worth a
                // cycle.
                Ok(None) | Err(_) => break,
            }
        }

        let mut batch: Vec<TableRef> = batch.into_iter().collect();
        // Deduplicated by the set, then ordered so a cycle's work is stable.
        batch.sort();
        Some(batch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table(name: &str) -> TableRef {
        TableRef::new("prod", ["db"], name)
    }

    #[tokio::test(start_paused = true)]
    async fn a_burst_of_commits_becomes_one_batch() {
        // The whole point: a streaming writer committing every few seconds
        // should produce one cycle, not one per commit.
        let (events, mut stream) = channel(Duration::from_secs(5));

        for _ in 0..10 {
            events.notify(table("events"));
        }

        let batch = stream.next_batch().await.unwrap();
        assert_eq!(batch, vec![table("events")]);
    }

    #[tokio::test(start_paused = true)]
    async fn several_tables_in_one_window_arrive_together() {
        let (events, mut stream) = channel(Duration::from_secs(5));

        events.notify(table("b"));
        events.notify(table("a"));
        events.notify(table("c"));

        let batch = stream.next_batch().await.unwrap();
        // Sorted, so a cycle's work is stable rather than arrival-ordered.
        assert_eq!(batch, vec![table("a"), table("b"), table("c")]);
    }

    #[tokio::test(start_paused = true)]
    async fn the_stream_waits_rather_than_polling() {
        // Returning an empty batch on a timer would be the polling this
        // replaces. Nothing is sent, so nothing should be produced.
        let (_events, mut stream) = channel(Duration::from_millis(10));

        let result = tokio::time::timeout(Duration::from_secs(60), stream.next_batch()).await;
        assert!(result.is_err(), "a batch appeared with no events");
    }

    #[tokio::test]
    async fn the_stream_ends_when_every_sender_is_gone() {
        let (events, mut stream) = channel(Duration::from_millis(1));
        drop(events);
        assert!(stream.next_batch().await.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn a_full_queue_drops_rather_than_blocking() {
        // A maintenance engine must never be able to stall the thing notifying
        // it. The n-th notification for one table says nothing the first did
        // not, so dropping is correct rather than merely tolerable.
        let (events, _stream) = channel(Duration::from_secs(1));

        let accepted = (0..CHANNEL_DEPTH + 100)
            .filter(|i| events.notify(table(&format!("t{i}"))))
            .count();

        assert_eq!(accepted, CHANNEL_DEPTH);
    }
}
