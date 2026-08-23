//! Running maintenance on a schedule.
//!
//! `bergman run` executes one cycle and exits, which is the right shape for a
//! cron job or a Kubernetes `CronJob` and is what most deployments should use.
//! The daemon exists for the cases where that is not enough: a long-lived
//! process that scrapes metrics, and — where the catalog can say so — one that
//! reacts to commits rather than polling on a timer.
//!
//! Nothing here holds state that matters. A daemon killed mid-cycle leaves the
//! same thing a killed `run` leaves: files nothing references, which the orphan
//! scanner reclaims. Restarting replans from the tables' current snapshots.

mod trigger;

pub use trigger::{Trigger, TriggerSet};

use std::sync::Arc;
use std::time::Duration;

use crate::error::Result;
use crate::{Bergman, RunReport};

/// How a daemon decides when to work.
#[derive(Debug, Clone)]
pub struct DaemonConfig {
    /// The default cadence, for tables whose rule sets no `schedule`.
    pub interval: Duration,

    /// Stop after this many cycles.
    ///
    /// For tests and for `--once`; `None` runs until told to stop.
    pub max_cycles: Option<u64>,
}

impl Default for DaemonConfig {
    fn default() -> Self {
        Self {
            // Hourly. Evaluation is metadata-only and cheap, and the health
            // analyzer decides whether anything actually runs — so a short
            // interval costs a few metadata reads rather than data I/O.
            interval: Duration::from_secs(3600),
            max_cycles: None,
        }
    }
}

/// What one cycle did.
#[derive(Debug)]
pub struct Cycle {
    /// Which cycle this was, counting from one.
    pub number: u64,
    /// Why it ran.
    pub trigger: Trigger,
    /// What it did, or the error that stopped it.
    pub outcome: Result<RunReport>,
}

/// Runs maintenance cycles until cancelled.
#[derive(Debug)]
pub struct Daemon {
    bergman: Arc<Bergman>,
    config: DaemonConfig,
    triggers: TriggerSet,
}

impl Daemon {
    /// Build a daemon around a configured engine.
    pub fn new(bergman: Arc<Bergman>, config: DaemonConfig) -> Result<Self> {
        let triggers = TriggerSet::from_policy(bergman.policy(), config.interval)?;
        Ok(Self {
            bergman,
            config,
            triggers,
        })
    }

    /// The next moment any trigger fires, and which one.
    pub fn next_wakeup(&self, now: chrono::DateTime<chrono::Utc>) -> (Duration, Trigger) {
        self.triggers.next_after(now)
    }

    /// Run cycles until `shutdown` resolves, or the cycle limit is reached.
    ///
    /// `on_cycle` is called after each one. Returning the report rather than
    /// logging it keeps this loop free of any opinion about output — the binary
    /// renders, an embedder does whatever it likes.
    pub async fn run<F>(&self, mut on_cycle: F, shutdown: impl Future<Output = ()>) -> Result<u64>
    where
        F: FnMut(Cycle),
    {
        let mut completed = 0u64;
        let shutdown = std::pin::pin!(shutdown);
        let mut shutdown = shutdown;

        loop {
            if self.config.max_cycles.is_some_and(|max| completed >= max) {
                return Ok(completed);
            }

            let now = chrono::Utc::now();
            let (delay, trigger) = self.next_wakeup(now);

            // Sleeping to the window's edge rather than waking every interval
            // to find it shut. A daemon that logged "outside the window" sixty
            // times a night is a daemon whose logs nobody reads.
            let delay = match self.bergman.policy().window() {
                Some(window) => {
                    let wake = now + chrono::Duration::from_std(delay).unwrap_or_default();
                    let opens = crate::policy::next_open(window, wake);
                    (opens - now).to_std().unwrap_or(delay).max(delay)
                }
                None => delay,
            };

            tokio::select! {
                // Biased so that a shutdown that arrives while a timer is also
                // ready wins. Otherwise a daemon on a short interval could take
                // several cycles to notice it was asked to stop.
                biased;
                () = &mut shutdown => return Ok(completed),
                () = tokio::time::sleep(delay) => {}
            }

            completed += 1;
            let outcome = self.cycle().await;
            on_cycle(Cycle {
                number: completed,
                trigger,
                outcome,
            });
        }
    }

    /// Plan and run once.
    async fn cycle(&self) -> Result<RunReport> {
        let plan = self.bergman.plan().await?;
        self.bergman.run(&plan).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn a_shutdown_is_noticed_even_while_a_timer_is_also_ready() {
        // `select!` picks arbitrarily among ready branches, so an unbiased loop
        // on a short interval could take several cycles to notice it was asked
        // to stop. The `biased` keyword is what stops that, and this is the
        // test that would catch its removal.
        use tokio::sync::oneshot;

        let (tx, rx) = oneshot::channel::<()>();
        tx.send(()).unwrap();

        // Both branches are ready from the first poll: the shutdown has already
        // fired, and a zero delay makes the timer ready too.
        let shutdown = async {
            let _ = rx.await;
        };

        let ready_immediately = tokio::select! {
            biased;
            () = shutdown => "shutdown",
            () = tokio::time::sleep(Duration::ZERO) => "timer",
        };

        assert_eq!(ready_immediately, "shutdown");
    }

    #[test]
    fn the_default_interval_is_short_because_evaluation_is_cheap() {
        // A cycle over healthy tables is metadata reads and no data I/O, so an
        // hour is a cadence rather than a cost.
        assert_eq!(DaemonConfig::default().interval, Duration::from_secs(3600));
        assert_eq!(DaemonConfig::default().max_cycles, None);
    }
}
