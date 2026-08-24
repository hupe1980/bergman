//! When a cycle should run.
//!
//! A policy rule may carry a `schedule`; tables whose rule does not fall back
//! to the daemon's interval. The daemon sleeps until the *earliest* of them,
//! because a cycle evaluates every table anyway — the health analyzer is what
//! decides whether a given table has work, and it costs a handful of metadata
//! reads to ask.
//!
//! Waking more often than the busiest rule asks for would be waste; waking less
//! often would silently stretch that rule's cadence.

use std::time::Duration;

use chrono::{DateTime, Utc};

use crate::error::Result;
use crate::policy::Policy;

/// Why a cycle ran.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Trigger {
    /// The daemon's own interval elapsed.
    Interval,
    /// A rule's cron expression came due.
    Schedule {
        /// The rule pattern whose schedule fired.
        pattern: String,
    },
    /// Tables were reported as changed.
    Event {
        /// How many, after the debounce window collapsed a burst.
        tables: usize,
    },
}

impl std::fmt::Display for Trigger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Trigger::Interval => f.write_str("interval"),
            Trigger::Schedule { pattern } => write!(f, "schedule for \"{pattern}\""),
            Trigger::Event { tables } => write!(f, "{tables} changed tables"),
        }
    }
}

/// The schedules a policy declares, compiled once.
///
/// The daemon's own interval is not here: it is a deadline the caller carries
/// across wake-ups, and holding a copy of it would invite recomputing it from
/// `now`. See [`TriggerSet::next_after`].
#[derive(Debug)]
pub struct TriggerSet {
    schedules: Vec<(String, cron::Schedule)>,
}

impl TriggerSet {
    /// Compile every rule's schedule.
    ///
    /// Compiling here rather than per wakeup means a malformed expression is a
    /// startup failure — though policy validation has already rejected one, so
    /// reaching an error here would mean the two disagree.
    pub fn from_policy(policy: &Policy) -> Result<Self> {
        let mut schedules = Vec::new();
        for (pattern, expression) in policy.schedules() {
            schedules.push((
                pattern.to_string(),
                crate::policy::parse_schedule(expression)?,
            ));
        }
        Ok(Self { schedules })
    }

    /// How long until the next trigger fires, and which one.
    ///
    /// `interval_due` is the *absolute* moment the interval next comes round,
    /// and it is a parameter rather than `now + interval` because the daemon
    /// also wakes on commit notifications and recomputes this on every wake-up.
    /// Measured from `now`, a table committed to more often than the interval
    /// would reset the clock forever and the periodic sweep would never run.
    pub fn next_after(
        &self,
        now: DateTime<Utc>,
        interval_due: DateTime<Utc>,
    ) -> (Duration, Trigger) {
        let until_interval = (interval_due - now).to_std().unwrap_or(Duration::ZERO);
        let mut soonest = (until_interval, Trigger::Interval);

        for (pattern, schedule) in &self.schedules {
            let Some(next) = schedule.after(&now).next() else {
                // A schedule with no future occurrence — `0 0 30 2 *`, say.
                // It never fires, which is what the operator wrote.
                continue;
            };

            let delay = (next - now).to_std().unwrap_or(Duration::ZERO);
            if delay < soonest.0 {
                soonest = (
                    delay,
                    Trigger::Schedule {
                        pattern: pattern.clone(),
                    },
                );
            }
        }

        soonest
    }

    /// How many rule schedules are compiled.
    pub fn len(&self) -> usize {
        self.schedules.len()
    }

    /// Whether any rule declares a schedule.
    pub fn is_empty(&self) -> bool {
        self.schedules.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::policy::Config;

    fn policy(toml: &str) -> Policy {
        Policy::compile(&Config::from_toml(toml).unwrap()).unwrap()
    }

    /// The interval's deadline, `interval` from now — what a daemon that has
    /// just started, or has just run an interval cycle, holds.
    fn due_in(interval: Duration) -> (DateTime<Utc>, DateTime<Utc>) {
        let now = Utc::now();
        (now, now + chrono::Duration::from_std(interval).unwrap())
    }

    #[test]
    fn with_no_schedules_the_interval_governs() {
        let triggers =
            TriggerSet::from_policy(&policy("[[rules]]\nmatch = \"prod.**\"\n")).unwrap();

        assert!(triggers.is_empty());
        let (now, due) = due_in(Duration::from_secs(3600));
        let (delay, trigger) = triggers.next_after(now, due);
        assert_eq!(delay, Duration::from_secs(3600));
        assert_eq!(trigger, Trigger::Interval);
    }

    #[test]
    fn the_interval_counts_down_rather_than_restarting() {
        // The bug this signature exists to prevent. The daemon recomputes its
        // wake-up on every event, so an interval measured from `now` would
        // restart on each notification — and a table committed to more often
        // than the interval would keep the periodic sweep from ever running.
        // Tables that *stopped* being written would then never have their
        // snapshots expired, and no orphan scan would ever happen.
        let triggers =
            TriggerSet::from_policy(&policy("[[rules]]\nmatch = \"prod.**\"\n")).unwrap();

        let now = Utc::now();
        let due = now + chrono::Duration::seconds(3600);

        // Half an hour later — after any number of event-driven cycles — the
        // interval is half an hour away, not another full hour.
        let (delay, _) = triggers.next_after(now + chrono::Duration::seconds(1800), due);
        assert_eq!(delay, Duration::from_secs(1800));

        // And once it is overdue it fires immediately rather than going
        // negative or wrapping.
        let (delay, trigger) = triggers.next_after(now + chrono::Duration::seconds(7200), due);
        assert_eq!(delay, Duration::ZERO);
        assert_eq!(trigger, Trigger::Interval);
    }

    #[test]
    fn the_soonest_schedule_wins_over_a_longer_interval() {
        // A rule asking for every two hours must not be stretched to a daily
        // interval, so the daemon wakes for the rule.
        let triggers = TriggerSet::from_policy(&policy(
            "[[rules]]\nmatch = \"prod.**\"\nschedule = \"0 */2 * * *\"\n",
        ))
        .unwrap();

        let (now, due) = due_in(Duration::from_secs(86400));
        let (delay, trigger) = triggers.next_after(now, due);
        assert!(delay <= Duration::from_secs(2 * 3600), "{delay:?}");
        assert_eq!(
            trigger,
            Trigger::Schedule {
                pattern: "prod.**".into()
            }
        );
    }

    #[test]
    fn a_short_interval_wins_over_a_distant_schedule() {
        // The reverse: a daily rule must not stop a five-minute daemon waking.
        let triggers = TriggerSet::from_policy(&policy(
            "[[rules]]\nmatch = \"prod.**\"\nschedule = \"0 3 * * *\"\n",
        ))
        .unwrap();

        let (now, due) = due_in(Duration::from_secs(300));
        let (delay, trigger) = triggers.next_after(now, due);
        assert_eq!(delay, Duration::from_secs(300));
        assert_eq!(trigger, Trigger::Interval);
    }

    #[test]
    fn several_schedules_resolve_to_the_earliest() {
        let triggers = TriggerSet::from_policy(&policy(
            "[[rules]]\nmatch = \"prod.slow.**\"\nschedule = \"0 3 * * *\"\n\n\
                 [[rules]]\nmatch = \"prod.fast.**\"\nschedule = \"*/5 * * * *\"\n",
        ))
        .unwrap();

        assert_eq!(triggers.len(), 2);
        let (now, due) = due_in(Duration::from_secs(86400));
        let (delay, trigger) = triggers.next_after(now, due);
        assert!(delay <= Duration::from_secs(300), "{delay:?}");
        assert_eq!(
            trigger,
            Trigger::Schedule {
                pattern: "prod.fast.**".into()
            }
        );
    }

    #[test]
    fn the_trigger_says_which_rule_woke_the_daemon() {
        // A daemon that only logged "running" would leave an operator unable to
        // tell a schedule firing from an interval elapsing.
        assert_eq!(Trigger::Interval.to_string(), "interval");
        assert_eq!(
            Trigger::Schedule {
                pattern: "prod.**".into()
            }
            .to_string(),
            "schedule for \"prod.**\""
        );
    }
}
