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
}

impl std::fmt::Display for Trigger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Trigger::Interval => f.write_str("interval"),
            Trigger::Schedule { pattern } => write!(f, "schedule for \"{pattern}\""),
        }
    }
}

/// The schedules a policy declares, compiled once.
#[derive(Debug)]
pub struct TriggerSet {
    interval: Duration,
    schedules: Vec<(String, cron::Schedule)>,
}

impl TriggerSet {
    /// Compile every rule's schedule.
    ///
    /// Compiling here rather than per wakeup means a malformed expression is a
    /// startup failure — though policy validation has already rejected one, so
    /// reaching an error here would mean the two disagree.
    pub fn from_policy(policy: &Policy, interval: Duration) -> Result<Self> {
        let mut schedules = Vec::new();
        for (pattern, expression) in policy.schedules() {
            schedules.push((
                pattern.to_string(),
                crate::policy::parse_schedule(expression)?,
            ));
        }
        Ok(Self {
            interval,
            schedules,
        })
    }

    /// How long until the next trigger fires, and which one.
    pub fn next_after(&self, now: DateTime<Utc>) -> (Duration, Trigger) {
        let mut soonest = (self.interval, Trigger::Interval);

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

    #[test]
    fn with_no_schedules_the_interval_governs() {
        let triggers = TriggerSet::from_policy(
            &policy("[[rules]]\nmatch = \"prod.**\"\n"),
            Duration::from_secs(3600),
        )
        .unwrap();

        assert!(triggers.is_empty());
        let (delay, trigger) = triggers.next_after(Utc::now());
        assert_eq!(delay, Duration::from_secs(3600));
        assert_eq!(trigger, Trigger::Interval);
    }

    #[test]
    fn the_soonest_schedule_wins_over_a_longer_interval() {
        // A rule asking for every two hours must not be stretched to a daily
        // interval, so the daemon wakes for the rule.
        let triggers = TriggerSet::from_policy(
            &policy("[[rules]]\nmatch = \"prod.**\"\nschedule = \"0 */2 * * *\"\n"),
            Duration::from_secs(86400),
        )
        .unwrap();

        let (delay, trigger) = triggers.next_after(Utc::now());
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
        let triggers = TriggerSet::from_policy(
            &policy("[[rules]]\nmatch = \"prod.**\"\nschedule = \"0 3 * * *\"\n"),
            Duration::from_secs(300),
        )
        .unwrap();

        let (delay, trigger) = triggers.next_after(Utc::now());
        assert_eq!(delay, Duration::from_secs(300));
        assert_eq!(trigger, Trigger::Interval);
    }

    #[test]
    fn several_schedules_resolve_to_the_earliest() {
        let triggers = TriggerSet::from_policy(
            &policy(
                "[[rules]]\nmatch = \"prod.slow.**\"\nschedule = \"0 3 * * *\"\n\n\
                 [[rules]]\nmatch = \"prod.fast.**\"\nschedule = \"*/5 * * * *\"\n",
            ),
            Duration::from_secs(86400),
        )
        .unwrap();

        assert_eq!(triggers.len(), 2);
        let (delay, trigger) = triggers.next_after(Utc::now());
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
