//! Maintenance windows.
//!
//! `22:00-06:00 Europe/Berlin` — the hours a deployment is willing to have its
//! tables rewritten. The timezone is mandatory and not optional sugar: a window
//! written in local time silently moves when a replica is scheduled in another
//! region, and "do not compact during business hours" is exactly the promise
//! that must not move.

use chrono::{DateTime, NaiveTime, TimeZone, Utc};
use chrono_tz::Tz;

use crate::error::{Error, Result};

/// The hours maintenance may start in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaintenanceWindow {
    start: NaiveTime,
    end: NaiveTime,
    timezone: Tz,
}

impl MaintenanceWindow {
    /// Parse `HH:MM-HH:MM Area/City`.
    pub fn parse(raw: &str) -> Result<Self> {
        let raw = raw.trim();
        let (range, zone) = raw.split_once(char::is_whitespace).ok_or_else(|| {
            Error::policy(format!(
                "maintenance_window {raw:?} has no timezone; write it as \
                 \"22:00-06:00 Europe/Berlin\". A window without one moves when a \
                 replica is scheduled in another region."
            ))
        })?;

        let (start, end) = range.split_once('-').ok_or_else(|| {
            Error::policy(format!(
                "maintenance_window {raw:?} is not a range; expected \"HH:MM-HH:MM\""
            ))
        })?;

        let parse_time = |value: &str| {
            NaiveTime::parse_from_str(value.trim(), "%H:%M").map_err(|_| {
                Error::policy(format!(
                    "maintenance_window {raw:?}: {value:?} is not a time of day (HH:MM)"
                ))
            })
        };

        let start = parse_time(start)?;
        let end = parse_time(end)?;

        if start == end {
            return Err(Error::policy(format!(
                "maintenance_window {raw:?} starts and ends at the same time, so it \
                 never opens; omit it to allow maintenance at any hour"
            )));
        }

        let timezone: Tz = zone.trim().parse().map_err(|_| {
            Error::policy(format!(
                "maintenance_window {raw:?}: {:?} is not an IANA timezone",
                zone.trim()
            ))
        })?;

        Ok(Self {
            start,
            end,
            timezone,
        })
    }

    /// Whether `now` falls inside the window.
    pub fn contains(&self, now: DateTime<Utc>) -> bool {
        let local = now.with_timezone(&self.timezone).time();

        if self.start < self.end {
            // An ordinary daytime window: 09:00-17:00.
            local >= self.start && local < self.end
        } else {
            // A window that crosses midnight: 22:00-06:00. This is the common
            // case for maintenance and the one a naive `start <= t < end` gets
            // wrong by never opening at all.
            local >= self.start || local < self.end
        }
    }
}

impl std::fmt::Display for MaintenanceWindow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}-{} {}",
            self.start.format("%H:%M"),
            self.end.format("%H:%M"),
            self.timezone.name()
        )
    }
}

/// When the window next opens, in UTC.
///
/// Used by the daemon to sleep until the window rather than waking every
/// interval to discover it is still shut.
pub fn next_open(window: &MaintenanceWindow, now: DateTime<Utc>) -> DateTime<Utc> {
    if window.contains(now) {
        return now;
    }

    let local = now.with_timezone(&window.timezone);
    let today = local.date_naive();

    // At most two candidates matter: the window's start today, and tomorrow's.
    for days in 0..=1 {
        let Some(date) = today.checked_add_signed(chrono::Duration::days(days)) else {
            break;
        };
        let naive = date.and_time(window.start);
        // A local time can be ambiguous or non-existent across a DST boundary.
        // `latest()` picks a real instant in both cases rather than failing, so
        // a clock change costs at most an hour of window rather than a panic.
        if let Some(candidate) = window.timezone.from_local_datetime(&naive).latest() {
            let candidate = candidate.with_timezone(&Utc);
            if candidate > now {
                return candidate;
            }
        }
    }

    now
}

#[cfg(test)]
mod tests {
    use super::*;

    fn utc(hour: u32, minute: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 6, 15, hour, minute, 0).unwrap()
    }

    #[test]
    fn a_window_crossing_midnight_is_open_on_both_sides_of_it() {
        // The common shape for maintenance, and the one a naive
        // `start <= t < end` never opens at all.
        let window = MaintenanceWindow::parse("22:00-06:00 UTC").unwrap();

        assert!(window.contains(utc(23, 0)), "23:00 is inside 22:00-06:00");
        assert!(window.contains(utc(2, 0)), "02:00 is inside 22:00-06:00");
        assert!(window.contains(utc(22, 0)), "the start is inclusive");
        assert!(!window.contains(utc(6, 0)), "the end is exclusive");
        assert!(!window.contains(utc(12, 0)), "midday is outside");
    }

    #[test]
    fn an_ordinary_daytime_window_works_too() {
        let window = MaintenanceWindow::parse("09:00-17:00 UTC").unwrap();
        assert!(window.contains(utc(12, 0)));
        assert!(!window.contains(utc(20, 0)));
        assert!(!window.contains(utc(3, 0)));
    }

    #[test]
    fn the_window_is_evaluated_in_its_own_timezone() {
        // 23:00 UTC is 01:00 in Berlin (summer), which is inside a
        // 22:00-06:00 Berlin window — and 23:00 UTC would also be inside a
        // 22:00-06:00 UTC one, so the test uses an hour where they disagree.
        let berlin = MaintenanceWindow::parse("22:00-06:00 Europe/Berlin").unwrap();

        // 20:30 UTC is 22:30 in Berlin: inside Berlin's window, outside UTC's.
        assert!(berlin.contains(utc(20, 30)));
        assert!(
            !MaintenanceWindow::parse("22:00-06:00 UTC")
                .unwrap()
                .contains(utc(20, 30))
        );
    }

    #[test]
    fn a_window_without_a_timezone_is_refused() {
        // Local time moves when a replica is scheduled elsewhere, and
        // "not during business hours" must not move.
        let err = MaintenanceWindow::parse("22:00-06:00").unwrap_err();
        assert!(err.to_string().contains("no timezone"), "got: {err}");
    }

    #[test]
    fn malformed_windows_are_refused_with_the_shape_expected() {
        for raw in ["not-a-window UTC", "22:00 UTC", "25:00-26:00 UTC"] {
            assert!(MaintenanceWindow::parse(raw).is_err(), "accepted {raw:?}");
        }
        let err = MaintenanceWindow::parse("22:00-06:00 Mars/Olympus").unwrap_err();
        assert!(err.to_string().contains("IANA timezone"), "got: {err}");
    }

    #[test]
    fn a_zero_length_window_is_refused_rather_than_never_opening() {
        let err = MaintenanceWindow::parse("03:00-03:00 UTC").unwrap_err();
        assert!(err.to_string().contains("never opens"), "got: {err}");
    }

    #[test]
    fn next_open_is_now_when_the_window_is_already_open() {
        let window = MaintenanceWindow::parse("22:00-06:00 UTC").unwrap();
        let now = utc(23, 0);
        assert_eq!(next_open(&window, now), now);
    }

    #[test]
    fn next_open_finds_tonight_when_the_window_is_shut() {
        let window = MaintenanceWindow::parse("22:00-06:00 UTC").unwrap();
        let opens = next_open(&window, utc(12, 0));

        assert_eq!(opens, utc(22, 0));
        assert!(window.contains(opens));
    }

    #[test]
    fn windows_render_back_the_way_they_were_written() {
        let raw = "22:00-06:00 Europe/Berlin";
        assert_eq!(MaintenanceWindow::parse(raw).unwrap().to_string(), raw);
    }
}
