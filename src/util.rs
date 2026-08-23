//! Small shared helpers.

/// Render a byte count the way an operator reads one.
///
/// Binary units, because every Iceberg property that names a size
/// (`write.target-file-size-bytes`, `commit.manifest.target-size-bytes`) is
/// documented in them, and a report that said "512 MB" next to a configured
/// `536870912` would invite the reader to check the arithmetic.
pub fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 6] = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];

    if bytes < 1024 {
        return format!("{bytes} B");
    }

    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }

    if value >= 100.0 {
        format!("{value:.0} {}", UNITS[unit])
    } else if value >= 10.0 {
        format!("{value:.1} {}", UNITS[unit])
    } else {
        format!("{value:.2} {}", UNITS[unit])
    }
}

/// Render a duration in whole units, largest first.
pub fn human_duration(d: std::time::Duration) -> String {
    let secs = d.as_secs();
    match secs {
        0 => "0s".to_string(),
        s if s < 60 => format!("{s}s"),
        s if s < 3600 => format!("{}m", s / 60),
        s if s < 86400 => format!("{}h", s / 3600),
        s => format!("{}d", s / 86400),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn bytes_render_in_binary_units() {
        assert_eq!(human_bytes(0), "0 B");
        assert_eq!(human_bytes(512), "512 B");
        assert_eq!(human_bytes(1024), "1.00 KiB");
        assert_eq!(human_bytes(536_870_912), "512 MiB");
        assert_eq!(human_bytes(1_073_741_824), "1.00 GiB");
    }

    #[test]
    fn byte_precision_shrinks_as_the_number_grows() {
        // Three significant figures throughout: "1.00 KiB" and "512 MiB" are
        // both readable, "512.00 MiB" is noise.
        assert_eq!(human_bytes(1536), "1.50 KiB");
        assert_eq!(human_bytes(10 * 1024), "10.0 KiB");
        assert_eq!(human_bytes(100 * 1024), "100 KiB");
    }

    #[test]
    fn durations_render_in_whole_units() {
        assert_eq!(human_duration(Duration::from_secs(0)), "0s");
        assert_eq!(human_duration(Duration::from_secs(45)), "45s");
        assert_eq!(human_duration(Duration::from_secs(3600)), "1h");
        assert_eq!(human_duration(Duration::from_secs(7 * 86400)), "7d");
    }
}
