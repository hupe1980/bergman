//! Matching a glob pattern against a fully-qualified table name.

use globset::{GlobBuilder, GlobMatcher};

use crate::policy::TableRef;

/// A compiled table pattern.
///
/// Patterns are globs over `catalog.namespace…​.table` with `.` as the
/// separator, so `*` stops at a namespace boundary and `**` crosses them —
/// the same distinction `/` has in a filesystem glob, which is what most
/// people already have in their fingers.
#[derive(Debug)]
pub struct TableMatcher {
    matcher: GlobMatcher,
}

impl TableMatcher {
    /// Compile a pattern.
    pub fn new(pattern: &str) -> Result<Self, String> {
        if pattern.is_empty() {
            return Err("pattern is empty".to_string());
        }

        // `globset` separates path components with `/`, and Iceberg names
        // separate with `.`. Rather than translate the pattern (which would
        // mean parsing globs by hand to find which dots are separators and
        // which are inside a character class), both the pattern and the table
        // name are rendered with `/` as the separator and handed to globset
        // unchanged. The translation is therefore total and needs no glob
        // parser of our own.
        let translated = pattern.replace('.', "/");

        // `literal_separator` is what makes `*` stop at a namespace boundary
        // and `**` cross it. Without it globset's `*` matches separators too,
        // so `prod.analytics.*` would silently reach every table in every
        // nested namespace — a rule matching far more than it appears to, which
        // for a tool that deletes files is the wrong direction to be wrong in.
        let glob = GlobBuilder::new(&translated)
            .literal_separator(true)
            .build()
            .map_err(|e| {
                // globset's message quotes the translated pattern, which the
                // user never typed. Report theirs.
                let raw = e.to_string().replace(&translated, pattern);
                format!("invalid pattern: {raw}")
            })?;

        Ok(Self {
            matcher: glob.compile_matcher(),
        })
    }

    /// Whether this pattern matches a table.
    pub fn matches(&self, table: &TableRef) -> bool {
        let mut path = String::with_capacity(32);
        path.push_str(&table.catalog);
        for part in &table.namespace {
            path.push('/');
            path.push_str(part);
        }
        path.push('/');
        path.push_str(&table.name);

        self.matcher.is_match(&path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(catalog: &str, ns: &[&str], name: &str) -> TableRef {
        TableRef::new(catalog, ns.to_vec(), name)
    }

    #[test]
    fn exact_match() {
        let m = TableMatcher::new("prod.analytics.events").unwrap();
        assert!(m.matches(&t("prod", &["analytics"], "events")));
        assert!(!m.matches(&t("prod", &["analytics"], "orders")));
    }

    #[test]
    fn star_stops_at_a_namespace_boundary() {
        // This is the rule most likely to surprise, because Iceberg namespaces
        // are dotted and a nested namespace reads exactly like a table name.
        let m = TableMatcher::new("prod.analytics.*").unwrap();
        assert!(m.matches(&t("prod", &["analytics"], "events")));
        assert!(!m.matches(&t("prod", &["analytics", "web"], "events")));
    }

    #[test]
    fn double_star_crosses_namespace_boundaries() {
        let m = TableMatcher::new("prod.analytics.**").unwrap();
        assert!(m.matches(&t("prod", &["analytics"], "events")));
        assert!(m.matches(&t("prod", &["analytics", "web"], "events")));
        assert!(m.matches(&t("prod", &["analytics", "web", "raw"], "events")));
        assert!(!m.matches(&t("prod", &["finance"], "events")));
    }

    #[test]
    fn prefix_patterns_within_a_segment() {
        let m = TableMatcher::new("prod.streaming.events_*").unwrap();
        assert!(m.matches(&t("prod", &["streaming"], "events_raw")));
        assert!(!m.matches(&t("prod", &["streaming"], "orders")));
    }

    #[test]
    fn a_table_name_containing_a_dot_is_not_split() {
        // A name is one segment even when it contains a dot, so a single `*`
        // must not match across it. Iceberg permits such names, and treating
        // one as two segments would silently widen every pattern.
        let m = TableMatcher::new("prod.analytics.*").unwrap();
        assert!(m.matches(&t("prod", &["analytics"], "a.b")));

        // ...and the containment is real: `a.b` as one name is a different
        // table from namespace `a` holding table `b`, and a pattern written
        // for the nested form must not reach the dotted one.
        let nested = TableMatcher::new("prod.analytics.a.b").unwrap();
        assert!(nested.matches(&t("prod", &["analytics", "a"], "b")));
        assert!(!nested.matches(&t("prod", &["analytics"], "a.b")));
    }

    #[test]
    fn empty_pattern_is_refused() {
        assert!(TableMatcher::new("").is_err());
    }

    #[test]
    fn invalid_pattern_error_quotes_what_the_user_typed() {
        // globset sees the `/`-translated form; the operator only ever saw the
        // dotted one, so an error naming the translation would be baffling.
        let err = TableMatcher::new("prod.analytics.[").unwrap_err();
        assert!(err.contains("prod.analytics."), "got: {err}");
        assert!(
            !err.contains("prod/analytics/"),
            "leaked translation: {err}"
        );
    }
}
