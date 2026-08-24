//! Matching a glob pattern against a fully-qualified table name.
//!
//! # The rendering is injective
//!
//! Both sides are rendered with `/` as the separator and handed to `globset`,
//! and that rendering decides which rule governs a table — including whether a
//! `skip` applies. A naive join is not injective, because Iceberg permits a name
//! containing the separator:
//!
//! ```text
//! namespace ["a", "b"], table "c"    -> prod/a/b/c
//! namespace ["a"],      table "b/c"  -> prod/a/b/c   (!)
//! ```
//!
//! So `/` and `%` are percent-encoded per segment, `%` first, by the same rule
//! as [`crate::health::partition_path`]: an escape the encoding introduces
//! cannot be mistaken for one that was in the value.

use globset::{GlobBuilder, GlobMatcher};

use crate::policy::TableRef;

/// A compiled table pattern.
///
/// Patterns are globs over `catalog.namespace…​.table` with `.` as the
/// separator, so `*` stops at a namespace boundary and `**` crosses them —
/// the same distinction `/` has in a filesystem glob, which is what most
/// people already have in their fingers.
///
/// A `.` that is part of a *name* rather than a separator is written `\.`, so a
/// table genuinely called `a.b` is addressable as `prod.ns.a\.b` and is not
/// confused with the table `b` in the nested namespace `ns.a`.
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

        let translated = translate(pattern);

        // `literal_separator` is what makes `*` stop at a namespace boundary
        // and `**` cross it. Without it globset's `*` matches separators too,
        // so `prod.analytics.*` would silently reach every table in every
        // nested namespace — a rule matching far more than it appears to, which
        // for a tool that deletes files is the wrong direction to be wrong in.
        //
        // `backslash_escape` is set explicitly rather than left to its default,
        // which is off on Windows. A pattern that meant one thing on Linux and
        // another on Windows would make `\.` — the only way to address a table
        // whose name contains a dot — a platform-dependent rule.
        let glob = GlobBuilder::new(&translated)
            .literal_separator(true)
            .backslash_escape(true)
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
        self.matcher.is_match(render(table))
    }
}

/// Render a table the way a pattern is rendered, so the two can be compared.
///
/// Injective: see the module documentation for the collision this closes.
pub(crate) fn render(table: &TableRef) -> String {
    let mut path = String::with_capacity(32);
    encode_into(&table.catalog, &mut path);
    for part in &table.namespace {
        path.push('/');
        encode_into(part, &mut path);
    }
    path.push('/');
    encode_into(&table.name, &mut path);
    path
}

/// Percent-encode the two characters that would otherwise be ambiguous.
///
/// `%` first, so an escape this produces cannot be mistaken for one that was in
/// the name: a table literally called `b%2Fc` becomes `b%252Fc` and stays
/// distinct from `b/c`.
fn encode_into(segment: &str, out: &mut String) {
    for ch in segment.chars() {
        match ch {
            '%' => out.push_str("%25"),
            '/' => out.push_str("%2F"),
            other => out.push(other),
        }
    }
}

/// Turn a dotted pattern into the `/`-separated glob `globset` compiles.
///
/// Everything that is not a separator, an escaped separator, or a character the
/// rendering encodes is passed through untouched — so `*`, `**`, `?`, character
/// classes and brace alternates all reach globset exactly as written, and this
/// function needs no glob parser of its own.
fn translate(pattern: &str) -> String {
    let mut out = String::with_capacity(pattern.len() + 8);
    let mut chars = pattern.chars().peekable();

    while let Some(ch) = chars.next() {
        match ch {
            // `\.` is a dot inside a name rather than a separator. The escape is
            // consumed here and a plain `.` is emitted, which globset treats as
            // an ordinary literal.
            '\\' if chars.peek() == Some(&'.') => {
                chars.next();
                out.push('.');
            }
            '.' => out.push('/'),
            // The pattern is encoded the same way a name is, so an operator
            // writes the name as it actually is — `prod.ns.b/c` addresses the
            // table called `b/c` — and a literal `%` still stands for itself.
            '%' => out.push_str("%25"),
            '/' => out.push_str("%2F"),
            other => out.push(other),
        }
    }
    out
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
    fn an_escaped_dot_addresses_a_name_that_contains_one() {
        // Without an escape, a table genuinely called `a.b` could only ever be
        // reached by a wildcard — so it could not be named in a rule, and could
        // not be `skip`ped, which is the direction that matters for a tool that
        // deletes files.
        let dotted = TableMatcher::new(r"prod.analytics.a\.b").unwrap();
        assert!(dotted.matches(&t("prod", &["analytics"], "a.b")));
        assert!(
            !dotted.matches(&t("prod", &["analytics", "a"], "b")),
            "the escape must not also reach the nested table"
        );

        // The escape works in a namespace segment too.
        let ns = TableMatcher::new(r"prod.a\.b.events").unwrap();
        assert!(ns.matches(&t("prod", &["a.b"], "events")));
        assert!(!ns.matches(&t("prod", &["a", "b"], "events")));
    }

    #[test]
    fn a_separator_inside_a_name_cannot_impersonate_nesting() {
        // The injectivity property, and the collision it closes. A naive join
        // renders both of these as `prod/a/b/c`, so one rule would silently
        // govern both tables — including a `skip` written for one of them.
        let nested = t("prod", &["a", "b"], "c");
        let slashed = t("prod", &["a"], "b/c");
        assert_ne!(render(&nested), render(&slashed));

        let exact = TableMatcher::new("prod.a.b.c").unwrap();
        assert!(exact.matches(&nested));
        assert!(!exact.matches(&slashed));

        // The table with the separator in its name is still addressable — an
        // operator writes the name as it actually is.
        let by_name = TableMatcher::new("prod.a.b/c").unwrap();
        assert!(by_name.matches(&slashed));
        assert!(!by_name.matches(&nested));

        // And a single `*` reaches it, because the encoding introduces no
        // separator.
        let wild = TableMatcher::new("prod.a.*").unwrap();
        assert!(wild.matches(&slashed));
    }

    #[test]
    fn the_encoding_cannot_be_impersonated() {
        // `%` is encoded first, so a table literally named `b%2Fc` stays
        // distinct from one named `b/c` — the same rule partition rendering
        // follows, for the same reason.
        let literal = t("prod", &["a"], "b%2Fc");
        let slashed = t("prod", &["a"], "b/c");
        assert_ne!(render(&literal), render(&slashed));

        assert!(TableMatcher::new("prod.a.b%2Fc").unwrap().matches(&literal));
        assert!(!TableMatcher::new("prod.a.b%2Fc").unwrap().matches(&slashed));
        assert!(TableMatcher::new("prod.a.b/c").unwrap().matches(&slashed));
    }

    #[test]
    fn glob_syntax_still_reaches_globset_untouched() {
        // The translation must not eat metacharacters: everything but the
        // separator and the two encoded characters passes through.
        let classes = TableMatcher::new("prod.db.events_[0-9]").unwrap();
        assert!(classes.matches(&t("prod", &["db"], "events_1")));
        assert!(!classes.matches(&t("prod", &["db"], "events_x")));

        let alternates = TableMatcher::new("prod.db.{orders,events}").unwrap();
        assert!(alternates.matches(&t("prod", &["db"], "orders")));
        assert!(alternates.matches(&t("prod", &["db"], "events")));
        assert!(!alternates.matches(&t("prod", &["db"], "other")));

        let single = TableMatcher::new("prod.db.event?").unwrap();
        assert!(single.matches(&t("prod", &["db"], "events")));
        assert!(!single.matches(&t("prod", &["db"], "event")));
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
