//! Cron expressions.
//!
//! The `cron` crate parses the six-field form (`sec min hour dom mon dow`).
//! Every operator on earth writes the five-field crontab form (`min hour dom
//! mon dow`), because that is what `crontab(5)`, Kubernetes `CronJob`, and
//! every scheduler they have used take. Refusing it — with the crate's default
//! error, a caret pointing at a column — would be a papercut on the very first
//! thing anyone configures.
//!
//! So both are accepted, and a five-field expression is read as "at second
//! zero", which is what the writer meant.

use crate::error::{Error, Result};

/// Parse a cron expression, accepting five-, six- and seven-field forms.
pub fn parse(expression: &str) -> Result<cron::Schedule> {
    let normalized = normalize(expression);
    normalized.parse::<cron::Schedule>().map_err(|e| {
        Error::policy(format!(
            "invalid schedule {expression:?}: {e}. Expected a crontab expression \
             such as \"0 */2 * * *\" (min hour day month weekday)."
        ))
    })
}

/// Add the seconds field to a five-field expression.
fn normalize(expression: &str) -> String {
    let trimmed = expression.trim();
    if trimmed.split_whitespace().count() == 5 {
        format!("0 {trimmed}")
    } else {
        trimmed.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn five_field_crontab_is_accepted() {
        // The form every operator actually writes.
        for expression in ["0 */2 * * *", "*/15 * * * *", "0 3 * * 1"] {
            assert!(parse(expression).is_ok(), "rejected: {expression}");
        }
    }

    #[test]
    fn six_field_expressions_still_work() {
        assert!(parse("0 0 */2 * * *").is_ok());
    }

    #[test]
    fn five_field_expressions_run_at_second_zero() {
        // Not at "every second of the matching minute", which is what dropping
        // the field or defaulting it to `*` would mean — that would fire sixty
        // times an hour instead of once.
        assert_eq!(normalize("0 */2 * * *"), "0 0 */2 * * *");
    }

    #[test]
    fn a_nonsense_expression_is_refused_with_an_example() {
        let err = parse("every other tuesday").unwrap_err();
        let text = err.to_string();
        assert!(text.contains("every other tuesday"), "{text}");
        // The error has to show the shape, or the reader is left guessing which
        // of several cron dialects is wanted.
        assert!(text.contains("min hour day month weekday"), "{text}");
    }

    #[test]
    fn surrounding_whitespace_does_not_change_the_field_count() {
        assert!(parse("  0 */2 * * *  ").is_ok());
    }
}
