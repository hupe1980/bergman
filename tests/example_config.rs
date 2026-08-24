//! The shipped example configuration must parse and compile.
//!
//! Documentation that drifts from the schema is worse than none: a reader
//! copies it, gets an "unknown field" error, and concludes the tool is broken.
//! `deny_unknown_fields` makes that failure mode very easy to hit, so the
//! example is checked against the real parser on every run.

use bergman::policy::{Config, Policy};

#[test]
fn the_example_config_parses_and_compiles() {
    let text = include_str!("../bergman.example.toml");

    let config = Config::from_toml(text)
        .unwrap_or_else(|e| panic!("bergman.example.toml does not parse: {e}"));

    Policy::compile(&config)
        .unwrap_or_else(|e| panic!("bergman.example.toml does not compile: {e}"));

    assert_eq!(config.catalogs.len(), 1);
    assert_eq!(config.rules.len(), 3);
}

#[test]
fn the_example_config_matches_the_tables_its_comments_describe() {
    // The example documents `*` vs `**` in a comment. If the matcher's
    // behaviour ever diverges from that comment, this fails rather than the
    // comment quietly becoming a lie.
    let config = Config::from_toml(include_str!("../bergman.example.toml")).unwrap();
    let policy = Policy::compile(&config).unwrap();

    use bergman::policy::{Decision, TableRef};
    let props = Default::default();

    let flat = TableRef::new("prod", ["analytics"], "events");
    assert!(matches!(
        policy.decide(&flat, &props),
        Decision::Maintain(_)
    ));

    let nested = TableRef::new("prod", ["analytics", "web"], "events");
    assert_eq!(policy.decide(&nested, &props), Decision::Unmatched);

    // `prod.tmp.**` crosses namespaces, so both forms are skipped.
    let tmp = TableRef::new("prod", ["tmp"], "scratch");
    assert!(matches!(policy.decide(&tmp, &props), Decision::Skip { .. }));
    let tmp_nested = TableRef::new("prod", ["tmp", "deep"], "scratch");
    assert!(matches!(
        policy.decide(&tmp_nested, &props),
        Decision::Skip { .. }
    ));
}

#[test]
fn the_readme_quick_start_config_parses() {
    // The block a first-time user copies out of the README.
    let config = Config::from_toml(
        r#"
        [[catalogs]]
        name      = "prod"
        uri       = "http://localhost:8181/catalog"
        warehouse = "s3://lake/warehouse"
        token_env = "BERGMAN_CATALOG_TOKEN"

        [catalogs.properties]
        "s3.region"   = "eu-central-1"
        "s3.endpoint" = "https://s3.eu-central-1.amazonaws.com"

        [defaults.snapshots]
        max_age     = "7d"
        min_to_keep = 3

        [[rules]]
        match = "prod.analytics.*"

        [[rules]]
        match = "prod.tmp.*"
        skip  = true
        "#,
    )
    .expect("the README quick-start config must parse");

    Policy::compile(&config).expect("the README quick-start config must compile");
}

#[test]
fn an_operation_timeout_of_zero_is_refused() {
    // Zero would cancel every operation before it began, which is a config that
    // silently maintains nothing — the shape of failure the whole `limits`
    // section exists to make loud.
    let err = bergman::policy::Config::from_toml(
        "[limits]\noperation_timeout = \"0s\"\n\n[[rules]]\nmatch = \"prod.**\"\n",
    )
    .and_then(|config| bergman::policy::Policy::compile(&config).map(|_| ()))
    .unwrap_err();
    assert!(err.to_string().contains("operation_timeout"), "got: {err}");
}

#[test]
fn an_operation_timeout_is_read_in_the_units_operators_write() {
    // The same `humantime` vocabulary every other duration in the file uses.
    let config =
        bergman::policy::Config::from_toml("[limits]\noperation_timeout = \"45m\"\n").unwrap();
    assert_eq!(
        config.limits.operation_timeout,
        Some(std::time::Duration::from_secs(45 * 60))
    );
}

#[test]
fn no_operation_timeout_is_the_default() {
    // The right value depends on the largest group a deployment rewrites, and a
    // wrong one cancels honest work — so it is asked for rather than assumed.
    let config = bergman::policy::Config::default();
    assert_eq!(config.limits.operation_timeout, None);
}
