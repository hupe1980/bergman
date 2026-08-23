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
