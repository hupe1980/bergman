//! Per-partition condition, and the identity a partition is known by.
//!
//! Compaction works at partition granularity, so this is the level at which a
//! decision to rewrite is actually made: a table can look fine on average while
//! one partition is a thousand tiny files, and averaging over the table would
//! hide exactly the problem worth fixing.

use iceberg::spec::{PartitionSpec, Schema, Struct};
use serde::{Deserialize, Serialize};

/// A partition's identity.
///
/// Two things depend on this being exact rather than merely readable. The
/// planner groups a table's files by it, and execution matches the plan's
/// partitions back against freshly-scanned files — so two *different*
/// partitions that rendered to one string would have their files rewritten
/// together and written out under one partition value, filing rows where they
/// do not belong. [`partition_path`] is therefore injective, and it is the only
/// place partition identity is decided.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct PartitionKey {
    /// The partition spec this key was produced under.
    ///
    /// Part of the identity because a table whose spec has evolved holds files
    /// under several specs at once, and two files with the same rendered value
    /// under different specs are not interchangeable.
    pub spec_id: i32,
    /// The rendered partition value, or `unpartitioned`.
    pub value: String,
}

impl PartitionKey {
    /// The key for a file under an unpartitioned spec.
    pub fn unpartitioned(spec_id: i32) -> Self {
        Self {
            spec_id,
            value: UNPARTITIONED.to_string(),
        }
    }

    /// The key for one file's partition tuple.
    pub fn new(spec: &PartitionSpec, schema: &Schema, data: &Struct) -> Self {
        Self {
            spec_id: spec.spec_id(),
            value: partition_path(spec, schema, data),
        }
    }
}

/// What an unpartitioned table's single group is called.
///
/// A name rather than an empty string: this value appears in plans and audit
/// records, where blank reads as missing data rather than as "no partitioning".
pub const UNPARTITIONED: &str = "unpartitioned";

/// How a partition value that is genuinely null renders.
const NULL: &str = "null";

/// How a partition tuple shorter than its spec renders.
const ABSENT: &str = "absent";

/// Render a partition tuple to a stable, unambiguous string.
///
/// The shape is Hive's and Iceberg's own — `region=eu/day=2026-01-15` — built
/// from [`iceberg::spec::Transform::to_human_string`], so a `day` transform
/// renders as a date rather than as the integer actually stored. That is what
/// makes a plan line legible without Bergman modelling the spec's type system.
///
/// **Injective**, which is the property the whole type exists for: this string
/// is compared for equality to decide which files are rewritten together, so
/// two partitions that rendered alike would have their files merged and written
/// out under one of the two partition values — filing every row of the other
/// where it does not belong. Nothing fails; queries just start missing rows.
///
/// Upstream's own `PartitionSpec::partition_to_path` is not injective and does
/// not try to be: it is for building a directory name a human reads. Two
/// distinct failures have to be closed to get from there to here, and the
/// escaping below closes both.
pub fn partition_path(spec: &PartitionSpec, schema: &Schema, data: &Struct) -> String {
    if spec.fields().is_empty() {
        return UNPARTITIONED.to_string();
    }

    // The partition type binds the spec's fields to the schema they transform.
    // A spec that will not bind is one Bergman cannot describe, and falling
    // back to the raw tuple keeps grouping correct — every file under that spec
    // still gets the same key — while making the oddity visible in the plan.
    let Ok(partition_type) = spec.partition_type(schema) else {
        return format!("{data:?}");
    };
    let field_types = partition_type.fields();

    let mut out = String::new();
    for (index, field) in spec.fields().iter().enumerate() {
        if index > 0 {
            out.push('/');
        }
        let rendered = match (data.iter().nth(index), field_types.get(index)) {
            // A null value renders as the reserved token, unescaped. It is the
            // only thing that ever produces that spelling, because `escape`
            // encodes a *value* of "null" into something else.
            (Some(None), _) => NULL.to_string(),
            (Some(value), Some(declared)) => {
                escape(&field.transform.to_human_string(&declared.field_type, value))
            }
            // A tuple shorter than its spec is malformed metadata. Naming the
            // field as absent keeps the key distinct from one whose value is
            // genuinely null.
            _ => ABSENT.to_string(),
        };
        out.push_str(&field.name);
        out.push('=');
        out.push_str(&rendered);
    }
    out
}

/// Make one rendered value unmistakable for anything else.
///
/// Two ambiguities, each of which mis-files rows rather than merely printing
/// oddly:
///
/// 1. **The field separator.** A string value containing `/` renders as two
///    fields, so `region = "eu/day=2026-01-15"` produces exactly the key a
///    different, real partition produces. `%` is encoded too, and *first*, so an
///    escape introduced here cannot be mistaken for one already in the value.
///
/// 2. **The reserved tokens.** [`Transform::to_human_string`] renders a null
///    value as `null` — and a string column holding those four characters
///    renders identically. Same for `absent`. So a value coming out as a
///    reserved token has its first byte percent-encoded, which no real value can
///    produce: a genuine `%` has already become `%25`.
///
/// The result is injective and stays legible — the escape fires only for those
/// two strings and for values containing `%` or `/`.
///
/// [`Transform::to_human_string`]: iceberg::spec::Transform::to_human_string
fn escape(value: &str) -> String {
    let escaped = if value.contains(['%', '/']) {
        let mut out = String::with_capacity(value.len() + 4);
        for ch in value.chars() {
            match ch {
                '%' => out.push_str("%25"),
                '/' => out.push_str("%2F"),
                other => out.push(other),
            }
        }
        out
    } else {
        value.to_string()
    };

    match escaped.as_str() {
        NULL => "%6Eull".to_string(),
        ABSENT => "%61bsent".to_string(),
        _ => escaped,
    }
}

impl std::fmt::Display for PartitionKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.value)
    }
}

/// The files of one partition that a rewrite would read.
///
/// A partition is what *triggers* a rewrite; this is what the rewrite then
/// costs. The two are not the same set, and conflating them is how a compactor
/// reads fifty gigabytes to merge forty small files: a partition earns a
/// rewrite because a third of its files are small, and the other two thirds —
/// already at target — gain nothing from being read and written back.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Eligible {
    /// How many files a rewrite would read.
    pub count: usize,
    /// Their total size, which is what a byte budget is charged.
    pub bytes: u64,
    /// How many of them are above the oversized threshold.
    ///
    /// Tracked separately because it inverts one rule: a rewrite that splits an
    /// oversized file deliberately produces *more* files than it consumed, so
    /// the "N in, N out — not worth it" guard must not veto it.
    pub oversized: usize,
}

impl Eligible {
    /// Whether a rewrite would read anything.
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }
}

/// One partition's condition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionHealth {
    /// Which partition.
    pub key: PartitionKey,
    /// Live data files in it.
    pub data_file_count: usize,
    /// Their total size.
    pub data_bytes: u64,
    /// Their total rows.
    pub record_count: u64,
    /// Positional delete files applying to it.
    pub position_delete_count: usize,
    /// Equality delete files applying to it.
    pub equality_delete_count: usize,
    /// Rows named by those delete files, positional and equality together.
    pub delete_record_count: u64,
    /// Rows named by *equality* delete files alone.
    ///
    /// Tracked separately from [`Self::delete_record_count`] because the two
    /// cost different amounts to apply. A positional delete names a row by file
    /// and offset and becomes a row selection, so the row is never decoded. An
    /// equality delete has to be matched against the data, which the executor
    /// does as a hash anti-join — cheap, but the only delete cost that scales
    /// with the number of delete rows rather than the number of files.
    #[serde(default)]
    pub equality_delete_record_count: u64,
    /// Sizes of its live data files, ascending.
    pub file_sizes: Vec<u64>,
    /// When the newest of its live data files was added, in epoch milliseconds.
    ///
    /// The signal that a partition is still being written. Compacting the
    /// partition a streamer is appending to is the single most common way to
    /// spend a cycle losing commit races, so the planner leaves a partition
    /// alone until its newest file has settled (see
    /// `CompactionSettings::min_file_age`).
    ///
    /// `None` when no live file carries a timestamp, which is read as "old
    /// enough": a partition Bergman cannot date must not be excluded from
    /// maintenance forever.
    #[serde(default)]
    pub newest_file_ms: Option<i64>,
}

impl PartitionHealth {
    /// Start an empty partition.
    pub(crate) fn new(key: PartitionKey) -> Self {
        Self {
            key,
            data_file_count: 0,
            data_bytes: 0,
            record_count: 0,
            position_delete_count: 0,
            equality_delete_count: 0,
            delete_record_count: 0,
            equality_delete_record_count: 0,
            file_sizes: Vec::new(),
            newest_file_ms: None,
        }
    }

    /// Fraction of this partition's files below `threshold`.
    pub fn small_file_ratio(&self, threshold: u64) -> f64 {
        if self.file_sizes.is_empty() {
            return 0.0;
        }
        let small = self.file_sizes.iter().filter(|&&s| s < threshold).count();
        small as f64 / self.file_sizes.len() as f64
    }

    /// How many of this partition's files are below `threshold`.
    pub fn small_file_count(&self, threshold: u64) -> usize {
        self.file_sizes.iter().filter(|&&s| s < threshold).count()
    }

    /// How many of this partition's files are above `threshold`.
    ///
    /// The half of file-size health a small-file-only compactor forgets. One
    /// unsplittable file is a task no reader can parallelise, and nothing but a
    /// rewrite ever splits it.
    pub fn large_file_count(&self, threshold: u64) -> usize {
        self.file_sizes.iter().filter(|&&s| s > threshold).count()
    }

    /// The files a rewrite would actually read, by size alone.
    ///
    /// Size alone, because a manifest does not say which data file a delete
    /// applies to — deciding that needs the sequence-number rule the scan
    /// applies, and the scan is data-plane work the planner deliberately does
    /// not do. When the delete trigger fires the executor treats the whole
    /// partition as eligible instead, which is the honest reading: a
    /// partition-scoped delete may apply to any file in it.
    pub fn size_eligible(&self, small: u64, large: u64) -> Eligible {
        let mut eligible = Eligible::default();
        for &size in &self.file_sizes {
            if size > large {
                eligible.oversized += 1;
            } else if size >= small {
                continue;
            }
            eligible.count += 1;
            eligible.bytes = eligible.bytes.saturating_add(size);
        }
        eligible
    }

    /// Every file in the partition, as an eligible set.
    ///
    /// What the delete trigger selects: the deletes are what make the rewrite
    /// worth doing, and a delete can apply to a file of any size.
    pub fn all_eligible(&self) -> Eligible {
        Eligible {
            count: self.data_file_count,
            bytes: self.data_bytes,
            oversized: 0,
        }
    }

    /// Rows named by delete files, over live rows.
    pub fn delete_ratio(&self) -> f64 {
        if self.record_count == 0 {
            return 0.0;
        }
        self.delete_record_count as f64 / self.record_count as f64
    }

    /// Total live delete files.
    pub fn delete_file_count(&self) -> usize {
        self.position_delete_count + self.equality_delete_count
    }

    /// Whether the newest live file in this partition is at least `min_age` old.
    ///
    /// A partition still receiving writes is one a rewrite will lose a commit
    /// race over, and losing it costs the whole rewrite. Waiting is strictly
    /// cheaper than competing.
    pub fn has_settled(&self, min_age: std::time::Duration, now_ms: i64) -> bool {
        let Some(newest) = self.newest_file_ms else {
            // Undated files are treated as settled. The alternative — never
            // compacting a partition whose timestamps Bergman cannot read —
            // fails in the direction of doing nothing forever.
            return true;
        };
        let min_age_ms = i64::try_from(min_age.as_millis()).unwrap_or(i64::MAX);
        now_ms.saturating_sub(newest) >= min_age_ms
    }

    /// How many files rewriting `eligible` would produce.
    ///
    /// Used to reject rewrites that would not actually help: rewriting five
    /// files into five files spends I/O to achieve nothing, and a planner that
    /// cannot tell will do it every cycle forever.
    ///
    /// Over the *eligible* set rather than the whole partition, because that is
    /// what a rewrite reads — comparing a whole partition's output estimate
    /// against a whole partition's file count answers a question nobody asked.
    pub fn output_file_estimate(&self, eligible: Eligible, target_file_size: u64) -> usize {
        if target_file_size == 0 || eligible.bytes == 0 {
            return 0;
        }
        // Deletes remove rows, so the output is smaller than the input by
        // roughly the delete ratio. This is an estimate and is documented as
        // one — the exact figure is not knowable without reading the data.
        let live_fraction = (1.0 - self.delete_ratio()).max(0.0);
        let live_bytes = (eligible.bytes as f64 * live_fraction) as u64;
        live_bytes.div_ceil(target_file_size).max(1) as usize
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use iceberg::spec::{
        Literal, NestedField, PrimitiveType, Schema, Transform, Type, UnboundPartitionSpec,
    };

    use super::*;

    fn partition(sizes: &[u64], records: u64, deletes: u64) -> PartitionHealth {
        let mut p = PartitionHealth::new(PartitionKey::unpartitioned(0));
        p.data_file_count = sizes.len();
        p.data_bytes = sizes.iter().sum();
        p.record_count = records;
        p.delete_record_count = deletes;
        p.file_sizes = sizes.to_vec();
        p.file_sizes.sort_unstable();
        p
    }

    /// A schema and a spec partitioning it by `region` (identity) and `day`
    /// (a date transform over a timestamp).
    fn spec_and_schema() -> (PartitionSpec, Schema) {
        let schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "region", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(2, "ts", Type::Primitive(PrimitiveType::Timestamp)).into(),
            ])
            .build()
            .unwrap();

        let spec = UnboundPartitionSpec::builder()
            .with_spec_id(0)
            .add_partition_field(1, "region", Transform::Identity)
            .unwrap()
            .add_partition_field(2, "day", Transform::Day)
            .unwrap()
            .build()
            .bind(schema.clone())
            .unwrap();

        (spec, schema)
    }

    #[test]
    fn a_partition_renders_as_iceberg_spells_it() {
        // `name=value`, with a date transform rendered as a date rather than as
        // the integer actually stored. An operator reading a plan should
        // recognise the partition they configured.
        let (spec, schema) = spec_and_schema();
        let data = Struct::from_iter([
            Some(Literal::string("eu")),
            // 20 000 days after the epoch: 2024-10-04.
            Some(Literal::int(20_000)),
        ]);

        let key = PartitionKey::new(&spec, &schema, &data);
        assert_eq!(key.spec_id, 0);
        assert_eq!(key.value, "region=eu/day=2024-10-04");
    }

    #[test]
    fn a_value_containing_a_separator_cannot_impersonate_another_field() {
        // This is the property the whole type exists for. Without escaping,
        // `region = "eu/day=2024-10-04"` renders identically to a different
        // partition — and identical keys mean their files are rewritten
        // together and written out under one partition value.
        let (spec, schema) = spec_and_schema();

        let honest = PartitionKey::new(
            &spec,
            &schema,
            &Struct::from_iter([Some(Literal::string("eu")), Some(Literal::int(20_000))]),
        );
        let impostor = PartitionKey::new(
            &spec,
            &schema,
            &Struct::from_iter([
                Some(Literal::string("eu/day=2024-10-04")),
                Some(Literal::int(20_000)),
            ]),
        );

        assert_ne!(honest.value, impostor.value);
        assert_eq!(impostor.value, "region=eu%2Fday=2024-10-04/day=2024-10-04");
    }

    #[test]
    fn a_percent_in_a_value_survives_a_round_of_escaping() {
        // `%` is escaped first, so a value that already contains `%2F` does not
        // decode back into a separator.
        assert_eq!(escape("a%2Fb"), "a%252Fb");
        assert_eq!(escape("plain"), "plain");
    }

    #[test]
    fn a_null_partition_value_is_named_rather_than_blank() {
        let (spec, schema) = spec_and_schema();
        let data = Struct::from_iter([None, Some(Literal::int(20_000))]);
        assert_eq!(
            PartitionKey::new(&spec, &schema, &data).value,
            "region=null/day=2024-10-04"
        );
    }

    #[test]
    fn a_value_of_null_is_not_the_same_partition_as_a_null_value() {
        // The second way injectivity breaks, and the one upstream's own
        // renderer walks straight into: `to_human_string` prints a NULL value
        // as the word "null", and a *string column holding the four characters
        // n-u-l-l* prints identically. Those are two different partitions with
        // two different sets of files, and a key that conflated them would have
        // both rewritten together and written out under whichever tuple the
        // group's first file carried — silently filing every row of the other
        // partition where it does not belong.
        let (spec, schema) = spec_and_schema();

        let genuinely_null = PartitionKey::new(
            &spec,
            &schema,
            &Struct::from_iter([None, Some(Literal::int(20_000))]),
        );
        let the_word_null = PartitionKey::new(
            &spec,
            &schema,
            &Struct::from_iter([Some(Literal::string("null")), Some(Literal::int(20_000))]),
        );

        assert_ne!(genuinely_null, the_word_null);
        assert_eq!(genuinely_null.value, "region=null/day=2024-10-04");
        assert_eq!(the_word_null.value, "region=%6Eull/day=2024-10-04");
    }

    #[test]
    fn a_value_cannot_impersonate_a_reserved_token() {
        // Both tokens, and the escape that keeps them unreachable: a real `%`
        // has already become `%25`, so nothing but this rule ever produces
        // `%6E` or `%61`.
        assert_eq!(escape("null"), "%6Eull");
        assert_eq!(escape("absent"), "%61bsent");
        assert_eq!(escape("%6Eull"), "%256Eull", "and it does not round-trip");
        // Values that merely contain a token are unambiguous already.
        assert_eq!(escape("nullable"), "nullable");
        assert_eq!(escape("not null"), "not null");
    }

    #[test]
    fn an_unpartitioned_spec_renders_as_a_name() {
        let spec = PartitionSpec::unpartition_spec();
        let schema = Schema::builder().with_schema_id(0).build().unwrap();
        assert_eq!(
            PartitionKey::new(&spec, &schema, &Struct::empty()).value,
            "unpartitioned"
        );
    }

    #[test]
    fn the_same_value_under_two_specs_is_two_partitions() {
        // A table whose spec evolved holds files under both, and they are not
        // interchangeable: rewriting them together would write output under one
        // spec while claiming to replace files partitioned by the other.
        assert_ne!(
            PartitionKey::unpartitioned(0),
            PartitionKey::unpartitioned(1)
        );
    }

    #[test]
    fn a_partition_still_being_written_has_not_settled() {
        // The guard against fighting the streaming writer for the hot
        // partition — the top source of pointless commit conflicts.
        let mut p = partition(&[10; 10], 100, 0);
        let now_ms = 1_000_000_000;

        p.newest_file_ms = Some(now_ms - 60_000);
        assert!(!p.has_settled(Duration::from_secs(3600), now_ms));

        p.newest_file_ms = Some(now_ms - 7_200_000);
        assert!(p.has_settled(Duration::from_secs(3600), now_ms));
    }

    #[test]
    fn a_partition_with_no_timestamps_counts_as_settled() {
        // Failing the other way would leave a table Bergman cannot date
        // un-maintained forever, which is worse than one lost commit race.
        let p = partition(&[10; 10], 100, 0);
        assert!(p.newest_file_ms.is_none());
        assert!(p.has_settled(Duration::from_secs(3600), 1_000_000_000));
    }

    #[test]
    fn output_estimate_shrinks_with_the_delete_ratio() {
        // 1000 bytes with half the rows deleted is ~500 live bytes, one file
        // at a 512-byte target.
        let p = partition(&[500, 500], 100, 50);
        assert_eq!(p.output_file_estimate(p.all_eligible(), 512), 1);
    }

    #[test]
    fn output_estimate_of_a_large_partition_is_many_files() {
        let p = partition(&[1000; 10], 100, 0);
        assert_eq!(p.output_file_estimate(p.all_eligible(), 1000), 10);
    }

    #[test]
    fn output_estimate_is_at_least_one_file_for_a_nonempty_partition() {
        // A partition holding data always produces at least one file; a zero
        // here would make a planner believe a rewrite deletes the partition.
        let p = partition(&[10], 100, 0);
        assert_eq!(p.output_file_estimate(p.all_eligible(), 1_000_000), 1);
    }

    #[test]
    fn empty_partition_produces_no_files() {
        let p = partition(&[], 0, 0);
        assert_eq!(p.output_file_estimate(p.all_eligible(), 1000), 0);
    }

    #[test]
    fn a_partition_deleted_entirely_still_estimates_one_file() {
        // `live_fraction` is 0, so the byte estimate is 0 — but the `.max(1)`
        // keeps the answer honest as "a rewrite still produces a file", since
        // the delete ratio is an upper bound, not a certainty.
        let p = partition(&[1000], 100, 100);
        assert_eq!(p.output_file_estimate(p.all_eligible(), 512), 1);
    }

    #[test]
    fn the_estimate_is_over_the_files_a_rewrite_reads() {
        // The whole reason `Eligible` exists. This partition is 10 GB of
        // at-target files plus 40 tiny ones; a rewrite reads the tiny ones and
        // nothing else, and an estimate over the partition's totals would
        // charge the cycle's byte budget ten thousand times what the rewrite
        // costs.
        let mut sizes = vec![1000u64; 10];
        sizes.extend(std::iter::repeat_n(10u64, 40));
        let p = partition(&sizes, 1000, 0);

        let eligible = p.size_eligible(750, 1800);
        assert_eq!(eligible.count, 40);
        assert_eq!(eligible.bytes, 400);
        assert_eq!(eligible.oversized, 0);
        assert_eq!(p.output_file_estimate(eligible, 1000), 1);
    }

    #[test]
    fn a_file_at_target_is_neither_small_nor_oversized() {
        // The band between the two thresholds is the healthy one, and a file in
        // it must never be read: doing so is how a compactor rewrites a table
        // that was already fine, every cycle, forever.
        let p = partition(&[750, 1000, 1800], 100, 0);
        assert_eq!(p.size_eligible(750, 1800), Eligible::default());
        assert!(p.size_eligible(750, 1800).is_empty());
    }

    #[test]
    fn an_oversized_file_is_eligible_on_its_own() {
        // The half a small-file-only compactor forgets. Nothing else ever
        // splits it, and one unsplittable file is a task no reader can
        // parallelise.
        let p = partition(&[5000, 1000], 100, 0);
        let eligible = p.size_eligible(750, 1800);

        assert_eq!(eligible.count, 1);
        assert_eq!(eligible.oversized, 1);
        assert_eq!(eligible.bytes, 5000);
        assert_eq!(p.large_file_count(1800), 1);
    }

    #[test]
    fn a_file_is_counted_once_however_many_thresholds_it_crosses() {
        // A degenerate configuration where the bands overlap must not double
        // count: the byte figure feeds a budget, and a doubled one defers real
        // work to pay for work nobody does.
        let p = partition(&[10], 100, 0);
        let eligible = p.size_eligible(1000, 5);
        assert_eq!(eligible.count, 1);
        assert_eq!(eligible.bytes, 10);
    }
}
