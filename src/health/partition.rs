//! Per-partition condition.
//!
//! Compaction works at partition granularity, so this is the level at which a
//! decision to rewrite is actually made: a table can look fine on average while
//! one partition is a thousand tiny files, and averaging over the table would
//! hide exactly the problem worth fixing.

use serde::{Deserialize, Serialize};

/// A partition's identity, rendered for display and grouping.
///
/// Partition values are typed structs in the spec. Bergman renders them to a
/// stable string rather than modelling every type, because it never needs to
/// *interpret* a partition value — only to group files by it, name it in a
/// plan, and print it. Modelling the type system to achieve that would be a
/// large amount of code standing between an operator and a legible plan.
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
    /// The key for an unpartitioned table.
    pub fn unpartitioned(spec_id: i32) -> Self {
        Self {
            spec_id,
            value: "unpartitioned".to_string(),
        }
    }
}

impl std::fmt::Display for PartitionKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.value)
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
    /// Rows named by those delete files.
    pub delete_record_count: u64,
    /// Sizes of its live data files, ascending.
    pub file_sizes: Vec<u64>,
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
            file_sizes: Vec::new(),
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

    /// How many files a rewrite of this partition would produce.
    ///
    /// Used to reject rewrites that would not actually help: rewriting five
    /// files into five files spends I/O to achieve nothing, and a planner that
    /// cannot tell will do it every cycle forever.
    pub fn output_file_estimate(&self, target_file_size: u64) -> usize {
        if target_file_size == 0 || self.data_bytes == 0 {
            return 0;
        }
        // Deletes remove rows, so the output is smaller than the input by
        // roughly the delete ratio. This is an estimate and is documented as
        // one — the exact figure is not knowable without reading the data.
        let live_fraction = (1.0 - self.delete_ratio()).max(0.0);
        let live_bytes = (self.data_bytes as f64 * live_fraction) as u64;
        live_bytes.div_ceil(target_file_size).max(1) as usize
    }
}

#[cfg(test)]
mod tests {
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

    #[test]
    fn output_estimate_shrinks_with_the_delete_ratio() {
        // 1000 bytes with half the rows deleted is ~500 live bytes, one file
        // at a 512-byte target.
        let p = partition(&[500, 500], 100, 50);
        assert_eq!(p.output_file_estimate(512), 1);
    }

    #[test]
    fn output_estimate_of_a_large_partition_is_many_files() {
        let p = partition(&[1000; 10], 100, 0);
        assert_eq!(p.output_file_estimate(1000), 10);
    }

    #[test]
    fn output_estimate_is_at_least_one_file_for_a_nonempty_partition() {
        // A partition holding data always produces at least one file; a zero
        // here would make a planner believe a rewrite deletes the partition.
        let p = partition(&[10], 100, 0);
        assert_eq!(p.output_file_estimate(1_000_000), 1);
    }

    #[test]
    fn empty_partition_produces_no_files() {
        let p = partition(&[], 0, 0);
        assert_eq!(p.output_file_estimate(1000), 0);
    }

    #[test]
    fn a_partition_deleted_entirely_still_estimates_one_file() {
        // `live_fraction` is 0, so the byte estimate is 0 — but the `.max(1)`
        // keeps the answer honest as "a rewrite still produces a file", since
        // the delete ratio is an upper bound, not a certainty.
        let p = partition(&[1000], 100, 100);
        assert_eq!(p.output_file_estimate(512), 1);
    }
}
