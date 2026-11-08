//! Which files a table still needs.
//!
//! This is the most safety-critical computation in Bergman: everything that
//! deletes anything asks this module what may be kept, and a set that is
//! missing one live file is a set that destroys a table. It is therefore
//! deliberately conservative in one direction — an error anywhere aborts the
//! whole computation rather than returning a partial set, because a partial
//! reachable set is indistinguishable from a complete one and would license
//! deleting everything it failed to read.

use std::collections::{HashMap, HashSet};

use futures::stream::{self, StreamExt, TryStreamExt};
use iceberg::spec::ManifestList;
use iceberg::table::Table;

use crate::error::{Error, Result};

/// Every file a table's retained metadata refers to.
#[derive(Debug, Default, Clone)]
pub struct ReachableSet {
    /// Data and delete files.
    pub data_files: HashSet<String>,
    /// Manifests and manifest lists.
    pub metadata_files: HashSet<String>,
    /// Statistics and partition-statistics files (Puffin).
    pub statistics_files: HashSet<String>,
    /// Previous and current `metadata.json` files.
    pub metadata_json: HashSet<String>,
}

impl ReachableSet {
    /// How many files are reachable in total.
    pub fn len(&self) -> usize {
        self.data_files.len()
            + self.metadata_files.len()
            + self.statistics_files.len()
            + self.metadata_json.len()
    }

    /// Whether nothing is reachable.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Whether a path is reachable, comparing normalized forms.
    pub fn contains(&self, path: &str) -> bool {
        let normalized = normalize(path);
        self.data_files.contains(&normalized)
            || self.metadata_files.contains(&normalized)
            || self.statistics_files.contains(&normalized)
            || self.metadata_json.contains(&normalized)
    }

    fn insert_data(&mut self, path: &str) {
        self.data_files.insert(normalize(path));
    }

    fn insert_metadata(&mut self, path: &str) {
        self.metadata_files.insert(normalize(path));
    }
}

/// Normalize a path for comparison.
///
/// The same object is spelled differently by different writers: `s3://` and
/// `s3a://` for the same bucket, a doubled slash from a naive path join, a
/// trailing slash. A reachable set that missed one spelling would mark a live
/// file as garbage, so comparison happens on a normalized form — and the
/// normalization deliberately keeps only what identifies the *object*.
pub fn normalize(path: &str) -> String {
    // Scheme aliases first, so `s3a://b/k` and `s3://b/k` compare equal.
    let path = match path.split_once("://") {
        Some((scheme, rest)) => {
            let canonical = match scheme.to_ascii_lowercase().as_str() {
                "s3" | "s3a" | "s3n" => "s3",
                "gs" | "gcs" => "gs",
                "abfs" | "abfss" => "abfss",
                "wasb" | "wasbs" => "wasbs",
                other => return collapse_slashes(&format!("{other}://{rest}")),
            };
            format!("{canonical}://{rest}")
        }
        None => path.to_string(),
    };
    collapse_slashes(&path)
}

fn collapse_slashes(path: &str) -> String {
    let (prefix, rest) = match path.split_once("://") {
        Some((scheme, rest)) => (format!("{scheme}://"), rest.to_string()),
        None => (String::new(), path.to_string()),
    };

    let mut out = String::with_capacity(rest.len());
    let mut last_was_slash = false;
    for ch in rest.chars() {
        if ch == '/' {
            if !last_was_slash {
                out.push(ch);
            }
            last_was_slash = true;
        } else {
            out.push(ch);
            last_was_slash = false;
        }
    }
    while out.ends_with('/') {
        out.pop();
    }

    format!("{prefix}{out}")
}

/// Compute every file the table's retained metadata refers to.
///
/// Walks **all** retained snapshots, not only the current one: a snapshot kept
/// for time travel needs its data files as much as the current one does.
pub async fn compute(table: &Table, concurrency: usize) -> Result<ReachableSet> {
    let metadata = table.metadata();
    let mut reachable = ReachableSet::default();
    let concurrency = concurrency.max(1);

    // Statistics and partition statistics, which expiration must also clean up
    // and the orphan scanner must never delete.
    for stats in metadata.statistics_iter() {
        reachable
            .statistics_files
            .insert(normalize(&stats.statistics_path));
    }
    for stats in metadata.partition_statistics_iter() {
        reachable
            .statistics_files
            .insert(normalize(&stats.statistics_path));
    }

    // Every `metadata.json` the log still names, plus the current one. These
    // are ordinary objects under the table location and would otherwise look
    // exactly like garbage to a scanner.
    for entry in metadata.metadata_log() {
        reachable
            .metadata_json
            .insert(normalize(&entry.metadata_file));
    }
    if let Some(current) = table.metadata_location() {
        reachable.metadata_json.insert(normalize(current));
    }

    // The pointer a Hadoop-layout table uses to find its current metadata. A
    // REST catalog keeps the pointer itself and never writes this file, but a
    // table that was migrated from a Hadoop catalog still has one — and it
    // matches nothing in the metadata, so a scanner would see it as garbage and
    // delete the only thing that says which `metadata.json` is current.
    reachable.metadata_json.insert(normalize(&format!(
        "{}/metadata/version-hint.text",
        metadata.location().trim_end_matches('/')
    )));

    // **Two flat phases rather than a nested fan-out**, and that is the whole
    // shape of this function.
    //
    // Walking manifests inside a per-snapshot task reads better and bounds
    // nothing: `s` snapshots each reading `m` manifests puts `s × m` requests
    // in flight, so the two limits multiply and the real ceiling is whatever
    // their product happens to be. Flattening makes `concurrency` mean what it
    // says — the number of object-store requests this computation has
    // outstanding — which is what an embedder sharing a connection pool needs
    // it to mean.
    //
    // Phase one: every retained snapshot's manifest list. A snapshot kept for
    // time travel needs its data files as much as the current one does, so this
    // is all of them, not just the head.
    let manifest_lists: Vec<(String, ManifestList)> = stream::iter(metadata.snapshots())
        .map(|snapshot| async move {
            let path = snapshot.manifest_list().to_string();
            let list = crate::catalog::read::manifest_list(table, snapshot).await?;
            Ok::<_, Error>((path, list))
        })
        .buffer_unordered(concurrency)
        // `try_collect` aborts on the first error, which is the property this
        // computation needs: a partial reachable set looks exactly like a
        // complete one and would license deleting everything unread.
        .try_collect()
        .await?;

    // Phase two: every manifest named by any of them, in one flat pass.
    // Deduplicated first, because snapshots share manifests — an append-only
    // table's tenth snapshot names the same nine manifests its ninth did, so a
    // naive walk re-reads the whole history once per snapshot.
    let mut manifest_paths: Vec<String> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    for (list_path, list) in &manifest_lists {
        reachable.insert_metadata(list_path);
        for entry in list.entries() {
            reachable.insert_metadata(&entry.manifest_path);
            if seen.insert(normalize(&entry.manifest_path)) {
                manifest_paths.push(entry.manifest_path.clone());
            }
        }
    }

    // Keyed by path, so the entries a manifest holds are read once however many
    // snapshots name it. `ManifestFile` is needed to load it, so the first
    // entry seen for each path is kept.
    let mut by_path: HashMap<String, &iceberg::spec::ManifestFile> = HashMap::new();
    for (_, list) in &manifest_lists {
        for entry in list.entries() {
            by_path
                .entry(normalize(&entry.manifest_path))
                .or_insert(entry);
        }
    }

    let files: Vec<Vec<String>> = stream::iter(
        manifest_paths
            .iter()
            .filter_map(|p| by_path.get(&normalize(p))),
    )
    .map(|manifest_file| {
        async move {
            let manifest = crate::catalog::read::manifest(table, manifest_file).await?;

            // Every entry, including `Deleted` ones. A deleted entry names
            // a file this snapshot removed — but an *older* retained
            // snapshot still reads it, and reachability is the union over
            // all of them. The health analyzer skips these; this must not.
            Ok::<_, Error>(
                manifest
                    .entries()
                    .iter()
                    .map(|e| e.file_path().to_string())
                    .collect::<Vec<_>>(),
            )
        }
    })
    .buffer_unordered(concurrency)
    .try_collect()
    .await?;

    for paths in files {
        for path in paths {
            reachable.insert_data(&path);
        }
    }

    Ok(reachable)
}

/// Whether `path` lies inside `location`, comparing whole path segments.
///
/// Segment-wise, because object stores match prefixes as raw strings: a table
/// at `…/events` shares a string prefix with `…/events_archive`, and a
/// containment check that missed the difference would let one table's
/// maintenance delete another's files.
pub fn is_inside(location: &str, path: &str) -> bool {
    let location = normalize(location);
    let path = normalize(path);

    match path.strip_prefix(&location) {
        // The boundary must fall on a separator. Without this, `…/events` would
        // contain `…/events_archive/data.parquet`.
        Some(rest) => rest.starts_with('/'),
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scheme_aliases_normalize_together() {
        // A table written by Spark with `s3a://` and read by Bergman as `s3://`
        // is the same object; treating them as different marks live files as
        // garbage.
        assert_eq!(normalize("s3a://b/k/f.parquet"), "s3://b/k/f.parquet");
        assert_eq!(normalize("s3n://b/k/f.parquet"), "s3://b/k/f.parquet");
        assert_eq!(normalize("gcs://b/k"), "gs://b/k");
        assert_eq!(normalize("abfs://c/k"), "abfss://c/k");
    }

    #[test]
    fn doubled_slashes_and_trailing_slashes_collapse() {
        // A naive path join produces `location + "/" + name` where location
        // already ended in a slash.
        assert_eq!(normalize("s3://b//k///f.parquet"), "s3://b/k/f.parquet");
        assert_eq!(normalize("s3://b/k/"), "s3://b/k");
    }

    #[test]
    fn an_unknown_scheme_is_left_alone_but_still_cleaned() {
        assert_eq!(normalize("hdfs://n//a/b/"), "hdfs://n/a/b");
    }

    #[test]
    fn containment_is_segment_wise() {
        // The check that keeps one table's maintenance out of another's files.
        assert!(is_inside(
            "s3://b/wh/db/events",
            "s3://b/wh/db/events/data/f.parquet"
        ));
        assert!(!is_inside(
            "s3://b/wh/db/events",
            "s3://b/wh/db/events_archive/f.parquet"
        ));
        assert!(!is_inside(
            "s3://b/wh/db/events",
            "s3://b/wh/db/other/f.parquet"
        ));
    }

    #[test]
    fn containment_survives_spelling_differences() {
        assert!(is_inside("s3a://b/wh/t", "s3://b/wh/t/data/f.parquet"));
        assert!(is_inside("s3://b/wh/t/", "s3://b/wh/t/data/f.parquet"));
    }

    #[test]
    fn a_location_does_not_contain_itself() {
        // Nothing should ever propose deleting the table directory itself, and
        // a containment check that said yes would permit exactly that.
        assert!(!is_inside("s3://b/wh/t", "s3://b/wh/t"));
    }

    #[test]
    fn reachable_lookup_normalizes_the_query() {
        let mut set = ReachableSet::default();
        set.insert_data("s3://b/wh/t/data/f.parquet");
        // Asked with the other spelling, the answer must still be "reachable".
        assert!(set.contains("s3a://b/wh/t//data/f.parquet"));
        assert!(!set.contains("s3://b/wh/t/data/other.parquet"));
    }
}
