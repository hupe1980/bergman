//! The one way this crate reads a table's manifest lists and manifests.
//!
//! Every walk — health analysis, reachability, snapshot production — goes
//! through upstream's own readers on [`Table`], never through a parse of bytes
//! fetched by hand. Those readers are what decrypt an encrypted list or
//! manifest, and what apply manifest inheritance, including the v3 row-lineage
//! rule that gives each live data entry its `first_row_id`. A second read path
//! would be a second implementation of both, and the two would disagree on
//! exactly the tables where getting it wrong is silent.
//!
//! The error split is preserved: a failure to *fetch* is
//! [`Error::Storage`] and retryable, while bytes that do not parse are
//! [`Error::Metadata`] and are not — a corrupt manifest does not become readable
//! on the next attempt. The split reads upstream's [`iceberg::ErrorKind`],
//! never the message text.

use iceberg::ErrorKind;
use iceberg::spec::{Manifest, ManifestFile, ManifestList, SnapshotRef};
use iceberg::table::Table;

use crate::error::{Error, Result};

/// Load one snapshot's manifest list.
pub(crate) async fn manifest_list(table: &Table, snapshot: &SnapshotRef) -> Result<ManifestList> {
    table
        .manifest_list_reader(snapshot)
        .load()
        .await
        .map_err(|e| {
            classify(
                &table.identifier().to_string(),
                "unreadable manifest list",
                e,
            )
        })
}

/// Load one manifest, with every inherited field applied.
pub(crate) async fn manifest(table: &Table, manifest_file: &ManifestFile) -> Result<Manifest> {
    table
        .manifest_reader()
        .read(manifest_file)
        .await
        .map_err(|e| classify(&table.identifier().to_string(), "unreadable manifest", e))
}

fn classify(table: &str, what: &str, e: iceberg::Error) -> Error {
    match e.kind() {
        ErrorKind::DataInvalid => Error::metadata(table, format!("{what}: {e}")),
        _ => Error::Storage(Box::new(e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bytes_that_do_not_parse_are_metadata_and_a_failed_fetch_is_storage() {
        // The retry decision rides on this split: a storage error is retried
        // on the next cycle, a metadata error is reported and left alone.
        let parse = classify(
            "db.t",
            "unreadable manifest",
            iceberg::Error::new(ErrorKind::DataInvalid, "bad avro"),
        );
        assert!(matches!(parse, Error::Metadata { .. }), "{parse:?}");

        let fetch = classify(
            "db.t",
            "unreadable manifest",
            iceberg::Error::new(ErrorKind::Unexpected, "connection reset"),
        );
        assert!(matches!(fetch, Error::Storage(_)), "{fetch:?}");
    }
}
