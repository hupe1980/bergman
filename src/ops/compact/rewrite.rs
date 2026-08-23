//! Reading a file group with its deletes applied, and writing it back.
//!
//! This is the only part of Bergman that touches data, and the only part that
//! needs a query engine. The split of labour is deliberate, and each half is
//! done by whichever component does it best:
//!
//! | Stage | Who | Why |
//! |---|---|---|
//! | Scan + **positional** deletes | upstream `ArrowReader` | A positional delete becomes a Parquet `RowSelection`, so deleted rows are never decoded. That beats any join. |
//! | **Equality** deletes | `DataFusion` hash anti-join | Upstream applies these by building one predicate term *per delete row* and evaluating the tree against every batch — `data rows × delete rows`. A hash anti-join is their sum, and spills. |
//! | Sort | `DataFusion` | Spills, so no group is too large to sort. |
//! | Write | upstream `RollingFileWriter` | Produces real Iceberg `DataFile`s with full column metrics. |
//!
//! # The sequence-number rule is already applied
//!
//! An equality delete applies to a data file only when the delete's sequence
//! number is *greater* than the data file's. It would be easy to assume Bergman
//! must enforce that here, and wrong: upstream's scan already does it when it
//! builds each task's delete list (`delete_file_index.rs`). So
//! `FileScanTask::deletes` holds exactly the deletes that apply to that file,
//! and the anti-join is a plain equality join with no sequence column.
//!
//! That is also why tasks are grouped by their delete set below rather than
//! joined all at once: two data files in one group can have *different*
//! applicable delete sets, and anti-joining a file against a delete that does
//! not apply to it would remove rows that are still live.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::catalog::TableProvider;
use datafusion::catalog::streaming::StreamingTable;
use datafusion::common::Column;
use datafusion::common::JoinType;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Operator;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream;
use datafusion::prelude::{Expr, SessionContext, binary_expr};
use futures::{StreamExt, TryStreamExt};
use iceberg::arrow::ArrowReaderBuilder;
use iceberg::io::FileIO;
use iceberg::scan::FileScanTask;
use iceberg::spec::DataContentType;
use iceberg::table::Table;
use iceberg::writer::IcebergWriter;

use crate::error::{Error, Result};
use crate::policy::SortColumn;

/// What one group's rewrite produced.
pub(super) struct Written {
    pub files: Vec<iceberg::spec::DataFile>,
    /// Rows actually written, for the row-count contract.
    pub rows: u64,
}

/// Read a file group with its deletes applied and write replacements.
///
/// One plan for the whole group: each bucket contributes a scan with its own
/// anti-join, the buckets are unioned, and the sort — if any — runs once over
/// the union. Sorting per bucket instead would be subtly wrong in the most
/// common case there is: deletes arriving incrementally put a partition's files
/// in *different* buckets (a delete at sequence 6 applies to a file written at
/// 5 but not to one written at 7), so a per-bucket sort would leave the output
/// files sorted in runs rather than globally — clustered-looking metadata over
/// unclustered files.
pub(super) async fn rewrite_group(
    table: &Table,
    group: &[&FileScanTask],
    target: u64,
    sort: Option<&[SortColumn]>,
    memory_budget: u64,
) -> Result<Written> {
    let buckets = bucket_by_equality_deletes(group);
    let ctx = session(memory_budget)?;

    let mut combined: Option<datafusion::prelude::DataFrame> = None;
    for (index, (deletes, tasks)) in buckets.iter().enumerate() {
        let df = bucket_frame(&ctx, table, tasks, deletes, index).await?;
        combined = Some(match combined {
            None => df,
            Some(existing) => existing.union(df).map_err(datafusion_error)?,
        });
    }

    let Some(mut df) = combined else {
        // An empty group is not an error; there is simply nothing to write.
        return Ok(Written {
            files: Vec::new(),
            rows: 0,
        });
    };

    if let Some(columns) = sort {
        // A global sort across everything this group will write, so each output
        // file carries a tight min/max range for the sort columns. It spills,
        // so the group's size does not bound what can be sorted.
        //
        // Direction and null placement come from the resolved column rather
        // than being assumed ascending: where the sort came from the table's own
        // `sort-order`, writing the rows back in the opposite direction would
        // leave the table's metadata claiming a clustering its files do not
        // have.
        let exprs: Vec<_> = columns
            .iter()
            .map(|c| column(None, &c.name).sort(!c.descending, c.nulls_first))
            .collect();
        df = df.sort(exprs).map_err(datafusion_error)?;
    }

    let mut stream = df.execute_stream().await.map_err(datafusion_error)?;
    let mut writer = super::writer::open(table, group, target).await?;
    let mut rows = 0u64;

    while let Some(batch) = stream.next().await {
        let batch = batch.map_err(datafusion_error)?;
        rows += batch.num_rows() as u64;
        writer
            .write(batch)
            .await
            .map_err(|e| Error::Storage(Box::new(e)))?;
    }

    let files = writer
        .close()
        .await
        .map_err(|e| Error::Storage(Box::new(e)))?;

    Ok(Written { files, rows })
}

/// Group a file group's tasks by the set of equality deletes applying to them.
///
/// Keyed by the sorted delete paths, so two files with the same applicable
/// deletes are scanned together and files with different ones are kept apart.
/// In practice a partition has one such set, so this is usually one bucket.
fn bucket_by_equality_deletes<'a>(
    group: &[&'a FileScanTask],
) -> Vec<(Vec<EqualityDelete>, Vec<&'a FileScanTask>)> {
    let mut buckets: HashMap<Vec<String>, (Vec<EqualityDelete>, Vec<&FileScanTask>)> =
        HashMap::new();

    for task in group {
        let mut deletes: Vec<EqualityDelete> = task
            .deletes
            .iter()
            .filter(|d| d.file_type == DataContentType::EqualityDeletes)
            .map(|d| EqualityDelete {
                path: d.file_path.clone(),
                size: d.file_size_in_bytes,
                equality_ids: d.equality_ids.clone().unwrap_or_default(),
            })
            .collect();
        deletes.sort_by(|a, b| a.path.cmp(&b.path));

        let key: Vec<String> = deletes.iter().map(|d| d.path.clone()).collect();
        buckets
            .entry(key)
            .or_insert_with(|| (deletes, Vec::new()))
            .1
            .push(*task);
    }

    let mut out: Vec<_> = buckets.into_values().collect();
    // Deterministic order, so a rewrite of an unchanged group produces the same
    // output file layout twice.
    out.sort_by(|a, b| {
        a.0.first()
            .map(|d| &d.path)
            .cmp(&b.0.first().map(|d| &d.path))
    });
    out
}

/// One equality delete file and the columns it matches on.
#[derive(Debug, Clone)]
struct EqualityDelete {
    path: String,
    size: u64,
    equality_ids: Vec<i32>,
}

/// The name a `DataFusion` table is registered under.
const DATA_TABLE: &str = "data";
const DELETE_TABLE: &str = "eq_deletes";

/// Refer to a column by its exact name.
///
/// **Not** `datafusion::prelude::col`, which parses its argument as SQL and
/// normalizes an unquoted identifier to lower case. Iceberg column names are
/// case-sensitive and mixed case is ordinary — `customerId`, `eventTime` — so
/// `col("customerId")` looks for `customerid` and the plan fails to resolve.
/// Building the `Column` directly skips the parser and keeps the name verbatim.
fn column(table: Option<&str>, name: &str) -> Expr {
    match table {
        Some(table) => Expr::Column(Column::new(Some(table.to_string()), name)),
        None => Expr::Column(Column::new_unqualified(name)),
    }
}

/// One bucket's contribution to the plan: its scan, with its equality deletes
/// anti-joined away.
///
/// `index` disambiguates the registered table names, because every bucket in a
/// group shares one session.
async fn bucket_frame(
    ctx: &SessionContext,
    table: &Table,
    tasks: &[&FileScanTask],
    deletes: &[EqualityDelete],
    index: usize,
) -> Result<datafusion::prelude::DataFrame> {
    // Positional deletes stay on the task, where upstream turns them into a
    // Parquet row selection. Equality deletes are stripped, because we are
    // about to do them properly.
    let data_tasks: Vec<FileScanTask> = tasks
        .iter()
        .map(|task| {
            let mut task = (*task).clone();
            task.deletes
                .retain(|d| d.file_type != DataContentType::EqualityDeletes);
            task
        })
        .collect();

    let data_table = format!("{DATA_TABLE}_{index}");
    let data = read_tasks(table, data_tasks)?;
    let data_schema = data.schema();
    ctx.register_table(&data_table, streaming_table(data)?)
        .map_err(datafusion_error)?;

    let mut df = ctx.table(&data_table).await.map_err(datafusion_error)?;

    if !deletes.is_empty() {
        let delete_table = format!("{DELETE_TABLE}_{index}");
        let join_columns = equality_columns(table, deletes)?;
        let delete_tasks = delete_scan_tasks(table, deletes, &join_columns)?;
        let delete_rows = read_tasks(table, delete_tasks)?;

        ctx.register_table(&delete_table, streaming_table(delete_rows)?)
            .map_err(datafusion_error)?;
        let right = ctx.table(&delete_table).await.map_err(datafusion_error)?;

        // The whole point. `LeftAnti` keeps every data row with no match on the
        // delete side — a hash anti-join, linear in the two inputs, and
        // `DataFusion` spills its build side rather than failing.
        //
        // Null semantics: Iceberg's equality deletes match nulls, and SQL
        // equality does not, so the join uses `IS NOT DISTINCT FROM`. Plain
        // `=` would silently keep every row whose delete key contains a null.
        let on: Vec<Expr> = join_columns
            .iter()
            .map(|name| {
                binary_expr(
                    column(Some(data_table.as_str()), name),
                    Operator::IsNotDistinctFrom,
                    column(Some(delete_table.as_str()), name),
                )
            })
            .collect();

        df = df
            .join_on(right, JoinType::LeftAnti, on)
            .map_err(datafusion_error)?;
    }

    // Projected back to exactly the table's columns, in order. The anti-join
    // does not add columns, but a union needs both sides to agree on shape and
    // being explicit means a future change to the plan cannot quietly alter
    // what is written.
    let projection: Vec<Expr> = data_schema
        .fields()
        .iter()
        .map(|f| column(None, f.name()))
        .collect();
    df.select(projection).map_err(datafusion_error)
}

/// A session whose memory is bounded and whose spills go to disk.
///
/// Real accounting rather than an estimate from compressed file sizes:
/// `DataFusion` tracks what its operators actually reserve, and spills the sort
/// and the join build side when they reach it.
fn session(memory_budget: u64) -> Result<SessionContext> {
    use datafusion::execution::disk_manager::DiskManagerBuilder;
    use datafusion::execution::memory_pool::FairSpillPool;
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datafusion::prelude::SessionConfig;

    let runtime = RuntimeEnvBuilder::new()
        // Fair rather than greedy: the sort and the join build side are both
        // spillable and both want memory, and a greedy pool lets whichever
        // asks first starve the other.
        .with_memory_pool(Arc::new(FairSpillPool::new(memory_budget as usize)))
        .with_disk_manager_builder(DiskManagerBuilder::default())
        .build_arc()
        .map_err(datafusion_error)?;

    let config = SessionConfig::new()
        // One partition. Bergman's parallelism is across tables and file
        // groups, both of which are already bounded and both of which commit
        // separately; adding intra-group parallelism here would multiply the
        // memory budget by the core count without changing how much work a
        // cycle does.
        .with_target_partitions(1);

    Ok(SessionContext::new_with_config_rt(config, runtime))
}

/// Read a set of scan tasks through upstream's Arrow reader.
fn read_tasks(table: &Table, tasks: Vec<FileScanTask>) -> Result<SendableRecordBatchStream> {
    let file_io: FileIO = table.file_io().clone();
    // Borrows the caller's runtime rather than creating one — the library's
    // "bring your own runtime" contract reaches even here.
    let runtime = iceberg::Runtime::try_current()
        .map_err(|e| Error::config(format!("compaction needs a tokio runtime: {e}")))?;

    let schema: ArrowSchemaRef = Arc::new(
        iceberg::arrow::schema_to_arrow_schema(
            tasks
                .first()
                .map(|t| t.schema.as_ref())
                .ok_or_else(|| Error::config("a scan needs at least one file"))?,
        )
        .map_err(|e| Error::Storage(Box::new(e)))?,
    );

    let task_stream = futures::stream::iter(tasks.into_iter().map(Ok)).boxed();
    let reader = ArrowReaderBuilder::new(file_io, runtime).build();
    let batches = reader
        .read(task_stream)
        .map_err(|e| Error::Storage(Box::new(e)))?
        .stream()
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)));

    Ok(Box::pin(RecordBatchStreamAdapter::new(
        schema,
        Box::pin(batches),
    )))
}

/// Wrap a record-batch stream as a `DataFusion` table.
///
/// Streaming rather than buffered on both sides: the data side is arbitrarily
/// large by definition, and the delete side is what the join spills.
fn streaming_table(stream: SendableRecordBatchStream) -> Result<Arc<dyn TableProvider>> {
    let schema = stream.schema();
    let partition: Arc<dyn PartitionStream> = Arc::new(OneShotStream {
        schema: schema.clone(),
        stream: std::sync::Mutex::new(Some(stream)),
    });

    Ok(Arc::new(
        StreamingTable::try_new(schema, vec![partition]).map_err(datafusion_error)?,
    ))
}

/// A `PartitionStream` that yields its stream exactly once.
///
/// `DataFusion`'s interface allows a partition to be executed more than once;
/// this one cannot be, because it wraps a live reader that has already been
/// consumed.
///
/// A second execution therefore **fails loudly**. The tempting alternative —
/// returning an empty stream — is the worst possible behaviour here: on the
/// delete side of the anti-join, "no delete rows" means "delete nothing", so a
/// re-executed plan would resurrect every deleted row and report success. The
/// plans built above execute each side once; if that ever stops being true,
/// this turns it into a failed rewrite rather than a corrupted table.
struct OneShotStream {
    schema: ArrowSchemaRef,
    stream: std::sync::Mutex<Option<SendableRecordBatchStream>>,
}

impl std::fmt::Debug for OneShotStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("OneShotStream")
    }
}

impl PartitionStream for OneShotStream {
    fn schema(&self) -> &ArrowSchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        match self.stream.lock().expect("scan stream").take() {
            Some(stream) => stream,
            None => Box::pin(RecordBatchStreamAdapter::new(
                self.schema.clone(),
                futures::stream::once(async {
                    Err(datafusion::error::DataFusionError::Execution(
                        "a compaction scan was executed twice; its reader is already \
                         consumed and replaying it would silently drop rows"
                            .to_string(),
                    ))
                }),
            )),
        }
    }
}

/// The column names an equality delete matches on.
///
/// Every delete file in a bucket must agree on them: a bucket whose files match
/// on different columns cannot be one join. That is legal in the spec and rare
/// in practice, and it is refused rather than guessed at.
fn equality_columns(table: &Table, deletes: &[EqualityDelete]) -> Result<Vec<String>> {
    let schema = table.metadata().current_schema();

    let mut resolved: Option<Vec<String>> = None;
    for delete in deletes {
        if delete.equality_ids.is_empty() {
            return Err(Error::metadata(
                table.identifier().to_string(),
                format!(
                    "equality delete {:?} names no equality field ids; it cannot be applied",
                    delete.path
                ),
            ));
        }

        let names: Vec<String> = delete
            .equality_ids
            .iter()
            .map(|id| {
                schema
                    .field_by_id(*id)
                    .map(|f| f.name.clone())
                    .ok_or_else(|| {
                        Error::metadata(
                            table.identifier().to_string(),
                            format!(
                                "equality delete {:?} matches on field id {id}, which the \
                                 current schema does not have",
                                delete.path
                            ),
                        )
                    })
            })
            .collect::<Result<_>>()?;

        match &resolved {
            None => resolved = Some(names),
            Some(existing) if *existing == names => {}
            Some(existing) => {
                return Err(Error::Unsupported(format!(
                    "a file group carries equality deletes matching on different columns \
                     ({existing:?} and {names:?}); rewriting it would need one join per \
                     column set"
                )));
            }
        }
    }

    resolved.ok_or_else(|| Error::config("no equality deletes to resolve columns from"))
}

/// Turn equality delete files into scan tasks that read only their key columns.
fn delete_scan_tasks(
    table: &Table,
    deletes: &[EqualityDelete],
    columns: &[String],
) -> Result<Vec<FileScanTask>> {
    let schema = table.metadata().current_schema();

    // Only the equality columns are read. A delete file may carry the whole
    // row, and the join needs none of it.
    let field_ids: Vec<i32> = columns
        .iter()
        .map(|name| {
            schema.field_by_name(name).map(|f| f.id).ok_or_else(|| {
                Error::metadata(
                    table.identifier().to_string(),
                    format!("no column {name:?}"),
                )
            })
        })
        .collect::<Result<_>>()?;

    // Built rather than projected: upstream's `Schema` has no projection
    // helper, and the delete files only ever need their key columns.
    let fields: Vec<iceberg::spec::NestedFieldRef> = field_ids
        .iter()
        .map(|id| {
            schema.field_by_id(*id).cloned().ok_or_else(|| {
                Error::metadata(
                    table.identifier().to_string(),
                    format!("no field with id {id}"),
                )
            })
        })
        .collect::<Result<_>>()?;

    let projected = Arc::new(
        iceberg::spec::Schema::builder()
            .with_schema_id(schema.schema_id())
            .with_fields(fields)
            .build()
            .map_err(|e| Error::Storage(Box::new(e)))?,
    );

    Ok(deletes
        .iter()
        .map(|delete| FileScanTask {
            file_size_in_bytes: delete.size,
            start: 0,
            length: delete.size,
            record_count: None,
            data_file_path: delete.path.clone(),
            data_file_format: iceberg::spec::DataFileFormat::Parquet,
            schema: projected.clone(),
            project_field_ids: field_ids.clone(),
            predicate: None,
            // A delete file has no deletes of its own.
            deletes: Vec::new(),
            partition: None,
            partition_spec: None,
            name_mapping: None,
            case_sensitive: true,
        })
        .collect())
}

fn datafusion_error(e: impl std::fmt::Display) -> Error {
    Error::Storage(Box::new(iceberg::Error::new(
        iceberg::ErrorKind::Unexpected,
        e.to_string(),
    )))
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::RecordBatch;

    use super::*;

    fn task(path: &str, deletes: Vec<(&str, DataContentType)>) -> FileScanTask {
        FileScanTask {
            file_size_in_bytes: 100,
            start: 0,
            length: 100,
            record_count: Some(10),
            data_file_path: path.to_string(),
            data_file_format: iceberg::spec::DataFileFormat::Parquet,
            schema: iceberg::spec::Schema::builder().build().unwrap().into(),
            project_field_ids: vec![],
            predicate: None,
            deletes: deletes
                .into_iter()
                .map(|(p, t)| iceberg::scan::FileScanTaskDeleteFile {
                    file_path: p.to_string(),
                    file_size_in_bytes: 1,
                    file_type: t,
                    partition_spec_id: 0,
                    equality_ids: Some(vec![1]),
                })
                .collect(),
            partition: None,
            partition_spec: None,
            name_mapping: None,
            case_sensitive: true,
        }
    }

    #[test]
    fn files_sharing_a_delete_set_are_scanned_together() {
        let a = task(
            "a.parquet",
            vec![("d1.parquet", DataContentType::EqualityDeletes)],
        );
        let b = task(
            "b.parquet",
            vec![("d1.parquet", DataContentType::EqualityDeletes)],
        );
        let group = [&a, &b];

        let buckets = bucket_by_equality_deletes(&group);
        assert_eq!(buckets.len(), 1);
        assert_eq!(buckets[0].1.len(), 2);
    }

    #[test]
    fn files_with_different_delete_sets_are_kept_apart() {
        // The rule the module docs exist for. Upstream has already decided
        // which deletes apply to which file by sequence number, so anti-joining
        // `a` against `d2` — which does not apply to it — would remove rows
        // that are still live. Nothing would fail; the table would just lose
        // data.
        let a = task(
            "a.parquet",
            vec![("d1.parquet", DataContentType::EqualityDeletes)],
        );
        let b = task(
            "b.parquet",
            vec![("d2.parquet", DataContentType::EqualityDeletes)],
        );
        let group = [&a, &b];

        let buckets = bucket_by_equality_deletes(&group);
        assert_eq!(buckets.len(), 2);
        assert!(buckets.iter().all(|(_, tasks)| tasks.len() == 1));
    }

    #[test]
    fn positional_deletes_do_not_split_a_group_and_stay_on_the_task() {
        // A positional delete becomes a Parquet row selection inside the
        // reader, which never decodes the deleted rows. Pulling it into the
        // join would be slower and would gain nothing.
        let a = task(
            "a.parquet",
            vec![("p1.parquet", DataContentType::PositionDeletes)],
        );
        let b = task(
            "b.parquet",
            vec![("p2.parquet", DataContentType::PositionDeletes)],
        );
        let group = [&a, &b];

        let buckets = bucket_by_equality_deletes(&group);
        assert_eq!(
            buckets.len(),
            1,
            "positional deletes must not fragment a group"
        );
        assert!(buckets[0].0.is_empty(), "no equality deletes to join");
    }

    #[test]
    fn a_group_with_no_deletes_is_one_bucket_with_no_join() {
        let a = task("a.parquet", vec![]);
        let b = task("b.parquet", vec![]);
        let group = [&a, &b];

        let buckets = bucket_by_equality_deletes(&group);
        assert_eq!(buckets.len(), 1);
        assert!(buckets[0].0.is_empty());
    }

    #[test]
    fn delete_sets_are_compared_regardless_of_order() {
        let a = task(
            "a.parquet",
            vec![
                ("d1.parquet", DataContentType::EqualityDeletes),
                ("d2.parquet", DataContentType::EqualityDeletes),
            ],
        );
        let b = task(
            "b.parquet",
            vec![
                ("d2.parquet", DataContentType::EqualityDeletes),
                ("d1.parquet", DataContentType::EqualityDeletes),
            ],
        );
        let group = [&a, &b];

        assert_eq!(bucket_by_equality_deletes(&group).len(), 1);
    }

    /// The anti-join, exercised on its own against in-memory tables.
    ///
    /// The Iceberg-specific hazard here is null semantics: an equality delete
    /// naming `NULL` deletes rows whose value *is* null, while SQL `=` never
    /// matches null against null. A join written with `=` would therefore keep
    /// every row whose key is null — silently, and forever.
    async fn anti_join(data: Vec<Option<i32>>, deletes: Vec<Option<i32>>) -> Vec<Option<i32>> {
        use datafusion::arrow::array::Int32Array;
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::datasource::MemTable;

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)]));
        let table = |rows: Vec<Option<i32>>| {
            let batch =
                RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(rows))])
                    .unwrap();
            Arc::new(MemTable::try_new(schema.clone(), vec![vec![batch]]).unwrap())
        };

        let ctx = SessionContext::new();
        ctx.register_table(DATA_TABLE, table(data)).unwrap();
        ctx.register_table(DELETE_TABLE, table(deletes)).unwrap();

        let left = ctx.table(DATA_TABLE).await.unwrap();
        let right = ctx.table(DELETE_TABLE).await.unwrap();

        let on = vec![binary_expr(
            column(Some(DATA_TABLE), "id"),
            Operator::IsNotDistinctFrom,
            column(Some(DELETE_TABLE), "id"),
        )];

        let batches = left
            .join_on(right, JoinType::LeftAnti, on)
            .unwrap()
            .sort(vec![column(None, "id").sort(true, true)])
            .unwrap()
            .collect()
            .await
            .unwrap();

        batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    #[tokio::test]
    async fn the_anti_join_removes_exactly_the_deleted_keys() {
        let surviving = anti_join(
            vec![Some(1), Some(2), Some(3), Some(4)],
            vec![Some(2), Some(4)],
        )
        .await;
        assert_eq!(surviving, vec![Some(1), Some(3)]);
    }

    #[tokio::test]
    async fn a_null_delete_key_deletes_null_rows() {
        // Iceberg's equality deletes match nulls. Plain SQL `=` does not, so a
        // join written with `=` would keep this row — and keep it forever,
        // with nothing failing.
        let surviving = anti_join(vec![Some(1), None, Some(3)], vec![None]).await;
        assert_eq!(surviving, vec![Some(1), Some(3)]);
    }

    #[tokio::test]
    async fn a_null_row_survives_when_no_delete_names_null() {
        // The other direction of the same rule: null-safe equality must not
        // make nulls match *everything*.
        let surviving = anti_join(vec![Some(1), None, Some(3)], vec![Some(1)]).await;
        assert_eq!(surviving, vec![None, Some(3)]);
    }

    #[tokio::test]
    async fn deleting_nothing_keeps_everything() {
        let surviving = anti_join(vec![Some(1), Some(2)], vec![]).await;
        assert_eq!(surviving, vec![Some(1), Some(2)]);
    }

    #[tokio::test]
    async fn a_delete_key_matching_no_row_removes_nothing() {
        let surviving = anti_join(vec![Some(1), Some(2)], vec![Some(9)]).await;
        assert_eq!(surviving, vec![Some(1), Some(2)]);
    }

    #[tokio::test]
    async fn a_mixed_case_column_resolves() {
        // `datafusion::prelude::col` parses its argument as SQL and normalizes
        // an unquoted identifier to lower case. Iceberg names are
        // case-sensitive and `customerId` is ordinary, so using `col` here
        // makes the plan fail to resolve on any such table — and the tables
        // that hit it are a large fraction of real ones.
        use datafusion::arrow::array::{Int32Array, RecordBatch};
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::datasource::MemTable;

        let schema = Arc::new(Schema::new(vec![Field::new(
            "customerId",
            DataType::Int32,
            true,
        )]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1, 2]))])
                .unwrap();

        let ctx = SessionContext::new();
        ctx.register_table(
            "data_0",
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
        )
        .unwrap();

        let rows: usize = ctx
            .table("data_0")
            .await
            .unwrap()
            .select(vec![column(None, "customerId")])
            .expect("a mixed-case column must resolve")
            .sort(vec![column(None, "customerId").sort(true, true)])
            .expect("and must be sortable")
            .collect()
            .await
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum();

        assert_eq!(rows, 2);
    }

    #[tokio::test]
    async fn a_qualified_mixed_case_column_resolves() {
        // The join side of the same hazard: the anti-join qualifies both
        // columns with their table name.
        let expr = column(Some("data_0"), "customerId");
        match expr {
            Expr::Column(c) => {
                assert_eq!(c.name, "customerId", "the name must survive verbatim");
                assert_eq!(
                    c.relation.map(|r| r.to_string()),
                    Some("data_0".to_string())
                );
            }
            other => panic!("expected a column, got {other:?}"),
        }
    }
}
