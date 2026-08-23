//! The engine: the public entry point the CLI and every embedder drive.

use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;
use futures::stream::{self, StreamExt};
use iceberg::Catalog;
use uuid::Uuid;

use crate::catalog::CatalogConfig;
use crate::commit::TableCommitter;
use crate::error::{Error, Result};
use crate::health::TableHealth;
use crate::obs::{MaintenanceObserver, NoopObserver};
use crate::ops::store::{ObjectStore, OpendalStore};
use crate::plan::{
    Executability, MaintenancePlan, OperationKind, OperationOutcome, OperationResult, RunReport,
    TableOutcome, TablePlan, Uneventful, UneventfulReason, plan_table,
};
use crate::policy::{Config, Decision, Policy, TableRef};

/// A configured maintenance engine.
///
/// Holds connected catalogs and a compiled policy. Construction connects; every
/// method after that is a plain `async fn` on the caller's runtime.
#[derive(Debug)]
pub struct Bergman {
    policy: Policy,
    catalogs: Vec<ConnectedCatalog>,
    observer: Arc<dyn MaintenanceObserver>,
}

#[derive(Debug)]
struct ConnectedCatalog {
    config: CatalogConfig,
    client: Arc<dyn Catalog>,
    /// Bergman's own commit path, for the operations `iceberg::Transaction`
    /// cannot express (see [`crate::commit`]).
    committer: Arc<dyn TableCommitter>,
}

/// Builder for [`Bergman`].
#[derive(Debug)]
pub struct BergmanBuilder {
    config: Config,
    observer: Arc<dyn MaintenanceObserver>,
}

impl BergmanBuilder {
    /// Start from a configuration.
    pub fn new(config: Config) -> Self {
        Self {
            config,
            observer: Arc::new(NoopObserver),
        }
    }

    /// Attach an observer.
    ///
    /// This is the extension point: metrics, approval gates, an event bus.
    /// See [`crate::obs::MaintenanceObserver`].
    pub fn with_observer(mut self, observer: Arc<dyn MaintenanceObserver>) -> Self {
        self.observer = observer;
        self
    }

    /// Validate the configuration and connect to every catalog.
    pub async fn build(self) -> Result<Bergman> {
        if self.config.catalogs.is_empty() {
            return Err(Error::config("no catalogs are configured"));
        }

        // Every name must be distinct: it is the first segment of every
        // `TableRef`, so duplicates would make two different tables share an
        // identity in plans, audit records and rule matching alike.
        let mut seen = std::collections::HashSet::new();
        for catalog in &self.config.catalogs {
            catalog.validate()?;
            if !seen.insert(&catalog.name) {
                return Err(Error::config(format!(
                    "catalog name {:?} is used more than once",
                    catalog.name
                )));
            }
        }

        let policy = Policy::compile(&self.config)?;

        let mut catalogs = Vec::with_capacity(self.config.catalogs.len());
        for config in &self.config.catalogs {
            let client = config.connect().await?;
            let committer = config.committer().await?;
            catalogs.push(ConnectedCatalog {
                config: config.clone(),
                client,
                committer,
            });
        }

        Ok(Bergman {
            policy,
            catalogs,
            observer: self.observer,
        })
    }
}

impl Bergman {
    /// Connect with the default observer.
    pub async fn new(config: Config) -> Result<Self> {
        BergmanBuilder::new(config).build().await
    }

    /// Start a builder.
    pub fn builder(config: Config) -> BergmanBuilder {
        BergmanBuilder::new(config)
    }

    /// The compiled policy.
    pub fn policy(&self) -> &Policy {
        &self.policy
    }

    /// Examine every discovered table. Reads only.
    ///
    /// Useful before Bergman is trusted to write anything: it answers "what is
    /// wrong with my tables" without changing one.
    pub async fn inspect(&self) -> Result<Vec<TableHealth>> {
        let mut out = Vec::new();
        for catalog in &self.catalogs {
            for discovered in crate::catalog::discover(&catalog.config, &catalog.client).await? {
                match self.examine(catalog, &discovered.table).await {
                    Ok((health, _)) => out.push(health),
                    Err(e) => {
                        // One unreadable table does not stop the sweep. A tool
                        // that aborted on the first permission error would
                        // never finish in a real deployment.
                        tracing::warn!(table = %discovered.table, error = %e, "table could not be examined");
                    }
                }
            }
        }
        Ok(out)
    }

    /// Build a maintenance plan. Reads only.
    ///
    /// `plan` and [`Bergman::run`] build the identical plan through this method;
    /// `run` then executes it. That is what makes `bergman plan` a true preview
    /// rather than a separate code path that might disagree.
    pub async fn plan(&self) -> Result<MaintenancePlan> {
        let now = Utc::now();
        let mut tables = Vec::new();
        let mut uneventful = Vec::new();

        for catalog in &self.catalogs {
            let discovered = crate::catalog::discover(&catalog.config, &catalog.client).await?;

            // Bounded concurrency across tables: the analysis is a burst of
            // small metadata reads, and doing them one table at a time would
            // make a large catalog take minutes for no reason.
            let limit = self.policy.limits().max_parallel_tables.max(1);
            let results: Vec<(TableRef, Result<Option<TablePlan>>)> = stream::iter(discovered)
                .map(|d| async move {
                    let outcome = self.plan_one(catalog, &d.table, now).await;
                    (d.table, outcome)
                })
                .buffer_unordered(limit)
                .collect()
                .await;

            for (table, result) in results {
                match result {
                    Ok(Some(plan)) => tables.push(plan),
                    Ok(None) => {} // Already recorded by `plan_one`.
                    Err(e) => uneventful.push(Uneventful {
                        table,
                        reason: UneventfulReason::Failed {
                            error: e.to_string(),
                        },
                    }),
                }
            }
        }

        // Re-derive the uneventful entries deterministically rather than
        // pushing them from inside the concurrent stream, so plan output is
        // stable between runs and two plans can be diffed meaningfully.
        for catalog in &self.catalogs {
            for discovered in crate::catalog::discover(&catalog.config, &catalog.client).await? {
                if tables.iter().any(|t| t.table == discovered.table)
                    || uneventful.iter().any(|u| u.table == discovered.table)
                {
                    continue;
                }
                let reason = self.uneventful_reason(catalog, &discovered.table).await;
                uneventful.push(Uneventful {
                    table: discovered.table,
                    reason,
                });
            }
        }

        tables.sort_by(|a, b| a.table.cmp(&b.table));
        uneventful.sort_by(|a, b| a.table.cmp(&b.table));

        let mut plan = MaintenancePlan {
            generated_at: now,
            tables,
            uneventful,
        };

        if let Some(budget) = self.policy.limits().max_rewrite_bytes_per_run {
            let deferred = plan.apply_budget(budget);
            if !deferred.is_empty() {
                // Never silent: a truncated plan that said nothing would read
                // as a complete one.
                tracing::info!(
                    deferred = deferred.len(),
                    budget,
                    "byte budget exhausted; some tables deferred to the next cycle"
                );
            }
        }

        Ok(plan)
    }

    /// Execute a plan.
    ///
    /// The first method that writes anything.
    pub async fn run(&self, plan: &MaintenancePlan) -> Result<RunReport> {
        let run_id = Uuid::new_v4().to_string();
        let started_at = Utc::now();
        let mut outcomes = Vec::new();

        for table_plan in &plan.tables {
            let Some(catalog) = self.catalog_for(&table_plan.table) else {
                continue;
            };

            self.observer.table_started(&table_plan.table).await;

            let operations = self
                .run_table(catalog, table_plan, &run_id)
                .await
                .unwrap_or_else(|e| {
                    vec![OperationOutcome {
                        kind: OperationKind::ExpireSnapshots,
                        reason: "table could not be loaded".into(),
                        result: OperationResult::Failed {
                            error: e.to_string(),
                        },
                        duration: std::time::Duration::ZERO,
                    }]
                });

            outcomes.push(TableOutcome {
                table: table_plan.table.clone(),
                matched_rule: table_plan.policy.matched_rule.clone(),
                operations,
            });
        }

        Ok(RunReport {
            started_at,
            finished_at: Utc::now(),
            tables: outcomes,
            deferred: Vec::new(),
        })
    }

    /// Resolve one table's effective policy, for `bergman policy explain`.
    pub async fn explain(&self, table: &TableRef) -> Result<Decision> {
        let catalog = self
            .catalog_for(table)
            .ok_or_else(|| Error::config(format!("no catalog named {:?}", table.catalog)))?;

        let properties = match self.load(catalog, table).await {
            Ok(loaded) => loaded.metadata().properties().clone(),
            // Explaining a table that cannot be loaded is still useful: the
            // rule and defaults layers resolve without it, and saying so beats
            // refusing to answer.
            Err(_) => HashMap::new(),
        };

        Ok(self.policy.decide(table, &properties))
    }

    async fn run_table(
        &self,
        catalog: &ConnectedCatalog,
        table_plan: &TablePlan,
        _run_id: &str,
    ) -> Result<Vec<OperationOutcome>> {
        let table = self.load(catalog, &table_plan.table).await?;
        let mut outcomes = Vec::new();

        for operation in &table_plan.operations {
            let started = std::time::Instant::now();

            // A blocked operation is reported, never attempted. It stays in the
            // report because the table's need is real even when Bergman cannot
            // meet it.
            if let Executability::Blocked { reason } = &operation.executability {
                outcomes.push(OperationOutcome {
                    kind: operation.kind,
                    reason: operation.reason.clone(),
                    result: OperationResult::Blocked {
                        reason: reason.clone(),
                    },
                    duration: started.elapsed(),
                });
                continue;
            }

            // The approval gate. An observer that says no turns the operation
            // into a refusal, which is reported and needs attention.
            if !self
                .observer
                .operation_starting(&table_plan.table, operation.kind)
                .await
            {
                let result = OperationResult::Refused {
                    reason: "vetoed by observer".into(),
                };
                self.observer
                    .operation_finished(&table_plan.table, operation.kind, &result)
                    .await;
                outcomes.push(OperationOutcome {
                    kind: operation.kind,
                    reason: operation.reason.clone(),
                    result,
                    duration: started.elapsed(),
                });
                continue;
            }

            let result = self
                .execute(
                    catalog,
                    &table,
                    table_plan,
                    operation.kind,
                    &operation.targets,
                )
                .await
                .unwrap_or_else(|e| match e {
                    Error::Refused { reason, .. } => OperationResult::Refused { reason },
                    other if other.is_replan() => OperationResult::Conflicted {
                        detail: other.to_string(),
                    },
                    other => OperationResult::Failed {
                        error: other.to_string(),
                    },
                });

            self.observer
                .operation_finished(&table_plan.table, operation.kind, &result)
                .await;

            outcomes.push(OperationOutcome {
                kind: operation.kind,
                reason: operation.reason.clone(),
                result,
                duration: started.elapsed(),
            });
        }

        Ok(outcomes)
    }

    async fn execute(
        &self,
        catalog: &ConnectedCatalog,
        table: &iceberg::table::Table,
        table_plan: &TablePlan,
        kind: OperationKind,
        targets: &[crate::health::PartitionKey],
    ) -> Result<OperationResult> {
        let now = Utc::now();
        match kind {
            OperationKind::ExpireSnapshots => {
                crate::ops::expire::run(
                    &table_plan.table,
                    table,
                    &catalog.client,
                    &table_plan.policy.snapshots,
                    self.observer.as_ref(),
                    now,
                )
                .await
            }
            OperationKind::RemoveOrphans => {
                let store = self.object_store(catalog, table)?;
                crate::ops::orphans::run(
                    &table_plan.table,
                    table,
                    &catalog.client,
                    store.as_ref(),
                    &table_plan.policy.orphans,
                    self.observer.as_ref(),
                    now,
                )
                .await
            }
            OperationKind::RewriteManifests => {
                crate::ops::manifests::run(
                    &table_plan.table,
                    table,
                    &crate::catalog::to_table_ident(&table_plan.table)?,
                    catalog.committer.as_ref(),
                    &table_plan.policy.manifests,
                )
                .await
            }
            OperationKind::Compact => {
                crate::ops::compact::run(
                    &table_plan.table,
                    table,
                    &crate::catalog::to_table_ident(&table_plan.table)?,
                    catalog.committer.as_ref(),
                    &table_plan.policy.compaction,
                    // Exactly the partitions the plan named, so `run` rewrites
                    // what `plan` displayed rather than re-deciding.
                    targets,
                    self.observer.as_ref(),
                )
                .await
            }
        }
    }

    fn object_store(
        &self,
        catalog: &ConnectedCatalog,
        table: &iceberg::table::Table,
    ) -> Result<Box<dyn ObjectStore>> {
        Ok(Box::new(OpendalStore::for_location(
            table.metadata().location(),
            &catalog.config.properties,
        )?))
    }

    async fn plan_one(
        &self,
        catalog: &ConnectedCatalog,
        table: &TableRef,
        now: chrono::DateTime<Utc>,
    ) -> Result<Option<TablePlan>> {
        let Decision::Maintain(_) = self.policy.decide(table, &HashMap::new()) else {
            // Unmatched or skipped: no need to load the table at all, which is
            // what keeps a policy scoped to one namespace cheap against a
            // catalog holding thousands of tables.
            return Ok(None);
        };

        let (health, effective) = self.examine(catalog, table).await?;
        let Decision::Maintain(policy) = effective else {
            return Ok(None);
        };

        Ok(plan_table(&health, &policy, now))
    }

    async fn examine(
        &self,
        catalog: &ConnectedCatalog,
        table_ref: &TableRef,
    ) -> Result<(TableHealth, Decision)> {
        let table = self.load(catalog, table_ref).await?;
        let decision = self.policy.decide(table_ref, table.metadata().properties());

        // The manifest target size decides which manifests count as
        // undersized, and it comes from the resolved policy — so a table judged
        // under two different policies gives two different answers from one
        // read.
        let target = match &decision {
            Decision::Maintain(p) => p.manifests.target_size.value,
            _ => 8 * 1024 * 1024,
        };

        let health = crate::health::analyze(table_ref, &table, target, Utc::now()).await?;
        Ok((health, decision))
    }

    async fn uneventful_reason(
        &self,
        catalog: &ConnectedCatalog,
        table: &TableRef,
    ) -> UneventfulReason {
        match self.policy.decide(table, &HashMap::new()) {
            Decision::Unmatched => return UneventfulReason::Unmatched,
            Decision::Skip { pattern } => return UneventfulReason::Skipped { pattern },
            Decision::Maintain(_) => {}
        }

        match self.examine(catalog, table).await {
            Ok((health, _)) if health.is_empty() => UneventfulReason::Empty,
            Ok(_) => UneventfulReason::Healthy,
            Err(e) => UneventfulReason::Failed {
                error: e.to_string(),
            },
        }
    }

    async fn load(
        &self,
        catalog: &ConnectedCatalog,
        table: &TableRef,
    ) -> Result<iceberg::table::Table> {
        let ident = crate::catalog::to_table_ident(table)?;
        catalog.client.load_table(&ident).await.map_err(Error::from)
    }

    fn catalog_for(&self, table: &TableRef) -> Option<&ConnectedCatalog> {
        self.catalogs
            .iter()
            .find(|c| c.config.name == table.catalog)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_configuration_with_no_catalogs_is_refused() {
        let err = Bergman::new(Config::default()).await.unwrap_err();
        assert!(err.to_string().contains("no catalogs"));
    }

    #[tokio::test]
    async fn duplicate_catalog_names_are_refused() {
        // Two catalogs with one name would make two different tables share an
        // identity in plans, audit records and rule matching alike.
        let config = Config::from_toml(
            r#"
            [[catalogs]]
            name = "prod"
            uri = "http://localhost:8181"
            warehouse = "file:///tmp/wh"

            [[catalogs]]
            name = "prod"
            uri = "http://localhost:8182"
            warehouse = "file:///tmp/wh2"
            "#,
        )
        .unwrap();

        let err = Bergman::new(config).await.unwrap_err();
        assert!(err.to_string().contains("more than once"), "got: {err}");
    }
}
