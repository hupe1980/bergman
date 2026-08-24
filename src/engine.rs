//! The engine: the public entry point the CLI and every embedder drive.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use futures::stream::{self, StreamExt};
use iceberg::Catalog;
use uuid::Uuid;

use crate::catalog::{CatalogConfig, CredentialRefresh};
use crate::commit::TableCommitter;
use crate::error::{Error, Result};
use crate::health::TableHealth;
use crate::obs::{MaintenanceObserver, NoopObserver, OperationContext};
use crate::ops::store::{ObjectStore, OpendalStore};
use crate::plan::{
    MaintenancePlan, OperationKind, OperationOutcome, OperationResult, PlanContext, RunReport,
    TableOutcome, TablePlan, Uneventful, UneventfulReason, plan_table,
};
use crate::policy::{Config, Decision, Policy, TableFacts, TableRef};

/// A configured maintenance engine.
///
/// Holds connected catalogs and a compiled policy. Construction connects; every
/// method after that is a plain `async fn` on the caller's runtime.
#[derive(Debug)]
pub struct Bergman {
    policy: Policy,
    catalogs: Vec<ConnectedCatalog>,
    observer: Arc<dyn MaintenanceObserver>,
    /// When this process last scanned each table for orphans.
    ///
    /// Per-process on purpose; see [`PlanContext::last_orphan_scan`]. It is the
    /// only mutable state the engine holds, it is an optimisation rather than a
    /// correctness input, and losing it costs one extra listing.
    orphan_scans: Mutex<HashMap<TableRef, DateTime<Utc>>>,
    /// Every table location Bergman has examined, for the *cheap half* of
    /// orphan removal's nested-table check (see [`crate::ops::orphans`], check
    /// 5).
    ///
    /// Populated as tables are examined, which means it is structurally
    /// incomplete: a table no rule matches, one a rule skips, one outside this
    /// cycle's selection, or one that failed to load is simply absent — and a
    /// deliberately-excluded table is exactly the one most likely to be nested
    /// somewhere it should not be. That is why the check that has to hold is
    /// [`crate::ops::orphans::nested_table_root`], which reads the listing the
    /// scan is walking anyway and needs no ledger at all. This is a fast path
    /// that can refuse before a single object is listed, nothing more.
    locations: Mutex<HashMap<TableRef, String>>,
}

#[derive(Debug)]
struct ConnectedCatalog {
    config: CatalogConfig,
    client: Arc<dyn Catalog>,
    /// How to renew `client`'s bearer token, when it has one that expires.
    ///
    /// See [`crate::catalog::CredentialRefresh`] for why the read path needs
    /// this and the commit path does not.
    refresh: Option<Arc<dyn CredentialRefresh>>,
    /// Bergman's own commit path, for the operations `iceberg::Transaction`
    /// cannot express (see [`crate::commit`]).
    committer: Arc<dyn TableCommitter>,
}

/// Which of a catalog's tables a plan covers.
///
/// The three ways a cycle is scoped, and they are genuinely different: the
/// daemon's own interval means everything, a rule's `schedule` means that
/// rule's pattern, and an event means a named list. Naming them here keeps
/// `plan_where` one function rather than three.
enum Selection {
    /// Every table the catalogs hold.
    All,
    /// Every table one pattern matches.
    Matching(crate::policy::TableMatcher),
    /// Exactly these.
    Named(std::collections::HashSet<TableRef>),
}

impl Selection {
    fn keep(&self, table: &TableRef) -> bool {
        match self {
            Self::All => true,
            Self::Matching(matcher) => matcher.matches(table),
            Self::Named(wanted) => wanted.contains(table),
        }
    }
}

/// What one table's examination concluded.
enum Outcome {
    /// There is work to do.
    Work(Box<TablePlan>),
    /// There is not, and this is why.
    Nothing(UneventfulReason),
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
            let connection = config.connect().await?;
            let committer = config.committer().await?;
            catalogs.push(ConnectedCatalog {
                config: config.clone(),
                client: connection.client,
                refresh: connection.refresh,
                committer,
            });
        }

        Ok(Bergman {
            policy,
            catalogs,
            observer: self.observer,
            orphan_scans: Mutex::new(HashMap::new()),
            locations: Mutex::new(HashMap::new()),
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
        self.inspect_where(&Selection::All).await
    }

    /// Examine only the tables a pattern matches. Reads only.
    ///
    /// Scopes the *examination*, not the output. A table the pattern excludes
    /// is never read at all, which is what makes "what is wrong with this one
    /// namespace" cost a namespace's manifests rather than the warehouse's.
    pub async fn inspect_matching(&self, pattern: &str) -> Result<Vec<TableHealth>> {
        let matcher = crate::policy::TableMatcher::new(pattern)
            .map_err(|e| Error::config(format!("{pattern:?}: {e}")))?;
        self.inspect_where(&Selection::Matching(matcher)).await
    }

    async fn inspect_where(&self, selection: &Selection) -> Result<Vec<TableHealth>> {
        // One reading for the whole sweep, so two tables examined a minute apart
        // are still reported as of the same moment.
        let now = Utc::now();
        let mut out = Vec::new();
        let limit = self.policy.limits().max_parallel_tables.max(1);

        for catalog in &self.catalogs {
            let discovered = crate::catalog::discover(&catalog.config, &catalog.client).await?;

            // Same bounded concurrency `plan` uses: this is the same burst of
            // small metadata reads, and doing them one table at a time would
            // make inspecting a large catalog take minutes for no reason.
            let examined: Vec<(TableRef, Result<TableHealth>)> = stream::iter(discovered)
                .filter(|d| {
                    let wanted = selection.keep(&d.table);
                    async move { wanted }
                })
                .map(|d| async move {
                    let health = self
                        .examine(catalog, &d.table, now)
                        .await
                        .map(|(health, _)| health);
                    (d.table, health)
                })
                .buffer_unordered(limit)
                .collect()
                .await;

            for (table, health) in examined {
                match health {
                    Ok(health) => out.push(health),
                    // One unreadable table does not stop the sweep. A tool that
                    // aborted on the first permission error would never finish
                    // in a real deployment.
                    Err(e) => {
                        tracing::warn!(%table, error = %e, "table could not be examined")
                    }
                }
            }
        }

        // Completion order is whatever the concurrency produced; sorting makes
        // `bergman inspect` stable between runs.
        out.sort_by(|a, b| a.table.cmp(&b.table));
        Ok(out)
    }

    /// Every table the configured catalogs hold.
    ///
    /// Discovery only — no table's metadata is read. That is the difference
    /// between this and [`Bergman::inspect`], and it is the whole point:
    /// answering "which tables does my policy match" should cost a catalog
    /// listing, not a manifest walk of every table in the warehouse.
    pub async fn tables(&self) -> Result<Vec<TableRef>> {
        let mut out = Vec::new();
        for catalog in &self.catalogs {
            let discovered = crate::catalog::discover(&catalog.config, &catalog.client).await?;
            out.extend(discovered.into_iter().map(|d| d.table));
        }
        out.sort();
        Ok(out)
    }

    /// Build a maintenance plan. Reads only.
    ///
    /// `plan` and [`Bergman::run`] build the identical plan through this method;
    /// `run` then executes it. That is what makes `bergman plan` a true preview
    /// rather than a separate code path that might disagree.
    pub async fn plan(&self) -> Result<MaintenancePlan> {
        self.plan_where(&Selection::All).await
    }

    /// Plan only the tables a pattern matches.
    ///
    /// What a rule's `schedule` means: a rule asking to be evaluated every five
    /// minutes should cost five-minute evaluation of *its* tables, not of the
    /// whole catalog. Without this, one aggressive schedule would set the
    /// cadence for every table a deployment holds.
    pub async fn plan_matching(&self, pattern: &str) -> Result<MaintenancePlan> {
        let matcher = crate::policy::TableMatcher::new(pattern)
            .map_err(|e| Error::policy(format!("{pattern:?}: {e}")))?;
        self.plan_where(&Selection::Matching(matcher)).await
    }

    /// Plan only the tables named.
    ///
    /// What an event-driven cycle wants: reacting to one table's commit should
    /// not rescan a catalog of thousands. Tables that are not in the catalog,
    /// or that no rule matches, are simply absent from the plan.
    pub async fn plan_tables(&self, tables: &[TableRef]) -> Result<MaintenancePlan> {
        let wanted = Selection::Named(tables.iter().cloned().collect());
        self.plan_where(&wanted).await
    }

    async fn plan_where(&self, selection: &Selection) -> Result<MaintenancePlan> {
        let now = Utc::now();
        let mut tables = Vec::new();
        let mut uneventful = Vec::new();

        for catalog in &self.catalogs {
            let discovered = crate::catalog::discover(&catalog.config, &catalog.client).await?;

            // Bounded concurrency across tables: the analysis is a burst of
            // small metadata reads, and doing them one table at a time would
            // make a large catalog take minutes for no reason.
            let limit = self.policy.limits().max_parallel_tables.max(1);

            // One pass. Each table is discovered once and examined at most
            // once, and the same examination answers both "is there work?" and
            // "if not, why not?" — the two questions are the same metadata
            // read, and asking twice would double the cost of every healthy
            // table, which is most of them.
            let outcomes: Vec<(TableRef, Outcome)> = stream::iter(discovered)
                .filter(|d| {
                    let wanted = selection.keep(&d.table);
                    async move { wanted }
                })
                .map(|d| async move {
                    let outcome = self.examine_for_plan(catalog, &d.table, now).await;
                    (d.table, outcome)
                })
                .buffer_unordered(limit)
                .collect()
                .await;

            for (table, outcome) in outcomes {
                match outcome {
                    Outcome::Work(plan) => tables.push(*plan),
                    Outcome::Nothing(reason) => uneventful.push(Uneventful { table, reason }),
                }
            }
        }

        // Sorted rather than left in completion order, so two plans of an
        // unchanged catalog are identical and a diff between them reads as a
        // change in the world.
        tables.sort_by(|a, b| a.table.cmp(&b.table));
        uneventful.sort_by(|a, b| a.table.cmp(&b.table));

        let mut plan = MaintenancePlan {
            generated_at: now,
            tables,
            uneventful,
            deferred: Vec::new(),
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
            plan.deferred = deferred;
        }

        Ok(plan)
    }

    /// Renew every catalog client's bearer token, where it has one that expires.
    ///
    /// Reads nothing and writes nothing to any table; the only I/O is one
    /// `OAuth2` exchange per catalog configured with a `credential`. A catalog
    /// authenticating with a static `token`, or with nothing at all, is skipped.
    ///
    /// **A long-lived process should call this before each cycle**, as
    /// [`crate::sched::Daemon`] does: the catalog client exchanges its
    /// credential once at construction and never again, so a daemon holding a
    /// one-hour token keeps its cadence perfectly and stops reading a single
    /// table. A one-shot `bergman run` outlives no token.
    ///
    /// A catalog whose renewal fails is reported and skipped, not fatal —
    /// upstream fetches the replacement before discarding the old token, so the
    /// one in hand still works. Returns how many catalogs were renewed.
    pub async fn refresh_credentials(&self) -> usize {
        let mut renewed = 0;
        for catalog in &self.catalogs {
            let Some(refresh) = &catalog.refresh else {
                continue;
            };
            match refresh.refresh().await {
                Ok(()) => {
                    renewed += 1;
                    tracing::debug!(catalog = %catalog.config.name, "catalog credential renewed");
                }
                Err(e) => tracing::warn!(
                    catalog = %catalog.config.name,
                    error = %e,
                    "catalog credential could not be renewed; continuing with the token in hand"
                ),
            }
        }
        renewed
    }

    /// Execute a plan.
    ///
    /// The first method that writes anything.
    pub async fn run(&self, plan: &MaintenancePlan) -> Result<RunReport> {
        let run_id = Uuid::new_v4().to_string();
        let started_at = Utc::now();

        // A window governs when work *begins*. A cycle already under way runs
        // to completion: stopping mid-rewrite at the window's edge would leave
        // files written and uncommitted, which is worse than finishing.
        if !self.policy.window_is_open(started_at) {
            let window = self.policy.window().expect("closed implies declared");
            tracing::info!(%window, "outside the maintenance window; nothing will run");
            return Ok(RunReport {
                started_at,
                finished_at: Utc::now(),
                tables: Vec::new(),
                deferred: plan.tables.iter().map(|t| t.table.clone()).collect(),
            });
        }

        // Tables are maintained concurrently, bounded by `max_parallel_tables`.
        // Operations *within* a table stay strictly ordered — compact, then
        // rewrite manifests, then expire, then remove orphans — because that
        // ordering is a correctness property, not a preference (see
        // `OperationKind`). Two different tables share nothing but the object
        // store, so nothing is serialised between them.
        let limit = self.policy.limits().max_parallel_tables.max(1);
        let run_id = run_id.as_str();
        let mut outcomes: Vec<TableOutcome> = stream::iter(&plan.tables)
            .map(|table_plan| async move {
                let catalog = self.catalog_for(&table_plan.table)?;
                self.observer.table_started(&table_plan.table).await;

                let operations = self
                    .run_table(catalog, table_plan, run_id)
                    .await
                    .unwrap_or_else(|e| {
                        // The table could not be loaded at all, so no operation
                        // ran. Reporting it against the first operation the plan
                        // named — rather than an arbitrary one — keeps the
                        // failure attached to something the operator recognises.
                        let kind = table_plan
                            .operations
                            .first()
                            .map_or(OperationKind::ExpireSnapshots, |op| op.kind);
                        vec![OperationOutcome {
                            kind,
                            reason: "table could not be loaded".into(),
                            result: OperationResult::Failed {
                                error: e.to_string(),
                            },
                            duration: std::time::Duration::ZERO,
                        }]
                    });

                Some(TableOutcome {
                    table: table_plan.table.clone(),
                    matched_rule: table_plan.policy.matched_rule.clone(),
                    operations,
                    notes: table_plan.notes.clone(),
                })
            })
            .buffer_unordered(limit)
            // A table the plan names but no configured catalog holds. It cannot
            // be maintained and it is not a failure of this run.
            .filter_map(|outcome| async move { outcome })
            .collect()
            .await;

        // Completion order is whatever the concurrency produced; sorting makes
        // one run's report comparable with the next.
        outcomes.sort_by(|a, b| a.table.cmp(&b.table));

        Ok(RunReport {
            started_at,
            finished_at: Utc::now(),
            tables: outcomes,
            // Carried through from the plan, so a budgeted run reports what it
            // did not get to rather than looking complete.
            deferred: plan.deferred.clone(),
        })
    }

    /// Resolve one table's effective policy, for `bergman policy explain`.
    pub async fn explain(&self, table: &TableRef) -> Result<Decision> {
        let catalog = self
            .catalog_for(table)
            .ok_or_else(|| Error::config(format!("no catalog named {:?}", table.catalog)))?;

        let facts = match self.load(catalog, table).await {
            Ok(loaded) => TableFacts::from_metadata(loaded.metadata()),
            // Explaining a table that cannot be loaded is still useful: the
            // rule and defaults layers resolve without it, and saying so beats
            // refusing to answer.
            Err(_) => TableFacts::unknown(),
        };

        Ok(self.policy.decide(table, &facts))
    }

    async fn run_table(
        &self,
        catalog: &ConnectedCatalog,
        table_plan: &TablePlan,
        run_id: &str,
    ) -> Result<Vec<OperationOutcome>> {
        let table = self.load(catalog, &table_plan.table).await?;
        let mut outcomes = Vec::new();

        for operation in &table_plan.operations {
            let started = std::time::Instant::now();

            // The approval gate. An observer that says no turns the operation
            // into a refusal, which is reported and needs attention.
            let ctx = OperationContext {
                run_id,
                table: &table_plan.table,
                kind: operation.kind,
                matched_rule: &table_plan.policy.matched_rule,
                reason: &operation.reason,
            };

            if !self.observer.operation_starting(ctx).await {
                let result = OperationResult::Refused {
                    reason: "vetoed by observer".into(),
                };
                self.observer
                    .operation_finished(ctx, &result, started.elapsed())
                    .await;
                outcomes.push(OperationOutcome {
                    kind: operation.kind,
                    reason: operation.reason.clone(),
                    result,
                    duration: started.elapsed(),
                });
                continue;
            }

            // One place, because every operation passes through it and a
            // deadline enforced on three of four is not a deadline. Cancelling
            // is safe by construction: maintenance is crash-only, so a rewrite
            // dropped part-way leaves files nothing references — which is what
            // the orphan scanner reclaims — and a commit is one atomic request,
            // so it either happened or did not.
            let attempt = self.execute(catalog, &table, table_plan, ctx, &operation.targets);
            let outcome = match self.policy.limits().operation_timeout {
                Some(limit) => match tokio::time::timeout(limit, attempt).await {
                    Ok(outcome) => outcome,
                    Err(_) => Err(Error::Timeout {
                        table: table_plan.table.to_string(),
                        operation: operation.kind.as_str(),
                        after: limit,
                    }),
                },
                None => attempt.await,
            };

            let result = outcome.unwrap_or_else(|e| match e {
                Error::Refused { reason, .. } => OperationResult::Refused { reason },
                other if other.is_replan() => OperationResult::Conflicted {
                    detail: other.to_string(),
                },
                other => OperationResult::Failed {
                    error: other.to_string(),
                },
            });

            self.observer
                .operation_finished(ctx, &result, started.elapsed())
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
        ctx: OperationContext<'_>,
        #[cfg_attr(not(feature = "compaction"), allow(unused_variables))]
        targets: &[crate::health::PartitionKey],
    ) -> Result<OperationResult> {
        let now = Utc::now();
        let ident = crate::catalog::to_table_ident(&table_plan.table)?;
        let env = crate::ops::OpEnv {
            table,
            ident: &ident,
            loader: &catalog.client,
            committer: catalog.committer.as_ref(),
            observer: self.observer.as_ref(),
            ctx,
            now,
            max_deletes_per_run: self.policy.limits().max_deletes_per_run,
        };

        match ctx.kind {
            OperationKind::ExpireSnapshots => {
                crate::ops::expire::run(&env, &catalog.client, &table_plan.policy.snapshots).await
            }
            OperationKind::RemoveOrphans => {
                let store = self.object_store(catalog, table)?;
                let siblings = self.other_locations(&table_plan.table);

                // Recorded before the scan rather than after: a scan that
                // failed halfway through still listed the location, which is
                // the cost the cadence exists to bound.
                self.orphan_scans
                    .lock()
                    .expect("orphan scan ledger")
                    .insert(table_plan.table.clone(), now);

                crate::ops::orphans::run(
                    &env,
                    store.as_ref(),
                    &table_plan.policy.orphans,
                    &siblings,
                )
                .await
            }
            OperationKind::RewriteManifests => {
                crate::ops::manifests::run(&env, &table_plan.policy.manifests).await
            }
            // Exactly the partitions the plan named, so `run` rewrites what
            // `plan` displayed rather than re-deciding.
            #[cfg(feature = "compaction")]
            OperationKind::Compact => {
                crate::ops::compact::run(&env, &table_plan.policy.compaction, targets).await
            }

            // Planning stays feature-independent: it is pure, and the plan is a
            // true description of what the table needs whether or not this
            // build can act on it. Reporting the gap here — rather than
            // silently omitting the operation — keeps `plan` honest and tells
            // the operator exactly which feature to rebuild with.
            #[cfg(not(feature = "compaction"))]
            OperationKind::Compact => Ok(OperationResult::Refused {
                reason: "this build has no compaction; rebuild with --features compaction"
                    .to_string(),
            }),
        }
    }

    /// Every other table's location, for orphan removal's nested-table check.
    fn other_locations(&self, table: &TableRef) -> Vec<String> {
        self.locations
            .lock()
            .expect("location ledger")
            .iter()
            .filter(|(known, _)| *known != table)
            .map(|(_, location)| location.clone())
            .collect()
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

    /// Decide what happens to one table, in one examination.
    async fn examine_for_plan(
        &self,
        catalog: &ConnectedCatalog,
        table: &TableRef,
        now: chrono::DateTime<Utc>,
    ) -> Outcome {
        // A first pass with no table properties: a table no rule matches needs
        // no metadata read at all, which is what keeps a policy scoped to one
        // namespace cheap against a catalog holding thousands of tables.
        match self.policy.decide(table, &TableFacts::unknown()) {
            Decision::Unmatched => return Outcome::Nothing(UneventfulReason::Unmatched),
            Decision::Skip { pattern } => {
                return Outcome::Nothing(UneventfulReason::Skipped { pattern });
            }
            Decision::Maintain(_) => {}
        }

        let (health, decision) = match self.examine(catalog, table, now).await {
            Ok(examined) => examined,
            // One unreadable table does not stop the sweep, and the reason
            // reaches the plan rather than only a log line.
            Err(e) => {
                return Outcome::Nothing(UneventfulReason::Failed {
                    error: e.to_string(),
                });
            }
        };

        // Re-decided with the table's own properties, which is the layer that
        // can change a threshold — the first pass only established that some
        // rule matches.
        let Decision::Maintain(policy) = decision else {
            return Outcome::Nothing(UneventfulReason::Healthy);
        };

        let context = PlanContext {
            last_orphan_scan: self
                .orphan_scans
                .lock()
                .expect("orphan scan ledger")
                .get(table)
                .copied(),
        };

        match plan_table(&health, &policy, context, now) {
            Some(plan) => Outcome::Work(Box::new(plan)),
            // A table with nothing planned is either empty or fine, and those
            // are different situations: "no rule fired" invites a look at the
            // thresholds, "never written to" does not.
            None if health.is_empty() => Outcome::Nothing(UneventfulReason::Empty),
            None => Outcome::Nothing(UneventfulReason::Healthy),
        }
    }

    /// Load a table, resolve its policy, and measure it — all against one clock
    /// reading.
    ///
    /// `now` is the caller's, not a fresh `Utc::now()`. Snapshot ages and the
    /// settle-time check are both derived from it, and the planner then compares
    /// what it produced against *its* `now`: two readings inside one examination
    /// can disagree about whether a partition has settled, which is a decision
    /// that reads differently on every run for no reason anyone can see. The
    /// same rule operations follow (see [`crate::ops::OpEnv::now`]).
    async fn examine(
        &self,
        catalog: &ConnectedCatalog,
        table_ref: &TableRef,
        now: DateTime<Utc>,
    ) -> Result<(TableHealth, Decision)> {
        let table = self.load(catalog, table_ref).await?;
        let decision = self
            .policy
            .decide(table_ref, &TableFacts::from_metadata(table.metadata()));

        // The manifest target size decides which manifests count as
        // undersized, and it comes from the resolved policy — so a table judged
        // under two different policies gives two different answers from one
        // read.
        let target = match &decision {
            Decision::Maintain(p) => p.manifests.target_size.value,
            _ => 8 * 1024 * 1024,
        };

        let health = crate::health::analyze(table_ref, &table, target, now).await?;

        // Remembered so orphan removal can refuse a table that another table
        // lives inside (see `crate::ops::orphans`, check 5). Recorded here
        // rather than in a separate pass, because every table Bergman will ever
        // maintain comes through this function first.
        self.locations
            .lock()
            .expect("location ledger")
            .insert(table_ref.clone(), health.location.clone());

        Ok((health, decision))
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
