use super::shared_state::{
    PARKED_SCHED_WAIT, ResolvedWorker, SharedState, WORKER_ID, WorkerIdentity,
};
use crate::telemetry::events::{RawEvent, SchedStat};
use crate::telemetry::format::WorkerId;
use crate::telemetry::task_metadata::TaskId;
use std::collections::HashMap;
use std::sync::OnceLock;
use std::sync::RwLock;
use std::sync::atomic::Ordering;
use tokio::runtime::RuntimeMetrics;

/// Per-runtime state captured at hook registration time.
///
/// All tokio-specific concepts live here rather than in `SharedState`.
/// Each `RuntimeContext` belongs to exactly one tokio runtime.
pub(crate) struct RuntimeContext {
    /// Unique index for this runtime within the telemetry session.
    pub runtime_index: u64,
    /// Optional human-readable name, set via `with_runtime_name`.
    pub runtime_name: Option<String>,
    /// Set atomically after `builder.build()` once the worker count is known.
    /// The `u64` is the pre-reserved base worker ID for this runtime:
    ///   `worker_id = base + worker_index`
    /// Storing them together ensures `resolve_worker_id` can never see metrics
    /// without a valid base (eliminating a race between the two set calls).
    pub metrics_and_base: OnceLock<(RuntimeMetrics, u64)>,
    /// Maps worker_index → global worker_id within this runtime.
    /// Populated lazily the first time each worker thread resolves its identity.
    pub worker_ids: RwLock<HashMap<usize, u64>>, // worker_index → global worker_id
}

impl RuntimeContext {
    pub(crate) fn new(runtime_index: u64, runtime_name: Option<String>) -> Self {
        Self {
            runtime_index,
            runtime_name,
            metrics_and_base: OnceLock::new(),
            worker_ids: RwLock::new(HashMap::new()),
        }
    }

    /// Build segment metadata entries for this runtime, e.g. `("runtime.main", "0,1,2,3")`.
    /// Returns `None` if unnamed or no workers resolved yet.
    pub(crate) fn metadata_entry(&self) -> Option<(String, String)> {
        let name = self.runtime_name.as_deref()?;
        let ids = self.worker_ids.read().unwrap();
        if ids.is_empty() {
            return None;
        }
        let mut sorted: Vec<u64> = ids.values().copied().collect();
        sorted.sort_unstable();
        let csv = sorted
            .iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(",");
        Some((format!("runtime.{name}"), csv))
    }

    /// Sum of global queue depth for this runtime (0 if metrics not yet set).
    pub(crate) fn global_queue_depth(&self) -> usize {
        self.metrics_and_base
            .get()
            .map(|(m, _)| m.global_queue_depth())
            .unwrap_or(0)
    }

    /// Local queue depth for a resolved worker in this runtime.
    fn local_queue_depth(&self, worker_index: usize) -> usize {
        self.metrics_and_base
            .get()
            .map(|(m, _)| m.worker_local_queue_depth(worker_index))
            .unwrap_or(0)
    }
}

/// Resolve the current thread's worker identity against a single `RuntimeContext`.
///
/// Uses TLS to cache the result — once resolved on a thread, the identity is stable.
/// Returns the `WorkerIdentity` if this thread is a tokio worker, `None` otherwise.
pub(super) fn resolve_worker_id(
    ctx: &RuntimeContext,
    shared: &SharedState,
) -> Option<WorkerIdentity> {
    WORKER_ID.with(|cell| {
        // `Some(r)` means scan was already done; check inner identity.
        if let Some(r) = cell.get() {
            return r.identity;
        }
        let Some((m, base)) = ctx.metrics_and_base.get() else {
            // Metrics/base not yet set — leave TLS as None so we retry next time.
            return None;
        };
        let tid = std::thread::current().id();
        for i in 0..m.num_workers() {
            if m.worker_thread_id(i) == Some(tid) {
                // ID is stable: base was pre-reserved for this runtime's full worker
                // count at build time, so assignment is independent of resolution order.
                let global_id = base + i as u64;
                let _ = shared
                    .next_worker_id
                    .fetch_max(global_id + 1, Ordering::Relaxed);
                let identity = WorkerIdentity {
                    worker_id: WorkerId::from(global_id as usize),
                    runtime_index: ctx.runtime_index,
                    worker_index: i,
                };
                let resolved = ResolvedWorker {
                    identity: Some(identity),
                    #[cfg(feature = "cpu-profiling")]
                    tid_registered: false,
                };
                cell.set(Some(resolved));
                // Record worker_index → global_id in this context.
                ctx.worker_ids.write().unwrap().insert(i, global_id);
                #[cfg(feature = "cpu-profiling")]
                register_tid_if_needed(cell, i, shared);
                return Some(identity);
            }
        }
        // All workers scanned — confirmed not a worker of this runtime.
        cell.set(Some(ResolvedWorker::NOT_A_WORKER));
        None
    })
}

/// Register the current thread's OS tid for CPU profiling (once per thread).
#[cfg(feature = "cpu-profiling")]
fn register_tid_if_needed(
    cell: &std::cell::Cell<Option<ResolvedWorker>>,
    worker_index: usize,
    shared: &SharedState,
) {
    if let Some(mut r) = cell.get() {
        if !r.tid_registered {
            let os_tid = crate::telemetry::events::current_tid();
            shared.thread_roles.lock().unwrap().insert(
                os_tid,
                crate::telemetry::events::ThreadRole::Worker(worker_index),
            );
            r.tid_registered = true;
            cell.set(Some(r));
        }
    }
}

/// Get the current thread's worker index (255 = unknown).
/// Used by the `Traced` waker to record which worker issued the wake.
///
/// Reads TLS only — no scan needed. Tokio worker threads populate `WORKER_ID`
/// via `on_thread_unpark` / `on_before_task_poll` before any waker can fire.
/// Non-worker threads never appear in `RuntimeMetrics.worker_thread_id()`, so
/// a scan would return 255 anyway.
pub(crate) fn current_worker_id() -> u8 {
    WORKER_ID.with(|c| {
        c.get()
            .and_then(|r| r.identity)
            .map(|id| {
                if id.worker_index <= 254 {
                    id.worker_index as u8
                } else {
                    255
                }
            })
            .unwrap_or(255)
    })
}

// ── Event construction helpers ───────────────────────────────────────────────

pub(super) fn make_poll_start(
    ctx: &RuntimeContext,
    shared: &SharedState,
    location: &'static std::panic::Location<'static>,
    task_id: TaskId,
) -> RawEvent {
    let identity = resolve_worker_id(ctx, shared);
    let worker_local_queue_depth = identity
        .map(|id| ctx.local_queue_depth(id.worker_index))
        .unwrap_or(0);
    RawEvent::PollStart {
        timestamp_nanos: crate::telemetry::events::clock_monotonic_ns(),
        worker_id: identity.map(|id| id.worker_id).unwrap_or(WorkerId::UNKNOWN),
        worker_local_queue_depth,
        task_id,
        location,
    }
}

pub(super) fn make_poll_end(ctx: &RuntimeContext, shared: &SharedState) -> RawEvent {
    let identity = resolve_worker_id(ctx, shared);
    RawEvent::PollEnd {
        timestamp_nanos: crate::telemetry::events::clock_monotonic_ns(),
        worker_id: identity.map(|id| id.worker_id).unwrap_or(WorkerId::UNKNOWN),
    }
}

pub(super) fn make_worker_park(ctx: &RuntimeContext, shared: &SharedState) -> RawEvent {
    let identity = resolve_worker_id(ctx, shared);
    let worker_local_queue_depth = identity
        .map(|id| ctx.local_queue_depth(id.worker_index))
        .unwrap_or(0);
    let cpu_time_nanos = crate::telemetry::events::thread_cpu_time_nanos();
    if let Ok(ss) = SchedStat::read_current() {
        PARKED_SCHED_WAIT.with(|c| c.set(ss.wait_time_ns));
    }
    RawEvent::WorkerPark {
        timestamp_nanos: crate::telemetry::events::clock_monotonic_ns(),
        worker_id: identity.map(|id| id.worker_id).unwrap_or(WorkerId::UNKNOWN),
        worker_local_queue_depth,
        cpu_time_nanos,
    }
}

pub(super) fn make_worker_unpark(ctx: &RuntimeContext, shared: &SharedState) -> RawEvent {
    let identity = resolve_worker_id(ctx, shared);
    let worker_local_queue_depth = identity
        .map(|id| ctx.local_queue_depth(id.worker_index))
        .unwrap_or(0);
    let cpu_time_nanos = crate::telemetry::events::thread_cpu_time_nanos();
    let sched_wait_delta_nanos = if let Ok(ss) = SchedStat::read_current() {
        let prev = PARKED_SCHED_WAIT.with(|c| c.get());
        ss.wait_time_ns.saturating_sub(prev)
    } else {
        0
    };
    RawEvent::WorkerUnpark {
        timestamp_nanos: crate::telemetry::events::clock_monotonic_ns(),
        worker_id: identity.map(|id| id.worker_id).unwrap_or(WorkerId::UNKNOWN),
        worker_local_queue_depth,
        cpu_time_nanos,
        sched_wait_delta_nanos,
    }
}
