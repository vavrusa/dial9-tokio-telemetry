use crate::telemetry::buffer;
use crate::telemetry::collector::CentralCollector;
use crate::telemetry::events::RawEvent;
#[cfg(feature = "cpu-profiling")]
use crate::telemetry::events::ThreadRole;
use crate::telemetry::format::WorkerId;
use crate::telemetry::task_metadata::TaskId;
use std::cell::Cell;
#[cfg(feature = "cpu-profiling")]
use std::collections::HashMap;
use std::sync::Arc;
#[cfg(feature = "cpu-profiling")]
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// The per-worker-thread identity, known after the first successful scan.
#[derive(Debug, Clone, Copy)]
pub(super) struct WorkerIdentity {
    /// Global WorkerId (sequential integer from the global counter).
    pub worker_id: WorkerId,
    /// Runtime index this worker belongs to.
    pub runtime_index: u64,
    /// Worker index within its runtime (for per-runtime metrics queries).
    pub worker_index: usize,
}

/// Result of scanning the current thread's worker identity.
///
/// `identity` is `Some` when this thread is a tokio worker, `None` when it
/// is confirmed not to be (e.g. a blocking-pool thread or the main thread).
#[derive(Debug, Clone, Copy)]
pub(super) struct ResolvedWorker {
    pub identity: Option<WorkerIdentity>,
    /// Whether the OS tid has been registered for CPU profiling.
    #[cfg(feature = "cpu-profiling")]
    pub tid_registered: bool,
}

impl ResolvedWorker {
    /// Sentinel used to mark a thread as confirmed non-worker after a full scan.
    /// Stored as `Some(NOT_A_WORKER)` in `WORKER_ID` so future calls skip the scan.
    pub(super) const NOT_A_WORKER: Self = Self {
        identity: None,
        #[cfg(feature = "cpu-profiling")]
        tid_registered: false,
    };
}

thread_local! {
    /// Cached worker scan result for this thread.
    /// `None` = not yet scanned; `Some(r)` = scan complete (check `r.identity`).
    pub(super) static WORKER_ID: Cell<Option<ResolvedWorker>> = const { Cell::new(None) };
    /// schedstat wait_time_ns captured at park time, used to compute delta on unpark.
    pub(super) static PARKED_SCHED_WAIT: Cell<u64> = const { Cell::new(0) };
}

/// Runtime-agnostic core recording state.
///
/// No tokio imports. All runtime-specific logic lives in `RuntimeContext`.
pub(crate) struct SharedState {
    pub(crate) enabled: AtomicBool,
    pub(crate) collector: Arc<CentralCollector>,
    /// Absolute `CLOCK_MONOTONIC` nanosecond timestamp captured at trace start.
    pub(crate) start_time_ns: u64,
    /// Global worker ID counter. Each worker gets a unique ID on first resolution.
    pub(crate) next_worker_id: AtomicU64,
    /// Counter for allocating unique runtime indices.
    pub(crate) next_runtime_index: AtomicU64,
    /// Maps OS tid → thread role so that CPU samples returned from perf can be
    /// attributed to the correct worker or blocking-pool bucket at flush time.
    #[cfg(feature = "cpu-profiling")]
    pub(crate) thread_roles: Mutex<HashMap<u32, ThreadRole>>,
    #[cfg(feature = "cpu-profiling")]
    pub(crate) sched_profiler: Mutex<Option<crate::telemetry::cpu_profile::SchedProfiler>>,
}

impl SharedState {
    pub(super) fn new(start_time_ns: u64) -> Self {
        Self {
            enabled: AtomicBool::new(false),
            collector: Arc::new(CentralCollector::new()),
            start_time_ns,
            next_worker_id: AtomicU64::new(0),
            next_runtime_index: AtomicU64::new(0),
            #[cfg(feature = "cpu-profiling")]
            thread_roles: Mutex::new(HashMap::new()),
            #[cfg(feature = "cpu-profiling")]
            sched_profiler: Mutex::new(None),
        }
    }

    /// Allocate the next unique runtime index.
    pub(crate) fn alloc_runtime_index(&self) -> u64 {
        self.next_runtime_index.fetch_add(1, Ordering::Relaxed)
    }

    fn timestamp_nanos(&self) -> u64 {
        crate::telemetry::events::clock_monotonic_ns()
    }

    /// Create a wake event. Pragmatic exception: calls `tokio::task::try_id()`
    /// because `Traced` is inherently tokio-specific.
    pub(crate) fn create_wake_event(&self, woken_task_id: TaskId, waking_worker: u8) -> RawEvent {
        let waker_task_id = tokio::task::try_id().map(TaskId::from).unwrap_or_default();
        RawEvent::WakeEvent {
            timestamp_nanos: self.timestamp_nanos(),
            waker_task_id,
            woken_task_id,
            target_worker: waking_worker,
        }
    }

    pub(crate) fn record_queue_sample(&self, global_queue_depth: usize) {
        self.record_event(RawEvent::QueueSample {
            timestamp_nanos: self.timestamp_nanos(),
            global_queue_depth,
        });
    }

    pub(crate) fn record_event(&self, event: RawEvent) {
        if !self.enabled.load(Ordering::Relaxed) {
            return;
        }
        buffer::record_event(event, &self.collector);
    }
}
