use btdht::InfoHash;
use metrics::{
    Counter, Gauge, Histogram, Key, KeyName, Level, Metadata, Recorder, SharedString, Unit,
};
use state_monitor::{MonitoredValue, StateMonitor};
use std::{
    fmt,
    future::Future,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};
use tokio::{
    select,
    sync::watch,
    task,
    time::{self, MissedTickBehavior},
};
use tracing::{Instrument, Span};

pub(crate) struct RepositoryMonitor {
    pub info_hash: MonitoredValue<Option<InfoHash>>,

    // Total number of index requests sent.
    pub index_requests_sent: Counter,
    // Current number of sent index request for which responses haven't been received yet.
    pub index_requests_inflight: Gauge,
    // Total number of block requests sent.
    pub block_requests_sent: Counter,
    // Current number of sent block request for which responses haven't been received yet.
    pub block_requests_inflight: Gauge,
    // Total number of received requests
    pub requests_received: Counter,
    // Current number of send requests (index + block) for which responses haven't been handled yet
    // (they might be in-flight or queued).
    pub requests_pending: Gauge,
    // Time from sending a request to receiving its response.
    pub request_latency: Histogram,
    // Total number of timeouted requests.
    pub request_timeouts: Counter,
    // Time a request spends in the send queue.
    pub request_queue_time: Histogram,

    // Total number of responses sent.
    pub responses_sent: Counter,
    // Total number of responses received.
    pub responses_received: Counter,
    // Time a response spends in the receive queue.
    pub response_queue_time: Histogram,
    // Time to handle a response.
    pub response_handle_time: Histogram,

    pub scan_job: JobMonitor,
    pub merge_job: JobMonitor,
    pub prune_job: JobMonitor,
    pub trash_job: JobMonitor,

    span: Span,
    node: StateMonitor,
}

impl RepositoryMonitor {
    pub fn new<R>(node: StateMonitor, recorder: &R) -> Self
    where
        R: Recorder + ?Sized,
    {
        let span = tracing::info_span!("repo", message = node.id().name());

        let info_hash = node.make_value("info-hash", None);

        let index_requests_sent = create_counter(recorder, "index requests sent", Unit::Count);
        let index_requests_inflight =
            create_gauge(recorder, "index requests inflight", Unit::Count);
        let block_requests_sent = create_counter(recorder, "block requests sent", Unit::Count);
        let block_requests_inflight =
            create_gauge(recorder, "block requests inflight", Unit::Count);

        let requests_received = create_counter(recorder, "requests received", Unit::Count);
        let requests_pending = create_gauge(recorder, "requests pending", Unit::Count);
        let request_latency = create_histogram(recorder, "request latency", Unit::Seconds);
        let request_timeouts = create_counter(recorder, "request timeouts", Unit::Count);
        let request_queue_time = create_histogram(recorder, "request queue time", Unit::Seconds);

        let responses_sent = create_counter(recorder, "responses sent", Unit::Count);
        let responses_received = create_counter(recorder, "responses received", Unit::Count);
        let response_queue_time = create_histogram(recorder, "response queue time", Unit::Seconds);
        let response_handle_time =
            create_histogram(recorder, "response handle time", Unit::Seconds);

        let scan_job = JobMonitor::new(&node, recorder, "scan");
        let merge_job = JobMonitor::new(&node, recorder, "merge");
        let prune_job = JobMonitor::new(&node, recorder, "prune");
        let trash_job = JobMonitor::new(&node, recorder, "trash");

        Self {
            info_hash,

            index_requests_sent,
            index_requests_inflight,
            block_requests_sent,
            block_requests_inflight,
            requests_received,
            requests_pending,
            request_latency,
            request_timeouts,
            request_queue_time,

            responses_sent,
            responses_received,
            response_queue_time,
            response_handle_time,

            scan_job,
            merge_job,
            prune_job,
            trash_job,

            span,
            node,
        }
    }

    pub fn span(&self) -> &Span {
        &self.span
    }

    pub fn node(&self) -> &StateMonitor {
        &self.node
    }

    pub fn name(&self) -> &str {
        self.node.id().name()
    }
}

pub(crate) struct JobMonitor {
    tx: watch::Sender<bool>,
    name: String,
    counter: AtomicU64,
    time: Histogram,
}

impl JobMonitor {
    fn new<R>(parent_node: &StateMonitor, recorder: &R, name: &str) -> Self
    where
        R: Recorder + ?Sized,
    {
        let time = create_histogram(recorder, format!("{name} time"), Unit::Seconds);
        let state = parent_node.make_value(format!("{name} state"), JobState::Idle);

        Self::from_parts(name, time, state)
    }

    fn from_parts(name: &str, time: Histogram, state: MonitoredValue<JobState>) -> Self {
        let (tx, mut rx) = watch::channel(false);

        task::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(1));
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

            let mut start = None;

            loop {
                select! {
                    result = rx.changed() => {
                        if result.is_err() {
                            *state.get() = JobState::Idle;
                            break;
                        }

                        if *rx.borrow() {
                            start = Some(Instant::now());
                        } else {
                            start = None;
                            *state.get() = JobState::Idle;
                        }
                    }
                    _ = interval.tick(), if start.is_some() => {
                        *state.get() = JobState::Running(start.unwrap().elapsed());
                    }
                }
            }
        });

        Self {
            tx,
            name: name.to_string(),
            counter: AtomicU64::new(0),
            time,
        }
    }

    pub(crate) async fn run<F, E>(&self, f: F) -> bool
    where
        F: Future<Output = Result<(), E>>,
        E: fmt::Debug,
    {
        if self.tx.send_replace(true) {
            panic!("job monitor can monitor at most one job at a time");
        }

        async move {
            let guard = JobGuard::start(self);
            let start = Instant::now();

            let result = f.await;
            let is_ok = result.is_ok();

            self.time.record(start.elapsed());

            guard.complete(result);

            is_ok
        }
        .instrument(tracing::info_span!(
            "job",
            message = self.name,
            id = self.counter.fetch_add(1, Ordering::Relaxed),
        ))
        .await
    }
}

pub(crate) struct JobGuard<'a> {
    monitor: &'a JobMonitor,
    span: Span,
    completed: bool,
}

impl<'a> JobGuard<'a> {
    fn start(monitor: &'a JobMonitor) -> Self {
        let span = Span::current();

        tracing::trace!(parent: &span, "Job started");

        Self {
            monitor,
            span,
            completed: false,
        }
    }

    fn complete<E: fmt::Debug>(mut self, result: Result<(), E>) {
        self.completed = true;
        tracing::trace!(parent: &self.span, ?result, "Job completed");
    }
}

impl Drop for JobGuard<'_> {
    fn drop(&mut self) {
        if !self.completed {
            tracing::trace!(parent: &self.span, "Job interrupted");
        }

        self.monitor.tx.send(false).ok();
    }
}

enum JobState {
    Idle,
    Running(Duration),
}

impl fmt::Debug for JobState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Idle => write!(f, "idle"),
            Self::Running(duration) => write!(f, "running for {:.1}s", duration.as_secs_f64()),
        }
    }
}

fn create_counter<R: Recorder + ?Sized, N: Into<SharedString>>(
    recorder: &R,
    name: N,
    unit: Unit,
) -> Counter {
    let name = KeyName::from(name);
    recorder.describe_counter(name.clone(), Some(unit), "".into());
    recorder.register_counter(
        &Key::from_name(name),
        &Metadata::new(module_path!(), Level::INFO, None),
    )
}

fn create_gauge<R: Recorder + ?Sized, N: Into<SharedString>>(
    recorder: &R,
    name: N,
    unit: Unit,
) -> Gauge {
    let name = KeyName::from(name);
    recorder.describe_gauge(name.clone(), Some(unit), "".into());
    recorder.register_gauge(
        &Key::from_name(name),
        &Metadata::new(module_path!(), Level::INFO, None),
    )
}

fn create_histogram<R: Recorder + ?Sized, N: Into<SharedString>>(
    recorder: &R,
    name: N,
    unit: Unit,
) -> Histogram {
    let name = KeyName::from(name);
    recorder.describe_histogram(name.clone(), Some(unit), "".into());
    recorder.register_histogram(
        &Key::from_name(name),
        &Metadata::new(module_path!(), Level::INFO, None),
    )
}
