use crate::TaskData;
use crate::WorkerResolver;
use crate::execution_plans::{NetworkBroadcastExec, NetworkCoalesceExec, NetworkShuffleExec};
use crate::protobuf::StageKey;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::metrics::MetricValue;
use futures::Stream;
use moka::future::Cache;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::OnceCell;
use tokio_stream::StreamExt as TokioStreamExt;
use tokio_stream::wrappers::IntervalStream;
use tonic::{Request, Response, Status};

use super::generated::observability::{
    GetTaskMetricsResponse, GetWorkerSnapshotRequest, GetWorkerSnapshotResponse,
    GetWorkersRequest, GetWorkersResponse, TaskMetricsSummary, WatchWorkerSnapshotsRequest,
    WorkerSnapshot, WorkerSnapshotEvent,
};
use super::{
    ObservabilityService,
    generated::observability::{GetTaskMetricsRequest, PingRequest, PingResponse},
};

const DEFAULT_STREAM_INTERVAL_MS: u32 = 500;
const MIN_STREAM_INTERVAL_MS: u32 = 100;
const MAX_STREAM_INTERVAL_MS: u32 = 5_000;

pub struct ObservabilityServiceImpl {
    task_data_entries: Arc<Cache<StageKey, Arc<OnceCell<TaskData>>>>,
    worker_resolver: Arc<dyn WorkerResolver + Send + Sync>,
    worker_url: String,
}

impl ObservabilityServiceImpl {
    pub fn new(
        task_data_entries: Arc<Cache<StageKey, Arc<OnceCell<TaskData>>>>,
        worker_resolver: Arc<dyn WorkerResolver + Send + Sync>,
    ) -> Self {
        Self {
            task_data_entries,
            worker_resolver,
            worker_url: String::new(),
        }
    }

    pub fn with_worker_url(
        task_data_entries: Arc<Cache<StageKey, Arc<OnceCell<TaskData>>>>,
        worker_resolver: Arc<dyn WorkerResolver + Send + Sync>,
        worker_url: impl Into<String>,
    ) -> Self {
        Self {
            task_data_entries,
            worker_resolver,
            worker_url: worker_url.into(),
        }
    }
}

#[tonic::async_trait]
impl ObservabilityService for ObservabilityServiceImpl {
    type WatchWorkerSnapshotsStream =
        std::pin::Pin<Box<dyn Stream<Item = Result<WorkerSnapshotEvent, Status>> + Send + 'static>>;

    async fn ping(&self, _request: Request<PingRequest>) -> Result<Response<PingResponse>, Status> {
        Ok(Response::new(PingResponse { value: 1 }))
    }

    async fn get_task_metrics(
        &self,
        _request: Request<GetTaskMetricsRequest>,
    ) -> Result<Response<GetTaskMetricsResponse>, Status> {
        let mut task_summaries = Vec::new();

        for entry in self.task_data_entries.iter() {
            let (stage_key, task_data_cell) = entry;

            if let Some(task_data) = task_data_cell.get() {
                task_summaries.push(aggregate_task_metrics(&task_data.plan, stage_key.clone()));
            }
        }

        Ok(Response::new(GetTaskMetricsResponse { task_summaries }))
    }

    async fn get_workers(
        &self,
        _request: Request<GetWorkersRequest>,
    ) -> Result<Response<GetWorkersResponse>, Status> {
        let urls = self
            .worker_resolver
            .get_urls()
            .map_err(|e| Status::internal(format!("Failed to resolve workers: {e}")))?;

        let workers = urls
            .into_iter()
            .map(|url| super::WorkerInfo {
                url: url.to_string(),
            })
            .collect();

        Ok(Response::new(GetWorkersResponse { workers }))
    }

    async fn get_worker_snapshot(
        &self,
        _request: Request<GetWorkerSnapshotRequest>,
    ) -> Result<Response<GetWorkerSnapshotResponse>, Status> {
        let snapshot = build_worker_snapshot(&self.task_data_entries, &self.worker_url);
        Ok(Response::new(GetWorkerSnapshotResponse {
            snapshot: Some(snapshot),
        }))
    }

    async fn watch_worker_snapshots(
        &self,
        request: Request<WatchWorkerSnapshotsRequest>,
    ) -> Result<Response<Self::WatchWorkerSnapshotsStream>, Status> {
        let interval_ms = clamp_stream_interval_ms(request.into_inner().interval_ms);

        let mut ticker = tokio::time::interval(Duration::from_millis(interval_ms as u64));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let task_data_entries = Arc::clone(&self.task_data_entries);
        let worker_url = self.worker_url.clone();

        let mut sequence = 0u64;
        let stream = IntervalStream::new(ticker).map(move |_| {
            let snapshot = build_worker_snapshot(&task_data_entries, &worker_url);
            let event = WorkerSnapshotEvent {
                sequence,
                heartbeat: snapshot.active_tasks == 0,
                snapshot: Some(snapshot),
            };
            sequence = sequence.saturating_add(1);
            Ok(event)
        });

        Ok(Response::new(Box::pin(stream)))
    }
}

/// Converts internal StageKey to observability proto StageKey.
fn convert_stage_key(key: &StageKey) -> super::StageKey {
    super::StageKey {
        query_id: key.query_id.to_vec(),
        stage_id: key.stage_id,
        task_number: key.task_number,
    }
}

#[derive(Debug, Clone, Default)]
struct TaskLoad {
    output_rows: u64,
    elapsed_compute_ns: u64,
    current_memory_usage_bytes: u64,
    build_mem_used_bytes: u64,
    peak_mem_used_bytes: u64,
    spill_count: u64,
}

/// Aggregates operator metrics across a single task and returns a TaskMetricsSummary.
/// Walks the plan tree recursively, summing metrics across all operators.
/// Output rows are taken from the root node only. Stops at network boundaries
/// since those represent child stages running on other workers.
fn aggregate_task_metrics(
    plan: &Arc<dyn ExecutionPlan>,
    stage_key: Arc<StageKey>,
) -> TaskMetricsSummary {
    let load = aggregate_task_load(plan);

    TaskMetricsSummary {
        stage_key: Some(convert_stage_key(&stage_key)),
        output_rows: load.output_rows,
        elapsed_compute: load.elapsed_compute_ns,
        current_memory_usage: load
            .current_memory_usage_bytes
            .max(load.build_mem_used_bytes),
        spill_count: load.spill_count,
    }
}

fn aggregate_task_load(plan: &Arc<dyn ExecutionPlan>) -> TaskLoad {
    let mut load = TaskLoad::default();

    // Extract output_rows from root node only
    if let Some(metrics) = plan.metrics() {
        for metric in metrics.iter() {
            if let MetricValue::OutputRows(c) = metric.value() {
                load.output_rows = load.output_rows.saturating_add(c.value() as u64);
            }
        }
    }

    // Sum remaining metrics across all nodes.
    accumulate_metrics(plan, &mut load);

    load
}

fn build_worker_snapshot(
    task_data_entries: &Arc<Cache<StageKey, Arc<OnceCell<TaskData>>>>,
    worker_url: &str,
) -> WorkerSnapshot {
    let mut task_loads = Vec::new();

    for entry in task_data_entries.iter() {
        let (stage_key, task_data_cell) = entry;
        if let Some(task_data) = task_data_cell.get() {
            task_loads.push((stage_key.query_id.to_vec(), aggregate_task_load(&task_data.plan)));
        }
    }

    build_worker_snapshot_from_task_loads(worker_url, unix_timestamp_nanos(), task_loads)
}

fn build_worker_snapshot_from_task_loads(
    worker_url: &str,
    snapshot_unix_nanos: u64,
    task_loads: impl IntoIterator<Item = (Vec<u8>, TaskLoad)>,
) -> WorkerSnapshot {
    let mut active_query_ids = HashSet::new();
    let mut active_tasks = 0u32;

    let mut elapsed_compute_ns_total = 0u64;
    let mut build_mem_used_bytes = 0u64;
    let mut peak_mem_used_bytes = 0u64;
    let mut spill_count = 0u64;

    for (query_id, task_load) in task_loads {
        active_tasks = active_tasks.saturating_add(1);
        active_query_ids.insert(query_id);

        elapsed_compute_ns_total =
            elapsed_compute_ns_total.saturating_add(task_load.elapsed_compute_ns);

        let effective_build_mem = task_load
            .build_mem_used_bytes
            .max(task_load.current_memory_usage_bytes);
        let effective_peak_mem = task_load
            .peak_mem_used_bytes
            .max(task_load.current_memory_usage_bytes)
            .max(effective_build_mem);

        build_mem_used_bytes = build_mem_used_bytes.saturating_add(effective_build_mem);
        peak_mem_used_bytes = peak_mem_used_bytes.saturating_add(effective_peak_mem);
        spill_count = spill_count.saturating_add(task_load.spill_count);
    }

    let mut active_query_ids: Vec<Vec<u8>> = active_query_ids.into_iter().collect();
    active_query_ids.sort();

    WorkerSnapshot {
        snapshot_unix_nanos,
        worker_url: worker_url.to_string(),
        active_query_ids,
        active_tasks,
        elapsed_compute_ns_total,
        build_mem_used_bytes,
        peak_mem_used_bytes,
        spill_count,
    }
}

/// Recursively walks the plan tree, accumulating metrics from each node.
/// Stops at network boundary nodes since those represent child stages on other workers.
fn accumulate_metrics(plan: &Arc<dyn ExecutionPlan>, load: &mut TaskLoad) {
    if let Some(metrics) = plan.metrics() {
        for metric in metrics.iter() {
            match metric.value() {
                MetricValue::ElapsedCompute(t) => {
                    load.elapsed_compute_ns = load.elapsed_compute_ns.saturating_add(t.value() as u64);
                }
                MetricValue::CurrentMemoryUsage(g) => {
                    load.current_memory_usage_bytes =
                        load.current_memory_usage_bytes.saturating_add(g.value() as u64);
                }
                MetricValue::Gauge { name, gauge } => {
                    if name.as_ref() == "build_mem_used" || name.as_ref() == "stream_memory_usage" {
                        load.build_mem_used_bytes =
                            load.build_mem_used_bytes.saturating_add(gauge.value() as u64);
                    } else if name.as_ref() == "peak_mem_used" || name.as_ref() == "max_mem_used" {
                        load.peak_mem_used_bytes =
                            load.peak_mem_used_bytes.saturating_add(gauge.value() as u64);
                    }
                }
                MetricValue::SpillCount(c) => {
                    load.spill_count = load.spill_count.saturating_add(c.value() as u64);
                }
                _ => {}
            }
        }
    }

    for child in plan.children() {
        if child
            .as_any()
            .downcast_ref::<NetworkShuffleExec>()
            .is_some()
            || child
                .as_any()
                .downcast_ref::<NetworkCoalesceExec>()
                .is_some()
            || child
                .as_any()
                .downcast_ref::<NetworkBroadcastExec>()
                .is_some()
        {
            continue;
        }
        accumulate_metrics(child, load);
    }
}

fn clamp_stream_interval_ms(requested_ms: u32) -> u32 {
    if requested_ms == 0 {
        DEFAULT_STREAM_INTERVAL_MS
    } else {
        requested_ms.clamp(MIN_STREAM_INTERVAL_MS, MAX_STREAM_INTERVAL_MS)
    }
}

fn unix_timestamp_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| u64::try_from(duration.as_nanos()).ok())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::WorkerResolver;
    use datafusion::common::DataFusionError;
    use url::Url;

    struct TestWorkerResolver {
        urls: Vec<Url>,
    }

    impl WorkerResolver for TestWorkerResolver {
        fn get_urls(&self) -> Result<Vec<Url>, DataFusionError> {
            Ok(self.urls.clone())
        }
    }

    struct FailingWorkerResolver;

    impl WorkerResolver for FailingWorkerResolver {
        fn get_urls(&self) -> Result<Vec<Url>, DataFusionError> {
            Err(DataFusionError::Internal(
                "resolver unavailable".to_string(),
            ))
        }
    }

    #[tokio::test]
    async fn test_get_workers_returns_resolver_urls() {
        let urls = vec![
            Url::parse("http://worker1:8080").unwrap(),
            Url::parse("http://worker2:8080").unwrap(),
            Url::parse("http://worker3:8080").unwrap(),
        ];

        let cache = Arc::new(Cache::builder().build());
        let resolver = Arc::new(TestWorkerResolver { urls: urls.clone() });
        let service = ObservabilityServiceImpl::new(cache, resolver);

        let response = service
            .get_workers(Request::new(GetWorkersRequest {}))
            .await
            .unwrap();

        let returned_urls: Vec<String> = response
            .into_inner()
            .workers
            .into_iter()
            .map(|w| w.url)
            .collect();

        assert_eq!(
            returned_urls,
            vec![
                "http://worker1:8080/",
                "http://worker2:8080/",
                "http://worker3:8080/",
            ]
        );
    }

    #[tokio::test]
    async fn test_get_workers_empty_resolver() {
        let cache = Arc::new(Cache::builder().build());
        let resolver = Arc::new(TestWorkerResolver { urls: vec![] });
        let service = ObservabilityServiceImpl::new(cache, resolver);

        let response = service
            .get_workers(Request::new(GetWorkersRequest {}))
            .await
            .unwrap();

        assert!(response.into_inner().workers.is_empty());
    }

    #[tokio::test]
    async fn test_get_workers_failing_resolver_returns_error() {
        let cache = Arc::new(Cache::builder().build());
        let resolver = Arc::new(FailingWorkerResolver);
        let service = ObservabilityServiceImpl::new(cache, resolver);

        let result = service
            .get_workers(Request::new(GetWorkersRequest {}))
            .await;

        assert!(result.is_err());
        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::Internal);
    }

    #[test]
    fn test_build_worker_snapshot_from_task_loads_sums_metrics_and_deduplicates_queries() {
        let q1 = vec![0, 1, 2, 3];
        let q2 = vec![7, 8, 9, 10];

        let snapshot = build_worker_snapshot_from_task_loads(
            "http://worker-a:8080",
            42,
            vec![
                (
                    q1.clone(),
                    TaskLoad {
                        elapsed_compute_ns: 10,
                        current_memory_usage_bytes: 3,
                        build_mem_used_bytes: 5,
                        peak_mem_used_bytes: 9,
                        spill_count: 1,
                        output_rows: 11,
                    },
                ),
                (
                    q1.clone(),
                    TaskLoad {
                        elapsed_compute_ns: 20,
                        current_memory_usage_bytes: 4,
                        build_mem_used_bytes: 0,
                        peak_mem_used_bytes: 0,
                        spill_count: 2,
                        output_rows: 22,
                    },
                ),
                (
                    q2.clone(),
                    TaskLoad {
                        elapsed_compute_ns: 30,
                        current_memory_usage_bytes: 8,
                        build_mem_used_bytes: 6,
                        peak_mem_used_bytes: 12,
                        spill_count: 3,
                        output_rows: 33,
                    },
                ),
            ],
        );

        assert_eq!(snapshot.snapshot_unix_nanos, 42);
        assert_eq!(snapshot.worker_url, "http://worker-a:8080");
        assert_eq!(snapshot.active_tasks, 3);

        assert_eq!(snapshot.active_query_ids.len(), 2);
        assert_eq!(snapshot.active_query_ids[0], q1);
        assert_eq!(snapshot.active_query_ids[1], q2);

        assert_eq!(snapshot.elapsed_compute_ns_total, 60);
        assert_eq!(snapshot.build_mem_used_bytes, 19);
        assert_eq!(snapshot.peak_mem_used_bytes, 25);
        assert_eq!(snapshot.spill_count, 6);
    }

    #[test]
    fn test_clamp_stream_interval_ms() {
        assert_eq!(clamp_stream_interval_ms(0), DEFAULT_STREAM_INTERVAL_MS);
        assert_eq!(clamp_stream_interval_ms(1), MIN_STREAM_INTERVAL_MS);
        assert_eq!(
            clamp_stream_interval_ms(MAX_STREAM_INTERVAL_MS + 1),
            MAX_STREAM_INTERVAL_MS
        );
        assert_eq!(clamp_stream_interval_ms(1000), 1000);
    }

    #[tokio::test]
    async fn test_get_worker_snapshot_on_empty_cache() {
        let cache = Arc::new(Cache::builder().build());
        let resolver = Arc::new(TestWorkerResolver { urls: vec![] });
        let service = ObservabilityServiceImpl::with_worker_url(
            cache,
            resolver,
            "http://worker-empty:8080",
        );

        let response = service
            .get_worker_snapshot(Request::new(GetWorkerSnapshotRequest {}))
            .await
            .unwrap()
            .into_inner();

        let snapshot = response.snapshot.expect("snapshot must be present");
        assert_eq!(snapshot.worker_url, "http://worker-empty:8080");
        assert_eq!(snapshot.active_tasks, 0);
        assert!(snapshot.active_query_ids.is_empty());
        assert_eq!(snapshot.elapsed_compute_ns_total, 0);
        assert_eq!(snapshot.build_mem_used_bytes, 0);
        assert_eq!(snapshot.peak_mem_used_bytes, 0);
        assert_eq!(snapshot.spill_count, 0);
    }
}
