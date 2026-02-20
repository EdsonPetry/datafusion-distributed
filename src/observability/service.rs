use crate::TaskData;
use crate::WorkerResolver;
use crate::execution_plans::{NetworkBroadcastExec, NetworkCoalesceExec, NetworkShuffleExec};
use crate::protobuf::StageKey;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::metrics::MetricValue;
use moka::future::Cache;
use std::sync::Arc;
use tokio::sync::OnceCell;
use tonic::{Request, Response, Status};

use super::generated::observability::{
    GetTaskMetricsResponse, GetWorkersRequest, GetWorkersResponse, TaskMetricsSummary,
};
use super::{
    ObservabilityService,
    generated::observability::{GetTaskMetricsRequest, PingRequest, PingResponse},
};

pub struct ObservabilityServiceImpl {
    task_data_entries: Arc<Cache<StageKey, Arc<OnceCell<TaskData>>>>,
    worker_resolver: Arc<dyn WorkerResolver + Send + Sync>,
}

impl ObservabilityServiceImpl {
    pub fn new(
        task_data_entries: Arc<Cache<StageKey, Arc<OnceCell<TaskData>>>>,
        worker_resolver: Arc<dyn WorkerResolver + Send + Sync>,
    ) -> Self {
        Self {
            task_data_entries,
            worker_resolver,
        }
    }
}

#[tonic::async_trait]
impl ObservabilityService for ObservabilityServiceImpl {
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
}

/// Converts internal StageKey to observability proto StageKey
fn convert_stage_key(key: &StageKey) -> super::StageKey {
    super::StageKey {
        query_id: key.query_id.to_vec(),
        stage_id: key.stage_id,
        task_number: key.task_number,
    }
}

/// Aggregates operator metrics across a single task and returns a TaskMetricsSummary.
/// Walks the plan tree recursively, summing metrics across all operators.
/// Output rows are taken from the root node only. Stops at network boundaries
/// since those represent child stages running on other workers.
fn aggregate_task_metrics(
    plan: &Arc<dyn ExecutionPlan>,
    stage_key: Arc<StageKey>,
) -> TaskMetricsSummary {
    let mut output_rows = 0u64;
    let mut elapsed_compute = 0u64;
    let mut current_memory_usage = 0u64;
    let mut spill_count = 0u64;

    // Extract output_rows from root node only
    if let Some(metrics) = plan.metrics() {
        for metric in metrics.iter() {
            if let MetricValue::OutputRows(c) = metric.value() {
                output_rows += c.value() as u64;
            }
        }
    }

    // Sum remaining metrics across all nodes
    accumulate_metrics(
        plan,
        &mut elapsed_compute,
        &mut current_memory_usage,
        &mut spill_count,
    );

    TaskMetricsSummary {
        stage_key: Some(convert_stage_key(&stage_key)),
        output_rows,
        elapsed_compute,
        current_memory_usage,
        spill_count,
    }
}

/// Recursively walks the plan tree, accumulating metrics from each node.
/// Stops at network boundary nodes since they are child stages on other workers.
fn accumulate_metrics(
    plan: &Arc<dyn ExecutionPlan>,
    elapsed_compute: &mut u64,
    current_memory_usage: &mut u64,
    spill_count: &mut u64,
) {
    if let Some(metrics) = plan.metrics() {
        for metric in metrics.iter() {
            match metric.value() {
                MetricValue::ElapsedCompute(t) => *elapsed_compute += t.value() as u64,
                // FIXME: Guage drops the current memory usage snapshot before it's read, giving a
                // 0 value each time.
                MetricValue::CurrentMemoryUsage(g) => *current_memory_usage += g.value() as u64,
                MetricValue::SpillCount(c) => *spill_count += c.value() as u64,
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
        accumulate_metrics(child, elapsed_compute, current_memory_usage, spill_count);
    }
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
}
