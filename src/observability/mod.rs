mod generated;
mod service;

pub use generated::observability::observability_service_client::ObservabilityServiceClient;
pub use generated::observability::observability_service_server::{
    ObservabilityService, ObservabilityServiceServer,
};

pub use generated::observability::{
    GetTaskMetricsRequest, GetTaskMetricsResponse, GetWorkerSnapshotRequest,
    GetWorkerSnapshotResponse, GetWorkersRequest, GetWorkersResponse, PingRequest, PingResponse,
    StageKey, TaskMetricsSummary, WatchWorkerSnapshotsRequest, WorkerInfo, WorkerSnapshot,
    WorkerSnapshotEvent,
};
pub use service::ObservabilityServiceImpl;
