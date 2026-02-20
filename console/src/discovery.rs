use datafusion_distributed::{GetWorkersRequest, ObservabilityServiceClient};
use url::Url;

/// Connect to a seed worker's observability service and discover all workers
/// in the cluster via the GetWorkers RPC.
pub async fn discover_workers(seed_url: &Url) -> Result<Vec<Url>, Box<dyn std::error::Error>> {
    let mut client = ObservabilityServiceClient::connect(seed_url.to_string()).await?;
    let response = client.get_workers(GetWorkersRequest {}).await?;
    let workers = response.into_inner().workers;

    let urls: Vec<Url> = workers
        .into_iter()
        .map(|w| Url::parse(&w.url))
        .collect::<Result<Vec<_>, _>>()?;

    if urls.is_empty() {
        // If GetWorkers returned empty, fall back to seed
        Ok(vec![seed_url.clone()])
    } else {
        Ok(urls)
    }
}
