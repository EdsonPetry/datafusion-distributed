use datafusion::error::DataFusionError;
use datafusion_distributed::{Worker, WorkerResolver};
use std::error::Error;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use structopt::StructOpt;
use tonic::transport::Server;
use url::Url;

#[derive(StructOpt)]
#[structopt(
    name = "console_worker",
    about = "A localhost DataFusion worker with observability"
)]
struct Args {
    #[structopt(default_value = "6190")]
    port: u16,

    /// Comma-delimited list of all worker ports in the cluster (e.g. 6190,6191).
    /// This worker's own port should be included. Defaults to just this worker's port.
    #[structopt(long = "cluster-ports", use_delimiter = true)]
    cluster_ports: Vec<u16>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::from_args();

    let cluster_ports = if args.cluster_ports.is_empty() {
        vec![args.port]
    } else {
        args.cluster_ports
    };

    let worker_resolver: Arc<dyn WorkerResolver + Send + Sync> =
        Arc::new(LocalhostWorkerResolver {
            ports: cluster_ports,
        });

    let worker = Worker::default();

    Server::builder()
        .add_service(worker.to_observability_service(worker_resolver))
        .add_service(worker.into_flight_server())
        .serve(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), args.port))
        .await?;

    Ok(())
}

#[derive(Clone)]
struct LocalhostWorkerResolver {
    ports: Vec<u16>,
}

impl WorkerResolver for LocalhostWorkerResolver {
    fn get_urls(&self) -> Result<Vec<Url>, DataFusionError> {
        Ok(self
            .ports
            .iter()
            .map(|port| Url::parse(&format!("http://localhost:{port}")).unwrap())
            .collect())
    }
}
