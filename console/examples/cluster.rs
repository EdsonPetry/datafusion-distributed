use async_trait::async_trait;
use clap::Parser;
use datafusion::common::DataFusionError;
use datafusion_distributed::{Worker, WorkerResolver};
use std::error::Error;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::{Arc, RwLock};
use tokio::net::TcpListener;
use tonic::transport::Server;
use url::Url;

#[derive(Parser)]
#[command(
    name = "cluster",
    about = "Start an in-memory cluster of workers with observability"
)]
struct Args {
    /// Number of workers to start
    #[arg(long, default_value = "16")]
    workers: usize,

    /// Starting port. Workers bind to consecutive ports from this value.
    /// If 0, the OS assigns random ports.
    #[arg(long, default_value = "0")]
    base_port: u16,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    let mut ports = Vec::new();
    let mut listeners = Vec::new();

    // Bind all listeners first so we know all ports before starting workers
    for i in 0..args.workers {
        let addr = SocketAddr::new(
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            if args.base_port == 0 {
                0
            } else {
                args.base_port
                    .checked_add(i as u16)
                    .expect("port overflow: base_port + workers exceeds u16::MAX")
            },
        );
        let listener = TcpListener::bind(addr).await?;
        let port = listener.local_addr()?.port();
        ports.push(port);
        listeners.push(listener);
    }

    // Create a shared resolver that knows about all workers
    let resolver = LocalhostClusterResolver {
        ports: Arc::new(RwLock::new(ports.clone())),
    };

    for listener in listeners {
        let resolver = resolver.clone();
        tokio::spawn(async move {
            let worker = Worker::default();

            Server::builder()
                .add_service(worker.with_observability_service(resolver))
                .add_service(worker.into_flight_server())
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .expect("worker server failed");
        });
    }

    let ports_csv = ports
        .iter()
        .map(|p| p.to_string())
        .collect::<Vec<_>>()
        .join(",");

    println!("Started {} workers on ports: {ports_csv}\n", args.workers);
    println!("Console (connect to any worker for auto-discovery):");
    println!(
        "\tcargo run -p datafusion-distributed-console -- --connect http://localhost:{}",
        ports[0]
    );
    println!("TPC-DS runner:");
    println!(
        "\tcargo run -p datafusion-distributed-console --example tpcds_runner -- --cluster-ports {ports_csv}"
    );
    println!("Single query:");
    println!(
        "\tcargo run -p datafusion-distributed-console --example console_run -- --cluster-ports {ports_csv} \"SELECT 1\""
    );
    println!("Press Ctrl+C to stop all workers.");

    tokio::signal::ctrl_c().await?;

    Ok(())
}

#[derive(Clone)]
struct LocalhostClusterResolver {
    ports: Arc<RwLock<Vec<u16>>>,
}

#[async_trait]
impl WorkerResolver for LocalhostClusterResolver {
    fn get_urls(&self) -> Result<Vec<Url>, DataFusionError> {
        Ok(self
            .ports
            .read()
            .unwrap()
            .iter()
            .map(|port| Url::parse(&format!("http://localhost:{port}")).unwrap())
            .collect())
    }
}
