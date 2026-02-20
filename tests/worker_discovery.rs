#[cfg(all(feature = "integration", test))]
mod tests {
    use datafusion_distributed::test_utils::localhost::LocalHostWorkerResolver;
    use datafusion_distributed::{
        GetWorkersRequest, ObservabilityServiceClient, PingRequest, Worker,
    };
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::net::TcpListener;
    use tonic::transport::Server;

    #[tokio::test]
    async fn discover_workers_via_get_workers_rpc() {
        // Bind 3 listeners on random ports
        let listeners = futures::future::try_join_all(
            (0..3)
                .map(|_| TcpListener::bind("127.0.0.1:0"))
                .collect::<Vec<_>>(),
        )
        .await
        .unwrap();

        let ports: Vec<u16> = listeners
            .iter()
            .map(|l| l.local_addr().unwrap().port())
            .collect();

        let resolver = Arc::new(LocalHostWorkerResolver::new(ports.clone()));

        // Start all 3 workers with both Flight and Observability services
        for listener in listeners {
            let resolver = resolver.clone();
            let worker = Worker::default();
            let obs = worker.to_observability_service(resolver);
            let flight = worker.into_flight_server();
            let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
            tokio::spawn(async move {
                Server::builder()
                    .add_service(obs)
                    .add_service(flight)
                    .serve_with_incoming(incoming)
                    .await
                    .unwrap();
            });
        }

        tokio::time::sleep(Duration::from_millis(200)).await;

        // Connect to the first worker and discover the cluster
        let seed_url = format!("http://localhost:{}", ports[0]);
        let mut client = ObservabilityServiceClient::connect(seed_url).await.unwrap();

        let response = client
            .get_workers(GetWorkersRequest {})
            .await
            .unwrap();

        let discovered = response.into_inner().workers;
        assert_eq!(discovered.len(), 3, "Should discover all 3 workers");

        // Verify each discovered worker is reachable via Ping
        for worker_info in &discovered {
            let mut w_client = ObservabilityServiceClient::connect(worker_info.url.clone())
                .await
                .unwrap();
            let ping = w_client.ping(PingRequest {}).await.unwrap();
            assert_eq!(ping.into_inner().value, 1);
        }
    }

    #[tokio::test]
    async fn discover_workers_from_any_worker_returns_same_list() {
        // Bind 2 listeners
        let listeners = futures::future::try_join_all(
            (0..2)
                .map(|_| TcpListener::bind("127.0.0.1:0"))
                .collect::<Vec<_>>(),
        )
        .await
        .unwrap();

        let ports: Vec<u16> = listeners
            .iter()
            .map(|l| l.local_addr().unwrap().port())
            .collect();

        let resolver = Arc::new(LocalHostWorkerResolver::new(ports.clone()));

        for listener in listeners {
            let resolver = resolver.clone();
            let worker = Worker::default();
            let obs = worker.to_observability_service(resolver);
            let flight = worker.into_flight_server();
            let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
            tokio::spawn(async move {
                Server::builder()
                    .add_service(obs)
                    .add_service(flight)
                    .serve_with_incoming(incoming)
                    .await
                    .unwrap();
            });
        }

        tokio::time::sleep(Duration::from_millis(200)).await;

        // Query GetWorkers from worker 1
        let mut client1 =
            ObservabilityServiceClient::connect(format!("http://localhost:{}", ports[0]))
                .await
                .unwrap();
        let response1 = client1
            .get_workers(GetWorkersRequest {})
            .await
            .unwrap();
        let mut urls1: Vec<String> = response1
            .into_inner()
            .workers
            .into_iter()
            .map(|w| w.url)
            .collect();
        urls1.sort();

        // Query GetWorkers from worker 2
        let mut client2 =
            ObservabilityServiceClient::connect(format!("http://localhost:{}", ports[1]))
                .await
                .unwrap();
        let response2 = client2
            .get_workers(GetWorkersRequest {})
            .await
            .unwrap();
        let mut urls2: Vec<String> = response2
            .into_inner()
            .workers
            .into_iter()
            .map(|w| w.url)
            .collect();
        urls2.sort();

        // Both should return the same list
        assert_eq!(urls1, urls2, "All workers should report the same cluster topology");
    }
}
