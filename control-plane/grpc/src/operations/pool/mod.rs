/// Pool grpc Client related code
pub mod client;

/// Pool grpc Server related code
pub mod server;

/// Pool traits for the transport
pub mod traits;

#[cfg(test)]
mod test {
    use crate::{
        context::Context,
        operations::pool::{client::PoolClient, server::PoolServer, traits::PoolOperations},
    };
    use once_cell::sync::OnceCell;
    use std::{
        net::{IpAddr, Ipv4Addr, SocketAddr},
        sync::{Arc, Mutex},
        time::{Duration, Instant},
    };
    use stor_port::{transport_api::TimeoutOptions, types::v0::transport::Filter};
    use tokio::sync::oneshot::Sender;
    use tonic::transport::Uri;

    type CompleteSender = Arc<Mutex<Option<Sender<(bool, Instant)>>>>;
    static COMPLETE_CHAN: OnceCell<CompleteSender> = OnceCell::new();

    /// Generate an ephemeral self-signed TLS config, shared by both the client and the server, so
    /// the client presents (and the server verifies) the same certificate (mutual TLS).
    fn self_signed_tls(san: String) -> (crate::tls::TlsConfig, std::path::PathBuf) {
        let cert = rcgen::generate_simple_self_signed(vec![san]).unwrap();
        let dir = std::env::temp_dir().join(format!("grpc-timeout-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let cert_path = dir.join("cert.pem");
        let key_path = dir.join("key.pem");
        std::fs::write(&cert_path, cert.cert.pem()).unwrap();
        std::fs::write(&key_path, cert.key_pair.serialize_pem()).unwrap();
        let tls =
            crate::tls::TlsConfig::new(Some(cert_path.clone()), Some(cert_path), Some(key_path))
                .unwrap();
        (tls, dir)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn timeout() {
        let socket_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 50011);
        let uri = Uri::builder()
            .scheme("https")
            .path_and_query("")
            .authority(socket_addr.to_string())
            .build()
            .unwrap();

        let (tls, tls_dir) = self_signed_tls(socket_addr.ip().to_string());

        let server_tls = tls.clone();
        tokio::spawn(async move {
            let service = PoolServer::new(Arc::new(server::Server {}));
            let incoming = crate::tls::incoming(socket_addr, server_tls, false)
                .await
                .unwrap();
            tonic::transport::Server::builder()
                .add_service(service.into_grpc_server())
                .serve_with_incoming(incoming)
                .await
                .unwrap();
        });
        // todo: wait until the server is running
        tokio::time::sleep(Duration::from_millis(200)).await;

        COMPLETE_CHAN.get_or_init(|| Arc::new(Mutex::new(None)));

        let channel = COMPLETE_CHAN.get().unwrap().clone();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        *channel.lock().unwrap() = Some(sender);

        let timeout_opts = TimeoutOptions::new().with_req_timeout(Duration::from_secs(10));
        let client = PoolClient::new_with_tls(uri, timeout_opts, tls)
            .await
            .unwrap();

        let req_timeout = Duration::from_secs(1);
        let ctx = Context::new(TimeoutOptions::new().with_req_timeout(req_timeout));
        let before = std::time::Instant::now();
        let result = client.get(Filter::None, Some(ctx)).await;
        let (complete, timestamp) = receiver.await.unwrap();
        println!(
            "Request completed: {}, duration: {:?}",
            complete,
            timestamp - before
        );
        // remove the temporary TLS material before the assertions, which may panic
        std::fs::remove_dir_all(&tls_dir).unwrap();
        // client should have timed out!
        assert!(result.is_err());
        // timeout within the req_timeout, with 200ms slack
        assert!(before.elapsed() < (req_timeout + Duration::from_millis(200)));
        // server request should have been dropped
        assert!(!complete);
    }

    struct TimeoutTester {
        complete: bool,
    }
    impl TimeoutTester {
        fn new() -> Self {
            Self { complete: false }
        }
        fn complete(mut self) {
            self.complete = true;
        }
    }
    impl Drop for TimeoutTester {
        fn drop(&mut self) {
            let channel = COMPLETE_CHAN.get().unwrap().clone();
            let sender = channel.lock().unwrap().take().unwrap();
            sender
                .send((self.complete, std::time::Instant::now()))
                .unwrap();
        }
    }

    mod server {
        use crate::{
            context::Context,
            operations::pool::{
                test::TimeoutTester,
                traits::{
                    ClearErrorsRequest, CreatePoolInfo, DestroyPoolInfo, ExpandPoolInfo,
                    LabelPoolInfo, PoolCordonRequest, PoolCreateError, PoolOperations,
                    UnlabelPoolInfo,
                },
            },
        };
        use std::time::Duration;
        use stor_port::{
            transport_api::{v0::Pools, ReplyError},
            types::v0::transport::{Filter, Pool, PoolDeleteResult},
        };

        pub(super) struct Server {}
        #[tonic::async_trait]
        impl PoolOperations for Server {
            async fn create(
                &self,
                _pool: &dyn CreatePoolInfo,
                _ctx: Option<Context>,
            ) -> Result<Pool, PoolCreateError> {
                todo!()
            }
            async fn destroy(
                &self,
                _pool: &dyn DestroyPoolInfo,
                _ctx: Option<Context>,
            ) -> Result<Option<PoolDeleteResult>, ReplyError> {
                todo!()
            }
            async fn get(
                &self,
                _filter: Filter,
                _ctx: Option<Context>,
            ) -> Result<Pools, ReplyError> {
                let tester = TimeoutTester::new();
                tokio::time::sleep(Duration::from_secs(3)).await;
                tester.complete();
                Ok(Pools(vec![]))
            }
            async fn label(
                &self,
                _pool: &dyn LabelPoolInfo,
                _ctx: Option<Context>,
            ) -> Result<Pool, ReplyError> {
                todo!()
            }
            async fn unlabel(
                &self,
                _pool: &dyn UnlabelPoolInfo,
                _ctx: Option<Context>,
            ) -> Result<Pool, ReplyError> {
                todo!()
            }

            async fn cordon(&self, _info: PoolCordonRequest) -> Result<Pool, ReplyError> {
                todo!()
            }

            async fn uncordon(&self, _info: PoolCordonRequest) -> Result<Pool, ReplyError> {
                todo!()
            }

            async fn expand(&self, _info: &dyn ExpandPoolInfo) -> Result<Pool, ReplyError> {
                todo!()
            }

            async fn clear_errors(
                &self,
                _request: &ClearErrorsRequest,
            ) -> Result<Pool, ReplyError> {
                todo!()
            }

            async fn get_pool_health(
                &self,
                _pool_id: &stor_port::types::v0::transport::PoolId,
            ) -> Result<stor_port::types::v0::transport::GetPoolHealthResponse, ReplyError>
            {
                todo!()
            }
        }
    }
}
