/// Pool grpc Client related code
pub mod client;

/// Pool grpc Server related code
pub mod server;

/// Pool traits for the transport
pub mod traits;

#[cfg(test)]
mod test {
    use crate::{
        grpc_opts::Context,
        operations::pool::{client::PoolClient, server::PoolServer, traits::PoolOperations},
    };
    use common_lib::{mbus_api::TimeoutOptions, types::v0::message_bus::Filter};
    use once_cell::sync::OnceCell;
    use std::{
        net::{IpAddr, Ipv4Addr, SocketAddr},
        sync::{Arc, Mutex},
        time::{Duration, Instant},
    };
    use tokio::sync::oneshot::Sender;
    use tonic::transport::Uri;

    type CompleteSender = Arc<Mutex<Option<Sender<(bool, Instant)>>>>;
    static COMPLETE_CHAN: OnceCell<CompleteSender> = OnceCell::new();

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn timeout() {
        let socket_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::from(0)), 50011);
        let uri = Uri::builder()
            .scheme("https")
            .path_and_query("")
            .authority(socket_addr.to_string())
            .build()
            .unwrap();

        tokio::spawn(async move {
            let service = PoolServer::new(Arc::new(server::Server {}));
            tonic::transport::Server::builder()
                .add_service(service.into_grpc_server())
                .serve(socket_addr)
                .await
                .unwrap();
        });
        // todo: wait until the server is running
        tokio::time::sleep(Duration::from_millis(200)).await;

        COMPLETE_CHAN.get_or_init(|| Arc::new(Mutex::new(None)));

        let channel = COMPLETE_CHAN.get().unwrap().clone();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        *channel.lock().unwrap() = Some(sender);

        let timeout_opts = TimeoutOptions::new().with_timeout(Duration::from_secs(10));
        let client = PoolClient::new(uri, timeout_opts).await;

        let req_timeout = Duration::from_secs(1);
        let ctx = Context::new(TimeoutOptions::new().with_timeout(req_timeout));
        let before = std::time::Instant::now();
        let result = client.get(Filter::None, Some(ctx)).await;
        let (complete, timestamp) = receiver.await.unwrap();
        println!(
            "Request completed: {}, duration: {:?}",
            complete,
            timestamp - before
        );
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
            grpc_opts::Context,
            operations::pool::{
                test::TimeoutTester,
                traits::{CreatePoolInfo, DestroyPoolInfo, PoolOperations},
            },
        };
        use common_lib::{
            mbus_api::{v0::Pools, ReplyError},
            types::v0::message_bus::{Filter, Pool},
        };
        use std::time::Duration;

        pub(super) struct Server {}
        #[tonic::async_trait]
        impl PoolOperations for Server {
            async fn create(
                &self,
                _pool: &dyn CreatePoolInfo,
                _ctx: Option<Context>,
            ) -> Result<Pool, ReplyError> {
                todo!()
            }
            async fn destroy(
                &self,
                _pool: &dyn DestroyPoolInfo,
                _ctx: Option<Context>,
            ) -> Result<(), ReplyError> {
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
        }
    }
}
