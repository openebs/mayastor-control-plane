extern crate bytes;
extern crate prost;
extern crate prost_derive;
extern crate serde;
extern crate serde_derive;
extern crate serde_json;
extern crate tonic;
#[allow(dead_code)]
#[allow(clippy::type_complexity)]
#[allow(clippy::unit_arg)]
#[allow(clippy::redundant_closure)]
#[allow(clippy::upper_case_acronyms)]
pub mod mayastor {
    use bdev_rpc_client::BdevRpcClient;
    use mayastor_client::MayastorClient;

    use std::{
        net::{SocketAddr, TcpStream},
        str::FromStr,
        time::Duration,
    };
    use tonic::transport::Channel;

    #[derive(Debug)]
    pub enum Error {
        ParseError,
    }

    impl From<()> for Null {
        fn from(_: ()) -> Self {
            Self {}
        }
    }

    impl FromStr for NvmeAnaState {
        type Err = Error;
        fn from_str(state: &str) -> Result<Self, Self::Err> {
            match state {
                "optimized" => Ok(Self::NvmeAnaOptimizedState),
                "non_optimized" => Ok(Self::NvmeAnaNonOptimizedState),
                "inaccessible" => Ok(Self::NvmeAnaInaccessibleState),
                _ => Err(Error::ParseError),
            }
        }
    }

    include!(concat!(env!("OUT_DIR"), "/mayastor.rs"));

    /// Test Rpc Handle to connect to a mayastor instance via an endpoint
    /// Gives access to the mayastor client and the bdev client
    #[derive(Clone)]
    pub struct RpcHandle {
        pub name: String,
        pub endpoint: SocketAddr,
        pub mayastor: MayastorClient<Channel>,
        pub bdev: BdevRpcClient<Channel>,
    }

    impl RpcHandle {
        /// Connect to the container and return a handle to `Self`
        /// Note: The initial connection with a timeout is using blocking calls
        pub async fn connect(name: &str, endpoint: SocketAddr) -> Result<Self, String> {
            let mut attempts = 40;
            loop {
                if TcpStream::connect_timeout(&endpoint, Duration::from_millis(100)).is_ok() {
                    break;
                } else {
                    std::thread::sleep(Duration::from_millis(100));
                }
                attempts -= 1;
                if attempts == 0 {
                    return Err(format!("Failed to connect to {}/{}", name, endpoint));
                }
            }

            let mayastor = MayastorClient::connect(format!("http://{}", endpoint))
                .await
                .unwrap();
            let bdev = BdevRpcClient::connect(format!("http://{}", endpoint))
                .await
                .unwrap();

            Ok(Self {
                name: name.to_string(),
                mayastor,
                bdev,
                endpoint,
            })
        }
    }
}

pub mod csi {
    include!(concat!(env!("OUT_DIR"), "/csi.v1.rs"));
}

pub mod registration {
    include!(concat!(env!("OUT_DIR"), "/v1.registration.rs"));
}
