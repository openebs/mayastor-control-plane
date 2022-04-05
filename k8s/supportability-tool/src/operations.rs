use structopt::StructOpt;

/// Represents type of VolumeID
pub(crate) type VolumeID = openapi::apis::Uuid;

/// Represents type of PoolID
pub(crate) type PoolID = String;

/// Represents type of NodeID
pub(crate) type NodeID = String;

/// Types of operations supported by plugin
#[derive(StructOpt, Clone, Debug)]
pub(crate) enum Operations {
    /// 'Dump' creates an archive by collecting provided resource(s) information
    Dump(Resource),
}

/// Resources on which operation can be performed
#[derive(StructOpt, Clone, Debug)]
pub(crate) enum Resource {
    /// Collects entire system information
    System,

    /// Collects information about all volumes and its descendants (replicas/pools/nodes)
    #[structopt(name = "volumes")]
    Volumes,

    /// Collects information about particular volume and its descendants matching
    /// to given volume ID
    #[structopt(name = "volume")]
    Volume { id: VolumeID },

    /// Collects information about all pools and its descendants (nodes)
    #[structopt(name = "pools")]
    Pools,

    /// Collects information about particular pool and its descendants matching
    /// to given pool ID
    #[structopt(name = "pool")]
    Pool { id: PoolID },

    /// Collects information about all nodes
    #[structopt(name = "nodes")]
    Nodes,

    /// Collects information about particular node matching to given node ID
    #[structopt(name = "node")]
    Node { id: NodeID },
}
