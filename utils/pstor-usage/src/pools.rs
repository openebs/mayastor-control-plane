use crate::resources::{ResourceDelete, ResourceMgr, ResourceSample};
use anyhow::anyhow;
use openapi::{
    apis::{Url, Uuid},
    clients::tower::direct::ApiClient,
    models,
};

type NodeId = String;
/// Resource manager for pools.
pub(crate) struct PoolMgr {
    node_ids: Vec<NodeId>,
    size_bytes: u64,
    use_malloc: bool,
}
impl PoolMgr {
    /// New `ResourceMgr` that creates pools with `size_bytes` in bytes.
    pub(crate) async fn new_mgr(
        client: &ApiClient,
        size_bytes: u64,
        use_malloc: bool,
    ) -> anyhow::Result<impl ResourceMgr> {
        let node_ids = client.nodes_api().get_nodes().await?;
        let node_ids = node_ids.into_iter().map(|n| n.id).collect::<Vec<_>>();

        if let Some((dir, _)) = Self::pool_dir(use_malloc) {
            std::fs::create_dir_all(dir.clone())?;
            if !dir.is_dir() || !dir.exists() {
                return Err(anyhow!("file_based_dir should be a directory!"));
            }
        }

        if node_ids.is_empty() {
            return Err(anyhow!("No nodes available!"));
        }

        Ok(Self {
            node_ids,
            size_bytes: size_bytes + (4096 * 1024 * 2),
            use_malloc,
        })
    }
    fn pool_dir(pool_use_malloc: bool) -> Option<(std::path::PathBuf, std::path::PathBuf)> {
        if pool_use_malloc {
            None
        } else {
            Some((
                std::path::PathBuf::new().join("/tmp/pool/"),
                std::path::PathBuf::new().join("/host/tmp/pool"),
            ))
        }
    }
    fn pool_file(&self, pool_id: &Uuid) -> Option<(std::path::PathBuf, std::path::PathBuf)> {
        if let Some((host, container)) = Self::pool_dir(self.use_malloc) {
            Some((
                host.join(pool_id.to_string()),
                container.join(pool_id.to_string()),
            ))
        } else {
            None
        }
    }
    fn uri(&self, pool_id: &Uuid) -> anyhow::Result<Url> {
        let size_mb = self.size_bytes / (1024 * 1024);
        Ok(if let Some((host, container)) = self.pool_file(pool_id) {
            let file = std::fs::File::create(host)?;
            file.set_len(self.size_bytes)?;
            Url::parse(&format!("aio://{}", container.to_string_lossy(),)).expect("Valid Url")
        } else {
            Url::parse(&format!("malloc:///{}?size_mb={}", pool_id, size_mb)).expect("Valid Url")
        })
    }
}

#[async_trait::async_trait]
impl ResourceMgr for PoolMgr {
    type Output = Vec<models::Pool>;
    async fn create(&self, client: &ApiClient, count: u32) -> anyhow::Result<Self::Output> {
        let mut created_pools = Vec::with_capacity(count as usize);
        loop {
            if created_pools.len() >= count as usize {
                return Ok(created_pools);
            }

            for node_id in self.node_ids.iter() {
                let pool_id = Uuid::new_v4();
                let pool_disk = self.uri(&pool_id)?;

                let pool = client
                    .pools_api()
                    .put_node_pool(
                        &node_id.to_string(),
                        &pool_id.to_string(),
                        models::CreatePoolBody::new(vec![pool_disk.clone()]),
                    )
                    .await?;
                if let Some((file, _)) = self.pool_file(&pool_id) {
                    std::fs::remove_file(file)?;
                }
                created_pools.push(pool);
            }
        }
    }
    async fn delete(&self, client: &ApiClient, created: Self::Output) -> anyhow::Result<()> {
        created.delete(client).await
    }

    fn prepare_sample(&self, points: Vec<u64>) -> Box<dyn ResourceSample> {
        Box::new(PoolCount { points })
    }
}

#[async_trait::async_trait]
impl ResourceDelete for Vec<models::Pool> {
    async fn delete(&self, client: &ApiClient) -> anyhow::Result<()> {
        for pool in self {
            client.pools_api().del_pool(&pool.id).await?;
        }
        Ok(())
    }
}

struct PoolCount {
    points: Vec<u64>,
}
impl ResourceSample for PoolCount {
    fn points(&self) -> &Vec<u64> {
        &self.points
    }
    fn points_mut(&mut self) -> &mut Vec<u64> {
        &mut self.points
    }

    fn name(&self) -> String {
        "Pools".to_string()
    }

    fn format_point(&self, point: u64) -> String {
        point.to_string()
    }
}
