use crate::resources::{ResourceDelete, ResourceMgr, ResourceSample};
use openapi::{apis::Uuid, clients::tower::direct::ApiClient, models};

/// Resource manager for volumes.
pub(crate) struct VolMgr {
    n_replicas: u8,
    size_bytes: u64,
}
impl VolMgr {
    /// New `ResourceMgr` for volumes with `n_replicas` replicas and size in bytes.
    pub(crate) async fn new_mgr(
        n_replicas: u8,
        size_bytes: u64,
    ) -> anyhow::Result<impl ResourceMgr> {
        Ok(Self {
            n_replicas,
            size_bytes,
        })
    }
}
#[async_trait::async_trait]
impl ResourceMgr for VolMgr {
    type Output = Vec<models::Volume>;
    async fn create(&self, client: &ApiClient, count: u32) -> anyhow::Result<Self::Output> {
        let mut created_volumes = Vec::with_capacity(count as usize);
        for _ in 0 .. count {
            let volume = client
                .volumes_api()
                .put_volume(
                    &Uuid::new_v4(),
                    models::CreateVolumeBody::new(
                        models::VolumePolicy::new(false),
                        self.n_replicas,
                        self.size_bytes,
                    ),
                )
                .await?;
            created_volumes.push(volume);
        }
        Ok(created_volumes)
    }
    async fn delete(&self, client: &ApiClient, created: Self::Output) -> anyhow::Result<()> {
        created.delete(client).await
    }

    fn prepare_sample(&self, points: Vec<u64>) -> Box<dyn ResourceSample> {
        let replicas = self.n_replicas;
        Box::new(VolumeCount { points, replicas })
    }
}

#[async_trait::async_trait]
impl ResourceDelete for Vec<models::Volume> {
    async fn delete(&self, client: &ApiClient) -> anyhow::Result<()> {
        for volume in self {
            client.volumes_api().del_volume(&volume.spec.uuid).await?;
        }
        Ok(())
    }
}

struct VolumeCount {
    points: Vec<u64>,
    replicas: u8,
}
impl ResourceSample for VolumeCount {
    fn points(&self) -> &Vec<u64> {
        &self.points
    }
    fn points_mut(&mut self) -> &mut Vec<u64> {
        &mut self.points
    }

    fn name(&self) -> String {
        "Volumes ~Repl".to_string()
    }

    fn format_point(&self, point: u64) -> String {
        format!("{} ~{}", point, self.replicas)
    }
}
