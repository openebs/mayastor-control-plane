use once_cell::sync::OnceCell;
use snafu::Snafu;
use std::{collections::HashSet, sync::Mutex};
use tracing::trace;

static VOLUME_SERIALIZER: OnceCell<Mutex<HashSet<String>>> = OnceCell::new();

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)), context(suffix(false)), module(nvme_error))]
/// Possible volume serializer error kind sources.
pub(crate) enum VolumeSerializerError {
    #[snafu(display("VOLUME_SERIALIZER once cell is not accessible"))]
    OnceCellInaccessible,
    #[snafu(display("Nvme connect operation is in progress for volume: {}", id))]
    OperationInProgess { id: String },
}

impl From<VolumeSerializerError> for tonic::Status {
    fn from(error: VolumeSerializerError) -> Self {
        match error {
            VolumeSerializerError::OnceCellInaccessible => {
                tonic::Status::unavailable(error.to_string())
            }
            VolumeSerializerError::OperationInProgess { .. } => {
                tonic::Status::already_exists(error.to_string())
            }
        }
    }
}

/// Returns or inits the VOLUME_SERIALIZER hash set.
pub(crate) fn volume_serializer() -> &'static Mutex<HashSet<String>> {
    VOLUME_SERIALIZER.get_or_init(|| Mutex::new(HashSet::new()))
}

/// Volume Operation Guard for single operations.
pub(crate) struct VolumeOpGuard {
    item: String,
}

impl VolumeOpGuard {
    /// Inserts the held item to VOLUME_SERIALIZER.
    pub(crate) fn new(item: &str) -> Result<Self, VolumeSerializerError> {
        let mut inventory = volume_serializer().lock().unwrap();
        if inventory.get(item).is_some() {
            trace!(
                item,
                "VolumeId already exists in inventory, skipping connection"
            );
            return Err(VolumeSerializerError::OperationInProgess {
                id: item.to_string(),
            });
        } else {
            inventory.insert(item.to_string());
            trace!(item, "VolumeId inserted in inventory, starting connection")
        }
        Ok(Self {
            item: item.to_string(),
        })
    }
}

impl Drop for VolumeOpGuard {
    fn drop(&mut self) {
        let mut inventory = volume_serializer().lock().unwrap();
        inventory.remove(self.item.as_str());
        trace!(
            self.item,
            "VolumeId removed from inventory, connection complete"
        )
    }
}
