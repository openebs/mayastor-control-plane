use crate::{
    api::{ObjectKey, StorableObject, StoreObj},
    common::StorableObjectType,
    error::Error,
    etcd::Etcd,
    ApiVersion,
};
use etcd_client::{Client, LeaseGrantOptions, LeaseKeepAliveStream, LeaseKeeper, LockOptions};
use serde::{Deserialize, Serialize};
use std::{cmp::max, ops::Deref, sync::Arc, time::Duration};

/// Worker that keeps an etcd lease lock alive by sending keep alives
/// It removes the lease from `LeaseLockInfo` when it expires and adds it back once it
/// reestablishes the lease and the lock.
pub(crate) struct EtcdSingletonLock {
    client: Client,
    state: Option<LeaseKeeperState>,
    lease_ttl: Duration,
    lease_id: i64,
    lease_info: LeaseLockInfo,
    service_name: ControlPlaneService,
}

#[derive(Clone)]
pub(crate) struct LeaseLockInfo(Arc<parking_lot::Mutex<LeaseLockInfoInner>>);
impl LeaseLockInfo {
    /// Get the lease lock pair, (lease_id, lock_key)
    /// Returns `Error::NotReady` if the lease is not active
    pub(crate) fn lease_lock(&self) -> Result<(i64, String), Error> {
        let info = self.0.lock();
        match info.lease_id {
            Some(id) => Ok((id, info.lock_key.clone())),
            None => Err(Error::NotReady {
                reason: "waiting for the lease...".to_string(),
            }),
        }
    }
    /// Revokes the lease and releases the associated lock
    pub(crate) async fn revoke(&self) -> Result<(), Error> {
        let info = self.lease_info_inner();
        let mut client = info.client;
        if let Some(lease_id) = info.lease_id {
            client.lease_revoke(lease_id).await.ok();
        }
        client
            .unlock(info.lock_key)
            .await
            .map_err(|source| Error::FailedUnlock { source })?;
        Ok(())
    }

    /// Set the provided lease id.
    /// None => The lease is temporarily unavailable.
    /// Some(id) => This lease id is used for all put requests.
    fn set_lease(&self, lease_id: Option<i64>) {
        let mut lease_info = self.0.lock();
        lease_info.lease_id = lease_id;
    }

    /// New `Self` with the provided `lease_id` and `lock_key`
    fn new(lease_id: i64, lock_key: &str, client: &Client) -> Self {
        Self(Arc::new(parking_lot::Mutex::new(LeaseLockInfoInner::new(
            lease_id, lock_key, client,
        ))))
    }
    fn lease_info_inner(&self) -> LeaseLockInfoInner {
        self.0.lock().clone()
    }
}

#[derive(Clone)]
struct LeaseLockInfoInner {
    /// The etcd lease id, if active. Otherwise we cannot issue requests until we've renewed it.
    lease_id: Option<i64>,
    /// The key value for the etcd lock
    lock_key: String,
    /// etcd client
    client: Client,
}
impl LeaseLockInfoInner {
    fn new(lease_id: i64, lock_key: &str, client: &Client) -> Self {
        Self {
            lease_id: Some(lease_id),
            lock_key: lock_key.to_string(),
            client: client.clone(),
        }
    }
}

/// State of the `EtcdLeaseLockKeeper`
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
enum LeaseKeeperState {
    /// The lease has expired, we might need to reconnect or even grab a new lease
    LeaseExpired(LeaseExpired),
    /// We need to be granted a lease
    LeaseGrant(LeaseGrant),
    /// We're attempting to lock and set ourselves as the owner
    Locking(Locking),
    /// We're locked, so we need to kick-start the keep alive
    Locked(Locked),
    /// We're sending keep alives at half the lease_time ttl
    KeepAlive(KeepAlive),
    /// We're trying to reestablish the connection to etcd
    Reconnect(Reconnect),
    /// We've been replaced by another, time to give up :(
    Replaced(Replaced),
}

impl EtcdSingletonLock {
    /// Used to keep a lease alive and lock the `lock_owner_name` lock. This *NOT* a distributed
    /// lock, but simply a means of fail-over from an instance of `service_kind` to another (eg:
    /// when using a kubernetes deployment of *1* replica).
    /// When another instance of `service_kind` appears and take over the lock we shall *panic*
    /// as in this case we're not intending to keep the previous owner running. Only one instance of
    /// `service_kind` should run at a time!
    ///
    /// # Arguments
    /// * `service_kind` - The type of the service
    /// * `lease_time` - The lease's time to live as a `std::time::Duration`. Another service cannot
    /// take over until this time elapses without any lease keep alives being sent by the first one.
    ///
    /// Start the `Self` which attempts to get a lease and grab the `service_kind` lock.
    /// A background thread will attempt to keep the lease alive, and will handle reconnections if
    /// the connection to etcd is lost.
    pub(crate) async fn start(
        mut client: Client,
        service_kind: ControlPlaneService,
        lease_ttl: std::time::Duration,
    ) -> Result<LeaseLockInfo, Error> {
        let lock_owner_key_prefix = EtcdSingletonLock::lock_key(&service_kind);
        let lease_resp = client
            .lease_grant(*LeaseTtl::from(lease_ttl), None)
            .await
            .expect("Should init the lease");
        tracing::info!(
            lease.id = lease_resp.id(),
            lease.ttl = lease_resp.ttl(),
            "Granted new lease",
        );
        let lock_resp = tokio::time::timeout(
            lease_ttl,
            client.lock(
                lock_owner_key_prefix.as_str(),
                Some(LockOptions::new().with_lease(lease_resp.id())),
            ),
        )
        .await
        .map_err(|_| Error::Timeout {
            operation: format!("etcd lock '{lock_owner_key_prefix}'"),
            timeout: lease_ttl,
        })?
        .map_err(|e| Error::FailedLock {
            reason: e.to_string(),
        })?;

        let lock_key =
            String::from_utf8(lock_resp.key().to_vec()).map_err(|e| Error::FailedLock {
                reason: format!("Invalid etcd lock key, error: '{e}'"),
            })?;
        let lease_id = lease_resp.id();

        let lease_info = LeaseLockInfo::new(lease_id, &lock_key, &client);

        let mut keeper = Self {
            client,
            state: Some(LeaseKeeperState::Locked(Locked {
                lock_key: lock_key.clone(),
                lease_id: lease_resp.id(),
            })),
            lease_ttl,
            lease_id,
            lease_info: lease_info.clone(),
            service_name: service_kind,
        };
        keeper
            .set_owner_lease(lease_resp.id(), &lock_key)
            .await
            .map_err(|e| Error::FailedLock {
                reason: e.to_string(),
            })?;

        tokio::spawn(async move {
            keeper.keep_lock_alive_forever().await;
        });

        Ok(lease_info)
    }

    fn lease_ttl(&self) -> LeaseTtl {
        LeaseTtl::from(self.lease_ttl)
    }
    fn lease_id(&self) -> i64 {
        self.lease_id
    }
    fn lease_id_str(&self) -> String {
        format!("{:x}", self.lease_id)
    }

    /// Check if this service instance has been replaced by another. If it has then we move to the
    /// terminal `LeaseKeeperState::Replaced` state.
    async fn check_replaced(&mut self) -> Result<(), LeaseKeeperState> {
        match self.is_replaced().await {
            Some(true) => Err(LeaseKeeperState::Replaced(Replaced {})),
            Some(false) => Ok(()),
            None => Err(LeaseKeeperState::Reconnect(Reconnect::default())),
        }
    }
    /// Some(true) - We've been replaced by another instance of the service
    /// Some(false) - We haven't been replaced
    /// None - Don't know, etcd connection not available
    async fn is_replaced(&mut self) -> Option<bool> {
        match self.get_owner_lease_id().await {
            Ok(Some(name)) => Some(name != self.lease_id_str()),
            Ok(None) => Some(false),
            Err(_) => None,
        }
    }
    async fn keep_lock_alive_forever(&mut self) {
        loop {
            self.next().await;
        }
    }
    async fn unset_lease(&mut self) {
        self.lease_info.set_lease(None);
    }
    async fn set_lease(&mut self) {
        self.lease_info.set_lease(Some(self.lease_id));
    }

    /// Run 1 cycle of the state machine...
    async fn next(&mut self) {
        let state = self.state.take().expect("state to be present");
        let name = state.name();

        let (previous_state_name, new_state) = (
            name,
            match state {
                LeaseKeeperState::LeaseGrant(state) => self.clock(state).await,
                LeaseKeeperState::Locking(state) => self.clock(state).await,
                LeaseKeeperState::Locked(state) => self.clock(state).await,
                LeaseKeeperState::KeepAlive(state) => self.clock(state).await,
                LeaseKeeperState::Replaced(state) => self.clock(state).await,
                LeaseKeeperState::Reconnect(state) => self.clock(state).await,
                LeaseKeeperState::LeaseExpired(state) => self.clock(state).await,
            }
            .unwrap_or_else(|e| e),
        );

        if previous_state_name != new_state.name() {
            tracing::info!("{} => {}", previous_state_name, new_state.name());
        }
        self.state = Some(new_state);
    }

    fn lock_key(name: &ControlPlaneService) -> String {
        StoreLeaseLockKey::new(name).key()
    }
    /// Set this service as the lease winner. Useful to find out if a service has ever been replaced
    /// by another instance.
    async fn set_owner_lease(&mut self, lease_id: i64, lock_key: &str) -> Result<(), Error> {
        Etcd::from(
            &self.client,
            Some(LeaseLockInfo::new(lease_id, lock_key, &self.client)),
        )
        .put_obj(&StoreLeaseOwner::new(&self.service_name, self.lease_id))
        .await
    }
    /// Get the current owner lease id, or None if it does not exist.
    async fn get_owner_lease_id(&mut self) -> Result<Option<String>, Error> {
        let owner: Result<StoreLeaseOwner, Error> = Etcd::from(&self.client, None)
            .get_obj(&StoreLeaseOwnerKey::new(&self.service_name))
            .await;

        match owner {
            Ok(owner) => Ok(Some(owner.lease_id().to_string())),
            Err(Error::MissingEntry { .. }) => Ok(None),
            Err(e) => Err(e),
        }
    }
    /// Check if the etcd connection is available
    /// todo: check if this etcd instance is actually usable?
    async fn check_etcd_connection(&mut self) -> Result<(), LeaseKeeperState> {
        if self.client.status().await.is_err() {
            Err(LeaseKeeperState::Reconnect(Reconnect::default()))
        } else {
            Ok(())
        }
    }
}

#[derive(Copy, Clone)]
struct LeaseTtl(i64);
impl From<Duration> for LeaseTtl {
    fn from(src: Duration) -> LeaseTtl {
        let ttl = src.as_secs();
        let ttl = if ttl > i64::MAX as u64 {
            i64::MAX
        } else {
            ttl as i64
        };
        Self(ttl)
    }
}
impl Deref for LeaseTtl {
    type Target = i64;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl LeaseKeeperState {
    /// Name of the current state
    fn name(&self) -> &'static str {
        match self {
            LeaseKeeperState::LeaseExpired(_) => "LeaseExpired",
            LeaseKeeperState::LeaseGrant(_) => "LeaseGrant",
            LeaseKeeperState::Locking(_) => "Locking",
            LeaseKeeperState::Locked(_) => "Locked",
            LeaseKeeperState::KeepAlive(_) => "KeepAlive",
            LeaseKeeperState::Reconnect(_) => "Reconnect",
            LeaseKeeperState::Replaced(_) => "Replaced",
        }
    }
}
impl std::fmt::Display for LeaseKeeperState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
#[derive(Debug)]
struct LeaseExpired;
#[derive(Debug)]
struct LeaseGrant;
#[derive(Debug)]
struct Locking(i64);
#[derive(Debug)]
struct Locked {
    lock_key: String,
    lease_id: i64,
}
#[derive(Debug)]
struct KeepAlive {
    #[allow(dead_code)]
    lock_key: String,
    keeper: LeaseKeeper,
    stream: LeaseKeepAliveStream,
}
#[derive(Debug)]
struct Reconnect(Duration);
impl Default for Reconnect {
    fn default() -> Self {
        Self(Duration::from_secs(1))
    }
}
#[derive(Debug)]
struct Replaced;

/// Trait that defines the specific behaviour for `LeaseKeeper` depending on its state
#[async_trait::async_trait]
trait LeaseLockKeeperClocking<S> {
    /// Clock the state machine in the current state: `state` and return the next state
    async fn clock(&mut self, state: S) -> LockStatesResult;
}

/// The state as a Result<S,S> which is useful to automatically trace errors and also makes it clear
/// when a state transition follows the expected happy path.
type LockStatesResult = Result<LeaseKeeperState, LeaseKeeperState>;
impl From<LockStatesResult> for LeaseKeeperState {
    fn from(result: LockStatesResult) -> Self {
        result.unwrap_or_else(|e| e)
    }
}

#[async_trait::async_trait]
impl LeaseLockKeeperClocking<Reconnect> for EtcdSingletonLock {
    #[tracing::instrument(skip(self, state), err)]
    async fn clock(&mut self, state: Reconnect) -> LockStatesResult {
        tokio::time::sleep(state.0).await;
        let sleep = max(state.0 + Duration::from_secs(1), Duration::from_secs(5));
        if self.client.status().await.is_err() {
            Ok(LeaseKeeperState::Reconnect(Reconnect(sleep)))
        } else {
            Ok(LeaseKeeperState::LeaseExpired(LeaseExpired {}))
        }
    }
}

#[async_trait::async_trait]
impl LeaseLockKeeperClocking<LeaseGrant> for EtcdSingletonLock {
    #[tracing::instrument(skip(self, _state), err)]
    async fn clock(&mut self, _state: LeaseGrant) -> LockStatesResult {
        let lease_id = self.lease_id();
        match self.client.lease_time_to_live(lease_id, None).await {
            Ok(resp) if resp.ttl() >= 0 => Ok(LeaseKeeperState::Locking(Locking(self.lease_id))),
            _ => {
                let lease_ttl = *self.lease_ttl();
                match self
                    .client
                    .lease_grant(lease_ttl, Some(LeaseGrantOptions::new().with_id(lease_id)))
                    .await
                {
                    Ok(resp) => {
                        tracing::info!(
                            lease.id = resp.id(),
                            lease.ttl = resp.ttl(),
                            "Granted new lease",
                        );
                        Ok(LeaseKeeperState::Locking(Locking(resp.id())))
                    }
                    Err(error) => {
                        tracing::error!(
                            lease.id = self.lease_id,
                            error = %error,
                            "Failed to get lease grant",
                        );
                        Err(LeaseKeeperState::Reconnect(Reconnect::default()))
                    }
                }
            }
        }
    }
}

#[async_trait::async_trait]
impl LeaseLockKeeperClocking<LeaseExpired> for EtcdSingletonLock {
    #[tracing::instrument(skip(self, _state), err)]
    async fn clock(&mut self, _state: LeaseExpired) -> LockStatesResult {
        tracing::warn!(lease.id = self.lease_id, "Lease Expired!");
        self.unset_lease().await;

        match self.is_replaced().await {
            Some(true) => Err(LeaseKeeperState::Replaced(Replaced {})),
            Some(false) | None => {
                // etcd might have gone down long enough for us to loose the grant, retry
                self.check_etcd_connection().await?;
                Err(LeaseKeeperState::LeaseGrant(LeaseGrant {}))
            }
        }
    }
}

#[async_trait::async_trait]
impl LeaseLockKeeperClocking<Locking> for EtcdSingletonLock {
    #[tracing::instrument(skip(self, state), err)]
    async fn clock(&mut self, state: Locking) -> LockStatesResult {
        self.check_replaced().await?;
        let lock_key = Self::lock_key(&self.service_name);
        Ok(
            match tokio::time::timeout(
                self.lease_ttl / 2,
                self.client.lock(
                    lock_key.as_str(),
                    Some(LockOptions::new().with_lease(state.0)),
                ),
            )
            .await
            {
                Ok(result) => match result {
                    Ok(result) => {
                        let lock_key = String::from_utf8(result.key().to_vec()).unwrap();
                        self.set_owner_lease(state.0, &lock_key)
                            .await
                            .map_err(|_| LeaseKeeperState::Reconnect(Reconnect::default()))?;
                        LeaseKeeperState::Locked(Locked {
                            lock_key,
                            lease_id: state.0,
                        })
                    }
                    Err(error) => {
                        tracing::warn!(
                            lease.id = self.lease_id,
                            error = %error,
                            "Failed to lock. (lease expired?)",
                        );
                        LeaseKeeperState::LeaseExpired(LeaseExpired {})
                    }
                },
                Err(_) => {
                    tracing::error!(
                        lock.name = %lock_key,
                        lease.id = self.lease_id,
                        "timed out trying to obtain the lock",
                    );
                    LeaseKeeperState::LeaseExpired(LeaseExpired {})
                }
            },
        )
    }
}

#[async_trait::async_trait]
impl LeaseLockKeeperClocking<Locked> for EtcdSingletonLock {
    #[tracing::instrument(skip(self, state), err)]
    async fn clock(&mut self, state: Locked) -> LockStatesResult {
        tracing::info!(
            lock.name = %self.service_name,
            lock.key = %state.lock_key,
            lease.id = state.lease_id,
            "Locked service with lease"
        );
        self.set_lease().await;

        let (keeper, stream) = self
            .client
            .lease_keep_alive(state.lease_id)
            .await
            .map_err(|_e| Err(LeaseKeeperState::Reconnect(Reconnect::default())))?;
        Ok(LeaseKeeperState::KeepAlive(KeepAlive {
            lock_key: state.lock_key,
            keeper,
            stream,
        }))
    }
}

#[async_trait::async_trait]
impl LeaseLockKeeperClocking<KeepAlive> for EtcdSingletonLock {
    #[tracing::instrument(level = "trace", skip(self, state), err)]
    async fn clock(&mut self, mut state: KeepAlive) -> LockStatesResult {
        state
            .keeper
            .keep_alive()
            .await
            .map_err(|_| LeaseKeeperState::Reconnect(Reconnect::default()))?;

        let mut sleep = self.lease_ttl / 2;
        let lease_probe = state
            .stream
            .message()
            .await
            .map_err(|_| LeaseKeeperState::Reconnect(Reconnect::default()))?;

        if let Some(resp) = lease_probe {
            if resp.ttl() <= 0 {
                // we've lost the key, either because another have taken it or etcd
                // was not connectable for the TTL... try again
                return Ok(LeaseKeeperState::LeaseGrant(LeaseGrant {}));
            } else {
                sleep = Duration::from_secs(resp.ttl() as u64 / 2);
            }
        }

        tokio::time::sleep(sleep).await;
        Ok(LeaseKeeperState::KeepAlive(state))
    }
}

#[async_trait::async_trait]
impl LeaseLockKeeperClocking<Replaced> for EtcdSingletonLock {
    #[tracing::instrument(skip(self, _state), err)]
    async fn clock(&mut self, _state: Replaced) -> LockStatesResult {
        eprintln!(
            "Lost lock to another service instance: {}. Giving up...",
            self.service_name
        );
        std::process::exit(128);
    }
}

/// Key used by the store lock api to identify the lock.
/// The key is deleted when the lock is unlocked or if the lease is lost.
#[derive(Debug)]
pub struct StoreLeaseLockKey(ControlPlaneService);
impl StoreLeaseLockKey {
    /// return new `Self` with `name`
    pub fn new(name: &ControlPlaneService) -> Self {
        Self(name.clone())
    }
}
impl ObjectKey for StoreLeaseLockKey {
    type Kind = StorableObjectType;

    fn version(&self) -> ApiVersion {
        ApiVersion::V0
    }
    fn key_type(&self) -> StorableObjectType {
        StorableObjectType::StoreLeaseLock
    }
    fn key_uuid(&self) -> String {
        self.0.to_string()
    }
}

/// Key used to store the last owner ref.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct StoreLeaseOwnerKey(ControlPlaneService);
impl StoreLeaseOwnerKey {
    /// return new `Self` with `kind`
    pub fn new(kind: &ControlPlaneService) -> Self {
        Self(kind.clone())
    }
}
impl ObjectKey for StoreLeaseOwnerKey {
    type Kind = StorableObjectType;

    fn version(&self) -> ApiVersion {
        ApiVersion::V0
    }
    fn key_type(&self) -> StorableObjectType {
        StorableObjectType::StoreLeaseOwner
    }
    fn key_uuid(&self) -> String {
        self.0.to_string()
    }
}

/// A lease owner is the service instance which owns a lease
#[derive(Serialize, Deserialize, Debug)]
pub struct StoreLeaseOwner {
    kind: ControlPlaneService,
    lease_id: String,
    instance_name: String,
}
impl StoreLeaseOwner {
    /// return new `Self` with `kind` and `lease_id`
    pub fn new(kind: &ControlPlaneService, lease_id: i64) -> Self {
        Self {
            kind: kind.clone(),
            lease_id: format!("{lease_id:x}"),
            instance_name: std::env::var("MY_POD_NAME").unwrap_or_default(),
        }
    }
    /// Get the `lease_id` as a hex string
    pub fn lease_id(&self) -> &str {
        &self.lease_id
    }
}
impl StorableObject for StoreLeaseOwner {
    type Key = StoreLeaseOwnerKey;

    fn key(&self) -> Self::Key {
        Self::Key::new(&self.kind)
    }
}

pub(super) type ControlPlaneService = String;
