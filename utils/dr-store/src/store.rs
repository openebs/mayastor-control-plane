use crate::{
    error::{Error, Operation, Result},
    layout::Layout,
};
use bytes::Bytes;
use futures::StreamExt;
use object_store::{
    aws::{AmazonS3, AmazonS3Builder, AwsAuthorizer, AwsCredential},
    client::{HttpClient, HttpRequestBody},
    path::Path,
    DynObjectStore, ObjectMeta, ObjectStoreExt, PutPayload, RetryConfig,
};
use std::sync::Arc;

/// Where the bucket is and who this cluster is to it.
#[derive(Clone)]
pub struct S3Config {
    /// Endpoint of the s3 api, scheme included.
    pub endpoint: String,
    /// Region the request signature is scoped to. Stores that do not shard by region
    /// still require the signature to name one.
    pub region: String,
    /// Bucket every key in this relationship lives in.
    pub bucket: String,
    /// The access key id half of the credential.
    pub access_key_id: String,
    /// The secret half. Never logged: see this type's [`std::fmt::Debug`].
    pub secret_access_key: String,
}

impl std::fmt::Debug for S3Config {
    /// Everything but the secret, which must not reach a log even by way of a struct
    /// that happened to be in a span.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Config")
            .field("endpoint", &self.endpoint)
            .field("region", &self.region)
            .field("bucket", &self.bucket)
            .field("access_key_id", &self.access_key_id)
            .field("secret_access_key", &"<redacted>")
            .finish()
    }
}

/// The store a disaster recovery relationship is mediated through.
///
/// Deliberately small - read, write, head, list, delete, and the conditional delete the
/// design requires - so the store can be something other than a bucket without anything
/// above it changing. Not hypothetical: the OEP's `p2p` transport is the peer `agent-dr`
/// answering these calls out of its own volumes, and it could not be an `object_store`
/// backend, having no keys and no bytes to serve.
///
/// Every verb is idempotent. A key either holds those bytes or it does not, so a retry
/// of a call whose answer was lost cannot do damage.
#[async_trait::async_trait]
pub trait DrStore: std::fmt::Debug + Send + Sync {
    /// Where everything in this relationship lives.
    fn layout(&self) -> &Layout;

    /// Read a whole object.
    async fn get(&self, key: &str) -> Result<Bytes>;

    /// What the store knows about a key without transferring it.
    async fn head(&self, key: &str) -> Result<ObjectMeta>;

    /// Write a whole object, replacing whatever was there, and answer with the tag the
    /// store gave it.
    async fn put(&self, key: &str, data: Bytes) -> Result<Option<String>>;

    /// Every key under a prefix, in the store's own order.
    async fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>>;

    /// The prefixes one level below a prefix, each with its trailing separator.
    ///
    /// A delimited listing: what is below each of them is not walked, so the cost is the
    /// number of names at that level rather than of everything under it.
    async fn list_dirs(&self, prefix: &str) -> Result<Vec<String>>;

    /// As [`DrStore::list`], resuming after a key, exclusive.
    ///
    /// A key rather than a cursor the store issued, so a walk can be resumed by a
    /// different process, or by this one after a restart.
    async fn list_after(&self, prefix: &str, after: &str) -> Result<Vec<ObjectMeta>>;

    /// Remove an object. Removing what is not there is not a failure: the call says
    /// what the key should hold next, which is nothing.
    async fn delete(&self, key: &str) -> Result<()>;

    /// Remove an object only if it still carries this tag.
    ///
    /// The one operation the design requires beyond plain verbs, and the one an
    /// implementation can silently not honour - a store that ignores the condition
    /// deletes the object.
    async fn delete_if_match(&self, key: &str, etag: &str) -> Result<()>;

    /// Read a whole object, where absent is an answer rather than a failure.
    async fn get_opt(&self, key: &str) -> Result<Option<Bytes>> {
        match self.get(key).await {
            Ok(data) => Ok(Some(data)),
            Err(error) if error.not_found() => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// Whether a key holds anything.
    async fn exists(&self, key: &str) -> Result<bool> {
        match self.head(key).await {
            Ok(_) => Ok(true),
            Err(error) if error.not_found() => Ok(false),
            Err(error) => Err(error),
        }
    }

    /// Establish that this store evaluates a conditional delete, by watching it refuse
    /// one.
    ///
    /// Unambiguous in the way a conditional `PUT` probe is not: a store that ignores the
    /// header removes the object, so failing the probe announces itself instead of
    /// looking like success. Written to a key the caller owns, cleaned up either way.
    async fn probe_conditional_delete(&self, key: &str) -> Result<bool> {
        self.put(key, Bytes::from_static(b"conditional-delete-probe"))
            .await?;
        // A tag no store will have issued, so honouring the condition means refusing.
        let stale = "\"00000000000000000000000000000000\"";
        let honoured = match self.delete_if_match(key, stale).await {
            Err(Error::Precondition { .. }) => true,
            // It answered as though the tag matched; either way it did not evaluate it.
            Ok(()) => false,
            Err(error) => return Err(error),
        };
        self.delete(key).await?;
        Ok(honoured)
    }
}

/// A relationship's store: the bucket, and where everything in it lives.
///
/// The verbs are `object_store`'s, with this cluster's errors and this relationship's
/// keys. What it adds is the one verb `object_store` has no form of - see
/// [`Store::delete_if_match`].
#[derive(Clone, Debug)]
pub struct Store {
    inner: Arc<DynObjectStore>,
    layout: Layout,
    conditional: Conditional,
}

impl Store {
    /// A store on a bucket.
    pub fn s3(config: &S3Config, layout: Layout) -> Result<Self> {
        if config.bucket.is_empty() {
            return Err(Error::Config {
                reason: "a bucket must be named".to_string(),
            });
        }
        let s3 = AmazonS3Builder::new()
            .with_bucket_name(&config.bucket)
            .with_endpoint(&config.endpoint)
            .with_region(&config.region)
            .with_access_key_id(&config.access_key_id)
            .with_secret_access_key(&config.secret_access_key)
            // Reached by host and port rather than by bucket name, which is every store
            // that is not aws, and the deployer's among them.
            .with_virtual_hosted_style_request(false)
            .with_allow_http(true)
            .with_retry(RetryConfig::default())
            .build()
            .map_err(|error| Error::Config {
                reason: error.to_string(),
            })?;
        let conditional = Conditional::signed(config, &s3);
        Ok(Self {
            inner: Arc::new(s3),
            layout,
            conditional,
        })
    }

    /// A store in memory, for tests.
    ///
    /// Its conditional delete is emulated rather than evaluated by the store - see
    /// [`Conditional::Emulated`] - so a test of anything that races on one wants a real
    /// store instead.
    pub fn memory(layout: Layout) -> Self {
        let inner = Arc::new(object_store::memory::InMemory::new());
        Self {
            inner: inner.clone(),
            layout,
            conditional: Conditional::Emulated(inner),
        }
    }
}

#[async_trait::async_trait]
impl DrStore for Store {
    /// Where everything in this relationship lives.
    fn layout(&self) -> &Layout {
        &self.layout
    }

    /// Read a whole object.
    async fn get(&self, key: &str) -> Result<Bytes> {
        let answer = self
            .inner
            .get(&path(key))
            .await
            .map_err(|error| Error::from_store(Operation::Get, key, error))?;
        answer
            .bytes()
            .await
            .map_err(|error| Error::from_store(Operation::Get, key, error))
    }

    /// What the store knows about a key without transferring it.
    async fn head(&self, key: &str) -> Result<ObjectMeta> {
        self.inner
            .head(&path(key))
            .await
            .map_err(|error| Error::from_store(Operation::Head, key, error))
    }

    /// Write a whole object, replacing whatever was there, and answer with the tag the
    /// store gave it.
    async fn put(&self, key: &str, data: Bytes) -> Result<Option<String>> {
        let answer = self
            .inner
            .put(&path(key), PutPayload::from_bytes(data))
            .await
            .map_err(|error| Error::from_store(Operation::Put, key, error))?;
        Ok(answer.e_tag)
    }

    /// Every key under a prefix, in the store's own order.
    ///
    /// For the callers that know the prefix is bounded - a shard, a relationship's
    /// sides. [`Store::list_after`] is what walks a prefix that is not.
    async fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>> {
        collect(prefix, self.inner.list(Some(&path(prefix)))).await
    }

    /// The prefixes one level below a prefix, each with its trailing separator.
    async fn list_dirs(&self, prefix: &str) -> Result<Vec<String>> {
        let listing = self
            .inner
            .list_with_delimiter(Some(&path(prefix)))
            .await
            .map_err(|error| Error::from_store(Operation::List, prefix, error))?;
        Ok(listing
            .common_prefixes
            .into_iter()
            .map(|prefix| format!("{prefix}/"))
            .collect())
    }

    /// As [`Store::list`], resuming after a key.
    ///
    /// A key rather than a cursor the store issued, so a walk can be resumed by a
    /// different process, or by this one after a restart, without the store having kept
    /// anything for it. Exclusive: the key itself is not served again.
    async fn list_after(&self, prefix: &str, after: &str) -> Result<Vec<ObjectMeta>> {
        let listing = self
            .inner
            .list_with_offset(Some(&path(prefix)), &path(after));
        collect(prefix, listing).await
    }

    /// Remove an object. Removing what is not there is not a failure: the call says
    /// what the key should hold next, which is nothing.
    async fn delete(&self, key: &str) -> Result<()> {
        match self.inner.delete(&path(key)).await {
            Ok(()) => Ok(()),
            Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(error) => Err(Error::from_store(Operation::Delete, key, error)),
        }
    }

    /// Remove an object only if it still carries this tag.
    ///
    /// The one operation the design requires of a store beyond plain verbs, and the one
    /// `object_store` has no form of: its `delete` takes a path and nothing else. So the
    /// request is built here and signed with `object_store`'s own authorizer, rather
    /// than the signing being reproduced.
    ///
    /// It is also the operation an implementation can silently not honour - a store that
    /// ignores the condition deletes the object. [`Store::probe_conditional_delete`] is
    /// what establishes that this one does not, before a relationship relies on it.
    async fn delete_if_match(&self, key: &str, etag: &str) -> Result<()> {
        self.conditional.delete(key, etag).await
    }
}

/// Drain a listing into keys, with this cluster's error on it.
async fn collect(
    prefix: &str,
    listing: futures::stream::BoxStream<'static, object_store::Result<ObjectMeta>>,
) -> Result<Vec<ObjectMeta>> {
    listing
        .map(|entry| entry.map_err(|error| Error::from_store(Operation::List, prefix, error)))
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect()
}

/// How this store answers a conditional delete.
#[derive(Clone, Debug)]
enum Conditional {
    /// A signed `DELETE` carrying `If-Match`, which the store evaluates.
    Signed(Box<Signed>),
    /// Read the tag, compare it here, then delete - for the in-memory store, which has
    /// no request to condition. Not equivalent: the window between the read and the
    /// delete is exactly what the real one closes.
    Emulated(Arc<object_store::memory::InMemory>),
}

/// What it takes to sign one request against the bucket.
///
/// To be removed once `ObjectStore::delete_opts` lands upstream -
/// apache/arrow-rs-object-store#864, open and additive enough for a 0.14.x - at which
/// point this whole type goes and the conditional delete is one call like the rest.
#[derive(Clone, Debug)]
struct Signed {
    client: HttpClient,
    credentials: object_store::aws::AwsCredentialProvider,
    region: String,
    /// `<endpoint>/<bucket>`, the path-style base every key hangs off.
    base: String,
}

impl Conditional {
    fn signed(config: &S3Config, s3: &AmazonS3) -> Self {
        let endpoint = config.endpoint.trim_end_matches('/');
        let bucket = &config.bucket;
        Self::Signed(Box::new(Signed {
            client: HttpClient::new(reqwest::Client::new()),
            credentials: s3.credentials().clone(),
            region: config.region.clone(),
            base: format!("{endpoint}/{bucket}"),
        }))
    }

    async fn delete(&self, key: &str, etag: &str) -> Result<()> {
        match self {
            Self::Signed(signed) => signed.delete(key, etag).await,
            Self::Emulated(store) => {
                let meta = store
                    .head(&path(key))
                    .await
                    .map_err(|error| Error::from_store(Operation::DeleteIfMatch, key, error))?;
                if meta.e_tag.as_deref() != Some(etag) {
                    return Err(Error::Precondition {
                        operation: Operation::DeleteIfMatch,
                        key: key.to_string(),
                    });
                }
                store
                    .delete(&path(key))
                    .await
                    .map_err(|error| Error::from_store(Operation::DeleteIfMatch, key, error))
            }
        }
    }
}

impl Signed {
    async fn delete(&self, key: &str, etag: &str) -> Result<()> {
        let credential = self
            .credentials
            .get_credential()
            .await
            .map_err(|error| Error::from_store(Operation::DeleteIfMatch, key, error))?;
        let mut request = http::Request::builder()
            .method(http::Method::DELETE)
            .uri(format!("{}/{}", self.base, key))
            .header(http::header::IF_MATCH, etag)
            .body(HttpRequestBody::empty())
            .map_err(|error| Error::Config {
                reason: format!("could not build a conditional delete: {error}"),
            })?;
        let credential: &AwsCredential = &credential;
        AwsAuthorizer::new(credential, "s3", &self.region)
            .try_authorize(&mut request, None)
            .map_err(|error| Error::Config {
                reason: format!("could not sign a conditional delete: {error}"),
            })?;

        let answer = self.client.execute(request).await.map_err(|error| {
            // No answer at all: a timeout, a refused connection, a dropped socket.
            Error::Transient {
                operation: Operation::DeleteIfMatch,
                source: object_store::Error::Generic {
                    store: "S3",
                    source: Box::new(error),
                },
            }
        })?;
        match answer.status().as_u16() {
            // 204 is the delete, 200 is a store being generous about it.
            200 | 204 => Ok(()),
            404 => Err(Error::NotFound {
                key: key.to_string(),
            }),
            412 => Err(Error::Precondition {
                operation: Operation::DeleteIfMatch,
                key: key.to_string(),
            }),
            401 | 403 => Err(Error::Denied {
                operation: Operation::DeleteIfMatch,
                key: key.to_string(),
                source: object_store::Error::PermissionDenied {
                    path: key.to_string(),
                    source: format!("the store answered {}", answer.status()).into(),
                },
            }),
            status => Err(Error::Transient {
                operation: Operation::DeleteIfMatch,
                source: object_store::Error::Generic {
                    store: "S3",
                    source: format!("the store answered {status}").into(),
                },
            }),
        }
    }
}

/// A key as the store addresses it.
///
/// `from` rather than `parse`: the layout builds every key this crate uses, and a
/// listing prefix is not always a whole path segment.
fn path(key: &str) -> Path {
    Path::from(key)
}
