use hyper::service::Service;
use tower::Service as TowerSvc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let selector = kube_forward::TargetSelector::svc_label("app", "api-rest");
    let target = kube_forward::Target::new(selector, "https", "mayastor");
    let uri = kube_forward::HttpForward::new(target, None, kube::Client::try_default().await?)
        .uri()
        .await?;

    let proxy = kube_forward::HttpProxy::try_default().await?;
    let svc = hyper::service::service_fn(|request: hyper::Request<kube::client::Body>| {
        let mut proxy = proxy.clone();
        async move { proxy.call(request).await }
    });

    let request = hyper::Request::builder()
        .method("GET")
        .uri(&format!("{uri}/v0/nodes"))
        .body(kube::client::Body::empty())?;

    let result = svc.call(request).await?;
    tracing::info!(?result, "http request complete");

    Ok(())
}
