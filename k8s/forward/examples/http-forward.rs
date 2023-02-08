use hyper::service::Service;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let selector = kube_forward::TargetSelector::svc_label("app", "api-rest");
    let target = kube_forward::Target::new(selector, "http", "mayastor");
    let uri = kube_forward::HttpForward::new(target, None)
        .await?
        .uri()
        .await?;

    let proxy = kube_forward::HttpProxy::try_default().await?;
    let mut svc = hyper::service::service_fn(|request: hyper::Request<hyper::body::Body>| {
        let mut proxy = proxy.clone();
        async move { proxy.call(request).await }
    });

    let request = hyper::Request::builder()
        .method("GET")
        .uri(&format!("{uri}/v0/nodes"))
        .body(hyper::Body::empty())
        .unwrap();

    let result = svc.call(request).await?;
    tracing::info!(?result, "http request complete");

    Ok(())
}
