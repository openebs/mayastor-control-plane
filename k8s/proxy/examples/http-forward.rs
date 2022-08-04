use hyper::service::Service;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let selector = k8s_proxy::TargetSelector::svc_label("app", "api-rest");
    let target = k8s_proxy::Target::new(selector, "http", "mayastor");
    let uri = k8s_proxy::HttpForward::new(target).await?.uri().await?;

    let proxy = k8s_proxy::HttpProxy::try_default().await?;
    let mut svc = hyper::service::service_fn(|request: hyper::Request<hyper::body::Body>| {
        let mut proxy = proxy.clone();
        async move { proxy.call(request).await }
    });

    let request = hyper::Request::builder()
        .method("GET")
        .uri(&format!("{}/v0/nodes", uri))
        .body(hyper::Body::empty())
        .unwrap();

    let result = svc.call(request).await?;
    tracing::info!(?result, "http request complete");

    Ok(())
}
