#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let selector = k8s_proxy::TargetSelector::svc_label("app", "api-rest");
    let target = k8s_proxy::Target::new(selector, "http", "mayastor");
    let pf = k8s_proxy::PortForward::new(target, 30011).await?;

    let handle = pf.port_forward().await?;
    handle.await?;

    Ok(())
}
