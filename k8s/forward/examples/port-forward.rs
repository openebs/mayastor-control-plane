#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::fmt().init();

    let selector = kube_forward::TargetSelector::svc_label("app", "api-rest");
    let target = kube_forward::Target::new(selector, "http", "mayastor");
    let pf = kube_forward::PortForward::new(target, 30011).await?;

    let (_, handle) = pf.port_forward().await?;
    handle.await?;

    Ok(())
}
