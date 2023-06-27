use crate::infra::{async_trait, Builder, ComponentAction, ComposeTest, Error, Nats, StartOptions};
use composer::ContainerSpec;
use events_api::mbus_nats::message_bus_init;
use std::ffi::OsString;

/*
Steps to check the events.

-- Create nats box.
docker run --network cluster --rm -it natsio/nats-box

-- Check the events in the stream.
nats -s nats://nats:4222 str view events-stream

-- Check the number of events in the stream.
nats -s nats://nats:4222 str ls
*/

#[async_trait]
impl ComponentAction for Nats {
    fn configure(&self, options: &StartOptions, cfg: Builder) -> Result<Builder, Error> {
        Ok(if !options.eventing {
            cfg
        } else {
            cfg.add_container_spec(
                ContainerSpec::from_image("nats", "nats:2.9.20")
                    .with_cmd(&OsString::from("-js"))
                    .with_portmap("4222", "4222"),
            )
        })
    }
    async fn start(&self, options: &StartOptions, cfg: &ComposeTest) -> Result<(), Error> {
        if options.eventing {
            cfg.start("nats").await?;
        }
        if options.eventing || cfg.container_exists("nats").await {
            let _ = message_bus_init("localhost:4222", Some(1)).await;
        }
        Ok(())
    }
    async fn wait_on(&self, options: &StartOptions, cfg: &ComposeTest) -> Result<(), Error> {
        if options.eventing || cfg.container_exists("nats").await {
            let _ = message_bus_init("localhost:4222", Some(1)).await;
        }
        Ok(())
    }
}
