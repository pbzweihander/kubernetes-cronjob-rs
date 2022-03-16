mod reconcile;
mod types;

use anyhow::Result;
use futures_util::stream::StreamExt;
use kube::api::{Api, ListParams};
use kube::runtime::controller::{Context, Controller};
use kube::Client;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let client = Client::try_default().await?;

    let cronjob_api = Api::<types::CronJob>::all(client.clone());

    Controller::new(cronjob_api, ListParams::default())
        .shutdown_on_signal()
        .run(
            reconcile::reconcile,
            reconcile::error_policy,
            Context::new(reconcile::Data { client }),
        )
        .for_each(|res| async move {
            match res {
                Ok(o) => tracing::info!("reconciled {:?}", o),
                Err(e) => tracing::warn!("reconcile failed: {}", e),
            }
        })
        .await;

    tracing::info!("controller terminated");

    Ok(())
}
