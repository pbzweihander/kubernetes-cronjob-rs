#[path = "../types.rs"]
mod types;

use kube::CustomResourceExt;

fn main() {
    println!("{}", serde_yaml::to_string(&types::CronJob::crd()).unwrap())
}
