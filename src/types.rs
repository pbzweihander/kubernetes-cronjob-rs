use k8s_openapi::api::batch::v1::JobTemplateSpec;
use k8s_openapi::api::core::v1::ObjectReference;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// CronJobSpec defines the desired state of CronJob
#[derive(Serialize, Deserialize, JsonSchema, CustomResource, Clone, Debug)]
#[kube(
    group = "batch.example.com",
    version = "v1",
    kind = "CronJob",
    namespaced
)]
#[kube(status = "CronJobStatus")]
#[serde(rename_all = "camelCase")]
pub struct CronJobSpec {
    /// The schedule in Cron format, see <https://en.wikipedia.org/wiki/Cron>.
    pub schedule: String,

    /// Optional deadline in seconds for starting the job if it misses scheduled
    /// time for any reason.  Missed jobs executions will be counted as failed ones.
    #[serde(default)]
    pub starting_deadline_seconds: Option<i64>,

    /// Specifies how to treat concurrent executions of a Job.
    /// Valid values are:
    /// - "Allow" (default): allows CronJobs to run concurrently;
    /// - "Forbid": forbids concurrent runs, skipping next run if previous run hasn't finished yet;
    /// - "Replace": cancels currently running job and replaces it with a new one
    #[serde(default)]
    pub concurrency_policy: ConcurrencyPolicy,

    /// This flag tells the controller to suspend subsequent executions, it does
    /// not apply to already started executions.  Defaults to false.
    #[serde(default)]
    pub suspend: bool,

    /// Specifies the job that will be created when executing a CronJob.
    pub job_template: JobTemplateSpec,

    /// The number of successful finished jobs to retain.
    pub successful_jobs_history_limit: Option<usize>,

    /// The number of failed finished jobs to retain.
    pub failed_jobs_history_limit: Option<usize>,
}

#[derive(Serialize, Deserialize, JsonSchema, Clone, Debug, PartialEq, Eq)]
pub enum ConcurrencyPolicy {
    Allow,
    Forbid,
    Replace,
}

impl Default for ConcurrencyPolicy {
    fn default() -> Self {
        ConcurrencyPolicy::Allow
    }
}

#[derive(Serialize, Deserialize, JsonSchema, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct CronJobStatus {
    /// A list of pointers to currently running jobs.
    #[serde(default)]
    pub active: Vec<ObjectReference>,

    /// Information when was the last time the job was successfully scheduled.
    #[serde(default)]
    pub last_schedule_time: Option<Time>,
}
