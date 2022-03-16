use std::cmp::Ordering;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration as StdDuration;

use chrono::{DateTime, Duration, Utc};
use cron::Schedule;
use futures_util::stream::{FuturesUnordered, StreamExt, TryStreamExt};
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, OwnerReference, Time};
use kube::api::{Api, DeleteParams};
use kube::runtime::controller::{Context, ReconcilerAction};
use kube::{Resource, ResourceExt};
use thiserror::Error;

use crate::types::{ConcurrencyPolicy, CronJob, CronJobStatus};

const SCHEDULED_TIME_ANNOTATION: &str = "batch.example.com/scheduled-at";

pub struct Data {
    pub client: kube::Client,
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("MissingObjectKey: {0}")]
    MissingObjectKey(&'static str),
    #[error("Failed to list child jobs: {0}")]
    ListChildJobsFailed(#[source] kube::Error),
    #[error("Failed to serialize status: {0}")]
    SerializeStatusFailed(serde_json::Error),
    #[error("Failed to patch status: {0}")]
    PatchStatusFailed(#[source] kube::Error),
    #[error("Failed to parse cron schedule: {0}")]
    ParseScheduleFailed(#[source] cron::error::Error),
    #[error("Creation timestamp not found")]
    CreationTimestampNotFound,
    #[error("Time is out of range")]
    TimeOutOfRange(#[source] time::OutOfRangeError),
    #[error("Next schedule not found")]
    NextScheduleNotFound,
    #[error("Failed to delete active job: {0}")]
    DeleteActiveJobFailed(#[source] kube::Error),
    #[error("Failed to create job: {0}")]
    CreateJobFailed(#[source] kube::Error),
}

pub async fn reconcile(
    cron_job: Arc<CronJob>,
    ctx: Context<Data>,
) -> Result<ReconcilerAction, Error> {
    let mut cron_job = (*cron_job).clone();
    let client = &ctx.into_inner().client;

    let cron_job_api = Api::<CronJob>::namespaced(client.clone(), &cron_job.namespace().unwrap());
    let job_api = Api::<Job>::namespaced(client.clone(), &cron_job.namespace().unwrap());

    let child_jobs = job_api
        .list(&Default::default())
        .await
        .map_err(Error::ListChildJobsFailed)?
        .into_iter()
        .filter(|job| {
            job.metadata
                .owner_references
                .as_ref()
                .and_then(|owner_references| {
                    owner_references.iter().find(|owner_reference| {
                        Some(&owner_reference.uid) == cron_job.uid().as_ref()
                    })
                })
                .is_some()
        })
        .collect::<Vec<_>>();

    let mut active_jobs = Vec::new();
    let mut successful_jobs = Vec::new();
    let mut failed_jobs = Vec::new();

    let mut most_recent_time = None;

    for child_job in child_jobs {
        let finished_type = get_job_finished_type(&child_job);
        match finished_type.as_deref() {
            None => active_jobs.push(child_job.clone()),
            Some("Complete") => successful_jobs.push(child_job.clone()),
            Some("Failed") => failed_jobs.push(child_job.clone()),
            _ => {}
        }
        let scheduled_time_for_job = match get_scheduled_time_for_job(&child_job) {
            Ok(time) => time,
            Err(error) => {
                tracing::error!(%error, ?child_job, "unable to parse schedule time for child job");
                continue;
            }
        };
        if most_recent_time.is_none() || most_recent_time.unwrap() < scheduled_time_for_job {
            most_recent_time = Some(scheduled_time_for_job);
        }
    }

    tracing::info!(
        active_jobs = active_jobs.len(),
        successful_jobs = successful_jobs.len(),
        failed_jobs = failed_jobs.len(),
        "job count"
    );

    cron_job.status = Some(CronJobStatus {
        active: active_jobs.iter().map(|job| job.object_ref(&())).collect(),
        last_schedule_time: most_recent_time.map(Time),
    });

    cron_job_api
        .replace_status(
            &cron_job.name(),
            &Default::default(),
            serde_json::to_vec(&cron_job).map_err(Error::SerializeStatusFailed)?,
        )
        .await
        .map_err(Error::PatchStatusFailed)?;

    if let Some(failed_jobs_history_limit) = cron_job.spec.failed_jobs_history_limit.as_ref() {
        failed_jobs.sort_unstable_by(|left, right| {
            match (
                left.status
                    .as_ref()
                    .and_then(|status| status.start_time.as_ref()),
                right
                    .status
                    .as_ref()
                    .and_then(|status| status.start_time.as_ref()),
            ) {
                (None, None) => Ordering::Equal,
                (None, Some(_)) => Ordering::Less,
                (Some(_), None) => Ordering::Greater,
                (Some(left_time), Some(right_time)) => left_time.0.cmp(&right_time.0),
            }
        });
        failed_jobs
            .iter()
            .rev()
            .skip(*failed_jobs_history_limit)
            .map(|job| {
                let job_api = job_api.clone();
                async move {
                    let res = job_api.delete(&job.name(), &Default::default()).await;
                    if let Err(error) = res {
                        tracing::error!(%error, ?job, "unable to delete old failed job");
                    }
                }
            })
            .collect::<FuturesUnordered<_>>()
            .collect::<()>()
            .await;
    }

    if let Some(successful_jobs_history_limit) =
        cron_job.spec.successful_jobs_history_limit.as_ref()
    {
        successful_jobs.sort_unstable_by(|left, right| {
            match (
                left.status
                    .as_ref()
                    .and_then(|status| status.start_time.as_ref()),
                right
                    .status
                    .as_ref()
                    .and_then(|status| status.start_time.as_ref()),
            ) {
                (None, None) => Ordering::Equal,
                (None, Some(_)) => Ordering::Less,
                (Some(_), None) => Ordering::Greater,
                (Some(left_time), Some(right_time)) => left_time.0.cmp(&right_time.0),
            }
        });
        successful_jobs
            .iter()
            .rev()
            .skip(*successful_jobs_history_limit)
            .map(|job| {
                let job_api = job_api.clone();
                async move {
                    let res = job_api.delete(&job.name(), &Default::default()).await;
                    if let Err(error) = res {
                        tracing::error!(%error, ?job, "unable to delete old successful job");
                    }
                }
            })
            .collect::<FuturesUnordered<_>>()
            .collect::<()>()
            .await;
    }

    if cron_job.spec.suspend {
        tracing::info!("cronjob suspended, skipping");
        return Ok(ReconcilerAction {
            requeue_after: None,
        });
    }

    let now = Utc::now();

    let (missed_run, next_run) = get_next_schedule(&cron_job, now)?;

    let requeue_after = if now < next_run {
        next_run - now
    } else {
        Duration::seconds(0)
    };
    let scheduled_reconciler_action = ReconcilerAction {
        requeue_after: Some(requeue_after.to_std().map_err(Error::TimeOutOfRange)?),
    };
    let scheduling_span = tracing::info_span!("scheduling", %now, %next_run);
    let _scheduling_span_enter = scheduling_span.enter();

    let missed_run = if let Some(run) = missed_run {
        run
    } else {
        tracing::info!("no upcoming scheduled times, sleeping until next");
        return Ok(scheduled_reconciler_action);
    };

    let current_run_span = tracing::info_span!("current run", %missed_run);
    let _current_run_span_enter = current_run_span.enter();

    let too_late =
        if let Some(starting_deadline_seconds) = cron_job.spec.starting_deadline_seconds.as_ref() {
            (missed_run + Duration::seconds(*starting_deadline_seconds)) < now
        } else {
            false
        };
    if too_late {
        tracing::info!("missed starting deadline for last run, sleeping till next");
        return Ok(scheduled_reconciler_action);
    }

    if cron_job.spec.concurrency_policy == ConcurrencyPolicy::Forbid && !active_jobs.is_empty() {
        tracing::info!(
            num_active = active_jobs.len(),
            "concurrency policy blocks concurrent runs, skipping"
        )
    }

    if cron_job.spec.concurrency_policy == ConcurrencyPolicy::Replace {
        active_jobs
            .iter()
            .map(|job| {
                let job_api = job_api.clone();
                async move {
                    job_api
                        .delete(&job.name(), &DeleteParams::background())
                        .await
                        .map(|_| ())
                }
            })
            .collect::<FuturesUnordered<_>>()
            .try_collect::<_>()
            .await
            .map_err(Error::DeleteActiveJobFailed)?;
    }

    let job = construct_job_for_cron_job(&cron_job, missed_run)?;

    job_api
        .create(&Default::default(), &job)
        .await
        .map_err(Error::CreateJobFailed)?;

    tracing::info!(?job, "created Job for CronJob run");

    Ok(scheduled_reconciler_action)
}

pub fn error_policy(error: &Error, _ctx: Context<Data>) -> ReconcilerAction {
    tracing::error!(%error);
    ReconcilerAction {
        requeue_after: Some(StdDuration::from_secs(3)),
    }
}

fn get_job_finished_type(job: &Job) -> Option<String> {
    for condition in job
        .status
        .clone()
        .unwrap_or_default()
        .conditions
        .unwrap_or_default()
    {
        if (condition.type_ == "Complete" || condition.type_ == "Failed")
            && condition.status == "True"
        {
            return Some(condition.type_);
        }
    }
    None
}

fn get_scheduled_time_for_job(job: &Job) -> Result<DateTime<Utc>, anyhow::Error> {
    let time_raw = job
        .annotations()
        .get(SCHEDULED_TIME_ANNOTATION)
        .ok_or_else(|| anyhow::Error::msg("failed to find scheduled time annotation"))?;
    if time_raw.is_empty() {
        return Err(anyhow::Error::msg("scheduled time annotation is empty"));
    }
    let time_parsed = DateTime::parse_from_rfc3339(&*time_raw)?;
    Ok(time_parsed.with_timezone(&Utc))
}

/// returns (last missed time, next time)
fn get_next_schedule(
    cron_job: &CronJob,
    now: DateTime<Utc>,
) -> Result<(Option<DateTime<Utc>>, DateTime<Utc>), Error> {
    let sched = Schedule::from_str(&cron_job.spec.schedule).map_err(Error::ParseScheduleFailed)?;
    let mut earlist_time = if let Some(time) = cron_job
        .status
        .clone()
        .and_then(|status| status.last_schedule_time)
    {
        time.0
    } else {
        cron_job
            .meta()
            .creation_timestamp
            .clone()
            .ok_or(Error::CreationTimestampNotFound)?
            .0
    };

    if let Some(starting_deadline_seconds) = cron_job.spec.starting_deadline_seconds.as_ref() {
        let scheduling_deadline = now - Duration::seconds(*starting_deadline_seconds);

        if scheduling_deadline > earlist_time {
            earlist_time = scheduling_deadline;
        }
    }
    if earlist_time > now {
        return Ok((
            None,
            sched
                .after(&earlist_time)
                .next()
                .ok_or(Error::NextScheduleNotFound)?,
        ));
    }

    let last_missed = sched
        .after(&earlist_time)
        .take(100)
        .filter(|t| t < &now)
        .last();

    Ok((
        last_missed,
        sched
            .after(&now)
            .next()
            .ok_or(Error::NextScheduleNotFound)?,
    ))
}

fn construct_job_for_cron_job(
    cron_job: &CronJob,
    scheduled_time: DateTime<Utc>,
) -> Result<Job, Error> {
    let name = format!("{}-{}", cron_job.name(), scheduled_time.timestamp());

    let job_template_spec = cron_job.spec.job_template.clone();
    let mut job = Job {
        metadata: ObjectMeta {
            name: Some(name),
            owner_references: Some(vec![OwnerReference {
                controller: Some(true),
                ..object_to_owner_reference::<CronJob>(cron_job.metadata.clone())?
            }]),
            ..job_template_spec.metadata.unwrap_or_default()
        },
        spec: job_template_spec.spec,
        status: None,
    };
    job.metadata
        .annotations
        .get_or_insert(Default::default())
        .insert(
            SCHEDULED_TIME_ANNOTATION.to_string(),
            scheduled_time.to_rfc3339(),
        );

    Ok(job)
}

fn object_to_owner_reference<K: Resource<DynamicType = ()>>(
    meta: ObjectMeta,
) -> Result<OwnerReference, Error> {
    Ok(OwnerReference {
        api_version: K::api_version(&()).to_string(),
        kind: K::kind(&()).to_string(),
        name: meta.name.ok_or(Error::MissingObjectKey(".metadata.name"))?,
        uid: meta.uid.ok_or(Error::MissingObjectKey(".metadata.uid"))?,
        ..OwnerReference::default()
    })
}
