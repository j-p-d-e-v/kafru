use crate::schedule::{
    Schedule, ScheduleData, ScheduleListConditions, ScheduleStatus
};
use chrono::Utc;
use tracing::{instrument, info, error};
use tokio::runtime::{Builder, RuntimeMetrics};
use crate::queue::{
    Queue,
    QueueData,
    QueueStatus
};
use crate::Command;
use crate::metric::{Metric, MetricData, MetricKind};
use tokio::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct Scheduler {
    rx: Receiver<Command>
}
use crossbeam::channel::Receiver;

impl Scheduler{
    pub async fn new(rx: Receiver<Command>) -> Self {
        Self {
            rx
        }
    }

    #[instrument(skip_all)]
    /// Initiates monitoring of schedules and pushes tasks to the queue for execution.
    /// 
    /// # Parameters
    /// 
    /// - `rx`: a channel crossbeam channel ```Receiver```.
    /// - `scheduler_name`: (Optional) The name of the scheduler to watch. If provided, the function will specifically monitor the named scheduler. Default: default
    /// - `poll_interval`: (Optional) The interval, in seconds, at which to poll the scheduler for updates or new schedule. If `None`, the function will use a default polling interval. The interval determines how frequently the scheduler is checked for changes or new schedule. Default: 60 seconds
    pub async fn watch(self, scheduler_name: Option<String>, poll_interval: Option<u64>) -> Result<(),String> {
        let poll_interval = poll_interval.unwrap_or(60);
        let scheduler_name = scheduler_name.unwrap_or(String::from("default"));
        match Builder::new_multi_thread()
        .thread_name(scheduler_name.clone())
        .worker_threads(1)
        .enable_all()
        .build() {
            Ok(runtime)=> {
                let mut is_paused: bool = false;
                loop {
                    if let Ok(recv) = self.rx.try_recv() {
                        match recv {
                            Command::SchedulerResume => {
                                info!("resumed scheduler {}",scheduler_name);
                                is_paused = false;
                            },
                            Command::SchedulerPause => {
                                info!("paused scheduler {}",scheduler_name);
                                is_paused = true;
                            },
                            Command::SchedulerForceShutdown => {
                                info!("forced shutdown scheduler {}",scheduler_name);
                                runtime.shutdown_background();
                                break;
                            }
                            Command::SchedulerGracefulShutdown => {
                                loop {
                                    if runtime.metrics().num_alive_tasks() == 0 {
                                        info!("graceful shutdown scheduler {}",scheduler_name);
                                        break;
                                    }
                                    tokio::time::sleep_until(Instant::now() + Duration::from_secs(1)).await;
                                }
                            }
                            _ => {}
                        }
                    }
                    if is_paused {
                        tokio::time::sleep_until(Instant::now() + Duration::from_secs(1)).await;
                        continue;
                    }

                    let metric: Metric = Metric::new().await;
                    let rt_metrics: RuntimeMetrics = runtime.metrics();
                    let metric_name: String = scheduler_name.clone();
                    runtime.spawn(async move {
                        if let Err(error) = metric.create(MetricData {
                            name: Some(metric_name),
                            kind: Some(MetricKind::Scheduler),
                            num_alive_tasks: Some(rt_metrics.num_alive_tasks()),
                            num_workers: Some(rt_metrics.num_workers()),
                            ..Default::default()
                        }).await {
                            info!("scheduler metrics error: {}",error);
                        }
                        let schedule: Schedule = Schedule::new().await;
                        match schedule.list(ScheduleListConditions {
                            until_schedule: Some(Utc::now()),
                            start_schedule: Some(Utc::now()),
                            upcoming:Some(true),
                            status:Some(vec![ScheduleStatus::Enabled.to_string()]),
                            ..Default::default()
                        }).await {
                            Ok(records) => {
                                info!("got {} records",records.len());
                                let queue: Queue = Queue::new().await;
                                for record in records {
                                    let record_name: String = record.name.unwrap();
                                    let record_status: ScheduleStatus = record.status.unwrap();
                                    if let Err(error) = schedule.update(record.id.unwrap(),ScheduleData {
                                        status: if record.one_time == true { Some(ScheduleStatus::Disabled) } else { Some(record_status) } ,
                                        ..Default::default()
                                    },false).await {
                                        error!("schedule update error [{}]: {}",&record_name,error);
                                    }
                                    if let Err(error) = queue.push(QueueData {
                                        name: Some(record_name.clone()),
                                        queue: record.queue,
                                        parameters: record.parameters,
                                        message: Some(format!("Scheduled at {}",Utc::now())),
                                        handler: record.handler,
                                        status: Some(QueueStatus::Waiting),
                                        ..Default::default()
                                    }).await {
                                        error!("queue scheduling error [{}]: {}",&record_name,error);
                                    }
                                }
                            }
                            Err(error) => {
                                error!("schedule list: {}",error);
                            }
                        }
                        drop(schedule);
                    }).await.unwrap();
                    info!("scheduler sleeping in {} second(s)",poll_interval);
                    tokio::time::sleep_until(Instant::now() + Duration::from_secs(poll_interval)).await;
                    info!("scheduler wake up");
                }
                Ok(())
            }
            Err(error) => {
                Err(error.to_string())
            }
        }
    }
}