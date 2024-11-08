use crate::database::Db;
use crate::task::TaskRegistry;
use crate::queue::{
    Queue,
    QueueData,
    QueueStatus,
    QueueListConditions
};
use std::sync::Arc;
use tracing::{instrument, info, error};
use tokio::runtime::{Builder, RuntimeMetrics};
use crate::metric::{Metric, MetricData, MetricKind};
use crossbeam::channel::Receiver;
use crate::Command;
use tokio::time::{Duration, Instant};


/// A struct representing a worker that processes tasks from a queue.
///
/// The `Worker` struct manages the execution of tasks by periodically polling a queue and executing tasks
/// with available worker threads.
#[derive(Debug,Clone)]
pub struct Worker {
    rx: Receiver<Command>,
    db: Option<Arc<Db>>
}

impl Worker {
    /// Creates a new instance of the `Worker`.
    ///
    /// # Parameters
    /// 
    /// - `rx`: a channel crossbeam channel ```Receiver```.
    /// 
    /// # Returns
    /// 
    /// Returns a `Worker` instance.
    pub async fn new(rx: Receiver<Command>,db: Option<Arc<Db>>) -> Self {
        Self {
            rx,
            db
        }
    }

    /// Starts watching the task queue and processes tasks.
    ///
    /// This method sets up a multi-threaded Tokio runtime, polls the queue for tasks, and executes them
    /// using available worker threads. It periodically checks the queue, updates task statuses, and
    /// records metrics.
    ///
    /// # Parameters
    /// 
    /// - `task_registry`: An `Arc` of `TaskRegistry` for retrieving task handlers.
    /// - `num_threads`: The number of threads in the worker pool.
    /// - `queue_name`: (Optional) The name of the queue to poll. Defaults to "default" if not provided.
    /// - `poll_interval`: (Optional) The interval, in seconds, between queue polling. Defaults to 15 seconds if not provided.
    ///
    /// # Returns
    /// 
    /// Returns a `Result<(), String>`. On success, returns `Ok(())`. On failure, returns `Err(String)` with an error message.
    #[instrument(skip_all)]
    pub async fn watch(self, task_registry: Arc<TaskRegistry>, num_threads: usize, queue_name: Option<String>, poll_interval: Option<u64>) -> Result<(), String> {
        let poll_interval = poll_interval.unwrap_or(15);
        let queue_name = queue_name.unwrap_or(String::from("default"));
        info!("Thread pool for {} has been created with {} number of threads", queue_name, num_threads);
        // Build a multi-threaded Tokio runtime
        match Builder::new_multi_thread()
            .thread_name(queue_name.clone())
            .worker_threads(num_threads)
            .enable_all()
            .build() {
            Ok(runtime) => {
                let mut is_paused: bool = false;
                loop {
                    let db: Option<Arc<Db>> = self.db.clone();
                    let busy_threads = runtime.metrics().num_alive_tasks();
                    info!("Thread status {}/{}", busy_threads, num_threads);
                    
                    if let Ok(recv) = self.rx.try_recv() {
                        match recv {
                                Command::WorkerResume => {
                                    info!("resumed worker {}",queue_name);
                                    is_paused = false;
                                },
                                Command::WorkerPause => {
                                    info!("paused worker {}",queue_name);
                                    is_paused = true;
                                },
                            Command::WorkerForceShutdown => {
                                info!("forced shutdown worker {}",queue_name);
                                runtime.shutdown_background();
                                break;
                            }
                            Command::WorkerGracefulShutdown => {
                                loop {
                                    if runtime.metrics().num_alive_tasks() == 0 {
                                        info!("graceful shutdown worker {}",queue_name);
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

                    if busy_threads < num_threads {
                        let idle_threads: usize = if busy_threads <= num_threads { num_threads - busy_threads } else { 0 };
                        let queue: Queue = Queue::new(db.clone()).await;
                        match queue.list(
                            QueueListConditions {
                                status: Some(vec![QueueStatus::Waiting.to_string()]),
                                queue: Some(vec![queue_name.clone()]),
                                limit: Some(idle_threads)
                            }).await {
                            Ok(records) => {
                                for record in records {
                                    let db: Option<Arc<Db>> = db.clone();
                                    let registry: Arc<TaskRegistry> = task_registry.clone();
                                    let rt_metrics: RuntimeMetrics = runtime.metrics();
                                    let metric: Metric = Metric::new(db.clone()).await;
                                    let metric_name: String = queue_name.clone();

                                    // Spawn a new task to process the queue record
                                    runtime.spawn(async move {
                                        if let Err(error) = metric.create(MetricData {
                                            name: Some(metric_name),
                                            kind: Some(MetricKind::Worker),
                                            num_alive_tasks: Some(rt_metrics.num_alive_tasks()),
                                            num_workers: Some(rt_metrics.num_workers()),
                                            ..Default::default()
                                        }).await {
                                            info!("Worker metrics error: {}", error);
                                        }

                                        let queue: Queue = Queue::new(db.clone()).await;
                                        let record_name: String = record.name.unwrap();

                                        // Update the queue record to InProgress status
                                        match queue.update(record.id.unwrap(), QueueData {
                                            status: Some(QueueStatus::InProgress),
                                            ..Default::default()
                                        }).await {
                                            Ok(record) => {
                                                info!("Received task [{}]", record_name);

                                                match registry.get(record.handler.unwrap()).await {
                                                    Ok(handler) => {
                                                        info!("Executing task [{}]", record_name);
                                                        match handler().run(record.parameters.unwrap()).await {
                                                            Ok(_) => {
                                                                if let Err(error) = queue.update(record.id.unwrap(), QueueData {
                                                                    status: Some(QueueStatus::Completed),
                                                                    ..Default::default()
                                                                }).await {
                                                                    error!("Task execution result error [{}]: {}", record_name, error);
                                                                }
                                                            }
                                                            Err(error) => {
                                                                if let Err(error) = queue.update(record.id.unwrap(), QueueData {
                                                                    status: Some(QueueStatus::Error),
                                                                    message: Some(error),
                                                                    ..Default::default()
                                                                }).await {
                                                                    error!("Task execution error [{}]: {}", record_name, error);
                                                                }
                                                            }
                                                        }
                                                    }
                                                    Err(error) => {
                                                        error!("Task registry error [{}]: {}", record_name, error);
                                                    }
                                                }
                                                info!("Exiting task [{}]", record_name);
                                            }
                                            Err(error) => {
                                                error!("Queue update error [{}]: {}", record_name, error);
                                            }
                                        }
                                    });
                                    tokio::time::sleep_until(Instant::now() + Duration::from_millis(100)).await;
                                }
                            }
                            Err(error) => {
                                error!("Queue list error: {}", error);
                            }
                        }
                    }
                    info!("Sleeping for {} second(s)", poll_interval);
                    tokio::time::sleep_until(Instant::now() + Duration::from_secs(poll_interval)).await;
                }
                Ok(())
            }
            Err(error) => {
                Err(error.to_string())
            }
        }
    }
}
