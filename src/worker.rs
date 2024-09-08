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

#[derive(Debug)]
pub struct Worker;

impl Worker{
    pub async fn new() -> Self {
        Self
    }
    #[instrument(skip_all)]
    pub async fn watch(self, task_registry: Arc<TaskRegistry>, num_threads: usize, queue_name: Option<String>, checks_delay: Option<u64>) -> Result<(),String> {
        let checks_delay = checks_delay.unwrap_or(15);
        let queue_name = queue_name.unwrap_or(String::from("default"));
        info!("thread pool for {} has been created with {} number of threads",&queue_name, num_threads);
        match Builder::new_multi_thread()
        .thread_name(queue_name.clone())
        .worker_threads(num_threads)
        .enable_all()
        .build() {
            Ok(runtime)=> {
                loop {
                    let busy_threads = runtime.metrics().num_alive_tasks();
                    info!("thread status {}/{}",busy_threads,num_threads);

                    if busy_threads < num_threads {               
                        let idle_threads: usize = if busy_threads <= num_threads { num_threads - busy_threads } else { 0 };     
                        let queue: Queue = Queue::new().await;
                        error!("idle_threads: {}",idle_threads);
                        match queue.list(
                            QueueListConditions {
                                status: Some(vec![QueueStatus::Waiting.to_string()]),
                                queue: Some(vec![queue_name.clone()]),
                                limit: Some(idle_threads)
                            }).await {
                            Ok(records) => {
                                for record in records {
                                    let registry: Arc<TaskRegistry>  = task_registry.clone();
                                    let rt_metrics: RuntimeMetrics = runtime.metrics();
                                    let metric: Metric = Metric::new().await;
                                    runtime.spawn(async move {
                                        if let Err(error) = metric.create(MetricData {
                                            kind: Some(MetricKind::Worker),
                                            num_alive_tasks: Some(rt_metrics.num_alive_tasks()),
                                            num_workers: Some(rt_metrics.num_workers()),
                                            ..Default::default()
                                        }).await {
                                            info!("worker metrics error: {}",error);
                                        }
                                        let queue: Queue = Queue::new().await;
                                        let record_name: String = record.name.unwrap();
                                        match queue.update(record.id.unwrap(),QueueData {
                                            status: Some(QueueStatus::InProgress),
                                            ..Default::default()
                                        }).await {
                                            Ok(record) => {
                                                info!("received task [{}]",&record_name);
                                                match registry.get(record.handler.unwrap()).await {
                                                    Ok(handler) => {
                                                        info!("executing task [{}]",&record_name);
                                                        match handler().run(record.parameters.unwrap()).await {                                                            
                                                            Ok(_) =>{ 
                                                                if let Err(error) = queue.update(record.id.unwrap(),QueueData {
                                                                    status: Some(QueueStatus::Completed),
                                                                    ..Default::default()
                                                                }).await {
                                                                    error!("task execution result error [{}]: {}",&record_name,error);
                                                                }
                                                            }
                                                            Err(error) => {
                                                                if let Err(error) = queue.update(record.id.unwrap(),QueueData {
                                                                    status: Some(QueueStatus::Error),
                                                                    message: Some(error),
                                                                    ..Default::default()
                                                                }).await {
                                                                    error!("task execution error [{}]: {}",&record_name,error);
                                                                }
                                                            }        
                                                        }
                                                    }
                                                    Err(error) => {
                                                        error!("task registry error [{}]: {}",&record_name,error);
                                                    }
                                                }
                                                info!("exiting task [{}]",&record_name);
                                            }
                                            Err(error) => {
                                                error!("queue update error [{}]: {}",&record_name,error);
                                            }
                                        }
                                    });
                                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                                }
                            }
                            Err(error) => {
                                error!("queue list: {}",error);
                            }
                        }
                    }
                    info!("sleeping in {} second(s)",checks_delay);
                    tokio::time::sleep(std::time::Duration::from_secs(checks_delay)).await;
                }
            }
            Err(error) => {
                Err(error.to_string())
            }
        }
    }
}