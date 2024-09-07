use crate::task::TaskRegistry;
use crate::queue::{
    Queue,
    QueueData,
    QueueStatus
};
use std::sync::Arc;
use tracing::{instrument, info, error};

#[derive(Debug)]
pub struct Worker{
    task_registry: Arc<TaskRegistry>,
    test_mode: bool
}

impl Worker{
    pub async fn new(task_registry: TaskRegistry, test_mode: bool) -> Self {
        Self {
            task_registry: Arc::new(task_registry),
            test_mode
        }
    }
    #[instrument(skip_all)]
    pub async fn watch(&self, num_threads: usize, queue_name: Option<String>, checks_delay: Option<u64>) -> Result<(),String> {
        let checks_delay = checks_delay.unwrap_or(15);
        let queue_name = queue_name.unwrap_or(String::from("default"));
        info!("thread pool for {} has been created with {} number of threads",&queue_name, num_threads);
        match tokio::runtime::Builder::new_multi_thread()
        .thread_name(queue_name.clone())
        .worker_threads(num_threads)
        .enable_all()
        .build() {
            Ok(tr)=> {
                loop {
                    let busy_threads = tr.metrics().num_alive_tasks();
                    info!("thread status {}/{}",busy_threads,num_threads);
                    if busy_threads < num_threads {               
                        let idle_threads: usize = num_threads - busy_threads;     
                        let queue: Queue = Queue::new().await;
                        match queue.list(vec![QueueStatus::Waiting.to_string()], vec![queue_name.clone()], Some(idle_threads)).await {
                            Ok(records) => {
                                if records.len() == 0 && self.test_mode && busy_threads == 0 {
                                    tr.shutdown_background();
                                    break;
                                }
                                for record in records {
                                    let registry: Arc<TaskRegistry>  = self.task_registry.clone();
                                    tr.spawn(async move {
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
                Ok(())
            }
            Err(error) => {
                Err(error.to_string())
            }
        }
    }
}