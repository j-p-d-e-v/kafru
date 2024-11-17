use crate::database::Db;
use crate::task::TaskRegistry;
use crate::queue::{
    Queue,
    QueueData,
    QueueStatus,
    QueueListConditions
};
use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use tokio::task::JoinHandle;
use tracing::{instrument, info, error};
use tokio::runtime::{Builder, RuntimeMetrics};
use crate::metric::{Metric, MetricData, MetricKind};
use crate::Command;
use tokio::time::{Duration, Instant};
use crate::agent::{Agent, AgentData, AgentKind, AgentStatus};


/// A struct representing a worker that processes tasks from a queue.
///
/// The `Worker` struct manages the execution of tasks by periodically polling a queue and executing tasks
/// with available worker threads.
#[derive(Debug,Clone)]
pub struct Worker {
    db: Option<Arc<Db>>,
    server: String,
    agent: Agent,
}

#[derive(Debug)]
pub struct WorkerTask {
    agent_data: AgentData,
    handle: JoinHandle<()>
}

impl WorkerTask {
    
    pub async fn add(tasks: Arc<Mutex<HashMap<String,WorkerTask>>>,queue_name: String, agent_data: AgentData, handle: JoinHandle<()>) {
        let mut data = tasks.lock().unwrap();
        let key: String = format!("{}-{}",queue_name,handle.id().to_string());
        data.insert(key, WorkerTask {
            agent_data,
            handle
        });
    }
    pub async fn remove(tasks: Arc<Mutex<HashMap<String,WorkerTask>>>,queue_name: String, runtime_id: u64) {
        let mut data = tasks.lock().unwrap();
        let key: String = format!("{}-{}",queue_name,runtime_id);
        data.remove(&key);
    } 
    pub async fn get_agent_data(tasks: Arc<Mutex<HashMap<String,WorkerTask>>>,queue_name: String, runtime_id: u64) -> Result<AgentData,String> {
        let data = tasks.lock().unwrap();
        let key: String = format!("{}-{}",queue_name,runtime_id);
        if let Some(item) = data.get(&key).clone() {
            return Ok(item.agent_data.to_owned());
        }
        return Err(format!("unable to get agent data for {}",key));
    } 
    pub async fn abort(tasks: Arc<Mutex<HashMap<String,WorkerTask>>>,queue_name: String, runtime_id: u64) -> Result<bool,String> {
        let data = tasks.lock().unwrap();
        let key: String = format!("{}-{}",queue_name,runtime_id);
        if let Some(item) = data.get(&key).clone() {
            item.handle.abort();
            return Ok(true);
        }
        return Err(format!("unable to get data for {}",key));
    } 
    pub async fn is_finished(tasks: Arc<Mutex<HashMap<String,JoinHandle<()>>>>,queue_name: String, runtime_id: u64) -> Result<bool,String> {
        let data = tasks.lock().unwrap();
        let key: String = format!("{}-{}",queue_name,runtime_id);
        if let Some(handle) = data.get(&key).clone() {
            handle.is_finished();
            return Ok(true);
        }
        return Err(format!("unable to get data for {}",key));
    } 

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
    pub async fn new(db: Option<Arc<Db>>, server: String) -> Self {
        let agent = Agent::new(db.clone()).await;
        Self {
            db,
            server,
            agent,
        }
    }

    pub async fn check_command(&self,task_handles: Arc<Mutex<HashMap<String,WorkerTask>>>) {
        todo!("test");
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
    pub async fn watch(&self, task_registry: Arc<TaskRegistry>, num_threads: usize, queue_name: Option<String>, poll_interval: Option<u64>) -> Result<(), String> {
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
                let queue_agent: AgentData = self.agent.register(AgentData {
                    name: queue_name.clone(),
                    kind: AgentKind::Queue,
                    server: self.server.clone(),
                    runtime_id: 0,
                    status: AgentStatus::Running,
                    ..Default::default()
                }).await?;
                let task_handles: Arc<Mutex<HashMap<String,WorkerTask>>> = Arc::new(Mutex::new(HashMap::new())); 
                loop {
                    let db: Option<Arc<Db>> = self.db.clone();
                    let busy_threads = runtime.metrics().num_alive_tasks();
                    info!("Thread status {}/{}", busy_threads, num_threads);
                    let _task_handles = task_handles.clone();  
                    self.check_command(task_handles.clone()).await;                  
                    //if let Ok(recv) = self.rx.try_recv() {
                    //    match recv {
                    //        Command::QueueResume => {
                    //            info!("resumed queue {}",queue_name);
                    //            is_paused = false;
                    //        },
                    //        Command::QueuePause => {
                    //            info!("paused queue {}",queue_name);
                    //            is_paused = true;
                    //        },
                    //        Command::QueueForceShutdown => {
                    //            info!("forced shutdown queue {}",queue_name);
                    //            runtime.shutdown_background();
                    //            break;
                    //        }
                    //        Command::QueueGracefulShutdown => {
                    //            loop {
                    //                if runtime.metrics().num_alive_tasks() == 0 {
                    //                    info!("graceful shutdown queue {}",queue_name);
                    //                    break;
                    //                }
                    //                tokio::time::sleep_until(Instant::now() + Duration::from_secs(1)).await;
                    //            }
                    //        }
                    //        _ => {}
                    //    }
                    //}
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
                                    let agent: Agent = Agent::new(db.clone()).await;
                                    let registry: Arc<TaskRegistry> = task_registry.clone();
                                    let rt_metrics: RuntimeMetrics = runtime.metrics();
                                    let metric: Metric = Metric::new(db.clone()).await;
                                    let metric_name: String = queue_name.clone();
                                    let queue_id = queue_agent.id.clone().unwrap();
                                    let _task_handles: Arc<Mutex<HashMap<String, WorkerTask>>> = task_handles.clone();
                                    // Spawn a new task to process the queue record
                                    let task_handle = runtime.spawn(async move {
                                        if let Err(error) = metric.create(MetricData {
                                            name: Some(metric_name.clone()),
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
                                                        let runtime_id: u64 = tokio::task::id().to_string().parse::<u64>().unwrap();
                                                        
                                                        match WorkerTask::get_agent_data(_task_handles.clone(), metric_name.clone(), runtime_id).await {
                                                            Ok(task_agent) => {
                                                                if let Err(error) = agent.update_by_id(task_agent.id.clone().unwrap(),AgentData {
                                                                    status: AgentStatus::Running,
                                                                    ..task_agent.clone()
                                                                }).await {
                                                                    error!("task agent update error [{}]: {}", record_name, error);
                                                                }     
                                                                match handler().run(record.parameters.unwrap()).await {
                                                                    Ok(_) => {
                                                                        if let Err(error) = queue.update(record.id.unwrap(), QueueData {
                                                                            status: Some(QueueStatus::Completed),
                                                                            ..Default::default()
                                                                        }).await {
                                                                            error!("task execution result error [{}]: {}", record_name, error);
                                                                        }
                                                                        if let Err(error) = agent.update_by_id(task_agent.id.clone().unwrap(),AgentData {
                                                                            status: AgentStatus::Completed,
                                                                            ..task_agent
                                                                        }).await {
                                                                            error!("task agent update error [{}]: {}", record_name, error);
                                                                        }                                                              
                                                                        WorkerTask::remove(_task_handles.clone(),metric_name.clone(),runtime_id.clone()).await;
                                                                    }
                                                                    Err(error) => {
                                                                        if let Err(error) = queue.update(record.id.unwrap(), QueueData {
                                                                            status: Some(QueueStatus::Error),
                                                                            message: Some(error),
                                                                            ..Default::default()
                                                                        }).await {
                                                                            error!("task execution error [{}]: {}", record_name, error);
                                                                        }
                                                                    }
                                                                }  
                                                            }
                                                            Err(error) => {
                                                                error!(error);
                                                            }
                                                        }                                                         
                                                    }
                                                    Err(error) => {
                                                        error!("task registry error [{}]: {}", record_name, error);
                                                    }
                                                }
                                                info!("exiting task [{}]", record_name);
                                            }
                                            Err(error) => {
                                                error!("queue update error [{}]: {}", record_name, error);
                                            }
                                        }
                                    });                 
                                    let runtime_id: u64 = task_handle.id().to_string().parse::<u64>().unwrap();
                                    let agent_name: String = format!("{}-{}",&queue_name,&runtime_id);
                                    match self.agent.register(AgentData {
                                        name: format!("{}-{}",&queue_name,&runtime_id),
                                        kind: AgentKind::Task,
                                        status: AgentStatus::Initialized,
                                        parent: Some(queue_id.clone()),
                                        runtime_id: runtime_id.clone(),
                                        ..Default::default()
                                    }).await {
                                        Ok(task_agent) => {
                                            let _task_handles: Arc<Mutex<HashMap<String, WorkerTask>>> = task_handles.clone();                      
                                            WorkerTask::add(_task_handles.clone(),queue_name.clone(), task_agent, task_handle).await;
                                        }
                                        Err(error) => {
                                            error!("{}",error);
                                            return Err(format!("unable to register task agent {}",agent_name));
                                        }
                                    }   
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
                //Ok(())
            }
            Err(error) => {
                Err(error.to_string())
            }
        }
    }
}
