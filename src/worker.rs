use crate::database::Db;
use crate::task::TaskRegistry;
use crate::queue::{
    Queue,
    QueueData,
    QueueStatus,
    QueueListConditions
};
use surrealdb::RecordId;
use std::sync::Arc;
use std::collections::HashMap;
use tokio::task::JoinHandle;
use tracing::{instrument, info, error};
use tokio::runtime::{Builder, RuntimeMetrics};
use crate::metric::{Metric, MetricData, MetricKind};
use crate::Command;
use tokio::time::{Duration, Instant};
use crate::agent::{Agent, AgentData, AgentFilter, AgentKind, AgentStatus};


/// A struct representing a worker that processes tasks from a queue.
///
/// The `Worker` struct manages the execution of tasks by periodically polling a queue and executing tasks
/// with available worker threads.
#[derive(Debug,Clone)]
pub struct Worker {
    db: Option<Arc<Db>>,
    server: String,
    agent: Agent,
    author: String
}


type WorkerTasksHandles = HashMap<String,Option<JoinHandle<()>>>;

#[derive(Debug)]
pub struct WorkerTasks{
    db: Option<Arc<Db>>,
    handles: WorkerTasksHandles
}

impl WorkerTasks{

    pub async fn new(data: WorkerTasks) -> Self {
        Self {
            ..data
        }
    }

    pub async fn to_name(queue_name: &String, runtime_id: &u64) -> String {        
        format!("{}-{}",queue_name,runtime_id)
    }
    
    pub async fn add(&mut self, queue_name: String, runtime_id: u64, handle: Option<JoinHandle<()>>) {
        let name: String = Self::to_name(&queue_name, &runtime_id).await;
        self.handles.insert(name.clone(),handle);
    }
    pub async fn remove(&mut self, name: String) {
        self.handles.remove(&name);
    } 
    
    pub async fn abort(&self, name: String) -> Result<bool,String> {
        if let Some(handle) = self.handles.get(&name).clone() {
            if let Some(h) = handle {
                h.abort();
            } 
            return Ok(true);
        }
        return Err(format!("unable to abort worker task {}",name));
    }

    pub async fn update_status(
        &self,
        queue: Option<QueueData>,
        agent:  Option<AgentData>,
    ) -> Result<(Option<QueueData>,Option<AgentData>),String> {
        let mut queue_data: Option<QueueData> = None;
        let mut agent_data: Option<AgentData> = None;
        if let Some(data) = queue {
            if let Ok(data) = Queue::new(self.db.clone()).await.update(data.id.clone().unwrap(), QueueData { 
                id: None,
                ..data
            } ).await {
                queue_data = Some(data);
            }
            else {
                return Err("unable to update queue status".to_string());
            }
        }
        if let Some(data) = agent {
            if let Ok(data) = Agent::new(self.db.clone()).await.update_by_id(data.id.clone().unwrap(), AgentData {
                id: None,
                ..data
            }).await {
                agent_data = Some(data);
            }    
            else {
                return Err("unable to update agent status".to_string());
            }
        }

        Ok((queue_data,agent_data))
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
    pub async fn new(db: Option<Arc<Db>>, server: String, author: String) -> Self {
        let agent = Agent::new(db.clone()).await;
        Self {
            db,
            server,
            agent,
            author
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
    pub async fn watch(&self, task_registry: Arc<TaskRegistry>, num_threads: usize, queue_name: Option<String>, poll_interval: Option<u64>) -> Result<(), String> {
        let poll_interval = poll_interval.unwrap_or(15);
        let queue_name = format!("{}-{}",self.server,queue_name.unwrap_or(String::from("default")));
        let mut stop_worker : bool = false;
        info!("Thread pool for {} has been created with {} number of threads", queue_name, num_threads);
        // Build a multi-threaded Tokio runtime
        match Builder::new_multi_thread()
            .thread_name(queue_name.clone())
            .worker_threads(num_threads)
            .enable_all()
            .build() {
            Ok(runtime) => {
                 todo!("Improve Code and Logic");
                // todo!("Test");
                // todo!("Document");
                
                let agent: Agent = Agent::new(self.db.clone()).await;
                let queue: Queue = Queue::new(self.db.clone()).await;
                let queue_agent: AgentData = self.agent.register(AgentData {
                    name: Some(WorkerTasks::to_name(&queue_name, &0).await),
                    kind: Some(AgentKind::Queue),
                    server: Some(self.server.clone()),
                    runtime_id: None,
                    status: Some(AgentStatus::Running),
                    ..Default::default()
                }).await?;
                let mut worker_tasks: WorkerTasks = WorkerTasks::new(WorkerTasks {
                    db: self.db.clone(), 
                    handles: HashMap::new()
                }).await;
                worker_tasks.add(queue_name.clone(), 0,  None).await;
                loop {
                    let db: Option<Arc<Db>> = self.db.clone();
                    let agent_names: Vec<String> = worker_tasks.handles.keys().map(|value| value.to_string()).collect();
                    let agents: Vec<AgentData> = self.agent.list(AgentFilter { 
                        names: Some(agent_names),
                        ..Default::default()
                    }).await?;
                    if agents.len() > 0 {       
                        for agent_data in agents {
                            if agent_data.command_is_executed == Some(false) {
                                if let Some(command) = agent_data.command {
                                    let agent_name = agent_data.name.unwrap();
                                    let agent_id = agent_data.id.unwrap();
                                    match command {
                                        Command::TaskRemove => { 
                                            info!("remove task {:#?}",agent_name.clone()); 
                                            worker_tasks.remove(agent_name.clone()).await;
                                            let agent: Agent = Agent::new(db.clone()).await;
                                            if let Err(error) = agent.remove(agent_id,false).await {
                                                error!(error);
                                            }
                                        },
                                        Command::TaskTerminate => {
                                            if let Some(agent_queue_id) = agent_data.queue_id {
                                                info!("terminate task {:#?}",queue_name.clone());   
                                                worker_tasks.abort(agent_name.clone()).await?;                                                 
                                                if let Err(error) = queue.set_status(agent_queue_id.clone(),
                                                    QueueStatus::Error,Some("task has been terminated".to_string())
                                                ).await {
                                                    return Err(error);
                                                }
                                                if let Err(error) = agent.update_by_id(agent_id,AgentData {
                                                    status: Some(AgentStatus::Terminated),
                                                    command_is_executed: Some(true),
                                                    message: None,
                                                    ..Default::default()
                                                }).await {
                                                    error!(error);
                                                }
                                            }
                                            else {
                                                error!("unable to terminate task, queue_id is missing")
                                            }
                                        },
                                        Command::QueueForceShutdown => {
                                            info!("force shutdown queue {}",&queue_name);
                                            stop_worker = true;                                            
                                            break;
                                        }
                                        Command::QueueGracefulShutdown => {
                                            loop {
                                                if runtime.metrics().num_alive_tasks() == 0 {
                                                    info!("graceful shutdown queue {}",queue_name);
                                                    stop_worker = true;
                                                    break;
                                                }
                                                tokio::time::sleep_until(Instant::now() + Duration::from_secs(1)).await;
                                            }
                                        }
                                        _ => {}
                                    }
                                }
                            }
                        }
                    }
                    if stop_worker {
                        break;
                    }
                    let busy_threads = runtime.metrics().num_alive_tasks();
                    info!("Thread status {}/{}", busy_threads, num_threads);
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
                                    let queue_agent_id = queue_agent.id.clone().unwrap();
                                    let queue_id = record.id.clone().unwrap();
                                    let author: String = self.author.clone();
                                    let server: String = self.server.clone();
                                    let queue: Queue = Queue::new(db.clone()).await;

                                    // Set InProgress in order not to be picked up
                                    if let Err(error) = queue.set_status(queue_id.clone(),QueueStatus::InProgress,None).await {
                                        return Err(error);
                                    }
                                    let task_handle = runtime.spawn(async move {
                                        let agent: Agent = Agent::new(db.clone()).await;
                                        let runtime_id: u64 = tokio::task::id().to_string().parse::<u64>().unwrap();                           
                                        let agent_name = format!("{}-{}",&metric_name,&runtime_id);  
                                        if let Err(error) = agent.register(AgentData {
                                            name: Some(agent_name.clone()),
                                            kind: Some(AgentKind::Task),
                                            status: Some(AgentStatus::Initialized),
                                            server: Some(server.clone()),
                                            parent_id: Some(queue_agent_id.clone()),
                                            author: Some(author.clone()),
                                            queue_id: Some(queue_id.clone()),
                                            runtime_id: Some(runtime_id.clone()),
                                            ..Default::default()
                                        }).await {
                                            error!("{}",error);
                                        } 
                                        if let Err(error) = metric.create(MetricData {
                                            name: Some(metric_name.clone()),
                                            kind: Some(MetricKind::Worker),
                                            num_alive_tasks: Some(rt_metrics.num_alive_tasks()),
                                            num_workers: Some(rt_metrics.num_workers()),
                                            ..Default::default()
                                        }).await {
                                            info!("Worker metrics error: {}", error);
                                        }
                                        let record_name: String = record.name.unwrap();
                                        let record_handler: String = record.handler.unwrap();
                                        info!("Received task [{}]", record_name);
                                        match registry.get(record_handler).await {
                                            Ok(handler) => {
                                                info!("Executing task [{}]", record_name);                           
                                                match agent.get_by_name(agent_name.clone(),server.clone()).await {
                                                    Ok(task_agent) => {
                                                        let agent_id: RecordId = task_agent.id.unwrap();
                                                        if let Err(error) = agent.update_by_id(agent_id.clone(),AgentData {
                                                            status: Some(AgentStatus::Running),
                                                            command_is_executed: Some(false),
                                                            message: None,
                                                            ..Default::default()
                                                        }).await {
                                                            error!("task execution result error [{}]: {}", record_name, error);
                                                        }      
                                                        match handler().run(record.parameters.unwrap()).await {
                                                            Ok(_) => {
                                                                if let Err(error) = queue.set_status(queue_id.clone(),QueueStatus::Completed,None).await {
                                                                    error!("task execution result error [{}]: {}", record_name, error);
                                                                }                                                                
                                                                if let Err(error) = agent.update_by_id(agent_id.clone(),AgentData {
                                                                    status: Some(AgentStatus::Completed),
                                                                    command_is_executed: Some(false),
                                                                    message: None,
                                                                    ..Default::default()
                                                                }).await {
                                                                    error!("agent execution result error [{}]: {}", record_name, error);
                                                                }                                                         
                                                            }
                                                            Err(error) => {
                                                                println!("Error: {}",error);
                                                                if let Err(error) = queue.set_status(queue_id.clone(),QueueStatus::Error,Some(error.clone())).await {
                                                                    error!("task execution result error [{}]: {}", record_name, error);
                                                                }                                                                
                                                                if let Err(error) = agent.update_by_id(agent_id.clone(),AgentData {
                                                                    status: Some(AgentStatus::Error),
                                                                    command_is_executed: Some(false),
                                                                    message: Some(error.clone()),
                                                                    ..Default::default()
                                                                }).await {
                                                                    error!("agent execution result error [{}]: {}", record_name, error);
                                                                }    
                                                            }
                                                        }                                                        
                                                        if let Err(error) = agent.send_command(agent_id.clone(), Command::TaskRemove, None, None).await {
                                                            error!("task remove command error [{}]: {}", record_name, error);
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
                                    });
                                    worker_tasks.add(
                                        queue_name.clone(), 
                                        task_handle.id().to_string().parse::<u64>().unwrap(), 
                                        Some(task_handle)
                                    ).await;
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
                if let Err(error) = agent.remove(queue_agent.id.unwrap().clone(),true).await {
                    error!("task agent remove error [{}]: {}", queue_name, error);
                }
                runtime.shutdown_background();
                Ok(())
            }
            Err(error) => {
                Err(error.to_string())
            }
        }
    }
}
