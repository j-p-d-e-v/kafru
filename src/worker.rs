use crate::database::Db;
use crate::task::TaskRegistry;
use crate::queue::{
    Queue,
    QueueData,
    QueueStatus,
    QueueListConditions
};
use surrealdb::RecordId;
use std::ops::Index;
use std::sync::Arc;
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
    author: String
}


type WorkerTaskHandles = HashMap<String,WorkerTaskHandle>;

#[derive(Debug)]
pub struct WorkerTask{
    db: Option<Arc<Db>>,
    handles: WorkerTaskHandles
}

#[derive(Debug)]
pub struct WorkerTaskHandle {
    id: u64,
    name: String,
    handle: Option<JoinHandle<()>>
}


impl WorkerTask{

    pub async fn new(data: WorkerTask) -> Self {
        Self {
            ..data
        }
    }

    pub async fn to_name(queue_name: &String, runtime_id: &u64) -> String {        
        format!("{}-{}",queue_name,runtime_id)
    }
    
    pub async fn add(&mut self, queue_name: String, runtime_id: u64, handle: Option<JoinHandle<()>>) {
        let name: String = Self::to_name(&queue_name, &runtime_id).await;
        self.handles.insert(name.clone(),WorkerTaskHandle {
            id: runtime_id,
            name,
            handle
        });
    }
    pub async fn remove(&mut self, name: String) {
        self.handles.remove(&name);
    } 
    
    pub async fn abort(&self, name: String) -> Result<bool,String> {
        if let Some(item) = self.handles.get(&name).clone() {
            if let Some(handle) = &item.handle {
                handle.abort();
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

    
    pub async fn check_command(&self,worker_task_handles: &WorkerTaskHandles) -> Result<Option<AgentData>,String> {
        for wt_key in worker_task_handles.keys() {
            if let Some(wt_data) = worker_task_handles.get(wt_key) {
                println!("wt_data.name.clone()={}",wt_data.name.clone());
                match self.agent.get_by_name(wt_data.name.clone()).await {
                    Ok(item) => {
                        if item.command_is_executed == Some(false) {
                            return Ok(Some(item))
                        }
                    }
                    Err(error) => {
                        return Err(error)
                    }
                }
            }
        }
        Ok(None)
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
                // todo!("check if we can avoid accessing the task handles inside the thread. it seems possible to do since the queueid is attached to the agent data.")
                // todo!("Improve Code and Logic");
                // todo!("Test");
                // todo!("Document");
                let queue_agent: AgentData = self.agent.register(AgentData {
                    name: Some(queue_name.clone()),
                    kind: Some(AgentKind::Queue),
                    server: Some(self.server.clone()),
                    runtime_id: None,
                    status: Some(AgentStatus::Running),
                    ..Default::default()
                }).await?;
                let mut worker_task_handles:HashMap<String,WorkerTaskHandle> = HashMap::new(); 
                let mut worker_task: WorkerTask = WorkerTask::new(WorkerTask {
                    db: self.db.clone(), 
                    handles: HashMap::new()
                }).await;
                worker_task.add(queue_name.clone(), 0,  None).await;
                loop {
                    let db: Option<Arc<Db>> = self.db.clone();
                    if let Some(mitem) = self.check_command(&mut worker_task_handles).await? {
                        if let Some(command) = mitem.command.clone() {
                            println!("Worker Command Received: {:?}",command);
                            let ag_queue_id: RecordId = mitem.queue_id.clone().unwrap();
                            let ag_name: String = mitem.name.unwrap();
                            match command {
                                Command::TaskRemove => { 
                                    info!("remove task {:#?}",ag_name.clone()); 
                                    worker_task.remove(ag_name.clone()).await;
                                    let agent: Agent = Agent::new(db.clone()).await;
                                    if let Ok(data) = agent.get_by_name(ag_name.clone()).await  {
                                        if let Err(error) = agent.remove(data.id.unwrap(),false).await {
                                            return Err(error);
                                        }
                                    }
                                },
                                Command::TaskTerminate => {
                                    info!("terminated task {:#?}",queue_name.clone());    
                                    worker_task.abort(ag_name.clone()).await?; 
                                    
                                    let queue: Queue = Queue::new(db.clone()).await;
                                    if let Err(error) = queue.set_status(ag_queue_id.clone(),QueueStatus::Error,Some("task has been terminated".to_string())).await {
                                        return Err(error);
                                    }

                                    let agent: Agent = Agent::new(db.clone()).await;
                                    if let Ok(data) = agent.get_by_name(ag_name.clone()).await  {
                                        if let Err(error) = agent.update_by_id(data.id.unwrap(),AgentData {
                                            status: Some(AgentStatus::Terminated),
                                            command_is_executed: Some(true),
                                            message: None,
                                            ..Default::default()
                                        }).await {
                                            return Err(error);
                                        }

                                    }
                                },
                                Command::QueueForceShutdown => {
                                    info!("forced shutdown queue {}",queue_name.clone());
                                    runtime.shutdown_background();
                                    if let Err(error) = self.agent.remove(mitem.id.clone().unwrap(),true).await {
                                        error!("task agent remove error [{}]: {}", queue_name.clone(), error);
                                    }
                                    break;
                                }
                                Command::QueueGracefulShutdown => {
                                    loop {
                                        if runtime.metrics().num_alive_tasks() == 0 {
                                            info!("graceful shutdown queue {}",queue_name);
                                            break;
                                        }
                                        if let Err(error) = self.agent.remove(mitem.id.clone().unwrap(),true).await {
                                            error!("task agent remove error [{}]: {}", queue_name.clone(), error);
                                        }
                                        tokio::time::sleep_until(Instant::now() + Duration::from_secs(1)).await;
                                    }
                                }
                                _ => {}
                            }
                        }
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
                                        println!("agent_name: {}",agent_name);
                                        if let Err(error) = agent.register(AgentData {
                                            name: Some(agent_name.clone()),
                                            kind: Some(AgentKind::Task),
                                            status: Some(AgentStatus::Initialized),
                                            server: Some(server.clone()),
                                            parent: Some(queue_agent_id.clone()),
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
                                                match agent.get_by_name(agent_name.clone()).await {
                                                    Ok(task_agent) => {
                                                        let agent_id: RecordId = task_agent.id.unwrap();
                                                        if let Err(error) = agent.update_by_id(agent_id.clone(),AgentData {
                                                            status: Some(AgentStatus::Running),
                                                            command_is_executed: Some(true),
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
                                                                    command_is_executed: Some(true),
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
                                                                    command_is_executed: Some(true),
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
                                                        println!("{}",error);
                                                    }
                                                }                                                         
                                            }
                                            Err(error) => {
                                                error!("task registry error [{}]: {}", record_name, error);
                                                println!("{}",error);
                                            }
                                        }
                                        info!("exiting task [{}]", record_name);
                                    });
                                    worker_task.add(
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
                Ok(())
            }
            Err(error) => {
                Err(error.to_string())
            }
        }
    }
}
