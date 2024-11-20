use crate::database::Db;
use crate::task::TaskRegistry;
use crate::queue::{
    Queue,
    QueueData,
    QueueStatus,
    QueueListConditions
};
use surrealdb::RecordId;
use tokio::sync::Mutex;
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

type TaskHandles = HashMap<String,WorkerTaskData>;

#[derive(Debug)]
pub struct WorkerTask{
    db: Option<Arc<Db>>
}

#[derive(Debug)]
pub struct WorkerTaskData {
    agent_data: AgentData,
    handle: Option<JoinHandle<()>>
}


impl WorkerTask{

    pub async fn new(data: WorkerTask) -> Self {
        Self {
            ..data
        }
    }
    
    pub async fn add(&self, worker_data: WorkerTaskData, task_handles: &mut TaskHandles,queue_name: String, runtime_id: u64) {
        let key: String = format!("{}-{}",queue_name,runtime_id);
        println!("add: {}",key);
        task_handles.insert(key, worker_data);
    }
    pub async fn remove(&self,task_handles: &mut TaskHandles, queue_name: String, runtime_id: u64) {
        let key: String = format!("{}-{}",queue_name,runtime_id);
        task_handles.remove(&key);
    } 
    pub async fn get_agent(&self,task_handles: &mut TaskHandles, queue_name: String, runtime_id: u64) -> Result<AgentData,String> {
        let key: String = format!("{}-{}",queue_name,runtime_id);
        println!("add: {:?}",task_handles.keys());
        if let Some(item) = task_handles.get(&key).clone() {
            return Ok(item.agent_data.to_owned());
        }
        return Err(format!("unable to get agent data for {}",key));
    } 
    pub async fn abort(&mut self,task_handles: &mut TaskHandles, queue_name: String, runtime_id: u64) -> Result<bool,String> {
        let key: String = format!("{}-{}",queue_name,runtime_id);
        if let Some(item) = task_handles.get(&key).clone() {
            if let Some(handle) = &item.handle {
                handle.abort();
            } 
            return Ok(true);
        }
        return Err(format!("unable to execute abort status for worker task {}",key));
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

    
    pub async fn check_command(&self,task_handles: &TaskHandles) -> Result<Option<AgentData>,String> {
        for hkey in task_handles.keys() {
            let hitem = task_handles.get(hkey).unwrap();
            let agdata: AgentData = hitem.agent_data.clone();
            match self.agent.get(agdata.id.unwrap()).await {
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
                let mut task_handles:HashMap<String,WorkerTaskData> = HashMap::new(); 
                let worker_task: WorkerTask = WorkerTask::new(WorkerTask {
                    db: self.db.clone()
                }).await;
                worker_task.add(
                    WorkerTaskData { 
                        handle: None, 
                        agent_data: queue_agent.clone()
                    },
                    &mut task_handles,
                    queue_name.clone(), 0
                ).await;
                loop {
                    let db: Option<Arc<Db>> = self.db.clone();
                    let got_command = self.check_command(&mut task_handles,).await?;
                    if let Some(mitem) = got_command {
                        if let Some(command) = mitem.command.clone() {
                            println!("Worker Command Received: {:?}",command);
                            let ag_runtime_id: u64 = mitem.runtime_id.clone().unwrap();
                            let ag_queue_id: RecordId = mitem.queue_id.clone().unwrap();
                            match command {
                                Command::TaskTerminate => { 
                                    info!("remove task {:#?}",queue_name.clone()); 
                                    worker_task.remove(&mut task_handles,queue_name.clone(), ag_runtime_id.clone()).await;
                                },
                                Command::TaskTerminate => {
                                    info!("terminated task {:#?}",queue_name.clone());    
                                    worker_task.abort(&mut task_handles,queue_name.clone(), ag_runtime_id.clone()).await?; 
                                    if let Err(error) = worker_task.update_status(
                                        Some(QueueData {
                                            id: Some(ag_queue_id),
                                            status: Some(QueueStatus::Error),
                                            message: Some("task has been terminated".to_string()),
                                            ..Default::default()
                                        }),
                                        Some(AgentData {
                                            id: Some(mitem.id.unwrap()),
                                            status: Some(AgentStatus::Terminated),
                                            command_is_executed: Some(true),
                                            message: None,
                                            ..Default::default()
                                        })
                                    ).await {
                                        error!("tark termination result error [{}]: {}", queue_name.clone(), error);
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
                                    let _worker_task: WorkerTask = WorkerTask::new(WorkerTask {
                                        db: db.clone()
                                    }).await;
                                    // Spawn a new task to process the queue record
                                    if let Err(error) = _worker_task.update_status(   
                                        Some(QueueData {
                                            id: Some(queue_id.clone()),
                                            status: Some(QueueStatus::InProgress),
                                            message: Some(String::new()),
                                            ..Default::default()
                                        }),
                                        None
                                    ).await {
                                        error!("task update result error [{}]: {}", queue_name.clone(), error);
                                    }     
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
                                        let record_name: String = record.name.unwrap();
                                        let record_handler: String = record.handler.unwrap();
                                        let record_id: RecordId = record.id.unwrap();
                                        info!("Received task [{}]", record_name);
                                        match registry.get(record_handler).await {
                                            Ok(handler) => {
                                                info!("Executing task [{}]", record_name);       
                                                let runtime_id: u64 = tokio::task::id().to_string().parse::<u64>().unwrap();                                                 
                                                match _worker_task.get_agent( metric_name.clone(), runtime_id).await {
                                                    Ok(task_agent) => {
                                                        if let Err(error) = _worker_task.update_status(
                                                            None,
                                                            Some(AgentData {
                                                                id: task_agent.id.clone(),
                                                                status: Some(AgentStatus::Running),
                                                                message: None,
                                                                ..Default::default()
                                                            })
                                                        ).await {
                                                            error!("task execution result error [{}]: {}", record_name, error);
                                                        }        
                                                        match handler().run(record.parameters.unwrap()).await {
                                                            Ok(_) => {
                                                                println!("completed=={}",record_id.clone());
                                                                if let Err(error) = _worker_task.update_status(
                                                                    Some(QueueData {
                                                                        id: Some(record_id.clone()),
                                                                        status: Some(QueueStatus::Completed),
                                                                        message: None,
                                                                        ..Default::default()
                                                                    }),
                                                                    Some(AgentData {
                                                                        id: task_agent.id.clone(),
                                                                        status: Some(AgentStatus::Completed),
                                                                        message: None,
                                                                        ..Default::default()
                                                                    })
                                                                ).await {
                                                                    error!("task execution result error [{}]: {}", record_name, error);
                                                                }                                                          
                                                            }
                                                            Err(error) => {
                                                                println!("Error: {}",error);
                                                                if let Err(error) = _worker_task.update_status(
                                                                    Some(QueueData {
                                                                        id: Some(record_id.clone()),
                                                                        status: Some(QueueStatus::Error),
                                                                        message: Some(error.clone()),
                                                                        ..Default::default()
                                                                    }),
                                                                    Some(AgentData {
                                                                        id: task_agent.id.clone(),
                                                                        status: Some(AgentStatus::Error),
                                                                        message: Some(error.clone()),
                                                                        ..Default::default()
                                                                    })
                                                                ).await {
                                                                    error!("task execution result error [{}]: {}", record_name, error);
                                                                }
                                                            }
                                                        }
                                                        //_worker_task.remove(metric_name.clone(),runtime_id.clone()).await;          
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
                                    let runtime_id: u64 = task_handle.id().to_string().parse::<u64>().unwrap();
                                    let agent_name: String = format!("{}-{}",&queue_name,&runtime_id);
                                    match self.agent.register(AgentData {
                                        name: Some(agent_name.clone()),
                                        kind: Some(AgentKind::Task),
                                        status: Some(AgentStatus::Initialized),
                                        server: Some(self.server.clone()),
                                        parent: Some(queue_agent_id.clone()),
                                        author: Some(self.author.clone()),
                                        queue_id: Some(queue_id),
                                        runtime_id: Some(runtime_id.clone()),
                                        ..Default::default()
                                    }).await {
                                        Ok(task_agent) => {      
                                            worker_task.add(WorkerTaskData {
                                                agent_data: task_agent, 
                                                handle: Some(task_handle)
                                            },queue_name.clone(), runtime_id).await;
                                        }
                                        Err(error) => {
                                            println!("{}",error);
                                            return Err(format!("unable to register task agent [{}]: {}",agent_name,error));
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
                Ok(())
            }
            Err(error) => {
                Err(error.to_string())
            }
        }
    }
}
