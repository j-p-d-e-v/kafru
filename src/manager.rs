
use crate::scheduler::Scheduler;
use crate::task::TaskRegistry;
use std::sync::Arc;
use crate::worker::Worker;
use tokio::task::JoinSet;
use crate::database::Db;

#[derive(Debug)]
pub struct Manager {
    pub join_set: JoinSet<()>,
    pub server: String,
    pub author: String
}

impl Manager {

    /// Initializes the `Manager` struct, enabling the use of worker and scheduler functionalities for task management.
    pub async fn new(server: String, author: String) -> Self {
        Self {
            join_set: JoinSet::new(),
            server,
            author
        }
    }

    /// Launches the worker that polls the task queue and executes tasks.
    /// # Parameters
    /// 
    /// - `queue_name`: the name of the queue/worker.
    /// - `num_threads`: the number of threads to spawn to execute tasks.
    /// - `task_registry`: the registry that contains the task execution handlers.
    /// - `poll_interval`: the value in seconds on how long polling will pause.
    /// - `receiver`: a channel crossbeam channel ```Receiver```.
    pub async fn worker(&mut self, queue_name: String, num_threads: usize, task_registry: Arc<TaskRegistry>, poll_interval: u64, db: Option<Arc<Db>>) -> Result<(),String> {
        let server: String = self.server.clone();
        let author: String = self.author.clone();
        self.join_set.spawn( async move {
            let worker  = Worker::new(db.clone(),server,author).await;
            let result = worker.watch(task_registry,num_threads, Some(queue_name),Some(poll_interval)).await;
            assert!(result.is_ok(),"{:?}",result.unwrap_err());
        });
        Ok(())
    }

    /// Launches the scheduler that polls for scheduled tasks and pushes them to the queue for execution.
    /// # Parameters
    /// 
    /// - `scheduler_name`: the name of the scheduler.
    /// - `poll_interval`: the value in seconds on how long polling will pause.
    /// - `receiver`: a channel crossbeam channel ```Receiver```.
    pub async fn scheduler(&mut self, scheduler_name: String, poll_interval: u64, db: Option<Arc<Db>>) -> Result<(),String> {
        let server: String = self.server.clone();
        let author: String = self.author.clone();
        self.join_set.spawn( async move {
            let scheduler  = Scheduler::new(db.clone(),server,author).await;
            let _ = scheduler.watch(Some(scheduler_name), Some(poll_interval)).await;
        });
        Ok(())
    }

    /// Waits for the scheduler and worker to complete their tasks.
    pub async fn wait(self) -> Vec<()> {
        self.join_set.join_all().await
    }

}