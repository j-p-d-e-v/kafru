
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

    /// Launches a worker that polls the task queue and executes tasks asynchronously.
    /// 
    /// This function spawns a worker in the background using the provided parameters.
    /// The worker monitors the task queue, executes tasks using the handlers in the task registry,
    /// and continues polling based on the specified interval.
    /// 
    /// # Parameters
    /// 
    /// - `queue_name`:
    ///   The name of the queue/worker to monitor and process tasks.
    /// - `num_threads`:
    ///   The number of threads to spawn for concurrent task execution.
    /// - `task_registry`:
    ///   An `Arc`-wrapped [`TaskRegistry`] containing the handlers for task execution.
    /// - `poll_interval`:
    ///   The duration, in seconds, to pause between polling cycles.
    /// - `db`:
    ///   An optional `Arc`-wrapped database instance for use by the worker.
    /// 
    /// # Returns
    /// 
    /// This function returns a `Result`:
    /// 
    /// - `Ok(())` if the worker was successfully launched.
    /// - `Err(String)` if an error occurs during the worker setup.
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
    ///
    /// This function runs a scheduler in the background, periodically polling for tasks
    /// that are due for execution. The scheduler identifies tasks based on the provided
    /// scheduler name and then enqueues them for processing. The polling interval determines
    /// how often the scheduler checks for tasks.
    ///
    /// # Parameters
    ///
    /// - `scheduler_name`:
    ///   A string representing the name of the scheduler. This is used to identify the
    ///   scheduler instance and may correspond to a specific set of tasks.
    /// - `poll_interval`:
    ///   The duration, in seconds, that the scheduler will pause between polling cycles.
    ///   A lower value increases polling frequency but may use more resources.
    /// - `db`:
    ///   An optional `Arc`-wrapped database instance used for retrieving scheduled tasks
    ///   and their associated data.
    ///
    /// # Returns
    ///
    /// This function returns a `Result`:
    ///
    /// - `Ok(())` if the scheduler was successfully launched and is polling as expected.
    /// - `Err(String)` if an error occurs during the setup or execution of the scheduler.
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