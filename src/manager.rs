
use crate::scheduler::Scheduler;
use tracing::Level;
use crate::task::TaskRegistry;
use std::sync::Arc;
use crate::worker::Worker;
use tokio::task::JoinSet;
#[derive(Debug,Default)]
pub struct Manager {
    join_set: JoinSet<()>
}

impl Manager {

    /// Initializes the `Manager` struct, enabling the use of worker and scheduler functionalities for task management.
    pub async fn new() -> Self {
        tracing_subscriber::fmt().with_max_level(Level::DEBUG).with_line_number(true).init();
        Self {
            join_set: JoinSet::new()
        }
    }

    /// Launches the worker that polls the task queue and executes tasks.
    pub async fn worker(&mut self, queue_name: String, num_threads: usize, task_registry: Arc<TaskRegistry>, poll_interval: u64) -> Result<(),String> {
        self.join_set.spawn( async move {
            let worker  = Worker::new().await;
            let result = worker.watch(task_registry,num_threads, Some(queue_name),Some(poll_interval)).await;
            assert!(result.is_ok(),"{:?}",result.unwrap_err());
        });
        Ok(())
    }

    /// Launches the scheduler that polls for scheduled tasks and pushes them to the queue for execution.
    pub async fn scheduler(&mut self, scheduler_name: String, poll_interval: u64) -> Result<(),String> {
        self.join_set.spawn( async move {
            let scheduler  = Scheduler::new().await;
            let _ = scheduler.watch(Some(scheduler_name), Some(poll_interval)).await;
        });
        Ok(())
    }

    /// Waits for the scheduler and worker to complete their tasks.
    pub async fn wait(self) -> Vec<()> {
        self.join_set.join_all().await
    }

}