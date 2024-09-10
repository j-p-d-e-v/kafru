
use crate::scheduler::Scheduler;
use tracing::Level;
use crate::task::TaskRegistry;
use std::sync::Arc;
use crate::worker::Worker;
use tokio::task::JoinSet;
use crossbeam::channel::{Sender, Receiver};
use crate::Command;

#[derive(Debug)]
pub struct Manager {
    pub join_set: JoinSet<()>,
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
    /// # Parameters
    /// 
    /// - `queue_name`: the name of the queue/worker.
    /// - `num_threads`: the number of threads to spawn to execute tasks.
    /// - `task_registry`: the registry that contains the task execution handlers.
    /// - `poll_interval`: the value in seconds on how long polling will pause.
    /// - `receiver`: a channel crossbeam channel ```Receiver```.
    pub async fn worker(&mut self, queue_name: String, num_threads: usize, task_registry: Arc<TaskRegistry>, poll_interval: u64, receiver: Receiver<Command>) -> Result<(),String> {
        self.join_set.spawn( async move {
            let worker  = Worker::new(receiver.clone()).await;
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
    pub async fn scheduler(&mut self, scheduler_name: String, poll_interval: u64, receiver: Receiver<Command>) -> Result<(),String> {
        self.join_set.spawn( async move {
            let scheduler  = Scheduler::new(receiver.clone()).await;
            let _ = scheduler.watch(Some(scheduler_name), Some(poll_interval)).await;
        });
        Ok(())
    }

    /// Waits for the scheduler and worker to complete their tasks.
    pub async fn wait(self) -> Vec<()> {
        self.join_set.join_all().await
    }

    pub async fn send_command(&self, command: Command, sender: Sender<Command>) -> Result<bool,String> {
        if let Err(error) = sender.send(command) {
            return Err(error.to_string());
        }
        Ok(true)
    }

}