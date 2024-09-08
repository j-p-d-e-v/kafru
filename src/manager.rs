
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
    pub async fn new() -> Self {
        tracing_subscriber::fmt().with_max_level(Level::DEBUG).with_line_number(true).init();
        Self {
            join_set: JoinSet::new()
        }
    }

    pub async fn worker(&mut self, queue_name: String, num_threads: usize, task_registry: Arc<TaskRegistry>, checks_delay: u64) -> Result<(),String> {
        self.join_set.spawn( async move {
            let worker  = Worker::new().await;
            let result = worker.watch(task_registry,num_threads, Some(queue_name),Some(checks_delay)).await;
            assert!(result.is_ok(),"{:?}",result.unwrap_err());
        });
        Ok(())
    }

    pub async fn scheduler(&mut self, scheduler_name: String, checks_delay: u64) -> Result<(),String> {
        self.join_set.spawn( async move {
            let scheduler  = Scheduler::new().await;
            let _ = scheduler.watch(Some(scheduler_name), Some(checks_delay)).await;
        });
        Ok(())
    }

    pub async fn join(self) -> Vec<()> {
        self.join_set.join_all().await
    }

}