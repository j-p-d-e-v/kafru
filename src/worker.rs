use std::sync::Arc;
use crate::task::TaskRegistry;
use crate::queue::{
    Queue,
    QueueData,
    QueueStatus
};

#[derive(Debug)]
pub struct Worker<'a>{
    task_registry: Arc<TaskRegistry>,
    queue: Queue<'a>
}

impl<'a> Worker<'a>{
    pub async fn new(task_registry: TaskRegistry) -> Self {
        Self {
            queue: Queue::new().await,
            task_registry: Arc::new(task_registry)
        }
    }

    pub async fn watch(&self) -> Result<(),String> {
        Ok(())
    }
}