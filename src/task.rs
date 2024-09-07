
use serde_json::Value;
use std::collections::HashMap;
use async_trait::async_trait;

#[async_trait]
pub trait TaskHandler: Send {
    async fn run(&self, params: HashMap<String,Value>) -> Result<(),String> ;
}
type TaskHandlerFactory = fn() -> Box<dyn TaskHandler>;

#[derive(Debug)]
pub struct TaskRegistry {
    items: HashMap<String, TaskHandlerFactory>
}

impl TaskRegistry {

    pub async fn new() -> Self {
        Self {
            items: HashMap::new()
        }
    }

    pub async fn register(&mut self,key: String, f: TaskHandlerFactory) -> bool {
        self.items.insert(key, f);
        true
    }

    pub async fn get(&self,key: String) -> Result<TaskHandlerFactory,String> {
        match self.items.get_key_value(&key) {
            Some(item) => Ok(item.1.to_owned()),
            None => Err(format!("{} not found in the task registry.",key))
        }
    }

    pub async fn tasks(self) -> HashMap<String, TaskHandlerFactory> {
        self.items
    }
}
