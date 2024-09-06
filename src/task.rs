
use serde_json::Value;
use std::collections::HashMap;
pub trait TaskHandler  {
    fn run(&self, params: HashMap<String,Value>) -> Result<(),String> ;
}
type TaskHandlerFactory = fn() -> Box<dyn TaskHandler>;

#[derive(Debug)]
pub struct TaskRegistry {
    items: HashMap<String, TaskHandlerFactory>
}

impl TaskRegistry {

    pub fn new() -> Self {
        Self {
            items: HashMap::new()
        }
    }

    pub fn register(&mut self,key: String, f: TaskHandlerFactory) -> bool {
        self.items.insert(key, f);
        true
    }

    pub fn tasks(self) -> HashMap<String, TaskHandlerFactory> {
        self.items
    }
}
