use serde_json::Value;
use std::collections::HashMap;
use async_trait::async_trait;

/// A trait for handling tasks.
/// 
/// Implement this trait for structs that need to handle specific tasks. The `run` method will be called to execute the task.
///
/// # Methods
/// 
/// - `run`: Executes the task with the provided parameters.
/// 
/// # Parameters
/// - `params`: A `HashMap` of parameters for the task, where the keys are `String` and values are `serde_json::Value`.
/// 
/// # Returns
/// Returns a `Result<(), String>`. On success, returns `Ok(())`. On failure, returns `Err(String)` with an error message.
#[async_trait]
pub trait TaskHandler: Send {
    async fn run(&self, params: HashMap<String, Value>) -> Result<(), String>;
}

/// A factory function type for creating `TaskHandler` instances.
/// 
/// This type alias represents a function that returns a `Box<dyn TaskHandler>`. It is used for registering and retrieving task handlers in the `TaskRegistry`.
pub type TaskHandlerFactory = fn() -> Box<dyn TaskHandler>;

/// A registry for managing task handlers.
/// 
/// The `TaskRegistry` stores and manages task handlers, allowing them to be registered, retrieved, and listed.
/// 
/// # Fields
/// 
/// - `items`: A `HashMap` mapping task handler names to their factory functions.
#[derive(Debug)]
pub struct TaskRegistry {
    items: HashMap<String, TaskHandlerFactory>
}

impl TaskRegistry {
    /// Creates a new `TaskRegistry` instance.
    /// 
    /// Initializes an empty registry for storing task handlers.
    /// 
    /// # Returns
    /// Returns a `TaskRegistry` instance with no registered task handlers.
    pub async fn new() -> Self {
        Self {
            items: HashMap::new()
        }
    }

    /// Registers a new task handler in the registry.
    /// 
    /// Adds a new task handler to the registry, associating it with a specified key.
    /// 
    /// # Parameters
    /// 
    /// - `key`: The name or identifier for the task handler.
    /// - `f`: A factory function that creates a `Box<dyn TaskHandler>`.
    /// 
    /// # Returns
    /// Returns `true` if the registration was successful.
    pub async fn register(&mut self, key: String, f: TaskHandlerFactory) -> bool {
        self.items.insert(key, f);
        true
    }

    /// Retrieves a task handler factory from the registry.
    /// 
    /// Looks up a task handler by its key and returns the associated factory function.
    /// 
    /// # Parameters
    /// 
    /// - `key`: The key of the task handler to retrieve.
    /// 
    /// # Returns
    /// Returns a `Result<TaskHandlerFactory, String>`. On success, returns `Ok(TaskHandlerFactory)`. On failure, returns `Err(String)` with an error message if the key is not found.
    pub async fn get(&self, key: String) -> Result<TaskHandlerFactory, String> {
        match self.items.get_key_value(&key) {
            Some(item) => Ok(item.1.to_owned()),
            None => Err(format!("{} not found in the task registry.", key))
        }
    }
    
    /// Lists all task handlers in the registry.
    /// 
    /// Returns a `HashMap` of all registered task handlers, mapping their keys to factory functions.
    /// 
    /// # Returns
    /// Returns a `HashMap<String, TaskHandlerFactory>` containing all registered task handlers.
    pub async fn tasks(self) -> HashMap<String, TaskHandlerFactory> {
        self.items
    }
}
