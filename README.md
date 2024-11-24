# kafru

**kafru** is a Rust crate designed for efficient task queue management, inspired by the task queue systems found in other programming environments. The name "kafru" is derived from the "Kafra" system in the popular game Ragnarok Online. 

This library offers robust background task execution with parallel processing through multithreading, enabling tasks to run concurrently and efficiently. Additionally, **kafru** features advanced scheduling capabilities, utilizing cron for precise task timing. It integrates seamlessly with SurrealDB for comprehensive management of queues, metrics, and schedules, ensuring reliable and organized task execution.

![Cute cat GIF](kafru-overview.gif)

## Features

- **Task Registry**: Manage and organize a collection of structs designed for task execution.
- **Scheduler**: Utilize cron-based scheduling to define when tasks are pushed to the queue, with options to set start and end times.
- **Queue**: Maintain a list of tasks awaiting execution.
- **Worker**: Execute tasks from the queue, referencing the task registry to determine the appropriate struct for each task.
- **SurrealDB Integration**: Store and manage scheduling details, queue information, and metrics using SurrealDB.

## Installation

```
cargo add kafru
```

## Usage

### 1. Run SurrealDB

For testing, you can start SurrealDB using an in-memory database:

```bash
cargo test -- --nocapture
```

```bash
surreal start -u kafru_admin -p kafru_password -b 0.0.0.0:4030 --allow-all memory
```

For running SurrealDB in file mode, refer to the [SurrealDB documentation](https://surrealdb.com/docs/surrealdb/introduction/start).

---

### 2. Environment Variables

The following environment variables can be used to configure the database:

| Key                  | Description                                                   | Default Value      |
|----------------------|---------------------------------------------------------------|--------------------|
| `KAFRU_DB_USERNAME`  | The database username.                                         | `kafru_admin`      |
| `KAFRU_DB_PASSWORD`  | The database password.                                         | `kafru_password`   |
| `KAFRU_DB_PORT`      | The port number of the database.                               | `4030`             |
| `KAFRU_DB_HOST`      | The database host or IP address.                               | `127.0.0.1`        |
| `KAFRU_DB_NAMESPACE` | The database namespace, useful for separating production and testing databases. | `kafru`            |
| `KAFRU_DB_NAME`      | The database name.                                             | `kafru_db`         |

---

## Registering Task Structs to the Task Registry

The task registry stores a collection of task structs that contain the logic to be executed.

### 3. Create a Task Struct

- Your struct must implement the `TaskHandler` trait.
- Define the code to execute within the `async fn run(&self, _params: std::collections::HashMap<String, Value>) -> Result<(), String>` method.

#### Sample

```rust
use async_trait::async_trait;
use kafru::task::TaskHandler;
use serde_json::Value;
use std::collections::HashMap;

pub struct MySampleStruct;

#[async_trait]
impl TaskHandler for MySampleStruct {
    async fn run(&self, _params: HashMap<String, Value>) -> Result<(), String> {
        let x = 1; // Replace this with your logic.
        let total = x + 1;
        // Perform your task logic here.
        Ok(())
    }
}
```

### 4. Register the Struct to the Task Registry

- After defining the task struct, register it in the task registry by providing a name for the struct.

#### Sample

```rust
use async_trait::async_trait;
use kafru::task::{TaskHandler, TaskRegistry};
use serde_json::Value;
use std::collections::HashMap;

pub struct MySampleStruct;

#[async_trait]
impl TaskHandler for MySampleStruct {
    async fn run(&self, _params: HashMap<String, Value>) -> Result<(), String> {
        // Task logic here.
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    let mut task_registry: TaskRegistry = TaskRegistry::new().await;
    task_registry.register("mytesthandler".to_string(), || Box::new(MySampleStruct)).await;
}
```

---

### 5. Running the Worker and Scheduler via the Manager

To execute tasks using both the worker (watcher) and the scheduler, follow these steps:

```rust
use std::sync::Arc;
use kafru::manager::Manager;
use kafru::task::TaskRegistry;
use std::sync::Arc;
use crate::database::Db;

// Initialize database instance
let db_instance = Db::new(None).await;

// Wrap the database instance in an Arc
let db: Arc<Db> = Arc::new(db_instance.unwrap());

// Declare the server e.g. MYLOCALHOSTABC
let server: String = "MYLOCALHOSTABC".to_string();

// Declare the author name.
let author: String = "Juan dela Cruz".to_string();

// Initialize the Manager struct.
let mut manager = Manager::new(server.clone(),author.clone()).await;

// Initialize the Task Registry struct and register the task.
let mut task_registry: TaskRegistry = TaskRegistry::new().await;
task_registry.register("mytesthandler".to_string(), || {
    Box::new(MySampleStruct {
        message: "Hello World".to_string(),
    })
}).await;

// Share the task_registry using Arc for thread safety.
let task_registry: Arc<TaskRegistry> = Arc::new(task_registry);

// Run the Worker, specifying the queue name, number of threads, the task registry, task poll interval (in seconds), and a database arc clone.
let _ = manager.worker("worker-default".to_string(), 5, task_registry.clone(), 15, Some(db.clone())).await;

// (Optional) Run the Scheduler by specifying the scheduler name, interval (in seconds), and a database arc clone.
let _ = manager.scheduler("scheduler-default".to_string(), 5, Some(db.clone())).await;

// Optionally, wait for both the worker and scheduler to finish. This will prevent the function from exiting prematurely.
let _ = manager.wait().await;

```

To send command to the scheduler, queue worker, and task.
```rust
use crate::agent::Agent;
use std::sync::Arc;
use crate::database::Db;
use crate::Command;

let author: String = "Juan dela Cruz".to_string();
let server: String = "MYLOCALHOSTABC".to_string();
let agent_name = Agent::to_name(server.clone(),"worker-default-0").await;
let agent_data = agent.get_by_name(agent_name,server.clone()).await?;
let agent_id = agent_data.id.unwrap();
let command = Command::QueueGracefulShutdown;
let message: String =  "test dela cruz".to_string();
let result = agent.send_command(agent_id, command, Some(author),Some(message)).await;
assert!(result.is_ok(),"{:?}",result.unwrap_err());
```
## More Examples

For more examples, you can go to: ```src/test/``` directory.

---

### Explanation

- **Task Registration**: Registers a task struct (`MySampleStruct`) in the task registry.
- **Task Registry**: Shared using `Arc` to enable safe concurrent access in a multi-threaded environment.
- **Worker**: Runs tasks from the queue, controlled by parameters such as:
  - `queue_name`: Name of the queue to pull tasks from.
  - `num_threads`: Number of threads to handle task execution.
  - `task_registry`: Registry that contains the task structs.
  - `poll_interval`: Polling interval (in seconds) to check for new tasks.
- **Scheduler**: (Optional) Runs scheduled tasks at specified intervals, similar to cron jobs.
- **Join**: Ensures that both the worker and scheduler keep running without the program exiting prematurely.


## Code Documentation

For detailed documentation, visit [docs.rs/kafru](https://docs.rs/kafru).

## License

This project is licensed under the Custom License Agreement. See the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please refer to the [CONTRIBUTING.md](CONTRIBUTING.md) file for guidelines.

## Contact

For questions or feedback, you can reach me at [jpmateo022@gmail.com](mailto:jpmateo022@gmail.com).