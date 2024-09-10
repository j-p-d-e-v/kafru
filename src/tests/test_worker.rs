

#[cfg(test)]
mod test_worker {
    use async_trait::async_trait;
    use crate::tests::test_helper::configure_database_env;
    use crate::queue::{Queue, QueueData};
    use crate::worker::Worker;
    use crate::task::{ TaskHandler, TaskRegistry };
    use fake::{
        Fake,
        faker::{
            name::en::Name,
            lorem::en::Sentence
        }
    };
    use tracing::Level;
    use std::ops::Range;
    use std::collections::HashMap;
    use serde_json::{Value, Number};
    use rand::{self, Rng};
    use std::sync::Arc;
    use crate::Command;
    use crossbeam::channel::bounded;

    pub struct MyTestStructA {
        message: String
    }
    #[async_trait]
    impl TaskHandler for MyTestStructA {
        async fn run(&self, params: std::collections::HashMap<String,Value>) -> Result<(),String> {
            println!("My Parameters: {:#?}",params);            
            println!("{}",Sentence(Range{start: 1, end:3}).fake::<String>());
            println!("message: {}",self.message);
            let sleep_ms = rand::thread_rng().gen_range(Range{ start:3, end: 10 }) * 1000;
            let value = rand::thread_rng().gen_range(Range{ start:0, end: 100 });
            if value % 2 == 0 {
                return Err(format!("oops its an even number: {}",value));
            }
            tokio::time::sleep(std::time::Duration::from_millis(sleep_ms)).await;
            return Ok(())
        }
    }

    #[tokio::test]
    async fn test_watcher(){
        tracing_subscriber::fmt().with_max_level(Level::DEBUG).with_line_number(true).init();
        configure_database_env();
        let queue: Queue = Queue::new().await;
        let mut task_registry: TaskRegistry = TaskRegistry::new().await;
        task_registry.register("mytesthandler".to_string(), || Box::new(MyTestStructA { message: "Hello World".to_string() })).await;

        // Purge tasks
        let result: Result<u64, String> = queue.purge().await;
        assert!(result.is_ok(),"{}",result.unwrap_err());

        // List tasks by WAITING ONLY
        for i in 0..10 {
            let _ = queue.push(QueueData {
                name: Some(format!("{}-{}",Name().fake::<String>(),i)),
                handler:Some("mytesthandler".to_string()),
                parameters: Some(HashMap::from([
                    (
                        "sentence".to_string(),
                        Value::String(Sentence( Range {start: 1, end: 5 }).fake::<String>())
                    ),
                    (
                        "number".to_string(),
                        Value::Number(Number::from(rand::thread_rng().gen_range(Range{start:100, end:1000})))
                    )
                ])),
                ..Default::default()
            }).await;
        }
        let task_registry: Arc<TaskRegistry> = Arc::new(task_registry);
        let  (tx, rx) = bounded::<Command>(1);
        let task = tokio::spawn(async move {
            let worker  = Worker::new(rx.clone()).await;
            let result = worker.watch(task_registry.clone(),5, Some("default".to_string()), Some(2)).await;
            assert!(result.is_ok(),"{:?}",result.unwrap_err());
        });
        
        for command in [
            Command::WorkerPause,
            Command::WorkerResume,
            Command::WorkerForceShutdown
        ] {        
            let result = tx.send(command);
            assert!(result.is_ok(),"{}",result.unwrap_err());
            tokio::time::sleep(std::time::Duration::from_secs(10)).await;
        }
        task.await.unwrap();
    }
}