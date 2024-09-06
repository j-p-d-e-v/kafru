pub mod database;
pub mod queue;
pub mod task;
pub mod worker;
pub mod tests;


/*
#[derive(Debug)]
pub struct Dispatcher {
    pub db: Arc<Db>,
    pub task: Task
}

impl Dispatcher {

    
    pub async fn new(db: Arc<Db>) -> Self {
        Self {
            task: Task::new(db.clone()).await,
            db
        }
    }

    pub async fn watch(&self) -> Result<Vec<Task>,String>{
        loop {
            if let Ok(tasks) = self.task.list("WAITING".to_string()).await {
                println!("Total  Waiting: {}",tasks.len());
            }
            tokio::time::sleep(std::time::Duration::from_secs(3)).await;
        }
    }
}
#[cfg(test)]
mod test_rayon {
    use super::*;
    use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
    use tokio::runtime::Handle;
    use std::{any::{Any, TypeId}, hash::Hash, time::{Duration, Instant}};
    use rayon::prelude::*;

    #[tokio::test] 
    pub async fn test_task() {

        match Db::new(DbConnection {
            username: "root".to_string(),
            password: "root".to_string(),
            host: "127.0.0.1".to_string(),
            port: 8000,
            database: "scheduler".to_string(),
            namespace: "scheduler".to_string(),
        }).await {
            Ok(instance) => {
                let tasks = Task::new(Arc::new(instance)).await;
                for i in 0..10 {
                    let _ = tasks.push(TaskData {
                        name: Some(format!("mytask-{}",i)),
                        handler: Some("MyTestHandler".to_string()),
                        parameters: Some(
                            HashMap::from([
                                ("param1".to_string(),Value::String("hello world".to_string())),
                                ("param2".to_string(),Value::Number(Number::from(123))),
                            ])
                        ),
                        ..Default::default()
                    }).await;
                }
            }
            Err(error) => {
                assert!(false,"{}",error);
            }
        }
    }

    #[test]
    fn test_handlers(){
        use serde_json::Value;
        use std::collections::HashMap;

        trait Handler  {
            fn run(&self, params: HashMap<String,Value>) -> Result<(),String> ;
        }

        #[derive(Debug)]
        pub struct MyTestHandler;

        impl Handler for MyTestHandler {
            fn run(&self, params: HashMap<String, Value>) -> Result<(),String> {
                println!("MyTestHandler::Params: {:#?}",params);   
                Ok(())
            }
        }

        #[derive(Debug)]
        pub struct MyAnotherTestHandler;

        impl Handler for MyAnotherTestHandler {
            fn run(&self, params: HashMap<String, Value>)  -> Result<(),String> {
                println!("MyAnotherTestHandler::Params: {:#?}",params);                
                Ok(())
            }
        }
        type HandlerFactory = fn() -> Box<dyn Handler>;
        let mut registry: HashMap<String, HandlerFactory > = HashMap::new();
        registry.insert("mytesthandler".to_string(), || Box::new(MyTestHandler));
        registry.insert("myanothertesthandler".to_string(), || Box::new(MyAnotherTestHandler));

        let params = HashMap::from([
            ("test1".to_string(),Value::String("Hello World".to_string())),
            ("test2".to_string(),Value::Bool(true)),
        ]);
        let _struct = registry.get_key_value("mytesthandler").unwrap().1;
        println!("{:#?}",_struct().run(params.clone()));
        let _struct = registry.get_key_value("myanothertesthandler").unwrap().1;
        println!("{:#?}",_struct().run(params.clone()));

    }

    use rayon::ThreadPoolBuilder;
    use std::sync::{Arc, atomic::{AtomicU16, Ordering} };
    use rand::prelude::*;

    #[test]
    fn test_thread_pool(){
        let tp: rayon::ThreadPool = ThreadPoolBuilder::new().num_threads(5).build().unwrap();
        
        tp.scope(|s| {
            let available_threads: Arc<AtomicU16> = Arc::new(AtomicU16::new(5));
            for i in 1..6 {
                let counter: Arc<AtomicU16> = available_threads.clone();
                s.spawn(move|_| {
                    counter.fetch_sub(1, Ordering::SeqCst);
                    println!("started thread: {}", i);
                    let sleep: u64 = rand::thread_rng().gen_range(1..2);
                    for _ in 0..5 {
                        std::thread::sleep(std::time::Duration::from_secs(sleep));
                    }
                    counter.fetch_add(1, Ordering::SeqCst);
                    println!("completed thread: {}", i);
                });
            }
            loop {
                let counter = available_threads.clone();
                std::thread::sleep(std::time::Duration::from_secs(3));
                println!("available_threads: {:?}",counter);
            }
        });
    }
}
*/