use chrono::{DateTime, Utc};
use serde::{Serialize, Deserialize};
use surrealdb::sql::Thing;
use surrealdb::Surreal;
use surrealdb::opt::auth::Root;
use surrealdb::engine::remote::ws::{Ws, Client};
use std::collections::HashMap;
use serde_json::{Value, Number};
use std::sync::Arc;


#[derive(Debug)]
pub struct DbConnection {
    username: String,
    password: String,
    host: String,
    port: u16,
    namespace: String,
    database: String
}

#[derive(Debug)]
pub struct Db {
    pub client: Surreal<Client>
}

impl Db {

    pub async fn new(config: DbConnection) -> Result<Self,String> {
        let address: String = format!("{}:{}", config.host,config.port);
        match Surreal::new::<Ws>(address).await {
            Ok(client) => {
                if let Err(error) = client.signin(Root {
                    username: config.username.as_str(),
                    password: config.password.as_str(),
                }).await {
                    return Err(error.to_string());
                }
                if let Err(error) = client.use_ns(config.namespace).use_db(config.database).await {
                    return Err(error.to_string());
                }
                Ok(Self {
                    client,
                })
            }
            Err(error) => {
                Err(error.to_string())
            }
        }   
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TaskData {
    id: Option<Thing>,
    name: Option<String>,
    handler: Option<String>,
    parameters: Option<HashMap<String,Value>>,
    status: String,
    result: Option<Value>,
    timestamp: DateTime<Utc>
}

impl  Default for TaskData {
    fn default() -> Self {
        Self {
            id: None,
            name: None,
            parameters: None,
            handler: None,
            status: "WAITING".to_string(),
            result: None,
            timestamp: Utc::now()
        }
    }
}

#[derive(Debug)]
pub struct Task {
    pub db: Arc<Db>
}

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

impl Task {

    pub async fn new(db: Arc<Db>) -> Self {
        Self { db }
    }

    pub async fn list(&self,status:String) -> Result<Vec<TaskData>,String> {
        match self.db.client.query("SELECT * FROM type::table($table) WHERE status=$status")
        .bind(("table","tasks"))
        .bind(("status",status)).await {
            Ok(mut response) => {
                match response.take::<Vec<TaskData>>(0) {
                    Ok(data)=> {
                        Ok(data)
                    },        
                    Err(error) => Err(error.to_string())
                }
            },        
            Err(error) => Err(error.to_string())
        }
    }

    pub async fn push(&self, data: TaskData ) -> Result<TaskData,String> {
        let id: String = uuid::Uuid::new_v4().to_string();
        match self.db.client.create::<Option<TaskData>>(("tasks",id)).content(data).await {
            Ok(result) => {
                if let Some(record) = result {
                    return Ok(record);
                }
                Err("unable to retrieve record".to_string())
            }
            Err(error) => Err(error.to_string())
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
    pub async fn test_dispatcher() {
        
        match Db::new(DbConnection {
            username: "root".to_string(),
            password: "root".to_string(),
            host: "127.0.0.1".to_string(),
            port: 8000,
            database: "scheduler".to_string(),
            namespace: "scheduler".to_string(),
        }).await {
            Ok(instance) => {
                let dispatcher = Dispatcher::new(Arc::new(instance)).await;
                assert!(dispatcher.watch().await.is_ok());
            },
            Err(error) => {
                assert!(false,"{}",error);
            }
        }
    }

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

    #[tokio::test]
    async fn test_db() {

        match Db::new(DbConnection {
            username: "root".to_string(),
            password: "root".to_string(),
            host: "127.0.0.1".to_string(),
            port: 8000,
            database: "scheduler".to_string(),
            namespace: "scheduler".to_string(),
        }).await {
            Ok(instance) => {
                #[derive(Debug, Serialize, Deserialize)]
                pub struct Info {
                    test: String
                }
                let id: String = uuid::Uuid::new_v4().to_string();
                match instance.client.create::<Option<Info>>(("test",id)).content(Info {
                    test: "hello world".to_string()
                }).await {
                    Ok(_) => {
                        assert!(true);
                    }
                    Err(error) => assert!(false,"{}",error.to_string())
                }
            }
            Err(error) => {
                assert!(false,"{}",error);
            }
        }
    }

    //#[test]
    //fn test_thread_pool_builder(){
    //    
    //    rayon::ThreadPoolBuilder::new().num_threads(1).build_global().unwrap();
    //    let pool = rayon_core::ThreadPoolBuilder::default().build().unwrap();
//
    //    let do_ita = || {
    //        println!("one a");
    //        pool.install(|| {});
    //        println!("two a");
    //    };
    //    let do_itb = || {
    //        println!("one b");
    //        pool.install(|| {});
    //        println!("two b");
    //    };
    //    rayon::join(|| do_ita(), || do_itb());
    //}

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