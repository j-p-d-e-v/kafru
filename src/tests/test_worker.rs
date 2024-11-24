

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
    use std::ops::Range;
    use std::collections::HashMap;
    use serde_json::{Value, Number};
    use rand::{self, Rng};
    use std::sync::Arc;
    use crate::Command;
    use crate::database::Db;
    use crate::agent::{Agent,AgentFilter,AgentStatus,AgentKind};

    pub struct MyTestStructA;
    #[async_trait]
    impl TaskHandler for MyTestStructA {
        async fn run(&self, params: std::collections::HashMap<String,Value>) -> Result<(),String> {
            println!("My Parameters: {:#?}",params);            
            let value = rand::thread_rng().gen_range(Range{ start:0, end: 100 });
            for i in 0..20 {
                println!("{}.) {}",i, Sentence(Range{start: 1, end:3}).fake::<String>());
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
            if value % 2 == 0 {
                return Err(format!("oops its an even number: {}",value));
            }
            return Ok(())
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_watcher_graceful_shutdown(){
        configure_database_env();
        let server: String = "server1".to_string();
        let db_instance = Db::new(None).await;
        assert!(db_instance.is_ok(),"{:?}",db_instance.err());
        let db: Arc<Db> = Arc::new(db_instance.unwrap());
        let queue: Queue = Queue::new(Some(db.clone())).await;
        let mut task_registry: TaskRegistry = TaskRegistry::new().await;
        task_registry.register("mytesthandler".to_string(), || Box::new(MyTestStructA {})).await;

        // Purge tasks
        let result: Result<u64, String> = queue.purge().await;
        assert!(result.is_ok(),"{}",result.unwrap_err());

        // List tasks by WAITING ONLY
        for i in 0..3 {
            let _ = queue.push(QueueData {
                name: Some(format!("{}-{}",Name().fake::<String>(),i)),
                handler:Some("mytesthandler".to_string()),
                queue: Some("server2-default".to_string()),
                parameters: Some(HashMap::from([
                    (
                        "sentence".to_string(),
                        Value::String(Sentence( Range {start: 1, end: 5 }).fake::<String>())
                    ),
                    (
                        "number".to_string(),
                        Value::Number(Number::from(rand::thread_rng().gen_range(Range{start:1000, end:3000})))
                    )
                ])),
                ..Default::default()
            }).await;
        }
        let agent: Agent = Agent::new(Some(db.clone())).await;
        let task_registry: Arc<TaskRegistry> = Arc::new(task_registry);
        let _server = server.clone();
        let tasks : tokio::task::JoinHandle<()> = tokio::spawn(async move {
            let worker  = Worker::new(Some(db.clone()),_server,"Juan dela Cruz".to_string()).await;
            let result = worker.watch(task_registry.clone(),5, Some("default".to_string()), Some(5)).await;
            assert!(result.is_ok(),"{:?}",result.unwrap_err());
        });

        tokio::time::sleep(std::time::Duration::from_millis(3000)).await;
        let queue_agent = agent.get_by_name("server1-default-0".to_string(),server).await;
        assert!(queue_agent.is_ok(),"{:?}",queue_agent.err());
        let result = agent.send_command(queue_agent.unwrap().id.unwrap(), Command::QueueGracefulShutdown, None, Some("test dela cruz".to_string())).await;
        assert!(result.is_ok(),"{:?}",result.err());
        let _ = tokio::join!(tasks);
    }
    #[tokio::test]
    #[serial_test::serial]
    async fn test_watcher_force_shutdown(){
        configure_database_env();
        let server: String = "server2".to_string();
        let db_instance = Db::new(None).await;
        assert!(db_instance.is_ok(),"{:?}",db_instance.err());
        let db: Arc<Db> = Arc::new(db_instance.unwrap());
        let queue: Queue = Queue::new(Some(db.clone())).await;
        let mut task_registry: TaskRegistry = TaskRegistry::new().await;
        task_registry.register("mytesthandler".to_string(), || Box::new(MyTestStructA {})).await;

        // Purge tasks
        let result: Result<u64, String> = queue.purge().await;
        assert!(result.is_ok(),"{}",result.unwrap_err());

        // List tasks by WAITING ONLY
        for i in 0..3 {
            let _ = queue.push(QueueData {
                name: Some(format!("{}-{}",Name().fake::<String>(),i)),
                handler:Some("mytesthandler".to_string()),
                queue: Some("server2-default".to_string()),
                parameters: Some(HashMap::from([
                    (
                        "sentence".to_string(),
                        Value::String(Sentence( Range {start: 1, end: 5 }).fake::<String>())
                    ),
                    (
                        "number".to_string(),
                        Value::Number(Number::from(rand::thread_rng().gen_range(Range{start:1000, end:3000})))
                    )
                ])),
                ..Default::default()
            }).await;
        }
        let agent: Agent = Agent::new(Some(db.clone())).await;
        let task_registry: Arc<TaskRegistry> = Arc::new(task_registry);
        let _server = server.clone();
        let tasks : tokio::task::JoinHandle<()> = tokio::spawn(async move {
            let worker  = Worker::new(Some(db.clone()),_server,"Juan dela Cruz".to_string()).await;
            let result = worker.watch(task_registry.clone(),5, Some("default".to_string()), Some(5)).await;
            assert!(result.is_ok(),"{:?}",result.unwrap_err());
        });

        tokio::time::sleep(std::time::Duration::from_millis(3000)).await;
        let queue_agent = agent.get_by_name("server2-default-0".to_string(),server).await;
        assert!(queue_agent.is_ok(),"{:?}",queue_agent.err());
        let result = agent.send_command(queue_agent.unwrap().id.unwrap(), Command::QueueForceShutdown, None, Some("test dela cruz".to_string())).await;
        assert!(result.is_ok(),"{:?}",result.err());
        let _ = tokio::join!(tasks);
    }
    
    #[tokio::test]
    #[serial_test::serial]
    async fn test_watcher_task_terminate(){
        configure_database_env();
        let server: String = "server3".to_string();
        let db_instance = Db::new(None).await;
        assert!(db_instance.is_ok(),"{:?}",db_instance.err());
        let db: Arc<Db> = Arc::new(db_instance.unwrap());
        let queue: Queue = Queue::new(Some(db.clone())).await;
        let mut task_registry: TaskRegistry = TaskRegistry::new().await;
        task_registry.register("mytesthandler".to_string(), || Box::new(MyTestStructA {})).await;
        // Purge tasks
        let result: Result<u64, String> = queue.purge().await;
        assert!(result.is_ok(),"{}",result.unwrap_err());
        // List tasks by WAITING ONLY
        for i in 0..3 {
            let _ = queue.push(QueueData {
                name: Some(format!("{}-{}",Name().fake::<String>(),i)),
                handler:Some("mytesthandler".to_string()),
                queue: Some("server3-default".to_string()),
                parameters: Some(HashMap::from([
                    (
                        "sentence".to_string(),
                        Value::String(Sentence( Range {start: 1, end: 5 }).fake::<String>())
                    ),
                    (
                        "number".to_string(),
                        Value::Number(Number::from(rand::thread_rng().gen_range(Range{start:1000, end:3000})))
                    )
                ])),
                ..Default::default()
            }).await;
        }
        let agent: Agent = Agent::new(Some(db.clone())).await;
        let task_registry: Arc<TaskRegistry> = Arc::new(task_registry);
        let _server = server.clone();
        let tasks : tokio::task::JoinHandle<()> = tokio::spawn(async move {
            let worker  = Worker::new(Some(db.clone()),_server,"Juan dela Cruz".to_string()).await;
            let result = worker.watch(task_registry.clone(),5, Some("default".to_string()), Some(5)).await;
            assert!(result.is_ok(),"{:?}",result.unwrap_err());
        });
        tokio::time::sleep(std::time::Duration::from_millis(5000)).await;
        loop {
            match agent.list(AgentFilter {
                kind: Some(AgentKind::Task),
                statuses: Some(Vec::from([AgentStatus::Running])),
                ..Default::default()
            }).await {
                Ok(data) => {
                    if data.len() > 0 {
                        let mut counter: u32 = 1;
                        for item in data {
                            if counter % 2 == 0 {
                                if let Err(error) = agent.send_command(item.id.clone().unwrap(), Command::TaskTerminate, None, Some("test dela cruz".to_string())).await {
                                    assert!(false,"unable to update command: {}",error);
                                }
                            }
                            counter += 1;
                        }
                        break;
                    }
                }
                Err(error)  => {
                    assert!(false,"unable to send command: {}",error);
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        }
        let queue_agent = agent.get_by_name("server3-default-0".to_string(),server).await;
        assert!(queue_agent.is_ok(),"{:?}",queue_agent.err());
        let result = agent.send_command(queue_agent.unwrap().id.unwrap(), Command::QueueGracefulShutdown, None, Some("test dela cruz".to_string())).await;
        assert!(result.is_ok(),"{:?}",result.err());
        let _ = tokio::join!(tasks);
    }
    
}