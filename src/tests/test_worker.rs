

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
    use crate::agent::{Agent,AgentData,AgentFilter,AgentStatus,AgentKind};

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
            tokio::time::sleep(std::time::Duration::from_millis(sleep_ms)).await;
            if value % 2 == 0 {
                return Err(format!("oops its an even number: {}",value));
            }
            return Ok(())
        }
    }

    #[tokio::test]
    async fn test_watcher(){
        configure_database_env();
        let server: String = "server1".to_string();
        let db_instance = Db::new(None).await;
        assert!(db_instance.is_ok(),"{:?}",db_instance.err());
        let db: Arc<Db> = Arc::new(db_instance.unwrap());
        let queue: Queue = Queue::new(Some(db.clone())).await;
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
                        Value::Number(Number::from(rand::thread_rng().gen_range(Range{start:1000, end:3000})))
                    )
                ])),
                ..Default::default()
            }).await;
        }
        let agent: Agent = Agent::new(Some(db.clone())).await;
        let task_registry: Arc<TaskRegistry> = Arc::new(task_registry);
        let _: tokio::task::JoinHandle<()> = tokio::spawn(async move {
            let worker  = Worker::new(Some(db.clone()),server.clone(),"Juan dela Cruz".to_string()).await;
            let result = worker.watch(task_registry.clone(),5, Some("default".to_string()), Some(5)).await;
            assert!(result.is_ok(),"{:?}",result.unwrap_err());
        });

        tokio::time::sleep(std::time::Duration::from_millis(5000)).await;
        let mut previous_count: usize = 99;
        loop {
            match agent.list(AgentFilter {
                kind: Some(AgentKind::Task),
                statuses: Some(Vec::from([AgentStatus::Initialized,AgentStatus::Running])),
                ..Default::default()
            }).await {
                Ok(data) => {
                    if previous_count != data.len() {
                        println!("Total Tasks Remaining: {}",data.len());
                        previous_count = data.len();
                    }
                    if data.len() == 0 {
                        assert!(true);
                        break;
                    }
                    for item in data {
                        if let Err(error) = agent.update_by_id(item.id.clone().unwrap(), AgentData {
                            command: Some(
                                Command::TaskTerminate),
                            ..Default::default()
                        }).await {
                            assert!(false,"unable to update command: {}",error);
                        }
                    }
                }
                Err(error)  => {
                    assert!(false,"unable to send command: {}",error);
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        }
    }
}