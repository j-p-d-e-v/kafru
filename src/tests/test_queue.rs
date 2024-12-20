

#[cfg(test)]
mod test_queue {
    use crate::tests::test_helper::configure_database_env;
    use crate::queue::{Queue, QueueData, QueueStatus, QueueListConditions};
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
    use crate::database::Db;
    use std::sync::Arc;

    #[tokio::test]
    #[serial_test::serial]
    async fn test_create_update_remove(){
        configure_database_env();
        let db_instance = Db::new(None).await;
        assert!(db_instance.is_ok(),"{:?}",db_instance.err());
        let db: Arc<Db> = Arc::new(db_instance.unwrap());
        let queue: Queue = Queue::new(Some(db.clone())).await;
        // Push task to the queue
        let result: Result<QueueData, String> = queue.push(QueueData {
            name: Some(Name().fake::<String>()),
            handler:Some("mytesthandler".to_string()),
            parameters: Some(HashMap::from([
                (
                    "myparam1".to_string(),
                    Value::String(Sentence( Range {start: 1, end: 5 }).fake::<String>())
                ),
                (
                    "myparam2".to_string(),
                    Value::Number(Number::from(123))
                )
            ])),
            queue:Some("mytestqueue".to_string()),
            ..Default::default()
        }).await;
        assert!(result.is_ok(),"{}",result.unwrap_err());
        let record: QueueData = result.unwrap();
        assert_eq!(record.status,Some(QueueStatus::Waiting));
        
        // Update task in the queue
        let result: Result<QueueData, String> = queue.update(record.id.unwrap(),QueueData {
            status: Some(QueueStatus::Completed),
            ..Default::default()
        }).await;
        assert!(result.is_ok(),"{}",result.unwrap_err());
        let record: QueueData = result.unwrap();
        assert_eq!(record.status,Some(QueueStatus::Completed));

        // Get task in the queue
        let result: Result<QueueData, String> = queue.get(record.id.clone().unwrap()).await;
        assert!(result.is_ok(),"{}",result.unwrap_err());
        let _record: QueueData = result.unwrap();
        assert_eq!(record.id,_record.id);

        // Remove the task
        let result: Result<QueueData, String> = queue.remove(record.id.clone().unwrap()).await;
        assert!(result.is_ok(),"{}",result.unwrap_err());
    }
    #[tokio::test]
    #[serial_test::serial]
    pub async fn test_list_purge(){
        configure_database_env();
        let db_instance = Db::new(None).await;
        assert!(db_instance.is_ok(),"{:?}",db_instance.err());
        let db: Arc<Db> = Arc::new(db_instance.unwrap());
        let queue: Queue = Queue::new(Some(db.clone())).await;
        // Purge tasks
        let result: Result<u64, String> = queue.purge().await;
        assert!(result.is_ok(),"{}",result.unwrap_err());

        // List tasks by WAITING ONLY
        for i in 0..10 {
            let _ = queue.push(QueueData {
                name: Some(format!("{}-{}",Name().fake::<String>(),i)),
                handler:Some("mytesthandler".to_string()),
                queue: Some("myloopqueue".to_string()),
                ..Default::default()
            }).await;
        }
        let result: Result<Vec<QueueData>, String> = queue.list(
            QueueListConditions {
                status: Some(vec!["Waiting".to_string()]),
                queue: Some(vec!["myloopqueue".to_string()]),
                limit: Some(5)
            }).await;
        assert!(result.is_ok(),"{}",result.unwrap_err());
        assert_eq!(result.unwrap().len(),5);
        
        // List all tasks assert!(result.is_ok(),"{}",result.unwrap_err());
        let result: Result<Vec<QueueData>, String> = queue.list(QueueListConditions::default()).await;
        assert!(result.is_ok(),"{}",result.unwrap_err());
        assert!(result.unwrap().len()>=10);

        // Cleanup using Purge
        let result: Result<u64, String> = queue.purge().await;
        assert!(result.is_ok(),"{}",result.unwrap_err());
        assert!(result.unwrap() > 0);
    }
    
}