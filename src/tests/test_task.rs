
#[cfg(test)]
mod test_task {
    use async_trait::async_trait;
    use crate::task::{
        TaskHandler,
        TaskRegistry
    };
    use serde_json::Value;

    pub struct MyTestStructA;

    #[async_trait]
    impl TaskHandler for MyTestStructA {
        async fn run(&self, _params: std::collections::HashMap<String,Value>) -> Result<(),String> {
            Ok(())
        }
    }
    pub struct MyTestStructB;

    #[async_trait]
    impl TaskHandler for MyTestStructB {
        async fn run(&self, _params: std::collections::HashMap<String,Value>) -> Result<(),String> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test(){
        let mut task_registry: TaskRegistry = TaskRegistry::new().await;
        assert!(task_registry.register("myteststructa".to_string(), || Box::new(MyTestStructA)).await);
        assert!(task_registry.register("myteststructb".to_string(), || Box::new(MyTestStructB)).await);
        assert!(task_registry.tasks().await.len() >0 );
    }
}