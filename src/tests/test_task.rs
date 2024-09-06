
#[cfg(test)]
mod test_database {
    use crate::task::{
        TaskHandler,
        TaskRegistry
    };
    use serde_json::Value;

    pub struct MyTestStructA;
    impl TaskHandler for MyTestStructA {
        fn run(&self, _params: std::collections::HashMap<String,Value>) -> Result<(),String> {
            Ok(())
        }
    }
    pub struct MyTestStructB;
    impl TaskHandler for MyTestStructB {
        fn run(&self, _params: std::collections::HashMap<String,Value>) -> Result<(),String> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test(){
        let mut task_registry: TaskRegistry = TaskRegistry::new();
        assert!(task_registry.register("myteststructa".to_string(), || Box::new(MyTestStructA)));
        assert!(task_registry.register("myteststructb".to_string(), || Box::new(MyTestStructB)));
        assert!(task_registry.tasks().len() >0 );
    }
}