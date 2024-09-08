#[cfg(test)]
mod test_manager {
    use async_trait::async_trait;
    use crate::tests::test_helper::configure_database_env;
    use crate::schedule::{Schedule, ScheduleData, ScheduleStatus};
    use crate::cron_schedule::CronSchedule;
    use chrono::{Days,Utc};
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
    use crate::task::{ TaskHandler, TaskRegistry };
    use rand::{self, Rng};
    use std::sync::Arc;

    pub struct MyTestStructA {
        message: String
    }
    use crate::manager::Manager;
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
    async fn test(){
        configure_database_env();

        let schedule: Schedule = Schedule::new().await;
        let mut manager: Manager = Manager::new().await;

        // Purge records
        let result = schedule.purge().await;
        assert!(result.is_ok(),"{:?}",result.unwrap_err());

        //Create sample schedules
        for i in 1..11 {
            let result: Result<ScheduleData, String> = schedule.create(ScheduleData {
                name: Some(format!("{} - {}",i,Name().fake::<String>())),
                queue: Some("default".to_string()),
                cron_expression: Some(CronSchedule::new().set_minute(format!("*/{}",i))),
                handler:Some("mytesthandler".to_string()),
                start_schedule: Utc::now().checked_sub_days(Days::new(1)),
                until_schedule: Utc::now().checked_add_days(Days::new(3)),
                one_time: if i % 2 == 0 { true } else { false },
                status: Some(ScheduleStatus::Enabled),
                parameters: Some(HashMap::from([
                    ( "schedule1".to_string(), Value::String(Sentence( Range {start: 1, end: 5 }).fake::<String>()) ),
                    ( "schedule2".to_string(), Value::Number(Number::from(123)) )
                ])),
                ..Default::default()
            }).await;
            assert!(result.is_ok(),"{}",result.unwrap_err());
        }
        let mut task_registry: TaskRegistry = TaskRegistry::new().await;
        task_registry.register("mytesthandler".to_string(), || Box::new(MyTestStructA { message: "Hello World".to_string() })).await;
        let task_registry: Arc<TaskRegistry> = Arc::new(task_registry);
        let _ = manager.worker("default".to_string(), 5, task_registry.clone(), 15).await;
        let _ = manager.scheduler("kafru_test_scheduler".to_string(), 5).await;
        let _ = manager.join().await;
    }

    
    #[tokio::test]
    async fn test_advance(){
        configure_database_env();

        let schedule: Schedule = Schedule::new().await;
        let mut manager: Manager = Manager::new().await;

        // Purge records
        let result = schedule.purge().await;
        assert!(result.is_ok(),"{:?}",result.unwrap_err());

        //Create sample schedules
        for i in 1..11 {
            let result: Result<ScheduleData, String> = schedule.create(ScheduleData {
                name: Some(format!("{} - {}",i,Name().fake::<String>())),
                queue: Some("default".to_string()),
                cron_expression: Some(CronSchedule::new().set_minute(format!("*/{}",i))),
                handler:Some("mytesthandler".to_string()),
                start_schedule: Utc::now().checked_add_days(Days::new(1)),
                until_schedule: Utc::now().checked_add_days(Days::new(3)),
                one_time: if i % 2 == 0 { true } else { false },
                status: Some(ScheduleStatus::Enabled),
                parameters: Some(HashMap::from([
                    ( "schedule1".to_string(), Value::String(Sentence( Range {start: 1, end: 5 }).fake::<String>()) ),
                    ( "schedule2".to_string(), Value::Number(Number::from(123)) )
                ])),
                ..Default::default()
            }).await;
            assert!(result.is_ok(),"{}",result.unwrap_err());
        }
        let mut task_registry: TaskRegistry = TaskRegistry::new().await;
        task_registry.register("mytesthandler".to_string(), || Box::new(MyTestStructA { message: "Hello World".to_string() })).await;
        let task_registry: Arc<TaskRegistry> = Arc::new(task_registry);
        let _ = manager.worker("default".to_string(), 5, task_registry.clone(), 15).await;
        let _ = manager.scheduler("kafru_test_scheduler".to_string(), 5).await;
        let _ = manager.join().await;
    }
}