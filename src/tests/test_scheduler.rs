

#[cfg(test)]
mod test_scheduler {
    use crate::tests::test_helper::configure_database_env;
    use crate::schedule::{Schedule, ScheduleData, ScheduleStatus};
    use crate::scheduler::Scheduler;
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
    use crossbeam::channel::bounded;
    use crate::Command;
    use crate::database::Db;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_scheduler(){
        configure_database_env();
        let db_instance = Db::new(None).await;
        assert!(db_instance.is_ok(),"{:?}",db_instance.err());
        let db: Arc<Db> = Arc::new(db_instance.unwrap());
        let schedule: Schedule = Schedule::new(Some(db.clone())).await;
        // Purge schedules
        let result: Result<u64, String> = schedule.purge().await;
        assert!(result.is_ok(),"{}",result.unwrap_err());
        
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
                    (
                        "schedule1".to_string(),
                        Value::String(Sentence( Range {start: 1, end: 5 }).fake::<String>())
                    ),
                    (
                        "schedule2".to_string(),
                        Value::Number(Number::from(123))
                    )
                ])),
                ..Default::default()
            }).await;
            assert!(result.is_ok(),"{}",result.unwrap_err());
        }
        let  (tx, rx) = bounded::<Command>(1);
        let scheduler  = Scheduler::new(rx,Some(db.clone())).await;
        let task = tokio::spawn(async {
            let result = scheduler.watch(Some("kafru_test_scheduler".to_string()), Some(5)).await;
            assert!(result.is_ok(),"{:?}",result.unwrap_err());
        });
        for command in [
            Command::SchedulerPause,
            Command::SchedulerResume,
            Command::SchedulerForceShutdown
        ] {        
            let result = tx.send(command.clone());
            assert!(result.is_ok(),"{}",result.unwrap_err());
            tokio::time::sleep(std::time::Duration::from_secs(3)).await;
            println!("command: {:?}",command);
        }
        task.await.unwrap();
    }
}