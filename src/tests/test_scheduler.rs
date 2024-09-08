

#[cfg(test)]
mod test_worker {
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
    use tracing::Level;
    use std::ops::Range;
    use std::collections::HashMap;
    use serde_json::{Value, Number};

    #[tokio::test]
    async fn test_scheduler(){
        tracing_subscriber::fmt().with_max_level(Level::DEBUG).with_line_number(true).init();
        configure_database_env();
        let schedule: Schedule = Schedule::new().await;
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
        let scheduler  = Scheduler::new().await;
        let result = scheduler.watch(Some("kafru_test_scheduler".to_string()), Some(15)).await;
        assert!(result.is_ok(),"{:?}",result.unwrap_err());
    }
}