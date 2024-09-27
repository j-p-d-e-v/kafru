
#[cfg(test)]
mod test_schedule {
    use crate::tests::test_helper::configure_database_env;
    use crate::schedule::{Schedule, ScheduleData, ScheduleStatus, ScheduleListConditions};
    use crate::cron_schedule::CronSchedule;
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
    use chrono::{Days, Utc};

    #[tokio::test]
    async fn test_create_update_remove(){
        configure_database_env();
        let schedule: Schedule = Schedule::new().await;

        // Purge records
        let result = schedule.purge().await;
        assert!(result.is_ok(),"{:?}",result.unwrap_err());

        // Create Record
        let result: Result<ScheduleData, String> = schedule.create(ScheduleData {
            name: Some(Name().fake::<String>()),
            queue: Some("default".to_string()),
            cron_expression: Some(CronSchedule::new().set_second("0".to_string()).set_minute("*/5".to_string())),
            handler:Some("mytesthandler".to_string()),
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
        let previous_record: ScheduleData = result.unwrap();
        assert_eq!(previous_record.status,Some(ScheduleStatus::Disabled));
        
        // Update record
        let result: Result<ScheduleData, String> = schedule.update(previous_record.id.unwrap(),ScheduleData {
            status: Some(ScheduleStatus::Enabled),
            start_schedule:Utc::now().checked_add_days(Days::new(1)),
            until_schedule: Utc::now().checked_add_days(Days::new(3)),
            ..Default::default()
        },true).await;
        assert!(result.is_ok(),"{}",result.unwrap_err());
        let next_record: ScheduleData = result.unwrap();
        assert_eq!(next_record.status,Some(ScheduleStatus::Enabled));
        assert!(next_record.next_schedule > previous_record.next_schedule,"Next Schedule: {:?}   Previous Schedule: {:?}",next_record.next_schedule,previous_record.next_schedule);

        // Get record
        let result: Result<ScheduleData, String> = schedule.get(next_record.id.clone().unwrap()).await;
        assert!(result.is_ok(),"{}",result.unwrap_err());
        let record: ScheduleData = result.unwrap();
        assert_eq!(next_record.id,record.id);

        // Remove the record
        let result: Result<ScheduleData, String> = schedule.remove(record.id.clone().unwrap()).await;
        assert!(result.is_ok(),"{}",result.unwrap_err());
    }
    
    #[tokio::test]
    pub async fn test_list_purge(){
        configure_database_env();
        let schedule: Schedule = Schedule::new().await;
        
        // Purge records
        let result = schedule.purge().await;
        assert!(result.is_ok(),"{:?}",result.unwrap_err());

        // List schedules
        for i in 1..11 {
            let result: Result<ScheduleData, String> = schedule.create(ScheduleData {
                name: Some(format!("{} - {}",i,Name().fake::<String>())),
                queue: Some("default".to_string()),
                cron_expression: Some(CronSchedule::new().set_minute(format!("*/{}",i))),
                handler:Some("mytesthandler".to_string()),
                until_schedule: Utc::now().checked_add_days(Days::new(3)),
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
        let result: Result<Vec<ScheduleData>, String> = schedule.list(ScheduleListConditions {
            upcoming: Some(false),
            ..Default::default()
        }).await;
        assert!(result.is_ok(),"{}",result.unwrap_err());
        assert!(result.unwrap().len()>=10);
        
        //// Cleanup using Purge
        let result: Result<u64, String> = schedule.purge().await;
        assert!(result.is_ok(),"{}",result.unwrap_err());
    }
    
}