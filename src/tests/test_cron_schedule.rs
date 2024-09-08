


#[cfg(test)]
mod test_cron_scheduler {
    use chrono::{DateTime, Utc};
    use crate::cron_schedule::CronSchedule;

    #[tokio::test]
    async fn test() {
        let mut cron_schedule = CronSchedule::new();
        let dt: DateTime<Utc> = Utc::now();
        let result: Result<Option<DateTime<Utc>>, String>  = cron_schedule.set_minute("*/6".to_string()).get_upcoming(Some(dt));
        assert!(result.is_ok(),"{}",result.unwrap_err());
        let schedule: Option<DateTime<Utc>> = result.unwrap();
        assert!(schedule.is_some());
        let next_schedule: DateTime<Utc> = schedule.unwrap();
        assert!(next_schedule > dt,"Start DateTime: {:?} | Next Schedule: {:?}",dt,next_schedule);
    }
}