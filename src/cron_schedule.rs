use std::str::FromStr;
use chrono::{DateTime, Utc};
use cron::Schedule;
use serde::{Serialize, Deserialize};


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CronSchedule {
    second: String,
    minute: String,
    hour: String,
    day_of_month: String,
    month: String,
    day_of_week: String,
    year: String,
}
impl Default for CronSchedule {
    fn default() -> Self {            
        Self {
            second: "*".to_string(),
            minute: "*".to_string(),
            hour: "*".to_string(),
            day_of_month: "*".to_string(),
            month: "*".to_string(),
            day_of_week: "?".to_string(),
            year: "*".to_string(),
        }
    }
}

impl CronSchedule {
    
    pub fn new() -> Self {
        Self {
            ..Default::default()
        }
    }
    
    pub fn get_upcoming(self,datetime: Option<DateTime<Utc>>) -> Result<Option<DateTime<Utc>>,String> {
        let cron_expr:String = self.get_expression();
        match Schedule::from_str(cron_expr.as_str()) {
            Ok(schedule) => {
                let datetime = datetime.unwrap_or(Utc::now());
                for item in schedule.after(&datetime).take(1) {
                    return Ok(Some(item))
                }
                Ok(None)
            }
            Err(error) => Err(error.to_string())
        }
    }
    ///Format: second  minute   hour   day of month   month   day of week   year   
    pub fn get_expression(&self) -> String {
        let expr = vec![
            self.second.clone(), 
            self.minute.clone(), 
            self.hour.clone(), 
            self.day_of_month.clone(),
            self.month.clone(),
            self.day_of_week.clone(),
            self.year.clone()
        ];
        expr.join(" ")
    }
    pub fn set_second(&mut self, value: String) -> Self {
        self.second = value;
        self.to_owned()
    }
    pub fn set_minute(&mut self, value: String) -> Self {
        self.minute = value;
        self.to_owned()
    }
    pub fn set_hour(&mut self, value: String) -> Self {
        self.hour = value;
        self.to_owned()
    }
    pub fn set_day_of_month(&mut self, value: String) -> Self {
        self.day_of_month = value;
        self.to_owned()
    }
    pub fn set_month(&mut self, value: String) -> Self {
        self.month = value;
        self.to_owned()
    }
    pub fn set_day_of_week(&mut self, value: String) -> Self {
        self.day_of_week = value;
        self.to_owned()
    }
    pub fn set_year(&mut self, value: String) -> Self {
        self.year = value;
        self.to_owned()
    }
}
