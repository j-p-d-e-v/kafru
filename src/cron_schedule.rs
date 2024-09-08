use std::str::FromStr;
use chrono::{DateTime, Utc};
use cron::Schedule;
use serde::{Serialize, Deserialize};


#[derive(Debug, Clone, Serialize, Deserialize)]
/// Represents a Quartz cron expression for scheduling tasks.
/// 
/// The expression format is as follows:
/// `second minute hour dayOfMonth month dayOfWeek year`
///
/// # Attributes
/// - `second`: The second field of the cron expression (0-59).
/// - `minute`: The minute field of the cron expression (0-59).
/// - `hour`: The hour field of the cron expression (0-23).
/// - `day_of_month`: The day of the month field of the cron expression (1-31).
/// - `month`: The month field of the cron expression (1-12).
/// - `day_of_week`: The day of the week field of the cron expression (0-6 where 0 is Sunday).
/// - `year`: The year field of the cron expression (optional).
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
    
    /// Initializes the `CronSchedule` with default values.
    ///
    /// The default cron expression is `* * * * ? *`.
    pub fn new() -> Self {
        Self {
            ..Default::default()
        }
    }

    /// Generates the upcoming schedule based on a given datetime. Note: The schedule is generated using UTC timezone.
    ///
    /// # Parameters
    /// - `datetime`: *(Optional)* The datetime to use with the cron expression for generating the upcoming schedule. Defaults to the current date and time.
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

    /// Generates a cron expression based on the provided configuration values.
    ///
    /// The format is as follows:
    /// - `second`
    /// - `minute`
    /// - `hour`
    /// - `day of month`
    /// - `month`
    /// - `day of week`
    /// - `year`
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

    /// Sets the value for the seconds field.
    ///
    /// # Parameters
    /// - `value`: The value to set for the seconds field.
    ///
    /// # Returns
    /// A new instance of the struct with the updated seconds value.
    pub fn set_second(&mut self, value: String) -> Self {
        self.second = value;
        self.to_owned()
    }

    /// Sets the value for the minutes field.
    ///
    /// # Parameters
    /// - `value`: The value to set for the minutes field.
    ///
    /// # Returns
    /// A new instance of the struct with the updated minutes value.
    pub fn set_minute(&mut self, value: String) -> Self {
        self.minute = value;
        self.to_owned()
    }

    /// Sets the value for the hours field.
    ///
    /// # Parameters
    /// - `value`: The value to set for the hours field.
    ///
    /// # Returns
    /// A new instance of the struct with the updated hours value.
    pub fn set_hour(&mut self, value: String) -> Self {
        self.hour = value;
        self.to_owned()
    }

    /// Sets the value for the day of the month field.
    ///
    /// # Parameters
    /// - `value`: The value to set for the day of the month field.
    ///
    /// # Returns
    /// A new instance of the struct with the updated day of the month value.
    pub fn set_day_of_month(&mut self, value: String) -> Self {
        self.day_of_month = value;
        self.to_owned()
    }

    /// Sets the value for the month field.
    ///
    /// # Parameters
    /// - `value`: The value to set for the month field.
    ///
    /// # Returns
    /// A new instance of the struct with the updated month value.
    pub fn set_month(&mut self, value: String) -> Self {
        self.month = value;
        self.to_owned()
    }

    /// Sets the value for the day of the week field.
    ///
    /// # Parameters
    /// - `value`: The value to set for the day of the week field.
    ///
    /// # Returns
    /// A new instance of the struct with the updated day of the week value.
    pub fn set_day_of_week(&mut self, value: String) -> Self {
        self.day_of_week = value;
        self.to_owned()
    }

    /// Sets the value for the year field.
    ///
    /// # Parameters
    /// - `value`: The value to set for the year field.
    ///
    /// # Returns
    /// A new instance of the struct with the updated year value.
    pub fn set_year(&mut self, value: String) -> Self {
        self.year = value;
        self.to_owned()
    }

}
