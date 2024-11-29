use crate::database::Db;
use serde::{Serialize, Deserialize};
use surrealdb::RecordId;
use serde_json::{Value, Number};
use crate::cron_schedule::CronSchedule;
use chrono::{DateTime, Duration, Utc};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ScheduleStatus {
    Enabled,
    Disabled,
}

impl std::fmt::Display for ScheduleStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Enabled => write!(f,"Enabled"),
            Self::Disabled => write!(f,"Disabled"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
/// The `ScheduleData` struct holds information about a schedule, including its configuration, status, and timing details.
///
/// # Fields
/// 
/// - `id`: (Optional) A unique identifier for the schedule. This can be used to reference or manage the schedule. Automatically assigned during creation.
/// - `name`: (Optional) The name of the schedule. This is used for identification and organization.
/// - `queue`: (Optional) The name of the queue to which this schedule's tasks will be added. This should match a queue that is being monitored by a worker.
/// - `cron_expression`: (Optional) A `CronSchedule` struct representing the cron expression used to determine when the schedule should run.
/// - `handler`: (Optional) The name of the handler registered in the task registry that will process the tasks. This should correspond to a registered task handler.
/// - `parameters`: (Optional) A map of parameters (`HashMap<String, Value>`) to pass to the task handler. These parameters can customize the behavior of the task.
/// - `status`: (Optional) The status of the schedule, represented by the `ScheduleStatus` enum. This indicates whether the schedule is enabled or dsiabled. Default: Disabled
/// - `one_time`: A boolean flag indicating whether the schedule is a one-time event or recurring. If `true`, the schedule will only run once.
/// - `start_schedule`: (Optional) The start date and time of the schedule, represented as a `DateTime<Utc>`. This indicates when the schedule should begin execution.
/// - `until_schedule`: (Optional) The end date and time of the schedule, represented as a `DateTime<Utc>`. This specifies until when the schedule should continue executing.
/// - `next_schedule`: (Optional) The next date and time when the schedule is expected to run, represented as a `DateTime<Utc>`. This is useful for determining the upcoming execution time.
/// - `date_created`: (Optional) The timestamp when the schedule was created, represented as a `DateTime<Utc>`. This is used for tracking the creation time of the schedule.
/// - `date_modified`: (Optional) The timestamp when the schedule was last modified, represented as a `DateTime<Utc>`. This is useful for tracking changes made to the schedule.
pub struct ScheduleData {
    pub id: Option<RecordId>,
    pub name: Option<String>,
    pub queue: Option<String>,
    pub cron_expression: Option<CronSchedule>,
    pub handler: Option<String>,
    pub parameters: Option<HashMap<String,Value>>,
    pub status: Option<ScheduleStatus>,
    pub one_time: bool,
    pub start_schedule: Option<DateTime<Utc>>,
    pub until_schedule: Option<DateTime<Utc>>,
    pub next_schedule: Option<DateTime<Utc>>,
    pub date_created: Option<DateTime<Utc>>,
    pub date_modified: Option<DateTime<Utc>>
}

impl  Default for ScheduleData {
    fn default() -> Self {
        Self {
            id: None,
            name: None,
            parameters: None,
            cron_expression: None,
            queue: None,
            handler: None,
            status: Some(ScheduleStatus::Disabled),
            one_time: false,
            start_schedule: None,
            next_schedule: None,
            until_schedule: None,
            date_created: None,
            date_modified: None
        }
    }
}

#[derive(Debug,Clone)]
pub struct Schedule<'a>{
    db: Arc<Db>,
    pub table: &'a str
}

#[derive(Debug, Clone)]
/// Represents the conditions used for filtering a list of schedules.
///
/// # Fields
///
/// - `status`: (Optional) A vector of schedule statuses to filter by. Only schedules matching one of these statuses will be included in the result.
/// - `queue`: (Optional) A vector of queue names to filter by. Only schedules associated with one of these queues will be included in the result.
/// - `handler`: (Optional) A vector of handler names to filter by. Only schedules using one of these handlers will be included in the result.
/// - `name`: (Optional) A vector of schedule names to filter by. Only schedules with names matching one of these values will be included in the result.
/// - `start_schedule`: (Optional) A start date and time (`DateTime<Utc>`) to filter by. Only schedules that start on or after this date and time will be included in the result.
/// - `until_schedule`: (Optional) An end date and time (`DateTime<Utc>`) to filter by. Only schedules that end on or before this date and time will be included in the result.
/// - `one_time`: (Optional) A boolean flag indicating whether to filter by one-time schedules only. If `true`, only one-time schedules will be included in the result.
/// - `upcoming`: (Optional) A boolean flag indicating whether to filter by upcoming schedules only. If `true`, only schedules that are upcoming (i.e., with a `next_schedule` in the future) will be included in the result.
/// - `limit`: (Optional) An optional integer specifying the maximum number of schedules to return. If `Some(n)` is provided, the result will be limited to `n` schedules.
pub struct ScheduleListConditions {
    pub status: Option<Vec<String>>,
    pub queue: Option<Vec<String>>,
    pub handler: Option<Vec<String>>,
    pub name: Option<Vec<String>>,
    pub start_schedule: Option<DateTime<Utc>>,
    pub until_schedule: Option<DateTime<Utc>>,
    pub one_time: Option<bool>,
    pub upcoming: Option<bool>,
    pub limit: Option<usize>
}

impl Default for ScheduleListConditions {
    fn default() -> Self {
        Self {            
            status: None,
            queue: None,
            handler: None,
            name: None,
            one_time: None,
            start_schedule: None,
            until_schedule: None,
            upcoming: Some(true),
            limit: None
        }
    }
}

impl<'a> Schedule<'a>{

    /// Initializes the `Schedule` struct and Database Connection.
    /// # Parameters
    /// - `db`: An optional `Arc<Db>` instance representing a shared database connection.
    ///         If provided, the `Schedule` will use this connection for its operations.
    pub async fn new(db: Option<Arc<Db>>) -> Self {
        let db: Arc<Db> = if let Some(value) = db {
            value.clone()
        }
        else {
            Arc::new(Db::new(None).await.unwrap())
        };
        Self { 
            db,
            table: "kafru_schedules"
        }
    }

    pub async fn list(&self,conditions: ScheduleListConditions) -> Result<Vec<ScheduleData>,String> {
        let mut bindings: HashMap<&str,Value> = HashMap::new();
        bindings.insert("table", Value::String(self.table.to_string()));
        let mut stmt = String::from("SELECT * FROM type::table($table)");
        let mut stmt_where: Vec<&str> = Vec::new();
        if conditions.status.is_some() {
            let values: Vec<Value> = conditions.status.unwrap().into_iter().map(|value| Value::String(value)).collect();
            bindings.insert("status", Value::Array(values));
            stmt_where.push("status IN $status");
        }
        if conditions.queue.is_some() {
            let values: Vec<Value> = conditions.queue.unwrap().into_iter().map(|value| Value::String(value)).collect();
            bindings.insert("queue", Value::Array(values));
            stmt_where.push("queue IN $queue");
        }
        if conditions.handler.is_some() {
            let values: Vec<Value> = conditions.handler.unwrap().into_iter().map(|value| Value::String(value)).collect();
            bindings.insert("handler", Value::Array(values));
            stmt_where.push("handler IN $handler");
        }
        if conditions.one_time.is_some() {
            bindings.insert("one_time", Value::Bool(conditions.one_time.unwrap()));
            stmt_where.push("one_time=$one_time");
        }
        if conditions.name.is_some() {
            let values: Vec<Value> = conditions.name.unwrap().into_iter().map(|value| Value::String(value)).collect();
            bindings.insert("name", Value::Array(values));
            stmt_where.push("name IN $name");
        }
        if let Some(start_schedule) = conditions.start_schedule {
            bindings.insert("start_schedule", Value::String(start_schedule.to_rfc3339()));
            stmt_where.push("start_schedule <= $start_schedule");
        }
        if let Some(until_schedule) = conditions.until_schedule {
            bindings.insert("until_schedule", Value::String(until_schedule.to_rfc3339()));
            stmt_where.push("until_schedule >= $until_schedule");
        }
        
        if let Some(upcoming) = conditions.upcoming {
            if upcoming {
                let today: String = Utc::now().to_rfc3339();
                bindings.insert("next_schedule", Value::String(today));
                stmt_where.push("next_schedule <= $next_schedule");
            }
        }

        if stmt_where.len() > 0 {
            stmt.push_str(format!(" WHERE {}",stmt_where.join(" AND ")).as_str());
        }
        
        stmt.push_str(" ORDER BY next_schedule ASC");

        if conditions.limit.is_some() {
            bindings.insert("limit", Value::Number(Number::from(conditions.limit.unwrap())));
            stmt.push_str(" LIMIT $limit");
        }
        match self.db.client.query(stmt)
        .bind(bindings).await {
            Ok(mut response) => {
                match response.take::<Vec<ScheduleData>>(0) {
                    Ok(data)=> {
                        Ok(data)
                    },        
                    Err(error) => Err(error.to_string())
                }
            },        
            Err(error) => Err(error.to_string())
        }
    }

    pub async fn create(&self, mut data: ScheduleData ) -> Result<ScheduleData,String> {
        let id: String = uuid::Uuid::new_v4().to_string();
        data.date_created = Some(Utc::now());       
        data.start_schedule = Some(data.start_schedule.unwrap_or(Utc::now()));
        if let Some(cron_expression) = data.cron_expression.clone() {
            // If recurring schedule otherwise the next schedule is manually set.
            let cron_expression = cron_expression;
            data.next_schedule = cron_expression.get_upcoming(data.start_schedule).unwrap();
        }
        else {
            if data.next_schedule.is_none() {
                return Err("cron expression is required when the next scheduled time is not provided".to_string());
            }
        }
        match self.db.client.create::<Option<ScheduleData>>((self.table,id)).content(data).await {
            Ok(result) => {
                if let Some(record) = result {
                    return Ok(record);
                }
                Err("unable to push record".to_string())
            }
            Err(error) => Err(error.to_string())
        }
    }

    pub async fn remove(&self, id: RecordId ) -> Result<ScheduleData,String> {
        match self.db.client.delete::<Option<ScheduleData>>(id).await {
            Ok(result) => {
                if let Some(record) = result {
                    return Ok(record);
                }
                Err("unable to remove record".to_string())
            }
            Err(error) => Err(error.to_string())
        }
    }

    pub async fn purge(&self) -> Result<u64,String> {
        match self.db.client.query("
            SELECT count() as total FROM type::table($table) GROUP ALL;
            DELETE FROM type::table($table);
        ").bind(("table",self.table.to_owned())).await {
            Ok(mut response) => {
                match response.take::<Option<Value>>(0){
                    Ok(data) => {
                        let mut total: u64 = 0;
                        if let Some(item) = data {
                            total = item.get("total").unwrap().as_u64().unwrap();
                        }
                        Ok(total)
                    }
                    Err(error) => Err(error.to_string())
                }
            }
            Err(error) => Err(error.to_string())
        }
    }
    pub async fn get(&self, id: RecordId) -> Result<ScheduleData,String> {
        match self.db.client.select::<Option<ScheduleData>>(id).await {
            Ok(data) => {
                if let Some(record) = data {
                    return Ok(record)
                }
                Err("unable to retrieve record".to_string())
            }
            Err(error) => Err(error.to_string())
        }
    }

    pub async fn update(&self, id: RecordId, data: ScheduleData) -> Result<ScheduleData,String> {
        match self.get(id.clone()).await  {
            Ok(record)=> {
                let cron_expression: Option<CronSchedule> = if data.cron_expression.is_none() {  record.cron_expression } else { data.cron_expression };
                let start_schedule: DateTime<Utc> = if data.start_schedule.is_none() {  record.start_schedule.unwrap() } else { data.start_schedule.unwrap() };
                let next_schedule: Option<DateTime<Utc>> =  if data.one_time {  None } else { 
                    if data.next_schedule.is_none() {
                        if let Some(cron_expression) = cron_expression.clone() {
                            let cron_upcoming_start = if start_schedule > Utc::now() { start_schedule } else { Utc::now() + Duration::minutes(1) };
                            cron_expression.get_upcoming(Some(cron_upcoming_start)).unwrap() 
                        }
                        else {
                            return Err("cron expression is required when the next scheduled time is not provided".to_string())
                        }
                    }
                    else {
                        data.next_schedule
                    }
                };
                let data: ScheduleData = ScheduleData {
                    name: if data.name.is_none() { record.name } else { data.name },
                    parameters: if data.parameters.is_none() {  record.parameters } else { data.parameters },
                    handler: if data.handler.is_none() {  record.handler } else { data.handler },
                    status: if data.status.is_none() {  record.status } else { data.status },
                    until_schedule: if data.until_schedule.is_none() {  record.until_schedule } else { data.until_schedule },
                    cron_expression: cron_expression.clone(),
                    one_time: data.one_time,
                    start_schedule: Some(start_schedule),
                    next_schedule,
                    queue: if data.queue.is_none() {  record.queue } else { data.queue },
                    date_created: if data.date_created.is_none() {  record.date_created } else { data.date_created },
                    date_modified: Some(Utc::now()),
                    ..Default::default()
                };
                match self.db.client.update::<Option<ScheduleData>>(record.id.unwrap()).content(data).await {
                    Ok(result) => {
                        if let Some(record) = result {
                            return Ok(record);
                        }
                        Err("unable to update record".to_string())
                    }
                    Err(error) => Err(error.to_string())
                }
            }
            Err(error) => Err(error.to_string())
        }
    }

}
