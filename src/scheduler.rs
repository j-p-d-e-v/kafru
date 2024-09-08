use crate::database::Db;
use serde::{Serialize, Deserialize};
use surrealdb::sql::Thing;
use serde_json::{Value, Number};
use crate::cron_schedule::CronSchedule;
use chrono::{DateTime, Utc};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SchedulerStatus {
    Enabled,
    Disabled,
}

impl std::fmt::Display for SchedulerStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Enabled => write!(f,"Enabled"),
            Self::Disabled => write!(f,"Disabled"),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SchedulerData {
    pub id: Option<Thing>,
    pub name: Option<String>,
    pub queue: Option<String>,
    pub cron_expression: Option<CronSchedule>,
    pub handler: Option<String>,
    pub parameters: Option<HashMap<String,Value>>,
    pub status: Option<SchedulerStatus>,
    pub one_time: bool,
    pub start_schedule: Option<DateTime<Utc>>,
    pub until_schedule: Option<DateTime<Utc>>,
    pub next_schedule: Option<DateTime<Utc>>,
    pub date_created: Option<DateTime<Utc>>,
    pub date_modified: Option<DateTime<Utc>>
}

impl  Default for SchedulerData {
    fn default() -> Self {
        Self {
            id: None,
            name: None,
            parameters: None,
            cron_expression: None,
            queue: None,
            handler: None,
            status: Some(SchedulerStatus::Disabled),
            one_time: false,
            start_schedule: None,
            next_schedule: None,
            until_schedule: None,
            date_created: None,
            date_modified: None
        }
    }
}

#[derive(Debug)]
pub struct Scheduler<'a>{
    db: Db,
    pub table: &'a str
}

#[derive(Debug, Clone)]
pub struct SchedulerListConditions {
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

impl Default for SchedulerListConditions {
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

impl<'a> Scheduler<'a>{

    pub async fn new() -> Self {
        Self { 
            db: Db::new(None).await.unwrap(),
            table: "kafru_scheduler"
        }
    }

    pub async fn list(&self,conditions: SchedulerListConditions) -> Result<Vec<SchedulerData>,String> {
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
            stmt_where.push("handler IN $one_time");
        }
        if conditions.name.is_some() {
            let values: Vec<Value> = conditions.name.unwrap().into_iter().map(|value| Value::String(value)).collect();
            bindings.insert("name", Value::Array(values));
            stmt_where.push("name IN $name");
        }
        
        if let Some(start_schedule) = conditions.start_schedule {
            bindings.insert("start_schedule", Value::String(start_schedule.to_string()));
            stmt_where.push("start_schedule <= $start_schedule");
        }
        
        if let Some(until_schedule) = conditions.until_schedule {
            bindings.insert("until_schedule", Value::String(until_schedule.to_string()));
            stmt_where.push("until_schedule >= $until_schedule");
        }
        
        if let Some(upcoming) = conditions.upcoming {
            if upcoming {
                let today: DateTime<Utc> = Utc::now();
                bindings.insert("next_schedule", Value::String(today.to_string()));
                stmt_where.push("next_schedule <= $next_chedule");
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
                match response.take::<Vec<SchedulerData>>(0) {
                    Ok(data)=> {
                        Ok(data)
                    },        
                    Err(error) => Err(error.to_string())
                }
            },        
            Err(error) => Err(error.to_string())
        }
    }

    pub async fn create(&self, mut data: SchedulerData ) -> Result<SchedulerData,String> {
        let id: String = uuid::Uuid::new_v4().to_string();
        let cron_expression = data.cron_expression.clone().unwrap_or_default();
        data.date_created = Some(Utc::now());       
        data.start_schedule = Some(data.start_schedule.unwrap_or(Utc::now()));
        data.next_schedule = cron_expression.get_upcoming(data.start_schedule).unwrap();
        match self.db.client.create::<Option<SchedulerData>>((self.table,id)).content(data).await {
            Ok(result) => {
                if let Some(record) = result {
                    return Ok(record);
                }
                Err("unable to push record".to_string())
            }
            Err(error) => Err(error.to_string())
        }
    }

    pub async fn remove(&self, id: Thing ) -> Result<SchedulerData,String> {
        match self.db.client.delete::<Option<SchedulerData>>((self.table,id)).await {
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
        ").bind(("table",self.table)).await {
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
    pub async fn get(&self, id: Thing) -> Result<SchedulerData,String> {
        match self.db.client.select::<Option<SchedulerData>>((self.table,id)).await {
            Ok(data) => {
                if let Some(record) = data {
                    return Ok(record)
                }
                Err("unable to retrieve record".to_string())
            }
            Err(error) => Err(error.to_string())
        }
    }

    pub async fn update(&self, id: Thing, data: SchedulerData, use_start_schedule_in_next_schedule: bool ) -> Result<SchedulerData,String> {
        match self.get(id.clone()).await  {
            Ok(record)=> {
                let cron_expression: Option<CronSchedule> = if data.cron_expression.is_none() {  record.cron_expression } else { data.cron_expression };
                let start_schedule: DateTime<Utc> = if data.start_schedule.is_none() {  record.start_schedule.unwrap() } else { data.start_schedule.unwrap() };
                let next_schedule_datetime: DateTime<Utc> = if use_start_schedule_in_next_schedule {
                    start_schedule
                } else {
                    Utc::now()                    
                };
                let data: SchedulerData = SchedulerData {
                    name: if data.name.is_none() { record.name } else { data.name },
                    parameters: if data.parameters.is_none() {  record.parameters } else { data.parameters },
                    handler: if data.handler.is_none() {  record.handler } else { data.handler },
                    status: if data.status.is_none() {  record.status } else { data.status },
                    until_schedule: if data.until_schedule.is_none() {  record.until_schedule } else { data.until_schedule },
                    cron_expression: cron_expression.clone(),
                    one_time: data.one_time,
                    start_schedule: Some(start_schedule),
                    next_schedule: if data.one_time {  None } else { cron_expression.unwrap().get_upcoming(Some(next_schedule_datetime)).unwrap() },
                    queue: if data.queue.is_none() {  record.queue } else { data.queue },
                    date_created: if data.date_created.is_none() {  record.date_created } else { data.date_created },
                    date_modified: Some(Utc::now()),
                    ..Default::default()
                };
                match self.db.client.update::<Option<SchedulerData>>((self.table,record.id.unwrap())).content(data).await {
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
