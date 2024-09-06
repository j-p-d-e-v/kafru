


use chrono::{DateTime, Utc};
use serde::{ Deserialize, Serialize};
use surrealdb::sql::Thing;
use std::collections::HashMap;
use serde_json::{Value, Number};
use crate::database::Db;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum QueueStatus {
    Waiting,
    InProgress,
    Error,
    Completed
}

impl std::fmt::Display for QueueStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Waiting => write!(f,"Waiting"),
            Self::InProgress => write!(f,"InProgress"),
            Self::Error => write!(f,"Error"),
            Self::Completed => write!(f,"Completed"),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QueueData {
    pub id: Option<Thing>,
    pub name: Option<String>,
    pub handler: Option<String>,
    pub parameters: Option<HashMap<String,Value>>,
    pub status: Option<QueueStatus>,
    pub message: Option<String>,
    pub date_created: Option<DateTime<Utc>>,
    pub date_modified: Option<DateTime<Utc>>
}

impl  Default for QueueData {
    fn default() -> Self {
        Self {
            id: None,
            name: None,
            parameters: None,
            handler: None,
            status: None,
            message: None,
            date_created: None,
            date_modified: None
        }
    }
}

#[derive(Debug)]
pub struct Queue<'a>{
    db: Db,
    pub table: &'a str
}

impl<'a> Queue<'a>{

    pub async fn new() -> Self {
        Self { 
            db: Db::new(None).await.unwrap(),
            table: "kafru_queue"
        }
    }

    pub async fn list(&self,status:Vec<String>, limit: Option<u64>) -> Result<Vec<QueueData>,String> {
        let mut bindings: HashMap<&str,Value> = HashMap::new();
        bindings.insert("table", Value::String(self.table.to_string()));
        let mut stmt = String::from("SELECT * FROM type::table($table)");
        if status.len() > 0 {
            let status: Vec<Value> = status.into_iter().map(|value| Value::String(value)).collect();
            bindings.insert("status", Value::Array(status));
            stmt.push_str(" WHERE status IN $status");
        }
        
        stmt.push_str(" ORDER BY date_created ASC");

        if let Some(value) = limit {
            bindings.insert("limit", Value::Number(Number::from(value)));
            stmt.push_str(" LIMIT $limit");
        }
        match self.db.client.query(stmt)
        .bind(bindings).await {
            Ok(mut response) => {
                match response.take::<Vec<QueueData>>(0) {
                    Ok(data)=> {
                        Ok(data)
                    },        
                    Err(error) => Err(error.to_string())
                }
            },        
            Err(error) => Err(error.to_string())
        }
    }

    pub async fn push(&self, mut data: QueueData ) -> Result<QueueData,String> {
        let id: String = uuid::Uuid::new_v4().to_string();
        data.date_created = Some(Utc::now());
        data.status = Some(QueueStatus::Waiting);
        match self.db.client.create::<Option<QueueData>>((self.table,id)).content(data).await {
            Ok(result) => {
                if let Some(record) = result {
                    return Ok(record);
                }
                Err("unable to push record".to_string())
            }
            Err(error) => Err(error.to_string())
        }
    }

    pub async fn remove(&self, id: Thing ) -> Result<QueueData,String> {
        match self.db.client.delete::<Option<QueueData>>((self.table,id)).await {
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
    pub async fn get(&self, id: Thing) -> Result<QueueData,String> {
        match self.db.client.select::<Option<QueueData>>((self.table,id)).await {
            Ok(data) => {
                if let Some(record) = data {
                    return Ok(record)
                }
                Err("unable to retrieve record".to_string())
            }
            Err(error) => Err(error.to_string())
        }
    }

    pub async fn update(&self, id: Thing, data: QueueData ) -> Result<QueueData,String> {
        match self.get(id.clone()).await  {
            Ok(record)=> {
                let data: QueueData = QueueData {                            
                    name: if data.name.is_none() { record.name } else { data.name },
                    parameters: if data.parameters.is_none() {  record.parameters } else { data.parameters },
                    handler: if data.handler.is_none() {  record.handler } else { data.handler },
                    status: if data.status.is_none() {  record.status } else { data.status },
                    message: if data.message.is_none() {  record.message } else { data.message },
                    date_created: if data.date_created.is_none() {  record.date_created } else { data.date_created },
                    date_modified: Some(Utc::now()),
                    ..Default::default()
                };
                match self.db.client.update::<Option<QueueData>>((self.table,record.id.unwrap())).content(data).await {
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
