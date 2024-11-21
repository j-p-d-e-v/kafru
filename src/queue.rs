


use chrono::{DateTime, Utc};
use serde::{ Deserialize, Serialize};
use surrealdb::RecordId;
use std::collections::HashMap;
use serde_json::{Value, Number};
use crate::database::Db;
use std::sync::Arc;

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
/// Attributes for filtering task queues:
/// 
/// - `id`: *(Optional)* Automatically assigned upon task creation.
/// - `name`: The name of the task.
/// - `queue`: The name of the queue; should match the queue being monitored by the worker. Default: queue
/// - `parameters`: The parameters to pass to the task struct.
/// - `handler`: The name of the handler registered in the task registry.
/// - `status`: The status of the queue. Refer to the `QueueStatus` enum for possible values.
/// - `message`: *(Optional)* Used for recording notes about the queue, such as error messages.
/// - `date_created`: *(Optional)* The timestamp when the record was created. Automatically assigned.
/// - `date_modified`: *(Optional)* The timestamp when the record was last modified. Automatically assigned.
pub struct QueueData {
    pub id: Option<RecordId>,
    pub name: Option<String>,
    pub queue: Option<String>,
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
            queue: Some("default".to_string()),
            handler: None,
            status: None,
            message: None,
            date_created: None,
            date_modified: None
        }
    }
}

#[derive(Debug,Clone)]
pub struct Queue<'a>{
    db: Arc<Db>,
    pub table: &'a str
}

#[derive(Debug, Serialize, Deserialize)]
/// Attributes for filtering task queues:
/// 
/// - `status`: The list of queue statuses to filter by.
/// - `names`: The queue name(s) to filter by.
/// - `limit`: The number of items to retrieve.
pub struct QueueListConditions {
    pub status: Option<Vec<String>>,
    pub queue: Option<Vec<String>>,
    pub limit: Option<usize>
}

impl Default for QueueListConditions {
    fn default() -> Self {
        Self {            
            status: None,
            queue: None,
            limit: None
        }
    }
}

impl<'a> Queue<'a>{

    /// Initializes the `Queue` struct and Database Connection.
    /// # Parameters
    /// - `db`: An optional `Arc<Db>` instance representing a shared database connection.
    ///         If provided, the `Queue` will use this connection for its operations.
    pub async fn new(db: Option<Arc<Db>>) -> Self {
        let db: Arc<Db> = if db.is_some() {
            db.unwrap().clone()
        } else {
            Arc::new(Db::new(None).await.unwrap())
        };
        Self { 
            db,
            table: "kafru_queues"
        }
    }


    /// Lists all task queues.
    ///
    /// # Parameters
    /// - `conditions`: Filter configuration used to retrieve specific task queues using `QueueListConditions` struct.
    pub async fn list(&self, conditions: QueueListConditions) -> Result<Vec<QueueData>,String> {
        let mut bindings: HashMap<&str,Value> = HashMap::new();
        bindings.insert("table", Value::String(self.table.to_string()));
        let mut stmt = String::from("SELECT * FROM type::table($table)");
        let mut stmt_where: Vec<&str> = Vec::new();
        if let Some(status) = conditions.status {
            let status: Vec<Value> = status.into_iter().map(|value| Value::String(value)).collect();
            bindings.insert("status", Value::Array(status));
            stmt_where.push("status IN $status");
        }
        if let Some(queue) = conditions.queue {        
            let queue: Vec<Value> = queue.into_iter().map(|queue| Value::String(queue)).collect();
            bindings.insert("queue", Value::Array(queue));
            stmt_where.push("queue IN $queue");
        }
        if stmt_where.len() > 0 {
            stmt.push_str(format!(" WHERE {}",stmt_where.join(" AND ")).as_str());
        }
        
        stmt.push_str(" ORDER BY date_created ASC");

        if let Some(limit) = conditions.limit { 
            bindings.insert("limit", Value::Number(Number::from(limit)));
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

    /// Pushes queue information, which will then be picked up by the worker for execution.
    ///
    /// # Parameters
    /// - `data`: The queue data, provided as a `QueueData` struct.
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

    /// Removes a queue record by its ID.
    ///
    /// # Parameters
    /// - `id`: The ID of the queue record to remove
    pub async fn remove(&self, id: RecordId ) -> Result<QueueData,String> {
        match self.db.client.delete::<Option<QueueData>>(id).await {
            Ok(result) => {
                if let Some(record) = result {
                    return Ok(record);
                }
                Err("unable to remove record".to_string())
            }
            Err(error) => Err(error.to_string())
        }
    }
    
    /// Removes all queue records.
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

    /// Retrieves a queue record by its ID.
    ///
    /// # Parameters
    /// - `id`: The ID of the queue record to retrieve.
    pub async fn get(&self, id: RecordId) -> Result<QueueData,String> {
        match self.db.client.select::<Option<QueueData>>(id).await {
            Ok(data) => {
                if let Some(record) = data {
                    return Ok(record)
                }
                Err("unable to retrieve record".to_string())
            }
            Err(error) => Err(error.to_string())
        }
    }
    
    pub async fn set_status(&self, id: RecordId, status: QueueStatus, message: Option<String> ) -> Result<QueueData,String> {
        match self.get(id.clone()).await {
            Ok(record)=> {
                let data: QueueData = QueueData {  
                    status: Some(status),                          
                    date_modified: Some(Utc::now()),  
                    message,
                    ..record
                };
                match self.db.client.update::<Option<QueueData>>(id).content(data).await {
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
    
    /// Updates information for a specific queue record.
    ///
    /// # Parameters
    /// - `id`: The ID of the queue record to update.
    /// - `data`: The updated queue data, provided as a `QueueData` struct.
    pub async fn update(&self, id: RecordId, data: QueueData ) -> Result<QueueData,String> {
        match self.get(id.clone()).await  {
            Ok(record)=> {
                let data: QueueData = QueueData {                            
                    name: if data.name.is_none() { record.name } else { data.name },
                    parameters: if data.parameters.is_none() {  record.parameters } else { data.parameters },
                    handler: if data.handler.is_none() {  record.handler } else { data.handler },
                    status: if data.status.is_none() {  record.status } else { data.status },
                    message: if data.message.is_none() {  record.message } else { data.message },
                    queue: if data.queue.is_none() {  record.queue } else { data.queue },
                    date_created: if data.date_created.is_none() {  record.date_created } else { data.date_created },
                    date_modified: Some(Utc::now()),
                    ..Default::default()
                };
                match self.db.client.update::<Option<QueueData>>(record.id.unwrap()).content(data).await {
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
