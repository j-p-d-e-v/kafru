use crate::database::Db;
use serde::{Serialize, Deserialize};
use surrealdb::sql::Thing;
use serde_json::Value;
use chrono::{DateTime, Utc};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MetricKind {
    Scheduler,
    Worker,
}

impl std::fmt::Display for MetricKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Scheduler => write!(f,"Scheduler"),
            Self::Worker => write!(f,"Worker"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricData {
    pub id: Option<Thing>,
    pub kind: Option<MetricKind>,
    pub name: Option<String>,
    pub num_alive_tasks: Option<usize>,
    pub num_workers: Option<usize>,
    pub timestamp: Option<DateTime<Utc>>
}

impl  Default for MetricData {
    fn default() -> Self {
        Self {
            id: None,
            kind: None,
            name: None,
            num_alive_tasks: None,
            num_workers: None,
            timestamp: Some(Utc::now())
        }
    }
}

#[derive(Debug)]
pub struct Metric<'a>{
    db: Db,
    pub table: &'a str
}

#[derive(Debug, Clone)]
pub struct MetricListConditions {
    pub kind: Option<Vec<MetricKind>>,
}

impl Default for MetricListConditions {
    fn default() -> Self {
        Self {       
            kind: None,
        }
    }
}

impl<'a> Metric<'a>{

    /// Initializes the metrics struct and sets up the database client.
    pub async fn new() -> Self {
        Self { 
            db: Db::new(None).await.unwrap(),
            table: "kafru_metrics"
        }
    }

    /// Provides a list of available metrics and their information.
    pub async fn list(&self,conditions: MetricListConditions) -> Result<Vec<MetricData>,String> {
        let mut bindings: HashMap<&str,Value> = HashMap::new();
        bindings.insert("table", Value::String(self.table.to_string()));
        let mut stmt = String::from("SELECT * FROM type::table($table)");
        let mut stmt_where: Vec<&str> = Vec::new();
        if let Some(kinds) = conditions.kind {
            let values: Vec<Value> = kinds.into_iter().map(|value| Value::String(value.to_string())).collect();
            bindings.insert("kind", Value::Array(values));
            stmt_where.push("kind IN $kind");
        }

        if stmt_where.len() > 0 {
            stmt.push_str(format!(" WHERE {}",stmt_where.join(" AND ")).as_str());
        }
        
        match self.db.client.query(stmt)
        .bind(bindings).await {
            Ok(mut response) => {
                match response.take::<Vec<MetricData>>(0) {
                    Ok(data)=> {
                        Ok(data)
                    },        
                    Err(error) => Err(error.to_string())
                }
            },        
            Err(error) => Err(error.to_string())
        }
    }

    /// Creates or updates metric information, replacing existing metrics identified by their metric name.
    pub async fn create(&self, mut data: MetricData ) -> Result<MetricData,String> {
        let id: String = uuid::Uuid::new_v4().to_string();
        if data.kind.is_none() {
            return Err("metric kind is required".to_string());
        }
        if data.name.is_none() {
            return Err("name is required".to_string());
        }
        data.timestamp = Some(Utc::now());

        let _ = self.remove(data.clone().name.unwrap()).await;
        match self.db.client.create::<Option<MetricData>>((self.table,id)).content(data).await {
            Ok(result) => {
                if let Some(record) = result {
                    return Ok(record);
                }
                Err("unable to push record".to_string())
            }
            Err(error) => Err(error.to_string())
        }
    }

    /// Removes metric information identified by its name.
    pub async fn remove(&self, name: String) -> Result<bool,String> {
        match self.db.client.query("DELETE FROM type::table($table) WHERE name=$name;")
        .bind(("table",self.table))
        .bind(("name",name)).await {
            Ok(mut response) => {
                match response.take::<Option<Value>>(0){
                    Ok(_) => {
                        return Ok(true);
                    }
                    Err(error) => Err(error.to_string())
                }
            }
            Err(error) => Err(error.to_string())
        }
    }
}
