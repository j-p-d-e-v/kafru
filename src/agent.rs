use core::fmt;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use surrealdb::RecordId;
use crate::database::Db;
use std::sync::Arc;
use tracing::{info, error, instrument};
use std::sync::atomic::{AtomicBool, Ordering};
use crate::Command;

#[derive(Debug,Clone, PartialEq, Deserialize, Serialize)]
pub enum  AgentKind {
    Queue,
    Scheduler,
    Task,
    None
}

impl fmt::Display for AgentKind {

    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {        
        match self {
            Self::Scheduler => write!(f,"Scheduler"),
            Self::Queue => write!(f,"Queue"),
            Self::Task => write!(f,"Task"),
            Self::None => write!(f,"None")
        }
    }
}

#[derive(Debug,Clone, PartialEq, Deserialize, Serialize)]
pub enum AgentStatus {
    Initialized,
    Running,
    Terminated,
    Error,
    Completed
}

impl fmt::Display for AgentStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Initialized => write!(f,"Initialized"),
            Self::Running => write!(f,"Running"),
            Self::Terminated => write!(f,"Terminated"),
            Self::Error => write!(f,"Error"),
            Self::Completed => write!(f,"Completed"),
        }
    }
}

#[derive(Debug,Clone,Deserialize,Serialize)]
pub struct AgentData {
    pub id: Option<RecordId>,
    pub parent: Option<RecordId>,
    pub queue_id: Option<RecordId>,
    pub name: String,
    pub kind: AgentKind,
    pub server: String,
    pub runtime_id: u64,
    pub status: AgentStatus,
    pub command: Option<Command>,
    pub command_is_executed: bool,
    pub message: String,
    pub created_at: DateTime<Utc>
}

impl Default for AgentData {
    fn default() -> Self {
        Self {
            id: None,
            parent: None,
            queue_id: None,
            name: String::new(),
            kind: AgentKind::None,
            server: String::new(),
            runtime_id: 0,
            status: AgentStatus::Initialized,
            command: None,
            command_is_executed: false,
            message: String::new(),
            created_at: Utc::now()
        }
    }
}

#[derive(Debug,Clone,Deserialize,Serialize, Default)]
pub struct AgentFilter {
    pub id: Option<RecordId>,
    pub parent: Option<RecordId>,
    pub queue_id: Option<RecordId>,
    pub name: Option<String>,
    pub kind: Option<AgentKind>,
    pub server: Option<String>,
    pub runtime_id: Option<u64>,
    pub status: Option<AgentStatus>,
    pub command: Option<Command>,
    pub command_is_executed: Option<bool>,
    pub message: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Agent {
    db: Arc<Db>,
    table: String,
}

static AGENT_TABLE_CREATED: AtomicBool = AtomicBool::new(false);

impl Agent {

    pub async fn new(db: Option<Arc<Db>>) -> Self {
        let db: Arc<Db> = db.unwrap_or(
            Arc::new(Db::new(None).await.unwrap())
        );
        let table: String = "kafru_agents".to_string();
        Self {
            db,
            table
        }
    }
    #[instrument(skip_all)]
    pub async fn purge(&self,server: String) -> Result<bool,String> {
        match self.db.client.query("DELETE type::table($table) WHERE server = $server")
            .bind(("table",self.table.clone()))
            .bind(("server",server.clone()))
        .await {
            Ok(_) => {
                Ok(true)
            },
            Err(error) => {
                error!("{}",error);
                Err(format!("unable to purge {} for server {}",self.table,server))
            }
        }
    }

    #[instrument(skip_all)]
    pub async fn create_table(&self, server: String) -> Result<bool,String> {
        let is_created: bool = AGENT_TABLE_CREATED.load(Ordering::Relaxed);
        if is_created {
            return Ok(true);
        }
        let stmt: String = format!("DEFINE TABLE IF NOT EXISTS {};",self.table);
        match self.db.client.query(stmt).await {
            Ok(_) => {
                info!("table {} has been created",&self.table);
                if let Err(error) = self.purge(server).await {                
                    return Err(error);
                }
                AGENT_TABLE_CREATED.store(true, Ordering::Relaxed);
                Ok(true)
            }
            Err(error) => {
                error!("{}",error);
                Err(format!("unable to create table {}",&self.table))
            }
        }
    }

    pub async fn remove(&self) -> Result<bool,String> {
        todo!("remove the record");
    }

    pub async fn get_by_id(&self,id: RecordId) -> Result<AgentData, String> {

        match self.db.client.select::<Option<AgentData>>(id.clone()).await {
            Ok(data) => {
                if let Some(item) = data {
                    return Ok(item);
                }
                return Err(format!("unable to find agent with id: {:?}",id));
            }
            Err(error) => {
                error!("{}",error);
                Err(format!("database error when retrieving agent data with id: {:?}",id))
            }
        }
    }

    pub async fn list(&self,filters: AgentFilter) -> Result<Vec<AgentData>,String> {
        let mut  stmt: String = "SELECT * FROM  type::table($table)".to_string();
        let mut where_stmt: Vec<String> = Vec::new();

        // WHERE placeholders
        if filters.id.is_some() {
            where_stmt.push("type::thing(id)=$id".to_string());
        }
        if filters.parent.is_some() {
            where_stmt.push("type::thing(parent)=$parent".to_string());
        }
        if filters.command.is_some() {
            where_stmt.push("command=$command".to_string());
        }
        if filters.command_is_executed.is_some() {
            where_stmt.push("command_is_executed=$command_is_executed".to_string());
        }
        if filters.kind.is_some() {
            where_stmt.push("kind=$kind".to_string());
        }
        if filters.name.is_some() {
            where_stmt.push("name=$name".to_string());
        }
        if filters.queue_id.is_some() {
            where_stmt.push("queue_id=$queue_id".to_string());
        }
        if filters.server.is_some() {
            where_stmt.push("server=$server".to_string());
        }
        if filters.status.is_some() {
            where_stmt.push("status=$status".to_string());
        }
        if !where_stmt.is_empty() {
            stmt = format!("{} WHERE {}",stmt,where_stmt.join(" AND "));
        }

        // VALUE Binding
        let mut query = self.db.client.query(stmt).bind(("table",self.table.clone()));
        
        if let Some(value) = filters.id {
            query = query.bind(("id",value));
        }
        if let Some(value) = filters.parent {
            query = query.bind(("parent",value));
        }
        if let Some(value) = filters.command {
            query = query.bind(("command",value));
        }
        if let Some(value) = filters.command_is_executed {
            query = query.bind(("command_is_executed",value));
        }
        if let Some(value) = filters.kind {
            query = query.bind(("kind",value));
        }
        if let Some(value) = filters.name {
            query = query.bind(("name",value));
        }
        if let Some(value) = filters.queue_id {
            query = query.bind(("queue_id",value));
        }
        if let Some(value) = filters.server {
            query = query.bind(("server",value));
        }
        if let Some(value) = filters.status {
            query = query.bind(("status",value));
        }

        match query.await {
            Ok(mut response) => {
                if let Ok(data) =  response.take::<Vec<AgentData>>(0) {
                    return Ok(data);
                }
                return Err(format!("unable to retrive agent data"));
            }
            Err(error) => {
                error!("{}",error);
                return Err("database error when retrieving agent data".to_string());
            }
        }
    }

    pub async fn update_by_id(&self,id: RecordId, data:AgentData) -> Result<AgentData,String> {
        match self.db.client.update::<Option<AgentData>>(id).content(data.clone()).await {
            Ok(response) => {
                if let Some(data) = response {
                    return Ok(data);
                }
                Err(format!("agent {} under server {} not found",data.name,data.server))
            }
            Err(error) => {
                error!("{}",error);
                Err(format!("unable to update agent {} under server {}",data.name,data.server))
            }
        }
    }

    
    pub async fn update_by_parent_id(&self,parent: RecordId, data:AgentData) -> Result<AgentData,String> {
        let stmt: String = "SELECT * FROM type::table($table) WHERE type::thing(parent)=$parent and runtime_id=$runtime_id".to_string();
        match self.db.client.query(stmt)
        .bind(("table",self.table.clone()))
        .bind(("parent",parent))
        .bind(("runtime_id",data.runtime_id.clone())).await {
            Ok(mut response) => {
                if let Ok(agent_data) = response.take::<Option<AgentData>>(0) {
                    match agent_data  {
                        Some(item) => {
                            match self.db.client.update::<Option<AgentData>>(item.id.unwrap()).content(data.clone()).await {
                                Ok(response) => {
                                    if let Some(data) = response {
                                        return Ok(data);
                                    }
                                    return Err(format!("agent {} under server {} not found",&data.name,&data.server));
                                }
                                Err(error) => {
                                    error!("{}",error);
                                    return Err(format!("unable to update agent {} under server {}",&item.name,&item.server));
                                }
                            }
                        }
                        None => {
                            return Err(format!("query for agent {} under server {} returns nothing",&data.name,&data.server));                            
                        }
                    }
                }
                Err(format!("database error for agent {} under server {}",&data.name,&data.server))       
            }
            Err(error) =>{
                error!("{}",error);
                Err(format!("database error for agent {} under server {}",&data.name,&data.server))
            }
        }
    }

    pub async fn register(&self, data: AgentData) -> Result<AgentData,String>{
        if data.name.len() == 0 {
            return Err("name is required".to_string());
        }
        if data.kind == AgentKind::None {
            return Err("kind is required".to_string())
        }
        if data.runtime_id == 0 && data.kind == AgentKind::Task {
            return Err("runtime_id must be greater than 0 for task".to_string());
        }
        if data.queue_id.is_none() && data.kind == AgentKind::Task {
            return Err("runtime_id must be greater than 0 for task".to_string());
        }
        if let Err(error) = self.create_table(data.server.clone()).await {
            return Err(error);
        }
        match self.db.client.create::<Option<AgentData>>(self.table.clone()).content(data.clone()).await {
            Ok(response) => {
                if let Some(data) = response {
                    return Ok(data);
                }
                return Err(format!("no data found for agent {} at server {}",data.name,data.server))
            }
            Err(error) => {
                error!("{}",error);
                println!("{:?}",error);
                Err(format!("unable to register agent {} at server {}",data.name,data.server))
            }
        }
    }
}


#[cfg(test)]
pub mod test_agent {
    use super::*;
    use crate::tests::test_helper::configure_database_env;

    #[tokio::test]
    pub async fn test() {
        configure_database_env();
        let db: Arc<Db> = Arc::new(
            Db::new(None).await.unwrap()
        );
        let agent: Agent = Agent::new(Some(db)).await;
        let result: Result<bool, String> = agent.create_table("server1".to_string()).await;
        assert!(result.is_ok(),"{:?}",result.err());
        assert!(result.unwrap());

        let agents_data = agent.list(AgentFilter {
            server: Some("server1".to_string()),
            ..Default::default()
        }).await;
        assert!(agents_data.is_ok(),"{:?}",agents_data.err());
        assert!(agents_data.unwrap().len() == 0);

        for s in 1..3 {
            let server: String = format!("server{}",s);
            for i in 1..11 {
                let result = agent.register(AgentData {
                    name: format!("queue-{}-server-{}",i,s),
                    kind: AgentKind::Queue,
                    server: server.clone(),
                    runtime_id: i,
                    ..Default::default()
                }).await;
                assert!(result.is_ok(),"{:?}",result.err());
                let data = result.unwrap();
                let updated_result = agent.update_by_id(data.id.clone().unwrap(), AgentData {
                    status: AgentStatus::Completed,
                    ..data
                } ).await;
                assert!(updated_result.is_ok(),"{:?}",updated_result.err());
                let updated_data = updated_result.unwrap();
                assert!(data.status != updated_data.status);
            }
        }
        let agents_data = agent.list(AgentFilter {
            server: Some("server1".to_string()),
            ..Default::default()
        }).await;
        assert!(agents_data.is_ok(),"{:?}",agents_data.err());
        assert!(agents_data.unwrap().len() == 10);
    }
}