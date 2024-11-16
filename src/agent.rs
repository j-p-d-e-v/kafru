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
    Worker,
    Scheduler,
    Task,
    None
}

impl fmt::Display for AgentKind {

    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {        
        match self {
            Self::Scheduler => write!(f,"Scheduler"),
            Self::Worker => write!(f,"Worker"),
            Self::Task => write!(f,"Task"),
            Self::None => write!(f,"None")
        }
    }
}

#[derive(Debug,Clone,Deserialize,Serialize)]
pub struct AgentData {
    pub id: Option<RecordId>,
    pub name: String,
    pub kind: AgentKind,
    pub server: String,
    pub runtime_id: u64,
    pub command: Option<Command>,
    pub command_is_executed: bool,
    pub message: String,
    pub created_at: DateTime<Utc>
}

impl Default for AgentData {
    fn default() -> Self {
        Self {
            id: None,
            name: String::new(),
            kind: AgentKind::None,
            server: String::new(),
            runtime_id: 0,
            command: None,
            command_is_executed: false,
            message: String::new(),
            created_at: Utc::now()
        }
    }
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
        println!("is_created: {}",is_created);
        if is_created {
            return Ok(true);
        }
        let stmt: String = format!(r#"
            DEFINE TABLE IF NOT EXISTS {table};
        "#, table=self.table);
        match self.db.client.query(stmt).bind(("table",self.table.clone())).await {
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

    pub async fn register(&self, data: AgentData) -> Result<AgentData,String>{

        if data.name.len() == 0 {
            return Err("name is required".to_string());
        }
        if data.kind == AgentKind::None {
            return Err("kind is required".to_string())
        }
        if data.runtime_id == 0 {
            return Err("runtime_id must be greater than 0".to_string());
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

        for s in 1..3 {
            let server: String = format!("server{}",s);
            for i in 1..11 {

                let result = agent.register(AgentData {
                    name: format!("worker-{}-server-{}",i,s),
                    kind: AgentKind::Worker,
                    server: server.clone(),
                    runtime_id: i,
                    ..Default::default()
                }).await;
                println!("{}",i);
                assert!(result.is_ok(),"{:?}",result.err());
                assert!(result.unwrap().id.is_some());
            }
        }
    }
}