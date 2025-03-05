use core::fmt;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use surrealdb::RecordId;
use crate::database::Db;
use std::sync::Arc;
use tracing::{info, error, instrument};
use std::sync::atomic::{AtomicBool, Ordering};
use crate::Command;

/// An enum representing the types of agents.
///
/// This enum is used to classify the different kinds of agents in the system. Each variant 
/// represents a specific role or functionality that the agent serves.
///
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

/// An enum representing the possible statuses of an agent.
///
/// This enum defines the different states that an agent can be in, providing a way to track 
/// the lifecycle or status of an agent during its execution. The statuses can be used to indicate 
/// whether the agent has been initialized, is actively running, has been terminated, encountered an 
/// error, or has completed its task.
///
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

/// A struct representing the data of an agent.
///
/// This struct holds the detailed information of an agent, including optional fields such as its 
/// unique identifier, task ID, status, commands, and server-related information. The fields are 
/// serializable using `serde`, and the `skip_serializing_if` attribute ensures that fields with 
/// `None` values are not included during serialization.
///
#[derive(Debug,Clone,Deserialize,Serialize,PartialEq)]
pub struct AgentData {
    #[serde(skip_serializing_if="Option::is_none")]
    pub id: Option<RecordId>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub parent_id: Option<RecordId>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub queue_id: Option<RecordId>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub kind: Option<AgentKind>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub server: Option<String>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub task_id: Option<u64>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub status: Option<AgentStatus>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub command: Option<Command>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub command_is_executed: Option<bool>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub message: Option<String>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub author: Option<String>,
    pub date_modified: DateTime<Utc>
}

impl Default for AgentData {
    fn default() -> Self {
        Self {
            id: None,
            parent_id: None,
            queue_id: None,
            name: None,
            kind: None,
            server: None,
            task_id: None,
            status: None,
            command: None,
            command_is_executed: None,
            message: None,
            author: None,
            date_modified: Utc::now()
        }
    }
}

/// A struct representing the filters that can be applied when querying agent records.
///
/// This struct allows specifying various optional fields that can be used to filter the agents 
/// based on different attributes such as their ID, name, status, task ID, and more. Each field 
/// is optional, meaning the filter can be customized based on the requirements of the query.
///
#[derive(Debug,Clone,Deserialize,Serialize, Default)]
pub struct AgentFilter {
    pub id: Option<RecordId>,
    pub ids: Option<Vec<RecordId>>,
    pub parent_id: Option<RecordId>,
    pub parent_ids: Option<Vec<RecordId>>,
    pub queue_id: Option<RecordId>,
    pub queue_ids: Option<Vec<RecordId>>,
    pub name: Option<String>,
    pub names: Option<Vec<String>>,
    pub kind: Option<AgentKind>,
    pub server: Option<String>,
    pub task_id: Option<u64>,
    pub status: Option<AgentStatus>,
    pub statuses: Option<Vec<AgentStatus>>,
    pub command: Option<Command>,
    pub commands: Option<Vec<Command>>,
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

    /// Creates a new instance of the struct with an optional database connection.
    ///
    /// This function initializes a new instance of the struct by either using the provided database connection (`db`) 
    /// or by creating a new one if `db` is `None`. The database connection is wrapped in an `Arc` for shared ownership.
    /// It also sets a default table name (`"kafru_agents"`) for database operations.
    ///
    /// # Arguments
    /// * `db` - An optional `Arc<Db>` representing the database connection. If `None` is provided, a new database connection 
    ///   will be created.
    ///
    /// # Returns
    /// * `Self` - A new instance of the struct initialized with the provided or newly created database connection and 
    ///   the default table name.
    ///
    pub async fn new(db: Option<Arc<Db>>) -> Self {
        let db: Arc<Db> = if let Some(value) = db {
            value.clone()
        }
        else {
            Arc::new(Db::new(None).await.unwrap())
        };
        let table: String = "kafru_agents".to_string();
        Self {
            db,
            table
        }
    }

    /// Purges agent data for a specific server.
    ///
    /// This function deletes all agent records associated with a given server from the database. The operation is performed
    /// on the table specified by `self.table`, which is dynamically bound to the query. If the operation is successful, the
    /// function returns `Ok(true)`. If an error occurs during the database query, an error message is returned.
    ///
    /// # Arguments
    /// * `server` - The name of the server for which agent data will be purged. All records associated with this server will be deleted.
    ///
    /// # Returns
    /// * `Ok(true)` - If the data was successfully purged for the given server.
    /// * `Err(String)` - An error message if the purge operation fails.
    ///
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
    
    /// Creates a table for agent data if it does not already exist.
    ///
    /// This function attempts to create a table in the database using the name stored in `self.table`. If the table
    /// has already been created, it simply returns `true`. If the table creation is successful, it proceeds to purge
    /// old agent data and updates a global flag `AGENT_TABLE_CREATED` to indicate that the table is now created.
    ///
    /// # Arguments
    /// * `server` - The name of the server for which the table should be created. This is used later in the function
    ///   to trigger any additional purging of related data.
    ///
    /// # Returns
    /// * `Ok(true)` - If the table was successfully created or already exists, and the data purging operation was successful.
    /// * `Err(String)` - An error message if the table creation fails or any related data purging fails.
    ///
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

    /// Removes an agent record by its unique identifier, with an option to delete related records.
    ///
    /// This function deletes an agent record from the database using the provided `id`. If the `include_related`
    /// flag is set to `true`, it will also delete related records that reference this agent by its `parent_id`.
    /// If successful, it returns `true`, otherwise an error message is returned.
    ///
    /// # Arguments
    /// * `id` - The unique identifier (`RecordId`) of the agent to remove.
    /// * `include_related` - A boolean flag indicating whether related records should also be deleted.
    ///   - If `true`, it will delete any records with a `parent_id` matching the agent's `id`.
    ///   - If `false`, only the agent record will be deleted.
    ///
    /// # Returns
    /// * `Ok(true)` - If the agent record (and optionally related records) was successfully deleted.
    /// * `Err(String)` - An error message if the deletion fails.
    ///
    pub async fn remove(&self,id: RecordId, include_related: bool) -> Result<bool,String> {
        match self.db.client.delete::<Option<AgentData>>(id.clone()).await {
            Ok(_) => {
                if include_related {
                    if let Err(error)  = self.db.client.query("DELETE FROM type::table($table) WHERE parent_id=type::thing($parent_id)")
                    .bind(("table",self.table.clone()))
                    .bind(("parent_id",id.clone())).await {
                        error!("{}",error);
                        return Err(format!("database error when deleting related data of agent data with id: {:?}",id));    
                    }
                }
                Ok(true)
            }
            Err(error) => {
                error!("{}",error);
                Err(format!("database error when deleting agent data with id: {:?}",id))
            }
        }
    }
    
    /// Retrieves an agent record by its unique identifier.
    ///
    /// This function queries the database for an agent record using the provided `id`. If the record
    /// exists, it is returned. Otherwise, an error is returned indicating that the agent could not be found.
    ///
    /// # Arguments
    /// * `id` - The unique identifier (`RecordId`) of the agent to retrieve.
    ///
    /// # Returns
    /// * `Ok(AgentData)` - The agent record if found.
    /// * `Err(String)` - An error message if the agent record could not be found or if a database error occurs.
    ///
    pub async fn get(&self,id: RecordId) -> Result<AgentData, String> {

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

    /// Retrieves an agent record by its name and associated server.
    ///
    /// This function queries the database to fetch an agent record based on the provided `name`
    /// and `server` values. It uses the specified table in the database for the query.
    ///
    /// # Arguments
    /// * `name` - The name of the agent to search for.
    /// * `server` - The server associated with the agent.
    ///
    /// # Returns
    /// * `Ok(AgentData)` if a matching agent record is found.
    /// * `Err(String)` if no matching record is found or if a database error occurs.
    ///
    pub async fn get_by_name(&self,name: String,server: String) -> Result<AgentData, String> {
        match self.db.client.query("SELECT * FROM type::table($table) WHERE name=$name AND server=$server")
        .bind(("table",self.table.clone()))
        .bind(("name",name.clone()))
        .bind(("server",server.clone())).await {
            Ok(mut response) => {
                if let Ok(data) = response.take::<Option<AgentData>>(0) {
                    if let Some(item) = data {
                        return Ok(item);
                    }
                }
                return Err(format!("unable to find agent with name: {}, server: {}",name, server));
            }
            Err(error) => {
                error!("{}",error);
                Err(format!("database error when retrieving agent data with name:{}, server: {}",name, server))
            }
        }
    }

    /// Lists agent records based on the provided filters.
    ///
    /// This function queries the database to retrieve a list of agents that match the specified
    /// criteria. The filters are used to dynamically construct the query with optional conditions.
    ///
    /// # Arguments
    /// * `filters` - An instance of `AgentFilter`
    ///
    /// # Returns
    /// * `Ok(Vec<AgentData>)` - A vector of `AgentData` objects matching the filters.
    /// * `Err(String)` - An error message if the query fails or if data retrieval encounters an issue.
    ///
    pub async fn list(&self,filters: AgentFilter) -> Result<Vec<AgentData>,String> {
        let mut  stmt: String = "SELECT * FROM  type::table($table)".to_string();
        let mut where_stmt: Vec<String> = Vec::new();

        // WHERE placeholders
        if filters.id.is_some() {
            where_stmt.push("type::thing(id)=$id".to_string());
        }
        if filters.parent_id.is_some() {
            where_stmt.push("parent_id=type::thing($parent_id)".to_string());
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
        if filters.statuses.is_some() {
            where_stmt.push("status IN $statuses".to_string());
        }
        if filters.commands.is_some() {
            where_stmt.push("command IN $commands".to_string());
        }
        if filters.names.is_some() {
            where_stmt.push("name IN $names".to_string());
        }
        if filters.queue_ids.is_some() {
            where_stmt.push("queue_id IN $queue_ids".to_string());
        }
        if filters.ids.is_some() {
            where_stmt.push("id IN $ids".to_string());
        }
        if !where_stmt.is_empty() {
            stmt = format!("{} WHERE {}",stmt,where_stmt.join(" AND "));
        }

        // VALUE Binding
        let mut query = self.db.client.query(stmt).bind(("table",self.table.clone()));
        
        if let Some(value) = filters.id {
            query = query.bind(("id",value));
        }
        if let Some(value) = filters.parent_id {
            query = query.bind(("parent_id",value));
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
        if let Some(values) = filters.statuses {
            query = query.bind(("statuses",values));
        }
        if let Some(values) = filters.commands {
            query = query.bind(("commands",values));
        }
        if let Some(values) = filters.queue_ids {
            query = query.bind(("queue_ids",values));
        }
        if let Some(values) = filters.ids {
            query = query.bind(("ids",values));
        }
        if let Some(values) = filters.names {
            query = query.bind(("names",values));
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

    /// Updates an agent record in the database.
    ///
    /// This function retrieves the current record associated with the given `id` and updates its fields
    /// with the values provided in `data`. Any fields in `data` that are `None` will retain the values
    /// from the existing record.
    ///
    /// # Arguments
    /// * `id` - The unique identifier (`RecordId`) of the agent record to update.
    /// * `data` - An `AgentData` instance containing the updated field values. Fields with `None`
    ///   values will not overwrite the existing data.
    ///
    /// # Returns
    /// * `Ok(AgentData)` - The updated `AgentData` record after successfully applying the changes.
    /// * `Err(String)` - An error message if the update fails or if the record is not found.
    ///
    pub async fn update(&self,id: RecordId, data:AgentData) -> Result<AgentData,String> {
        match self.get(id.clone()).await  {
            Ok(record)=> {
                let data: AgentData = AgentData {                            
                    name: if data.name.is_none() { record.name } else { data.name },
                    server: if data.server.is_none() { record.server } else {data.server },
                    parent_id: if data.parent_id.is_none() { record.parent_id } else {data.parent_id },
                    kind: if data.kind.is_none() { record.kind } else {data.kind },
                    queue_id: if data.queue_id.is_none() { record.queue_id } else {data.queue_id },
                    status: if data.status.is_none() { record.status } else {data.status },
                    message: if data.message.is_none() { record.message } else {data.message },
                    author: if data.author.is_none() { record.author } else {data.author },
                    command: if data.command.is_none() { record.command } else {data.command },
                    command_is_executed: if data.command_is_executed.is_none() { record.command_is_executed } else {data.command_is_executed },
                    task_id: if data.task_id.is_none() { record.task_id } else {data.task_id },
                    date_modified: Utc::now(),
                    ..Default::default()
                };
                match self.db.client.update::<Option<AgentData>>(id).content(data.clone()).await {
                    Ok(response) => {
                        if let Some(data) = response {
                            return Ok(data);
                        }
                        Err(format!("agent {} under server {} not found",data.name.unwrap(),data.server.unwrap()))
                    }
                    Err(error) => {
                        error!("{}",error);
                        Err(format!("unable to update agent {} under server {}",data.name.unwrap(),data.server.unwrap()))
                    }
                }
            }
            Err(error) => Err(error.to_string())
        }
    }

    /// Sends a command to an agent by updating its record in the database.
    ///
    /// This function sets a command on an agent record, along with an optional message and author.
    /// The `command_is_executed` field is reset to `false` to indicate the command needs to be executed.
    ///
    /// # Arguments
    /// * `id` - The unique identifier (`RecordId`) of the agent to update.
    /// * `command` - The command to be assigned to the agent.
    /// * `message` - An optional message providing context or details about the command.
    /// * `author` - An optional author name or identifier for tracking who issued the command.
    ///
    /// # Returns
    /// * `Ok(AgentData)` - The updated agent record reflecting the assigned command.
    /// * `Err(String)` - An error message if the update fails or if the record is not found.
    ///
    pub async fn send_command(&self,id: RecordId, command:Command, message: Option<String>, author: Option<String>) -> Result<AgentData,String> {

        let data: AgentData = AgentData {                            
            command: Some(command),
            command_is_executed: Some(false),
            message,
            author,
            ..Default::default()
        };
        match self.db.client.update::<Option<AgentData>>(id).merge(data.clone()).await {
            Ok(response) => {
                if let Some(data) = response {
                    return Ok(data);
                }
                Err(format!("agent record not found"))
            }
            Err(error) => {
                error!("{}",error);
                Err(format!("unable to send command to agent"))
            }
        }
    }
    /// Registers a new agent in the database or updates an existing one if it already exists.
    ///
    /// This function performs validation on the provided `AgentData`, ensures the necessary database
    /// table exists, and creates a new agent record. If an agent with the same `name` and `server` 
    /// already exists, the existing record is removed before creating the new one.
    ///
    /// # Arguments
    /// * `data` - An `AgentData` instance containing the details of the agent to register.
    ///
    /// # Returns
    /// * `Ok(AgentData)` - The newly created or updated agent record.
    /// * `Err(String)` - An error message if registration fails or the input data is invalid.
    ///
    pub async fn register(&self, mut data: AgentData) -> Result<AgentData,String>{
        if let Some(value) = data.name.clone() {
            if value.len() < 3 {
                return Err("name must be atleast more than 3 characters".to_string());
            }
        }
        else {            
            return Err("name is required".to_string());
        }
        if data.kind.is_none() {
            return Err("kind is required".to_string())
        }
        if data.task_id == Some(0) && data.kind == Some(AgentKind::Task) {
            return Err(format!("task_id must be greater than 0 for task got {}",data.task_id.unwrap()));
        }
        if data.server.is_none() {
            return Err(format!("server is required"));
        }
        if let Err(error) = self.create_table(data.server.clone().unwrap()).await {
            return Err(error);
        }
        if data.command_is_executed.is_none() {
            data.command_is_executed = Some(false);
        }
        if let Ok(items) = self.list(AgentFilter {
            server: data.server.clone(),
            name: data.name.clone(),
            ..Default::default()
        }).await {
            if items.len() > 0 {
                if let Some(item) = items.first() {
                    self.remove(item.id.clone().unwrap(),true).await?;
                }
            }
        }
        match self.db.client.create::<Option<AgentData>>(self.table.clone()).content(data.clone()).await {
            Ok(response) => {
                if let Some(data) = response {
                    return Ok(data);
                }
                return Err(format!("no data found for agent {} at server {}",data.name.unwrap(),data.server.unwrap()))
            }
            Err(error) => {
                error!("{}",error);
                Err(format!("unable to register agent {} at server {}",data.name.unwrap(),data.server.unwrap()))
            }
        }
    }

    
    /// Converts a Tokio task ID into a `u64` value.
    ///
    /// # Arguments
    /// * `id` - The Tokio task ID to be converted.
    ///
    /// # Returns
    /// A `u64` representation of the given task ID.
    pub async fn to_id(id: tokio::task::Id) -> u64 {
        id.to_string().parse::<u64>().unwrap()
    }

    /// Generates a unique name for a task.
    ///
    /// # Arguments
    /// * `queue_name` - A reference to the name of the queue the task belongs to.
    /// * `task_id` - A unique identifier for the task.
    ///
    /// # Returns
    /// A string combining the queue name and task ID in the format `{queue_name}-{task_id}`.
    pub async fn to_name(queue_name: &String, task_id: &u64) -> String {
        format!("{}-{}", queue_name, task_id)
    }
}

