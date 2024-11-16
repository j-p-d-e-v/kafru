use surrealdb::Surreal;
use surrealdb::opt::auth::Root;
use surrealdb::engine::remote::ws::{Ws, Client};
use std::env;
use tracing::{error, instrument};

#[derive(Debug,Clone)]
pub struct DbConnection {
    pub username: String,
    pub password: String,
    pub host: String,
    pub port: u16,
    pub namespace: String,
    pub database: String
}

impl Default for DbConnection {

    /// ## Database Default Values
    ///
    /// The following environment variables are used for configuring the database:
    ///
    /// | Key                  | Description                                                                              | Default Value      |
    /// |----------------------|------------------------------------------------------------------------------------------|--------------------|
    /// | `KAFRU_DB_USERNAME`  | The database username.                                                                    | `kafru_admin`      |
    /// | `KAFRU_DB_PASSWORD`  | The database password.                                                                    | `kafru_password`   |
    /// | `KAFRU_DB_PORT`      | The port number of the database.                                                          | `4030`             |
    /// | `KAFRU_DB_HOST`      | The database host or IP address.                                                          | `127.0.0.1`        |
    /// | `KAFRU_DB_NAMESPACE` | The database namespace, useful for separating production and testing databases.            | `kafru`            |
    /// | `KAFRU_DB_NAME`      | The name of the database.                                                                 | `kafru_db`         |
    fn default() -> Self {
        Self {
            username: env::var("KAFRU_DB_USERNAME").unwrap_or(String::from("kafry_admin")),
            password: env::var("KAFRU_DB_PASSWORD").unwrap_or(String::from("kafru_password")),
            port: env::var("KAFRU_DB_PORT").unwrap_or("4030".to_string()).parse::<u16>().unwrap(),
            host: env::var("KAFRU_DB_HOST").unwrap_or(String::from("127.0.0.1")),
            namespace: env::var("KAFRU_DB_NAMESPACE").unwrap_or(String::from("karfru")),
            database: env::var("KAFRU_DB_NAME").unwrap_or(String::from("kafru_db")),
        }
    }
}

#[derive(Debug,Clone)]
pub struct Db {
    pub client: Surreal<Client>
}

impl Db {
    
    /// Initializes and creates a new database client instance.
    /// 
    /// # Parameters
    /// - `config`: *(Optional)* Database connection configuration. If not provided, the configuration can be automatically loaded from environment variables.
    #[instrument(skip_all)]
    pub async fn new(config: Option<DbConnection>) -> Result<Self,String> {
        let config: DbConnection = config.unwrap_or_default();
        let address: String = format!("{}:{}", config.host,config.port);
        match Surreal::new::<Ws>(address).await {
            Ok(client) => {
                if let Err(error) = client.signin(Root {
                    username: config.username.as_str(),
                    password: config.password.as_str(),
                }).await {
                    error!("{}",error);
                    return Err(error.to_string());
                }
                if let Err(error) = client.use_ns(config.namespace).use_db(config.database).await {
                    error!("{}",error);
                    return Err(error.to_string());
                }
                Ok(Self {
                    client
                })
            }
            Err(error) => {
                error!("{}",error);
                Err(error.to_string())
            }
        }   
    }
}
