use surrealdb::Surreal;
use surrealdb::opt::auth::Root;
use surrealdb::engine::remote::ws::{Ws, Client};
use std::env;
use std::sync::Arc;

#[derive(Debug)]
pub struct DbConnection {
    pub username: String,
    pub password: String,
    pub host: String,
    pub port: u16,
    pub namespace: String,
    pub database: String
}

impl Default for DbConnection {
    fn default() -> Self {
        Self {
            username: env::var("KAFRU_USERNAME").unwrap_or(String::new()),
            password: env::var("KAFRU_PASSWORD").unwrap_or(String::new()),
            port: env::var("KAFRU_PORT").unwrap_or("4030".to_string()).parse::<u16>().unwrap(),
            host: env::var("KAFRU_HOST").unwrap_or(String::new()),
            namespace: env::var("KAFRU_NAMESPACE").unwrap_or(String::new()),
            database: env::var("KAFRU_DB").unwrap_or(String::new()),
        }
    }
}

#[derive(Debug)]
pub struct Db {
    pub client: Arc<Surreal<Client>>
}

impl Db {
    pub async fn new(config: Option<DbConnection>) -> Result<Self,String> {
        let config: DbConnection = config.unwrap_or_default();
        let address: String = format!("{}:{}", config.host,config.port);
        match Surreal::new::<Ws>(address).await {
            Ok(client) => {
                if let Err(error) = client.signin(Root {
                    username: config.username.as_str(),
                    password: config.password.as_str(),
                }).await {
                    return Err(error.to_string());
                }
                if let Err(error) = client.use_ns(config.namespace).use_db(config.database).await {
                    return Err(error.to_string());
                }
                Ok(Self {
                    client: Arc::new(client),
                })
            }
            Err(error) => {
                Err(error.to_string())
            }
        }   
    }
}
