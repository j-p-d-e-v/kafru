use surrealdb::Surreal;
use surrealdb::opt::auth::Root;
use surrealdb::engine::remote::ws::{Ws, Client};
use std::env;
use std::sync::Arc;

#[derive(Debug)]
pub struct DbConnection {
    username: String,
    password: String,
    host: String,
    port: u16,
    namespace: String,
    database: String
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


#[cfg(test)]
mod test_database {
    use super::*;

    #[tokio::test]
    async fn test_connection_using_env(){
        env::set_var("KAFRU_USERNAME","test".to_string());
        env::set_var("KAFRU_PASSWORD","test".to_string());
        env::set_var("KAFRU_PORT","4030".to_string());
        env::set_var("KAFRU_HOST","127.0.0.1".to_string());
        env::set_var("KAFRU_NAMESPACE","kafru_dev".to_string());
        env::set_var("KAFRU_DB","kafru".to_string());
        let result: Result<Db, String> = Db::new(None).await;
        assert!(result.is_ok(),"{}",result.unwrap_err());
    }
    
    #[tokio::test]
    async fn test_connection(){
        let result: Result<Db, String> = Db::new(
            Some(
                DbConnection {
                    host: "127.0.0.1".to_string(),
                    username: "test".to_string(),
                    password: "test".to_string(),
                    port: 4030,
                    namespace: "kafru_dev".to_string(),
                    database: "kafru".to_string(),
                }
            )
        ).await;
        assert!(result.is_ok(),"{}",result.unwrap_err());
    }
}