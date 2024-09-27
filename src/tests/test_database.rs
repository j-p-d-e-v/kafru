#[cfg(test)]
mod test_database {
    use std::env;
    use crate::database::{Db,DbConnection};

    #[tokio::test]
    async fn test_connection_using_env(){
        env::set_var("KAFRU_DB_USERNAME","kafru_admin".to_string());
        env::set_var("KAFRU_DB_PASSWORD","kafru_password".to_string());
        env::set_var("KAFRU_DB_PORT","4030".to_string());
        env::set_var("KAFRU_DB_HOST","127.0.0.1".to_string());
        env::set_var("KAFRU_DB_NAMESPACE","kafru_dev".to_string());
        env::set_var("KAFRU_DB_NAME","kafru".to_string());
        let result: Result<Db, String> = Db::new(None).await;
        assert!(result.is_ok(),"{}",result.unwrap_err());
    }
    
    #[tokio::test]
    async fn test_connection(){
        let result: Result<Db, String> = Db::new(
            Some(
                DbConnection {
                    host: "127.0.0.1".to_string(),
                    username: "kafru_admin".to_string(),
                    password: "kafru_password".to_string(),
                    port: 4030,
                    namespace: "kafru_dev".to_string(),
                    database: "kafru".to_string(),
                }
            )
        ).await;
        assert!(result.is_ok(),"{}",result.unwrap_err());
    }
}