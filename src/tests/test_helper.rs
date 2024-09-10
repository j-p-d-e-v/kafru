
pub fn configure_database_env(){
    use std::env;
    
    env::set_var("KAFRU_DB_USERNAME","kafru_admin".to_string());
    env::set_var("KAFRU_DB_PASSWORD","kafru_password".to_string());
    env::set_var("KAFRU_DB_PORT","4030".to_string());
    env::set_var("KAFRU_DB_HOST","127.0.0.1".to_string());
    env::set_var("KAFRU_DB_NAMESPACE","kafru_dev".to_string());
    env::set_var("KAFRU_DB_NAME","kafru".to_string());
}