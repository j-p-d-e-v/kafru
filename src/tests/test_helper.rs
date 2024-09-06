
pub fn configure_database_env(){
    use std::env;
    
    env::set_var("KAFRU_USERNAME","test".to_string());
    env::set_var("KAFRU_PASSWORD","test".to_string());
    env::set_var("KAFRU_PORT","4030".to_string());
    env::set_var("KAFRU_HOST","127.0.0.1".to_string());
    env::set_var("KAFRU_NAMESPACE","kafru_dev".to_string());
    env::set_var("KAFRU_DB","kafru".to_string());
}