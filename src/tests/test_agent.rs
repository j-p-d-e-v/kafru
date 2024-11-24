
#[cfg(test)]
pub mod test_agent {
    use crate::agent::{Agent,AgentData,AgentFilter,AgentKind,AgentStatus};
    use crate::tests::test_helper::configure_database_env;
    use std::sync::Arc;
    use crate::database::Db;

    #[tokio::test]
    #[serial_test::serial]
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
                    name: Some(format!("queue-{}-server-{}",i,s)),
                    kind: Some(AgentKind::Queue),
                    server: Some(server.clone()),
                    task_id: Some(i),
                    ..Default::default()
                }).await;
                assert!(result.is_ok(),"{:?}",result.err());
                let data = result.unwrap();
                let updated_result = agent.update(data.id.clone().unwrap(), AgentData {
                    status: Some(AgentStatus::Completed),
                    ..data
                } ).await;
                assert!(updated_result.is_ok(),"{:?}",updated_result.err());
                let updated_data = updated_result.unwrap();
                assert!(data.status != updated_data.status);
            }
        }
        let agents_data = agent.list(AgentFilter {
            server: Some("server1".to_string()),
            statuses: Some(Vec::from([AgentStatus::Initialized,AgentStatus::Completed])),
            ..Default::default()
        }).await;
        assert!(agents_data.is_ok(),"{:?}",agents_data.err());
        assert!(agents_data.unwrap().len() == 10);
        let agents_data = agent.list(AgentFilter {
            server: Some("server1".to_string()),
            statuses: Some(Vec::from([AgentStatus::Running,AgentStatus::Error])),
            ..Default::default()
        }).await;
        assert!(agents_data.is_ok(),"{:?}",agents_data.err());
        assert!(agents_data.unwrap().len() == 0);
    }
}