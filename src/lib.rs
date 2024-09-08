pub mod database;
pub mod queue;
pub mod task;
pub mod worker;
pub mod tests;
pub mod schedule;
pub mod cron_schedule;
pub mod scheduler;
pub mod metric;
pub mod manager;
use serde::{Serialize, Deserialize};

#[derive(Debug,Serialize,Deserialize)]
pub enum Command {
    ShutdownWorker,
    ShutdownScheduler
}