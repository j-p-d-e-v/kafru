use serde::{Deserialize, Serialize};

/// Manages the database connection and client interactions.
pub mod database;

/// Manages the task queue, storing tasks awaiting execution.
pub mod queue;

/// Manages task definitions and execution details.
pub mod task;

/// Manages the execution and polling of tasks from the queue.
pub mod worker;

/// Manages schedule definitions for task execution.
pub mod schedule;

/// Parses CRON expressions and generates upcoming scheduled times.
pub mod cron_schedule;

/// Polls schedules and assigns tasks to the appropriate queue.
pub mod scheduler;

/// Defines metrics used by the Worker and Scheduler for tracking performance.
pub mod metric;

/// Manages the initialization and orchestration of the Worker and Scheduler.
pub mod manager;

// Unit and integration test cases for the modules.
pub mod tests;

// Agent for sending commands/signals to worker agent, scheduler agent
pub mod agent;

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
/// The available commands that can be send to scheduler and worker.
pub enum Command {
    /// Force shutdown the scheduler, it will not wait for the execution to complete.
    SchedulerForceShutdown,
    /// Force shutdown the scheduler, it will wait for the execution to complete.
    SchedulerGracefulShutdown,
    /// Resumes the paused queue.
    QueueForceShutdown,
    /// Force shutdown the queue, it will wait for the execution to complete.
    QueueGracefulShutdown,
    /// Terminate a Task.
    TaskTerminate,
    /// Remove a Task.
    TaskRemove,
}