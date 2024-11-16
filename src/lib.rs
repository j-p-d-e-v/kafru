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


#[derive(Debug, Deserialize, Serialize, Clone)]
/// The available commands that can be send to scheduler and worker.
pub enum Command {
    /// Resumes the paused scheduler.
    SchedulerResume,
    /// Suspend/Pause the scheduler.
    SchedulerPause,
    /// Force shutdown the scheduler, it will not wait for the execution to complete.
    SchedulerForceShutdown,
    /// Force shutdown the scheduler, it will wait for the execution to complete.
    SchedulerGracefulShutdown,
    /// Resumes the paused queue.
    QueueResume,
    /// Suspend/Pause the queue.
    QueuePause,
    /// Force shutdown the queue, it will not wait for the execution to complete.
    QueueForceShutdown,
    /// Force shutdown the queue, it will wait for the execution to complete.
    QueueGracefulShutdown,
    /// Terminate a Task.
    TaskTerminate,
}

//todo!("Replace command of Workers,Scheduler instead from TX/RX to SurrealDB Based Add Queue command");
//todo!("We can use task id to directly identify and abort a specific thread/task.");
//todo!("Create a global variable to hold the task id (possibly v)");
//todo!("Put the command in database for scalability");
//todo!("For Scability the kafru instance should have an identifier so we can target specific workers, queues, and scheduler when running multiple instances");