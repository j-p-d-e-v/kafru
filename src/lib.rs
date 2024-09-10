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
    /// Resumes the paused worker/queue.
    WorkerResume,
    /// Suspend/Pause the worker/queue.
    WorkerPause,
    /// Force shutdown the worker/queue, it will not wait for the execution to complete.
    WorkerForceShutdown,
    /// Force shutdown the worker/queue, it will wait for the execution to complete.
    WorkerGracefulShutdown
}