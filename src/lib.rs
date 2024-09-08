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
