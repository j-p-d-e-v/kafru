# kafru

**kafru** is a Rust crate inspired by Python's Celery, designed for efficient task queue management. The name "kafru" is inspired by the "Kafra" system from the popular game Ragnarok Online. It utilizes cron for scheduling tasks and integrates with SurrealDB to manage queues, metrics, and schedules.

## Features

- **Task Queueing**: Manage and execute tasks asynchronously with support for complex workflows.
- **Cron Scheduling**: Schedule tasks using cron expressions for recurring or one-time jobs.
- **SurrealDB Integration**: Store and manage task queues, schedules, and performance metrics using SurrealDB.
- **Metrics Tracking**: Track the number of tasks in the queue and the number of threads.

## Installation

```
cargo add karfu
```

## Usage


## Metrics

**kafru** provides the following metrics:
- **Number of Tasks in the Queue**: Current count of tasks waiting to be executed.
- **Number of Threads**: Number of active threads handling tasks.

## Documentation

For detailed documentation, visit [docs.rs/kafru](https://docs.rs/kafru).

## License

This project is licensed under the Custom License Agreement. See the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please refer to the [CONTRIBUTING.md](CONTRIBUTING.md) file for guidelines.

## Contact

For questions or feedback, you can reach me at [jpmateo022@gmail.com](mailto:jpmateo022@gmail.com).