# Examples

1. [Basic task submission and processing](./basic/): Demonstrate how to create a pool, add workers, and submit tasks for processing.
2. [Custom worker implementation](./custom-worker/): Show how to implement a custom worker that processes specific types of tasks.
3. [Retry mechanism with fixed delay](./retry-fixed-delay/): Illustrate how the pool retries tasks that fail, using a fixed delay policy.
4. [Exponential backoff retry policy](./backoff-retrypolicy): Showcase how to use an exponential backoff retry policy with jitter.
5. [Custom Retry Policy Function](./custom-retrypolicy-func/): Implement and use a custom retry policy function to determine retry delays.
6. [Rate limiting task submission](./ratelimit-tasks/): Demonstrate how to rate-limit task submissions to the pool.
7. [Handling Dead Tasks with Callback](./handling-deadtasks/): Show how tasks that exceed retry attempts are moved to dead tasks and handled via a callback.
8. [Worker Lifecycle Management](./worker-lifecycle/): Illustrate adding, pausing, resuming, and removing workers from the pool.
9. [Task Options: Immediate Retry and Bounce Retry](./immediate-bounce/): Demonstrate submitting tasks with immediate retry and bounce retry options.
10. [Task Timeouts and deadlines](./timeout-deadlines/): Show how to set total and per-attempt timeouts for tasks.
11. [Custom Error Handling with retryIf Function](./custom-retryif/): Implement a custom function to determine if a task error is retryable.
12. [Task Notifications](./task-notifications/): Utilize notifications to signal when tasks are queued and processed.
13. [Panic Handling](./panic-handling/): Demonstrate how the pool handles panics within worker tasks.
14. [Request Response](./request-response/): Use the `RequestResponse` type to manage tasks that require a response.
15. [Dynamic Adjustment of Retry Policies](./retry-policies/): Change retry policies at runtime based on metrics or external signals.
16. [Testing Concurrency and Data Race Prevention](./concurrency/): Create a high-concurrency scenario to test for data races and ensure thread safety.
17. [Graceful Shutdown and Cleanup](./shutdown/): Demonstrate how to gracefully shut down the pool and ensure all tasks are completed or properly terminated.
18. [Error Handling and Specific Error Cases](./error-handling/): Explore how the pool handles specific errors like `ErrPoolClosed` or `ErrMaxQueueSizeExceeded`.
