
Blocking use case: the developer wrote a worker that will create new tasks to the DP, that worker will wait for the task to complete from another worker: submit 1 -> submit 2 -> done 2 -> done 1

Non-blocking use case: the developer is simply sending tasks to the DP: submit 1 -> done 1 -> submit 2 -> done 2.

When dealing with blocking operations:

Execution Order (forward):
1. groupA-1 (start)
2. groupA-2 (start)
3. groupA-3 (start)
4. groupA-3 (complete)
5. groupA-2 (complete)
6. groupA-1 (complete)

Execution Order (reversed):
1. groupB-3 (start)
2. groupB-2 (start)
3. groupB-1 (start)
4. groupB-1 (complete)
5. groupB-2 (complete)
6. groupB-3 (complete)

When dealing with non-blocking operations:

Execution Order (forward):
1. groupA-1 (start)
2. groupA-1 (complete)
3. groupA-2 (start)
4. groupA-2 (complete)
5. groupA-3 (start)
6. groupA-3 (complete)

Execution Order (reversed):
1. groupB-3 (start)
2. groupB-3 (complete)
3. groupB-2 (start)
4. groupB-2 (complete)
5. groupB-1 (start)
6. groupB-1 (complete)


The DependencyPool shouldn't care WHERE tasks come from (worker or external) - it just needs to:

1. On Submit event:
   - Store task state
   - Check if task can run (based on mode and dependencies)
   - If yes -> submit to pool
   - If no -> keep in pending state

2. On Task Completion event: 
   - Update task state as completed
   - Check all pending tasks that depend on completed one
   - Submit any that can now run

----


We can't make one DP all at once

- IndependentDependencyPool: ideal for short tasks that doesn't block a worker to wait for another task, ideal for execution order
- BlockingDependencyPool: ideal for tasks that wait for other tasks, generally created from within a worker blocking it's execution


---

1. `IndependentPool` Example:
- Used for data processing pipeline where tasks have clear dependencies
- Tasks are all submitted upfront
- Each task runs independently after its dependencies complete
- Execution order: Load Data -> Clean Data -> Transform Data -> Save Results
- Perfect for ETL workflows, build systems, or any sequential processing

2. `BlockingPool` Example:
- Used for dynamic workflow where tasks create subtasks
- Main task creates and waits for subtasks to complete
- Subtasks can run in parallel if no dependencies between them
- Shows document processing workflow:
  * Main task creates subtasks
  * Text extraction must complete before analysis
  * Sentiment analysis and keyword extraction can run in parallel
- Great for recursive tasks, document processing, or any workflow where tasks need to spawn and wait for subtasks

Key differences:
- Independent pool has all tasks known upfront
- Blocking pool allows dynamic task creation inside workers
- Independent tasks complete and move on
- Blocking tasks can wait for their children
