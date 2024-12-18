
Blocking use case: the developer wrote a worker that will create new tasks to the DP, that worker will wait for the task to complete from another worker: submit 1 -> submit 2 -> done 2 -> done 1

Non-blocking use case: the developer is simply sending tasks to the DP: submit 1 -> done 1 -> submit 2 -> done 2.

When dealing with blocking operations:

Execution Order (ordered):
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
4. groupB-3 (complete)
5. groupB-2 (complete)
6. groupB-1 (complete)

When dealing with non-blocking operations:

Execution Order (ordered):
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

