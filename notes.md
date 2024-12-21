
- Round-robin
    - Root workers will move and will be dynamically defined 
    - We will keep a map of `map[groupID]map[taskID]Task` and `map[groupID][]taskID` and else
    - We will have X amount of root workers 
    - We will have a map of ownership `map[workerID]groupID` 
        - callback of queued/processed will confirm the ownership


