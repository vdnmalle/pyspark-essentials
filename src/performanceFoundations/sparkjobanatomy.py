'''

Stages & Tasks
Task
• the smallest unit of computation
• executed once, for one partition, by one executor
Stage
• contains tasks
• enforces no exchange of data = no partitions need data from other partitions
• depends on the previous stage = previous stage must complete before this one starts
A job contains stages
A stage contains tasks
An application contains jobs
Shuffle
• exchange of data between executors
• happens in between stages
• must complete before next stage starts

Dependencies
Narrow dependencies
• one input (parent) partition influences a single output (child) partition
• fast to compute
• examples: map, flatMap, filter, projections
Wide dependencies
• one input partition influences more than one output partitions
• involve a shuffle = data transfer between Spark executors
• are costly to compute
• examples: grouping, joining, sorting



Shuffles
Data exchanges between executors in the cluster
Expensive because of
• transferring data
• serialization/deserialization
• loading new data from shuffle files
Shuffles are performance bottlenecks because
• exchanging data takes time
• they need to be fully completed before next computations start
Shuffles limit parallelization





'''