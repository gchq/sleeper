Design Risks and Mitigations
============================

In this section we outline some of the risks in the current design of Sleeper, and document some of the potential
mitigations. There are 3 main risks to the current design:

- Running out of memory when loading information about all the current file references in a Sleeper table or tables
in to memory.
- Slow or out-of-date queries due to needing to periodically update the view of the file references in tables.
- Limited throughput of the state store committer.

Each of these risks is considered separately in the following subsections.

## Running out of memory when loading Sleeper table state.

See https://github.com/gchq/sleeper/issues/4214 for the issue that tracks possible work on this.

In many places in Sleeper, we need to load the current state of a table in to memory from the state store. This state
includes the details of the partitions and the file references. Examples of places where we load this state include:

- Garbage collection
- Creation of compaction jobs
- Metrics
- Queries
- The state store committer

All of these currently use AWS Lambda. Currently an invocation of a lambda can have at most 10240MB of memory. Hence
there is a risk of running out of memory if a table has lots of partitions and hence lots of file references, or if the
state for multiple tables are loaded in to memory at the same time.

Most of the places where we need to load the state only require the state of a single table to be in memory at a time.
For example, garbage collection happens independently for each Sleeper table and so only the state of one table needs
be in memory at a time. However, there are two places where the state of multiple tables needs to be held in memory at
once: the query executor lambda and the state store committer lambda; these are discussed separately below. Tests have
shown it is possible to hold the information about the file references and the partitions in a table with 132,000 leaf
partitions and tens of files per leaf partition in memory. Thus the memory limit may not be a problem in practice.
Moreover, the use of AWS lambda is a convenience rather than a fundamental requirement. Lambdas can be easily replaced by
ECS services or tasks which have a memory limit of 120GB. In the cases where we have large tables, the potential
additional cost savings due to use of on-demand lambda rather than a long-running ECS task are likely to be negligible.
Hence the current approach appears to be viable and in future it would be relatively easy to replace the use of lambda
with long-running services which could have more memory.

The property `sleeper.statestore.provider.cache.size` controls the number of tables for which the state store
information is held in memory at once. For the lambdas that do not need to hold the state of multiple tables in memory
at once, setting this to 1 will mean that only one state store is in memory even if a lambda instance is reused across
different tables. However, if we set this property to 1 for the query executor lambda then it means that if a lambda
instance receives queries for multiple tables then it will have to reload the statestore information each time a query
for a different table is received. This would result in slow queries. Again, this can be mitigated by replacing the
 query executor lambda with a long running ECS task that would enable it to hold the state of mulitple tables in memory
 at once. See the next subsection for other advantages that might bring.

The state store committer lambda will need to have the information about multiple tables in memory at once. If the
state store provider cache size is set to 1 then any time the committer lambda has to deal with a transaction for a
different table it will have to create its state from scratch from the snapshots and the transaction log which will be
slow. Hence in practice we want the state store committer lambda to have multiple tables in memory at once. This can
again be achieved by replacing the state store commit lambda by one or more long running ECS tasks.

## Slow or out-of-date queries due to needing to periodically update view of file references in tables

See https://github.com/gchq/sleeper/issues/4215 for the issue that tracks work on this.

Queries are executed using the `QueryExecutor` class. This is the basis of all approaches to queries, including the SQS
triggering lambda approach. When it first receives a query for a Sleeper table, the QueryExecutor loads the
file reference and partition information for the table from the state store. The query can then be executed.

When the next query for that table is received we check whether a certain amount of time has passed since the file
reference and partition information was updated. If it has then we update that information by going to the state store.
This adds a delay before running the query. If the next query is soon enough then the file reference and partition
information will not be updated. This means that there will not be a delay, but the queries will not have the absolutely
latest view of the state store, potentially resulting in them missing data because the latest files to have been
ingested will not be included, or in opening more files than is necessary because they are not aware that a compaction
has finished.

To mitigate these issues we propose to build a long-running query planner that is responsible for keeping an
up-to-date view of the file reference and partition information about tables. This can then be used to identify the
subqueries that need to be executed in order to run a query. Keeping the state store information up-to-date could be
done either by using a DynamoDB streams of the transaction log updates, or by querying for the latest DynamoDB updates
periodically, e.g. twice a second.

## Limited throughput of state store committer

NB Current testing shows that the throughput is acceptable for the expected parameters in which Sleeper should operate.

See https://github.com/gchq/sleeper/issues/4525 for the issue that tracks possible work on this.

The current state store is based on a transaction log stored in DynamoDB. To avoid contention when inserting the next
item in the log we apply inserts via a lambda that is triggered by a SQS FIFO queue. Updates to the state store are made
asynchronously by sending transactions to this FIFO queue.

The table id is used as the message group id when transactions are put on the queue. This means that only one
transaction for a single Sleeper table can be processed at a time (transactions for different tables can be processed
simulataneously). This avoids contention. In order to reduce the number of transactions submitted to the log, some
parts of the system create large batches of updates, e.g. commits of compaction jobs are sent to a queue and then
batched up before being processed by a lambda, this lambda splits the messages by table and sends separate messages
for each table. This has the effect of reducing the usefulness of the batching in situations where a lot of tables
are active.

When an SQS FIFO queue is triggering multiple lambda instances, with different message group ids being processed
simultaneously, there is no stickiness in terms of which lambda instance processes a message for a table, e.g. lambda
1 may process a message for table 1 whilst lambda 2 is processing a message for table 2, when these have finished
lambda 1 may receive a message for table 2, and lambda 2 may receive a message for table 1. This means that lambda 1
has to go to the DynamoDB transaction log to update its view of the state for table 2, and lambda 2 has to update its
view of the state for table 1. This reduces the overall throughput.

If we were prepared to run cdk when adding a new table, then each table could have its own state store, commit queue
and lambda. However, there may be limits on the number of tables that could be supported that way and it would mean
that adding a table would take several minutes.

Overall, there are several fundamental limits to the throughput of the state store updates. Batching helps considerably
but eventually if the batches get too large then they will take longer to process so there is still a limit.

There are several possible mitigations for the limitation to the throughput:

- Instead of having 1 FIFO queue of state store transactions for all Sleeper tables, we could have N FIFO queues.
Updates for a particular Sleeper table would always be sent to the same queue, e.g. by hashing the table id modulo N.
Each queue would trigger its own lambda. All lambdas would write to the same DynamoDB table (which allows varying N
over time). This would cause an increase in throughput for updates across multiple tables (at least where those tables
do not hash to the same value modulo N). A similar idea would be to have multiple compaction commit queues, so that
the batching of commits from them would be more likely to have messages for just one table. This would increase the
size of the batches applied and therefore reduce the overall number of transactions that need to be applied.
- Instead of processing the updates in lambdas, we could have a single long-running process that is responsible for
updating the transaction log for all Sleeper tables. Internally this could pull a message off the FIFO queue and pass
that to a thread that is responsible for a particular Sleeper table. This would help increase the throughput as there
would no longer be the switching problem where one lambda instance receives an update for one table, and the next
update for that table goes to a different lambda instance meaning that it needs to update its state from the
transaction log.
- We already perform significant batching of updates to the transaction log, e.g. the commit requests when compaction
jobs finish. There may be possibilities of increasing the batching, although bigger batches take longer to process
so this may only have limited impact.
- Backpressure - if the commit queue has unprocessed messages for a sustained period then processes such as the
creation of compaction jobs should probably wait before running. This may not be easy to achieve as SQS does not give
very reliable estimates of the backlog on the queue.
- Instead of having one transaction log per Sleeper table, we could split the partitions in the table up at a high level,
e.g. split the partition tree at the level where there are 16 subtrees. Each of those would have its own transaction
log. This poses its own challenges though, e.g. it becomes more expensive to retrieve the entire state of the table.
- To avoid needing to put the updates through a FIFO queue we could insert the transactions directly into DynamoDB
with a sort key that consists of the timestamp followed by a short unique id. If the timestamp was accurate then this
would result in transactions in the correct order. Sleeper does not need two transactions that are made at almost the
same time to be correctly ordered as some databases do to resolve conflicts; we just need the ordering to be roughly
correct (e.g. we want a file to be added before a compaction job is assigned). In practice clocks should be accurate
to roughly a millisecond, and AWS offers clocks which are accurate to a small number of microseconds (see
https://aws.amazon.com/blogs/compute/its-about-time-microsecond-accurate-clocks-on-amazon-ec2-instances/). It is
unclear how we would know when all transactions within a time window had been written.
- Replace SQS FIFO with a queue that allows us to directly pull all the information from a single table. This would
allow us to have one long running process per Sleeper table which could constantly pull the information from a single
table.

There are approaches that involve replacing the current state store with something that might naturally allow greater
throughput:

- The current approach of using a FIFO SQS queue and a Dynamo table in which entries for a Sleeper table are written
with a sort key which is a counter effectively has two ordered list of updates: the FIFO queue has an ordered list of
transactions, and so does the DynamoDB table. The difference is that the FIFO queue only supports retrieving the
next unprocessed message and cannot be used to retrieve updates from a certain point onwards. The DynamoDB table does
support retrieving messages from a particular point onwards, but does not natively support insertion in order - this
has to be performed by Sleeper with a conditional put. If we had a storage technology that supported storing data in
order and reading data from given offsets then that would allow us to simply send transactions to the storage, without
the need to go through a single point. We could still perform snapshotting and updates from the log as we do now.
Kafka may meet these needs.
- Instead of using a transaction log approach, we could trial using a store that supports transactions, snapshot
isolation and a high update rate, e.g. PostgreSQL or FoundationDB.
