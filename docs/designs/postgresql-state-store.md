# Use PostgreSQL for the state store

## Status

Proposed

## Context

There are currently three state stores in Sleeper. The DynamoDBStateStore will be deleted soon as DynamoDB does not
support snapshot isolation and therefore we cannot obtain a consistent view of the file references in a
table. The S3StateStore does not have this problem but as it requires the entire state to be written to S3 for each
update, it can take several seconds to apply an update. This is too slow for some use cases which may require
a million or more updates per day.

The transaction log state store stores each change to the state store as a new
item in DynamoDB, with optimistic concurrency control used to add new transactions. To avoid reading the entire
history of transactions when querying or updating the state store, we periodically create snapshots of the state.
To get an up-to-date view of the state store, the latest snapshot is queried and then updated by reading all subsequent
transactions from the DynamoDB transaction log. This state store enforces a sequential ordering to all the updates to
the state store. Due to challenges performing lots of concurrent updates, there is an option to apply updates to the
state store via an SQS FIFO queue which triggers a committer lambda. The table id is used to ensure that only one
lambda can be processing an update for a table at a time. This works well for a single table, but the update rate
drops when there are multiple tables as different lambda instances will process updates for the same table which
means that there is a need to refresh the state from the transaction log. Further testing is needed to determine
if the transaction log state store performs sufficiently well to enable our test scenarios to run successfully.

The transaction log state store also requires significant amount of clean up operations, e.g. of old transactions
and old snapshots. These will have some impact on the underlying DynamoDB table and this needs to be tested further.

We want to experiment with using a relational database as the state store. This should be simpler than the transaction
log state store and offer us the consistency guarantees we need. The trade-off is that it will not be serverless,
specifically it will not scale down to consume zero computational resources when there is no work to do.

The comments in the rest of this page are based on experiments with a prototype state store that uses the PostgreSQL
compatible variant of Amazon Aurora. That prototype implemented both the FileReferenceStore and the PartitionStore
using PostgreSQL, however as the load on the partition store is not significant, it will be easier to continue to
use the transaction log store as the partition store.

## Design Summary

We use the PostgreSQL compatible version of Amazon Aurora as the database service.

There are two main options for how to store the file reference data in PostgreSQL.

- Option 1: For each Sleeper table, there are two PostgreSQL tables. One stores the file references and one stores the
file reference counts. The file reference store has a primary key of (filename, partition id). When compacting files,
we remove the relevant file reference entries, add a new one for the output file, and decrement the relevant entries in
the file reference counts table. This option has the potential for contention. Suppose we have a table with 1024
leaf partitions, and we perform 11 ingests, each of which writes only 1 file but creates 1024 file references. A
compaction job is created for each partition. If there are a few hundred compaction tasks running simultaneously, then
many will finish at approximately the same time and they will all decrement the same counts. Experiments showed that
this can lead to failures of compaction jobs. Each state store update can be retried, and will eventually succeed but
there may be a delay. To partially mitigate this issue, a salt field can be added to the file reference count table.
The salt is the hash of the partition id modulo some number, e.g. 128. This means that the counts are split across
multiple rows, which has the advantage of decreasing the chance that multiple compaction jobs that finish at the same
time will attempt to update the same records.

- Option 2: For each Sleeper table, there is one PostgreSQL table containing the file references. We do not explicitly
store the file reference counts. If we just add the
file references and delete them when a compaction job finishes, we will lose track of what files are in the system
and not be able to garbage collect them. We can avoid this problem as follows. When we add a file and some references,
we also add a dummy file reference, i.e. one where the partiton id is "DUMMY". Normal operations on the state store
ignore these entries. When all non-dummy references to a file have been removed, only the dummy reference will remain.
To garbage collect files we can look for files for which there is only one reference (which must be the dummy
reference). We cannot delete the file then as we only delete files once the last update to them is more than a certain
amount of time ago. The garbage collection process now happens in two stages: the first stage looks for files with
only the dummy reference. When it finds one it sets the update time to the current time (when the dummy reference is
first created, its update time is set to Long.MAX_VALUE). The second phase of the garbage collection looks for files
with only the dummy reference and for which the update time is more than N seconds ago. Those can be deleted. This means
that the deletion of files will take at least two calls to the garbage collector, but we do not need to prioritise
deleting files as quickly as possible.

- Option 3: This is similar to option 2 - for each Sleeper table there is a table of file references but instead of
adding a dummy reference we add an entry for the file to a separate table. This entry simply records the filename
along with an update time which is initially Long.MAX_VALUE. Normal updates to the file references happen on the
main table. As in option 2, garbage collection needs to be a two state process. The first stage will have to find
all files in the second table for which there are zero corresponding entries in the file references table. The rest
of the garbage collection process will behave as in option 2.

Experiments showed that these options may be viable, but it is recommended that option 2 or 3 is pursued as
they reduce the contention the most.

## Implementation

### Stack

We will need an optional stack to deploy an Aurora PostgreSQL compatible instance. We can offer the option of a fixed
size instance as well as the serverless option. The Aurora instance can be deployed in the VPC used in other places in
Sleeper. We can use AWS Secrets Manager to control access to the database. All lambdas that need access to the state
store will need to run in the VPC. The Aurora instance will run until paused. We can update the pause system
functionality so that it pauses the Aurora instance. We should be aware that paused instances will automatically be
turned on every 7 days for security updates and then they are not turned off.

We will need a PostgreSQLStateStore implementation of FileReferenceStore. This will need a connection to the database
instance.

### Managing the number of concurrent connections to the Aurora instance

Each connection to the Aurora instance consumes a small amount of resource on the instance. If we have 500 compaction
tasks running at the same time and they all have a connection to the instance, that will put the instance under load
even if those connections are idle. Each execution of a compaction job should create a PostgreSQLStateStore, do the
checks it needs and then close the connection. We could add a method to the state store implementation that closes
any internal connections or we could create the state store with a connection supplier that provides a connection
when needed but then shuts that connection down when it has not been used for a while.

When we need to make updates to the state store, we can avoid needing to create a connection every time by reusing
the idea of asynchronous commits from the transaction log state store, i.e. an SQS queue that triggers a lambda to
perform the updates. However, in this case we do not want it to be a FIFO queue as we want to be able to make
concurrent updates. We can set the maximum concurrency of the lambda to control the number of simultaneous updates to
the state store. If multiple Sleeper tables are being updated then the maximum concurrency would need to be set high
enough to support updates to all of the tables, but when only one table is active this would mean that there could be
a large number of updates to one table. This may not be a problem in practice.

### Transactions

See the PostgreSQL manual on transaction isolation levels: https://www.postgresql.org/docs/current/transaction-iso.html.
We can use the serializable transaction level. Even if the design means there should be no conflicts at the row level,
it is possible that multiple simultaneous updates may lead to SQLExceptions due to it being unable to serialise the
transactions. We will need to retry the update in these scenarios.

Different operations on the state store will require different types of implementation - some can use prepared
statements, some will use PLPGSQL.
