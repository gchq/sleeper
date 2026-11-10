# Transaction log state store asynchronous commit retries

At time of writing the state store is implemented as state derived from a transaction log. There's an SQS FIFO queue of
commits, which is used to separate the commits per Sleeper table, so that they can be processed without contention. Each
commit adds a single transaction to the log. Not all commits go through this queue.

SQS FIFO is designed to provide guarantees on the delivery order of the messages, but we don't think we rely on this in
Sleeper. We think that if a commit fails, we can safely send it to the back of the queue to be retried later, and
continue processing commits in order of the queue. We'll look at potential impacts if we enable this.

## Commit types

Five types of state store commit are sent as asynchronous commit requests via the queue:

| Transaction                      | Producer                  |
|----------------------------------|---------------------------|
| AddFilesTransaction              | Ingest, bulk import       |
| AssignJobIdsTransaction          | Compaction job creation   |
| ReplaceFileReferencesTransaction | Compaction commit batcher |
| SplitPartitionTransaction        | Partition splitter        |
| DeleteFilesTransaction           | Garbage collector         |

The rest are always committed synchronously:

| Transaction                      | Producer                  |
|----------------------------------|---------------------------|
| SplitFileReferencesTransaction   | Compaction job creation   |
| ExtendPartitionTreeTransaction   | Bulk import pre-splitting |
| InitialisePartitions             | Client                    |
| ClearFiles                       | Client                    |
| ClearPartitions                  | Client                    |

The synchronous commits are always in the log before any dependent queued commit is created.

## Dependent commits

Every producer reads the current state of the state store, and commits are created based on that state. The producers
never take into account any state that is still in the commit queue. We'll go through each of the dependencies.

#### AddFiles vs AssignJobIds

The compaction job creator only looks at state committed to the state store, so a job ID assignment can only be created
after the files have been successfully committed.

#### AddFiles vs SplitPartition

Splitting partitions doesn't operate on files at all. When a partition is split, the files on that partition become
references on a non-leaf partition, and are pushed down the tree later as a separate operation.

#### ReplaceFileReferences vs DeleteFiles

The garbage collector only looks at files that have no remaining references, which can only be the case after the file
reference replacement has been successfully committed.

#### Partition vs file transactions

File and partition transactions are actually two separate transaction logs. The causality between the two logs is
implicit, in the fact that a file reference can only contain the ID for a partition that we've read from the state
store. Operations on files and partitions always happen independently, so their relative ordering doesn't matter.

#### Two SplitPartitions where one splits the other's child

Partition splitting only happens on state that has been committed to the state store. A new child partition can only be
split once the transaction that created it has been successfully committed.

## Potential problems

While the FIFO ordering doesn't matter for correctness of the state store, there are some related failure modes.

### Delayed transactions can become permanently invalid

If a compaction is created, and its AssignJobIdsTransaction fails or is delayed, another compaction can be created for
the same files. If the AssignJobIdsTransaction is re-processed after the new compaction is created and its file
assignments are committed, then the old commit will always fail.

If a SplitPartitionTransaction fails or is delayed, the partition can be re-split. If the SplitPartitionTransaction is
re-processed after the re-split is committed, then the old commit will always fail.

In both of these cases retries are pointless, but only once the new commit is successful. We'll want to avoid this ever
showing up on the dead letter queue, because it doesn't represent a real failure and would result in misleading alarms.
If these transactions fail to validate in a way that indicates they've been superseded, we can discard them. For
compaction, we could report this to the job tracker. For partition splitting, it's not clear how we would report on
this.

#### Compaction creation conflicts

If we allow retries of AssignJobIdsTransaction, we can get further transactions created that assign the same input
files. This creates conflicts between transactions that assign the same input files to different compaction jobs.

Unlike in partition splitting, compaction jobs are validated and sent in batches. The compaction dispatcher will only
send jobs once the whole batch is validated as having its file assignments fully committed to the state store. We need
to verify that there can be no partial commit of a batch of jobs, otherwise a batch could be stuck in the dispatcher
with its input files stuck assigned to those jobs. TODO
