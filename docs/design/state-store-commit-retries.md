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

The synchronous commits are always in the log before any dependent queued commit is created. There could still be
unexpected behaviour that results from the two separate paths.

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
files. This can create conflicts between transactions that assign the same input files to different compaction jobs.

Unlike in partition splitting, compaction jobs are validated and sent in batches. The compaction dispatcher will only
send jobs once the whole batch is validated as having its file assignments fully committed to the state store. We need
to verify that there can be no partial commit of a batch of jobs, otherwise a batch could be stuck in the dispatcher
with its input files stuck assigned to those jobs.

This is dealt with by committing file assignments to the state store for a whole batch of compaction jobs at once.
The whole batch is committed atomically in a single transaction, so the input files can only be stuck assigned if the
dispatcher fails to send the batch to the compaction jobs queue. In that case the message to dispatch the batch will be
retried in SQS. This can produce duplicate copies of a job, but the impact of that is duplication of work, and a failed
commit the second time it runs. We'll deal with that failed commit in a separate section.

### Failures in batched transactions

AddFilesTransaction, AssignJobIdsTransaction and ReplaceFileReferencesTransaction all apply a batch of multiple
operations in a single transaction. For AddFilesTransaction and ReplaceFileReferencesTransaction, or ingest and
compaction commit, if any of the individual operations are invalid, the transaction is still applied to the log, but is
validated at read time to decide how the state should be updated. For AssignJobIdsTransaction, or file assignment, the
whole transaction is rejected if any of the files cannot be assigned, in order to refuse the batch as a whole.

#### Impact when file assignment fails

In the case where a batch of compaction jobs fails all retries to send, it will reach the dead letter queue. This needs
manual intervention to unlock the files or proceed with the compaction jobs, but it's not necessarily related to state
store commits, except where those changes impact this functionality or vice versa. We have a separate issue for that
analysis: https://github.com/gchq/sleeper/issues/8655

In the case where a batch of compaction jobs fails because the file assignments failed to commit to the state store,
the commit will end up on the dead letter queue for the state store committer. It won't cause any problem for the
system because compaction creation will run again and the files will still be compacted because they were never locked.
It does cause a problem in that if there's a redrive of failed commits, it can never succeed if any of the files have
since been locked for compaction.

If we discard an invalid AssignJobIdsTransaction, this would result in a dead letter for the pending compaction
creation batch queue, but a redrive could never succeed because the commit to assign the input files has been discarded.
If we don't discard an invalid AssignJobIdsTransaction, it can still become permanently invalid and this still means a
redrive of the compaction creation batch can never succeed. We could consider handling for this.

#### Impact when compaction or ingest commit fails

When a compaction job is duplicated, or processed twice, the second commit cannot be applied to the state store.
Compaction commits are applied in batches, where many commits are included in a single transaction. Each transaction can
include both passing and failing commits. We handle this by accepting every compaction commit transaction, whether it
contains failed commits or not, and we validate them at read-time, applying only successful commits to the state. This
also allows us to report on failed commits in the job tracker by processing these transactions in the follower.

This means that every failed compaction commit is always retryable, because when it is added to the log the appropriate
action will be taken to update the state and the job tracker based on the validity of the transaction.

The same thing applies to ingest, since they are also validated at read-time and reported to the job tracker in the same
way.

### Duplicate delivery problems

For the five transaction types on the queue, we can look at what will happen if they are delivered more than once. There
are a few ways this could happen:

1. The state store committer applies a commit to the transaction log, but dies before the message is deleted from the
   queue.
2. The state store committer calls the DynamoDB API to write a transaction log entry. This succeeds in DynamoDB, but the
   response from the API is lost. The committer either fails immediately, or the AWS SDK retries the request, which
   then fails because the record already exists, and is thrown as a DuplicateTransactionNumberException.
3. The producer of the commit retries sending the commit at the application level. The producers send commits with
   SqsFifoStateStoreCommitRequestSender, which sets a random deduplication ID on the SQS message.

In the first two cases the message returns to SQS and can be retried, either directly or as a redrive of a dead letter,
despite having committed successfully.

In the second case, it's also possible to add a duplicate transaction to the log without ever retrying the commit
message. This will only happen for transactions validated at read time, where a DuplicateTransactionNumberException or
a throttling exception both result in a retry that is not rejected by validation. For transactions validated at write
time the retry is rejected by validation and sent back to SQS.

The impact of this duplication will be the same whether it gets back to SQS or not, except that a retry through SQS can
result in dead letters.

In the third case the same commit message is added to the queue twice at the producer side. Each attempt will also
upload a new copy of the transaction body to S3 if it is too big. If sending fails the producer will re-run and build a
new transaction. There's no handling to delete the transaction body file if it does fail, so that needs adding.

#### Transactions validated at read time

For AddFiles and ReplaceFileReferences, two copies can potentially both be added to the log. If parts do not validate,
the transaction will still be added. The state will not be updated because it will detect that it has already been
applied and skip it. There is an exception to this where the files being added have been fully deleted from the state
since the transaction was originally applied, but we'll address that separately. This still has a couple of impacts:

1. The jobs are reported to the job tracker as failed on the second commit.
2. If the transaction is big enough that its body is held in S3, both log entries will point to the same file.

The first point is okay and correct, because the reports will still show them as successful, and will also show the
failure. This does leave a risk that it can be misleading, or that future code might not take this case into account,
and incorrectly consider the job as failed.

The second point means that when the first transaction is deleted from the log during cleanup, the second transaction
will become unreadable. This probably needs some handling.

#### Transactions validated at write time

For AssignJobIds, DeleteFiles and SplitPartition, a second copy of the same transaction will fail validation before it
is added to the log, and will fail. We could consider discarding these validation failures to avoid noisy messages on
the dead letter queue. These will be permanently invalid, and a redrive could never succeed.

It's also worth noting that DeleteFiles is always safe to discard, because if the deletion is not recorded, the garbage
collector will repeat the deletion, which is idempotent, and create a fresh commit.

#### Phantom files

When an AddFilesTransaction is retried, particularly due to a redrive from the dead letter queue, it is possible that it
may have already been successful in the past, but the files have already been compacted and garbage collected, so that
they are no longer recorded in the state store. In this case, they will be added back to the state store again. This
creates references to files that have already been deleted by garbage collection, and puts the table in an invalid
state.

In order to recover from this, you would need to create an empty file, add transactions that fake running a compaction
of the phantom file into the empty file, and re-GC the phantom file. That empty file could then be removed by real
compactions.

### Conflict between manual operations and asynchronous commits

If a manual operation in a client clears the state of a table, there can still be asynchronous commits on the queue.
For example, if you clear partitions there may be a transaction on the queue to add files to a partition that has now
been deleted, but those files would still be added to partitions that no longer exist.

This is an additional operational concern if you want to delete a Sleeper table. We could consider what happens during
deletion or reinitialisation in more depth.

## Outcomes of this analysis

We have plans to improve on this in issues
[#8618 - State store commit failures in lambda are ignored](https://github.com/gchq/sleeper/issues/8618) and
[#8364 - State store commits do not recover after a temporary failure, and redrive may produce unexpected behaviour](https://github.com/gchq/sleeper/issues/8364). Here are some outcomes of this analysis.

From a standpoint of ordering of commits, it's safe to enable retries on the queue.

For validation failures adding commits, we can introduce handling to avoid retries, track failures for reporting, and
delete the invalid commits. This lets us focus retries on infrastructure problems.

For commits validated at read time, failures are already handled and reported correctly, but there's a case where the
same transaction body file can be included in multiple log entries, and that needs extra handling. At time of writing
this is just ingest and compaction commit.

When sending commits, we need to add some handling to ensure the transaction log body file is deleted if it fails.

We need to review how batches of pending compaction jobs are handled when they fail.

### Transaction body file references

We can consider some options for how to improve on the case where the same transaction body file is used for multiple
log entries.

1. When the committer brings itself up to date, check whether any of the entries it reads have a body file it's about to
   add to the log. This would only catch the case where the retry happens in the same invocation, so it might not be
   worth the complication.
2. Detect when a message is redelivered, and copy the transaction body file to a fresh key before committing. This could
   orphan the original copy of the file, and we can't reliably detect when a message is delivered, although SQS gives
   an ApproximateReceiveCount.
3. Before deleting transactions, read the rest of the transaction log from the deletion point to the head, and don't
   delete any body files that we find references for. This can still result in unreadable commits if a committer writes
   a retry to the log at the same time as the deletion happens.
4. Give each commit a unique ID, and track that that ID has been committed in the same DynamoDB transaction as the log
   entry is added. This could solve the problem, but may decrease the throughput through the committer, which is
   important to the scalability of the system.
5. Ignore transaction log entries where the transaction body file has been deleted. We may be able to rely on the fact
   that in all cases where the same file is in multiple entries, the second entry will not validate at read time, and
   will result in no changes to the state.
6. Ensure that transaction body files are only deleted after it has remained unreferenced for some longer period of
   time. This can involve extra tracking that can happen at transaction deletion.
7. Give each commit a unique ID, save it in the log entry, but don't track it anywhere else. Don't use it as an
   idempotency token in the committer, but consider where else we could use it. This is option 4 without making the
   committer idempotent.
8. Have the state store committer always make a copy of a transaction body file before adding to the log. An S3 copy is
   cheap, but it does add latency to the committer. We'd need some cleanup for these files. This is option 2 without the
   check for a redelivery.
9. Instead of deleting transaction body files in the transaction deleter, use an S3 lifecycle rule instead. This would
   rely on a snapshot being created, so if snapshots are disabled for any reason this would cause problems.

Option 5 seems appealing because of its simplicity. We'd need to be careful about documenting the invariant we rely on
for this.

In the case where phantom files are added back to the state store, option 5 would also mean that those are ignored if it
used a transaction body file, and that has already been deleted. Whether this actually happens will depend on the delay
before GC, the delay before transactions are deleted, and whether the transaction is big enough. Usually data is added
in a small number of files, so those transactions may not be big enough to be held in a file.

### Pending compaction job batch failures

TODO

### Phantom files

TODO
