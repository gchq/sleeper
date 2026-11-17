# Transaction log state store asynchronous commit retries

At time of writing the state store is implemented as state derived from a transaction log. There's an SQS FIFO queue of
commits, which is used to separate the commits per Sleeper table, so that they can be processed without contention. Each
commit adds a single transaction to the log. Not all commits go through this queue.

SQS FIFO is designed to provide guarantees on the delivery order of the messages, but we don't think we rely on this in
Sleeper. We think that if a commit fails, we can safely send it to the back of the queue to be retried later, and
continue processing commits in order of the queue. In practice, if we mark a single message as failed, with a FIFO queue
SQS will keep that message at the head of the queue for retries rather than sending it to the back. We can consider
retrying the message in place or re-sending it as a new message.

We'll look at potential impacts of this. We'll look at the potential approaches we can consider to retry commits.

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
| InitialisePartitionsTransaction  | Client                    |
| ClearFilesTransaction            | Client                    |

The synchronous commits are always in the log before any dependent queued commit is created. There could still be
unexpected behaviour that results from the two separate paths, and we'll look at that in its own section.

## Dependent commits

Every producer reads the current state of the state store, and commits are created based on that state. The producers
never take into account any state that is still in the commit queue. We'll go through each of the dependencies.

### AddFiles vs AssignJobIds

The compaction job creator only looks at state committed to the state store, so a job ID assignment can only be created
after the files have been successfully committed.

### AddFiles vs SplitPartition

Splitting partitions doesn't operate on files at all. When a partition is split, the files on that partition become
references on a non-leaf partition, and are pushed down the tree later as a separate operation.

### AssignJobIds vs ReplaceFileReferences

The compaction dispatcher only sends compaction jobs to be run once the jobs are assigned to their input files.

### ReplaceFileReferences vs DeleteFiles

The garbage collector only looks at files that have no remaining references, which can only be the case after the file
reference replacement has been successfully committed.

### Partition vs file transactions

File and partition transactions are actually two separate transaction logs. The causality between the two logs is
implicit, in the fact that a file reference can only contain the ID for a partition that we've read from the state
store. Operations on files and partitions always happen independently, so their relative ordering doesn't matter.

### Two SplitPartitions where one splits the other's child

Partition splitting only happens on state that has been committed to the state store. A new child partition can only be
split once the transaction that created it has been successfully committed.

## Potential failure modes

While the FIFO ordering doesn't matter for correctness of the state store, there are some related failure modes.

### Impacts of not retrying commits

For AssignJobIdsTransaction, SplitPartitionTransaction and DeleteFilesTransaction, if these fail to commit the process
will run again automatically and repeat the work they would have done, so they don't really need to be retried.

For AddFilesTransaction, if it is not committed we lose data from an ingest or bulk import, the files that were being
added are left in S3 with no references, and they are never cleaned up. To detect this you could try to compare the
files in the state store with the files in the data bucket's data file prefix, but this would include files from jobs
that are still in progress. You could look at the files' age, but it would be difficult to differentiate files that were
never added from files that were incorrectly removed from the state store during a failed GC. You could see the jobs
that have failed to commit based on the job tracker, but that wouldn't tell you the paths of the files it was trying to
add. You could check the logs from the processes that wrote these files. In practice it could be very difficult to find
files dropped this way definitively.

For ReplaceFileReferencesTransaction, if it is not committed then all the compactions' input files are left locked for
compaction. They will never be compacted again until this is resolved. It would be possible to detect this by looking at
the file references in the state store. You could potentially reconstruct a compaction job from this. For the output
filename, you could re-derive it from the partition ID and job ID, and check if the output file exists in S3. You could
potentially recreate a ReplaceFileReferencesTransaction from the available data even if the commit has been deleted. You
would also need to find the count of rows in the output file, either from the file or from the job tracker.

### Delayed transactions can become permanently invalid

If a compaction is created, and its AssignJobIdsTransaction fails or is delayed, another compaction can be created for
the same files. If the AssignJobIdsTransaction is re-processed after the new compaction is created and its file
assignments are committed, then the old commit will always fail.

If a SplitPartitionTransaction fails or is delayed, the partition can be re-split. If the SplitPartitionTransaction is
re-processed after the re-split is committed, then the old commit will always fail.

In both of these cases retries are pointless, but only once the new commit is successful. We'll want to avoid this ever
showing up on the dead letter queue, because it doesn't represent a real failure and would result in misleading alarms.
If these transactions fail to validate in a way that indicates they've been superseded, we can discard them. For
compaction, we could consider reporting this to the job tracker. For partition splitting, it's not clear how we would
report on this.

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

All of the asynchronous transactions except SplitPartitionTransaction apply a batch of multiple operations in a single
transaction. In all cases we need to be careful about what happens when only a part of the transaction is not valid, and
whether this has impacts on the system.

If we change any of this behaviour for existing transactions, then during upgrade you could get a state where different
processes disagree on the state of the Sleeper table. We would need some handling to avoid an upgrade changing the
behaviour of a transaction that has already been written.

For AddFilesTransaction and ReplaceFileReferencesTransaction, or ingest and compaction commit, if any of the individual
operations are invalid, the transaction is still applied to the log, but is validated at read time to decide how the
state should be updated. The others are validated before being written to the log.

For AddFilesTransaction, if any file is in the state store the whole transaction is ignored. This assumes that those
files are only ever added in an exact duplicate of this same transaction. It also prevents phantom files unless all of
the files have been deleted from the state.

For ReplaceFileReferencesTransaction, compaction commits are applied individually. If some are invalid, those commits
are ignored but others in the batch are still applied.

For AssignJobIdsTransaction, or compaction file assignment, the whole transaction is rejected if any of the files cannot
be assigned, in order to refuse the batch as a whole.

For DeleteFilesTransaction, if any file is not in the state store the whole transaction is rejected at write time. GC
should handle this in the next run.

#### Impact when file assignment fails

In the case where a batch of compaction jobs fails all retries to send, it will reach the dead letter queue. This needs
manual intervention to unlock the files or proceed with the compaction jobs, but it's not necessarily related to state
store commits, except where those changes impact this functionality or vice versa. We have a separate issue for that
analysis: https://github.com/gchq/sleeper/issues/8655

In the case where a batch of compaction jobs fails because the file assignments failed to commit to the state store,
the commit will end up on the dead letter queue for the state store committer. It won't cause any problem for the
system because compaction creation will run again and the files will still be compacted because they were never locked.
It does cause a problem for the user in that if there's a redrive of failed commits, it can never succeed if any of the
files have since been locked for compaction.

If we discard an invalid AssignJobIdsTransaction, this would result in a dead letter for the pending compaction
batch queue, but a redrive could never succeed because the commit to assign the input files has been discarded. If we
don't discard an invalid AssignJobIdsTransaction, it can still become permanently invalid if the input files are
reassigned, and if that happens a redrive of the compaction creation batch still can never succeed. We could consider
handling for this.

#### Impact when compaction or ingest commit fails

When a compaction job is duplicated, or processed twice, the second commit cannot be applied to the state store.
Compaction commits are applied in batches, where many commits are included in a single transaction. Each transaction can
include both passing and failing commits. We handle this by accepting every compaction commit transaction, whether it
contains failed commits or not, and we validate them at read-time, applying only successful commits to the state. This
also allows us to report on failed commits in the job tracker by processing these transactions in the follower.

This means that every failed compaction commit is always retryable, because when it is added to the log the appropriate
action will be taken to update the state and the job tracker based on the validity of the transaction.

The same thing mostly applies to ingest, since they are also validated at read-time and reported to the job tracker in
the same way. The exception is that for ingest there's a duplicate delivery problem that can result in phantom file
references. This is discussed below.

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
time the retry is rejected by validation and sent back to SQS. Note that both committers have specific handling to retry
on DynamoDB throttling exceptions.

The impact of this duplication will be the same whether it gets back to SQS or not, except that a retry through SQS can
result in dead letters.

In the third case the same commit message would be added to the queue twice at the producer side. Each attempt will also
upload a new copy of the transaction body to S3 if it is too big. In practice, if sending fails the producer will re-run
and build a new transaction, so this case should not occur as complete duplicates. We do need handling to delete the
transaction body file if it does fail, although it may not be possible to avoid all orphan files here.

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

Because the transaction is validated as a whole, this will only happen if all of the files in the transaction have been
compacted and garbage collected.

In order to recover from this, you would need to create an empty file, add transactions that fake running a compaction
of the phantom file into the empty file, and re-GC the phantom file. That empty file could then be removed by real
compactions.

### Conflict between manual operations and asynchronous commits

If a manual operation in a client clears the state of a table, there can still be asynchronous commits on the queue.
For example, if you clear partitions there may be a transaction on the queue to add files to a partition that has now
been deleted, but those files would still be added to partitions that no longer exist. If you clear files and actually
delete the files from S3, there may be a transaction on the queue that adds phantom references for the files you just
deleted.

This is an additional operational concern if you want to delete a Sleeper table. We could consider what happens during
deletion or reinitialisation in more depth.

## Potential approaches

We have plans to improve on this in issues
[#8618 - State store commit failures in lambda are ignored](https://github.com/gchq/sleeper/issues/8618) and
[#8364 - State store commits do not recover after a temporary failure, and redrive may produce unexpected behaviour](https://github.com/gchq/sleeper/issues/8364).
Here are some outcomes of this analysis.

From a standpoint of ordering of commits, it's safe to enable retries on the queue. We need to consider whether retries
need to be sent to the back of the queue or not.

We think we can discard validation failures when adding commits. We'll look at the details of this.

For commits validated at read time, failures are already handled and reported correctly, but there's a case where the
same transaction body file can be included in multiple log entries, and that needs extra handling. At time of writing
this is just ingest and compaction commit.

When sending commits, we need to add some handling to ensure the transaction log body file is deleted if it fails.

We need to review how batches of pending compaction jobs are handled when they fail.

We won't consider here what we should do if a commit fails all retries. We would like to guarantee that the system will
automatically recover from all failures. Nevertheless, there's a point at which the scale of operations may go beyond
what we can possibly handle. For example, DynamoDB may be unable to handle the load on the transaction log for a certain
Sleeper table. In these cases we can ensure that failures are reported appropriately, and if load decreases then the
system can recover. This recovery may or may not require redrive of dead letters, and we won't address this here.

### Retry mechanism

If we use SQS FIFO's built-in retries, any messages needing a retry will stay at the front of the queue. This may be
acceptable if we can avoid retries for non-retryable problems. If we consider re-sending the message to the back of the
queue, we wouldn't be able to use SQS' retry handling, so we probably want to avoid this.

This leaves a problem in that any retries in SQS mean that all processing of that Sleeper table will stall until the
message is processed. The failed message will not be redelivered until its visibility timeout, and no further messages
will be delivered for the Sleeper table until this timeout expires. For the AWS Lambda SQS integration, they recommend
not processing any messages after a failed message in a batch, and returning them all as batch item failures
(see the AWS documentation, [Handling errors for an SQS event source in Lambda](https://docs.aws.amazon.com/lambda/latest/dg/services-sqs-errorhandling.html)).
This is specifically to preserve ordering, so it seems like this does not apply to us.

We can set the retry delay explicitly if we call `ChangeMessageVisibility` on each failed message with a shortened
timeout. This will reduce the stall, but not eliminate it unless we set it to 0. If the committer fails before it makes
this call, it will stall for the full visibility timeout. We may need to reduce the default timeout as well.

We will want to ensure that any non-retryable failure is consumed and does not return back to SQS. This includes
deserialisation failures, transaction validation failures, and transactions whose body file has been deleted.

### Validation failures

For validation failures adding commits, we can introduce handling to detect and delete the invalid commits immediately.
This lets us focus retries on infrastructure problems.

We could consider reporting on the failures, but we may want to avoid updating a job tracker inside the state store
committer as that would add latency. For now we'll assume we don't need to report on this, since the write-time
validated commits all have clean handling if they are discarded.

There is an exception to this in pending compaction job batch failures, but we'll look at that separately. We assume we
can log validation failure in the committer and move on.

### Transaction body file references

We can consider some options for how to improve on the case where the same transaction body file is used for multiple
log entries.

1. When the committer brings itself up to date, check whether any of the entries it reads have a body file it's about to
   add to the log. This would only catch the case where the retry happens in the same invocation, so it might not be
   worth the complication.
2. Detect when a message is redelivered, and copy the transaction body file to a fresh key before committing. This could
   orphan the original copy of the file, and we can't reliably detect when a message is redelivered, although SQS gives
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
6. Ensure that transaction body files are only deleted after they have remained unreferenced for some longer period of
   time. This can involve extra tracking that can happen at transaction deletion.
7. Give each commit a unique ID, save it in the log entry, but don't track it anywhere else. Don't use it as an
   idempotency token in the committer, but consider where else we could use it. This is option 4 without making the
   committer idempotent.
8. Have the state store committer always make a copy of a transaction body file before adding to the log. An S3 copy is
   cheap, but it does add latency to the committer. We'd need some cleanup for these files. This is option 2 without the
   check for a redelivery.
9. Instead of deleting transaction body files in the transaction deleter, use an S3 lifecycle rule. This would rely on a
   snapshot being created, so if snapshots are disabled for any reason this would cause problems.

Option 5 seems appealing because of its simplicity. We'd need to be careful about documenting the invariant we rely on
for this. It would also mean that if a transaction body file is lost for any other reason, we would lose data. This may
be better or worse than a louder failure, and needs consideration.

In the case where phantom files are added back to the state store, option 5 would also mean that those are ignored if it
used a transaction body file, and that has already been deleted. This could also mean that different processes would
disagree on the state if the transaction body file is deleted for a transaction that has already been seen to add a
phantom file back to the table. Whether this actually happens will depend on the delay before GC, the delay before
transactions are deleted, and whether the transaction is big enough. Usually data is added in a small number of files,
so those transactions may not be big enough to be held in a file.

### Avoiding phantom files

We have a few options for ways to avoid references reappearing to files that have been deleted:

1. Check whether each file exists in S3 before committing it. This would add latency in the state store committer, but
   if we can tell how old the commit is we could just do that check for old commits. This means that old redrives would
   become slower to process. In practice not much of the load on the committer is for adding files, but there may be
   cases where a large number of files are added at once. One way to detect a commit's age would be to add a timestamp
   to the commit when we send it in the producer.
2. Reject old commits by age. If we can tell how old a commit is we could ignore them, but this would prevent all
   redrives beyond that age.
3. Retain a record of deleted files in the state store state. We can then validate that the file hasn't existed before.
   This would grow the state held for a Sleeper table, so it would need to be limited based on age. It might still not
   retain the record for long enough depending on the age of the redrive.
4. Give each commit a unique ID, use it as an idempotency token in the state store committer. This could solve the
   problem, but may decrease the throughput through the committer, which is important to the scalability of the system.
5. Add further control over redrives. A redrive tool could check whether each file exists. One way to do this would be
   to stop leaving the messages on the dead letter queue, and write them somewhere else instead. There's also an option
   to filter out commits that add files, either on a different queue or by organising some target store. That may give
   more ability to recognise the risk when redriving. A record of each commit's age may help here too.
6. When a commit goes to the dead letter queue, have an automatic extra check for whether files have actually been
   added. This would require reading the state store state, and one reason a commit can appear here is because the
   DynamoDB table for this is overloaded or otherwise unavailable.
7. Whenever we read data files, if we find a file does not exist, ignore the failure and carry on. Treat it the same as
   an empty file. This would rely on our ability to detect that the file does not exist, and affect how this case is
   handled. It would also rely on our ability to control everywhere the files are read. Any time we report on data files
   in the system to a client, this would risk giving incorrect information. Row counts would be wrong, the reference
   would be included in compaction, and if a file really goes missing it would be ignored.

All of these either do not fully handle the problem, or put additional load on the state store committer. We can think
carefully about how we could proceed.

The most actionable of these seems option 5, further control over redrives. In principle, we would like to provide
guarantees about how failures are handled and avoid ever having a permanent failure in the system. Automatic handling of
these failure cases seems generally preferable. Taking more control over redrives would be a move towards this, and
could give us a place to add automatic handling that we don't have right now.

Option 1 only puts additional load on the state store committer during a redrive of old commits. It may be difficult to
predict the impact of this, but it does seem lower than some of the other options. If we rely on a redrive tool from
option 5 without adding checks in the committer, the risk would be that someone might bypass the tool, instead of that
the committer may be impacted. We could avoid bypass of the tool by holding the commits for redrive somewhere other than
directly on the dead letter queue, e.g. in S3.

### Pending compaction job batch failures

There are a few reasons a batch of pending compaction jobs can reach the dead letter queue:

| Cause                                                           | Files locked to this batch? | Can a redrive succeed?      |
|-----------------------------------------------------------------|-----------------------------|-----------------------------|
| An input file reference has been split or locked to another job | No                          | Never                       |
| File assignment wasn't applied to the state store in time       | If assignment happens       | If assignment happens       |
| Something failed when sending jobs                              | Yes                         | Yes, with caveats           |

In the first row, since the input file assignments of the whole batch are committed atomically, the dispatcher can see
that none of its file assignments can ever succeed. From the fact that none of the files are locked to the batch, but
some of them have been otherwise processed, that means either it's actually already sent all the jobs and they've all
been committed, or the file assignments did not succeed, and can now never succeed.

In the second row, it's possible the file assignment will fail, but at some point the files will be included in new
compactions, and this will turn into the first row. It's also possible assignment will happen, in which case when a
redrive happens it will continue, either succeeding or potentially failing in the third row.

In the third row, there is a case where some of the jobs failed to send, and others have already finished and committed
to the state store. In this case, the dispatcher will see that some of the files in the batch are assigned to its jobs,
but some of them have already been processed and may no longer be visible in the table state. We need to ensure that
this batch will still eventually be sent and processed.

We can call these three cases a superseded batch, a slow batch, and a partially processed batch.

For a superseded batch, we can safely discard it and allow the files to be picked up by further compaction creation.
We'll want to ensure the file holding the batch in S3 is deleted. For a slow batch, we want to ensure that the batch is
eventually sent, or the files are made available for further compaction. For a partially processed batch, we want to
ensure that retries or redrive will continue sending the jobs that have not already run.

The handling of a slow or partially processed batch seems separate from the retries of state store commits, so we don't
need to cover that here.

This seems like enough to show that if a file assignment fails, and is discarded in the state store committer, we can
detect that a pending compaction job batch has been superseded if any of the file references have been assigned to a new
job, or if none of the file references exist. We will want to make sure this detection occurs, and the batch is deleted.

We also need to make sure that the compaction dispatcher will fully distinguish between the superseded case and the
partially processed case. If a file reference is missing, it could be because it has been superseded, or it could be
because the compaction it was originally assigned to has been committed. Because a batch's files are assigned
atomatically, we know that if any of the file assignments have been applied, the batch cannot have been superseded.
