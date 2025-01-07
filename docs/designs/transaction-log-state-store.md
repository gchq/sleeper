# Store a transaction log for the state store

## Status

Accepted

## Context

We have two implementations of the state store that tracks partitions and files in a Sleeper table. This takes some
effort to keep both working as the system changes, and both have problems.

The DynamoDB state store holds partitions and files as individual items in DynamoDB tables. This means that updates
which affect many items at once require splitting into separate transactions, and we can't always apply changes as
atomically or quickly as we would like. There's also a consistency issue when working with many items at once. As we
page through items to load them into memory, the data may change in DynamoDB in between pages.

The S3 state store keeps one file for partitions and one for files, both in an S3 bucket. A DynamoDB table is used to
track the current revision of each file, and each change means writing a whole new file. This means that each change
takes some time to process, and if two changes happen to the same file at once, it backs out and has to retry. Under
contention, many retries may happen. It's common for updates to fail entirely due to too many retries, or to take a long
time.

## Design Summary

Implement the state store using an event sourced model, storing a transaction log as well as snapshots of the state.

Store the transactions as items in DynamoDB. Store snapshots as S3 files.

The transaction log DynamoDB table has a hash key of the table ID, and range key of the transaction number in order. Use
a conditional check to ensure the transaction number set has not been used.

The snapshots DynamoDB table holds a reference to the latest snapshot held in S3, similar to the S3 state store
revisions table. This also holds the transaction number that snapshot was derived from.

## Consequences

This should result in a similar update process to the S3 state store, but without the need to save or load the whole
state at once. Since we only need to save one item per transaction, this may also result in quicker updates compared to
the DynamoDB state store. This would use a different set of patterns from those where the source of truth is a store of
the current state, and we'll look at some of the implications.

We'll look at how to model the state as derived from the transaction log, independent of the underlying store. To avoid
reading every transaction every time, we can store a snapshot of the state, and start from a certain point in the log.

We'll look at how to achieve ordering and durability of transactions. This is a slightly different approach for
distributed updates, and there are potential problems in how we store the transaction log.

We'll look at some applications for parallel models or storage formats, as this approach makes it easier to derive
different formats for the state. This can allow for queries instead of loading the whole state at once, or we can model
the data in some alternative ways for various purposes.

We'll also look at how this compares to an approach based on a relational database.

### Modelling state

The simplest approach is to hold a model in memory for the whole state of a Sleeper table. We can use this one, local
model for any updates or queries, and bring it up to date based on the ordered sequence of transactions. We can support
any transaction that we can apply to the model in memory.

Whenever a change occurs, we create a transaction. Anywhere that holds the model can bring itself up to date by reading
only the transactions it hasn't seen yet, starting after the latest transaction that's already been applied locally.
With DynamoDB, consistent reads can enforce that you're really up-to-date.

We can also skip to a certain point in the transaction log. We can have a separate process whose job is to write regular
snapshots of the model. This can run every few minutes, and write a copy of the whole model to S3. We can point to it in
DynamoDB, similar to the S3 state store's revision table. This lets us get up to date without reading the whole
transaction log. We can load the snapshot of the model, then load the transactions that have happened since the
snapshot, and apply them in memory.

### Transaction size

A DynamoDB item can have a maximum size of 400KB. It's unlikely a single transaction will exceed that, but we'd have to
guard against it. We can either pre-emptively split large transactions into smaller ones that we know will fit in a
DynamoDB item, or we can handle an exception from DynamoDB when an item is too large, and handle it some other way.

To split a transaction into smaller ones that will fit, we would need to handle this in our model, to split a
transaction without affecting the aspects of atomicity which matter to the system.

An alternative would be to detect that a transaction is too big, and write it to a file in S3 with just a pointer to
that file in DynamoDB. This could be significantly slower than a standard DynamoDB update, and may slow down reading
the transaction log.

### Distributed updates and ordering

#### Immediate ordering approach

To achieve ordered, durable updates, we can give each transaction a number. When we add a transaction, we use the next
number in sequence after the current latest transaction. We use a conditional check to refuse the update if there's
already a transaction with that number. We then need to retry if we're out of date.

This retry is comparable to an update in the S3 state store, but you don't need to store the whole state. You also don't
need to reload the whole state each time. Instead, you read the transactions you haven't seen yet and apply them to your
local model. As in the S3 implementation, you perform a conditional check on your local model before saving the update.
After your new transaction is saved, you could apply that to your local model as well, and keep it in memory to reuse
for other updates or queries.

There are still potential concurrency issues with this approach, since retries are still required under contention. We
don't know for sure whether this will reduce contention issues by a few percent relative to the S3 state store (in which
case the transaction log approach doesn't solve the problem), or eliminate them completely. Since each update is
smaller, it should be quicker. We could prototype this to gauge whether it will be eg. 5% quicker or 5x quicker.

#### Eventual consistency approach

If we wanted to avoid this retry, there is an alternative approach to store the transaction immediately. To build the
primary key, you could take a local timestamp at the writer, append some random data to the end, and use that to order
the transactions. This would provide resolution between transactions that happen at the same time, and a reader after
the fact would see a consistent view of which one happened first. We could then store this without checking for any
other transactions being written at the same time.

This produces a durability problem where if two writers' clocks are out of sync, one of them can insert a transaction
into the log in the past, according to the other writer. If two updates are mutually exclusive, one of them may be
inserted before the previous update, and cause the original update to be lost. The first writer may believe its update
was successful because there was a period of time before the second writer added a transaction before it.

We could design the system to allow for some slack and recover from transactions being undone over a short time period.
This would be complicated to achieve, although it may allow for improved performance as updates don't need to wait. The
increase in complexity means this may not be as practical as an approach where a full ordering is established
immediately.

### Parallel models

So far we've assumed that we'll always work with the entire state of a Sleeper table at once, with one model. With a
transaction log it can be more practical to add alternative models for read or update.

#### DynamoDB queries

The DynamoDB state store has advantages for queries, as we only need to read the relevant parts of the state. If we
want to retain this benefit, we could store the same DynamoDB structure we use now.

Similar to the process for S3 snapshots, we could regularly store a snapshot of the Sleeper table state as items in
DynamoDB tables, in whatever format is convenient for queries. One option would be to use the same tables as for the
DynamoDB state store, but use a snapshot ID instead of the table ID.

If we want this view to be 100% up to date, then when we perform a query we could still read the latest transactions
that have happened since the snapshot, and include that data in the result.

#### Status stores for reporting

If we capture events related to jobs as transactions in the log, that would allow us to produce a separate model from
the same transactions that can show what jobs have occurred, and every detail we track about them in the state store.

This could unify some updates to jobs that are currently done in a separate reporting status store, which we would
ideally like to happen simultaneously with some change in the state store, eg. a compaction job finishing.

#### Update models

If we ever decide to avoid holding the whole Sleeper table state in memory, we could create an alternative model to
apply a single update. Rather than hold the entire state in memory, we could load just the relevant state to perform the
conditional check, eg. from a DynamoDB queryable snapshot. When we bring this model up to date from the transaction log,
we can ignore transactions that are not relevant to the update.

This would add complexity to the way we model the table state, so we may prefer to avoid this. It is an option we could
consider.

## Resources

- [Martin Fowler's article on event sourcing](https://martinfowler.com/eaaDev/EventSourcing.html)
- [Greg Young's talk on event sourcing](https://www.youtube.com/watch?v=LDW0QWie21s)
