# Store a transaction log for the state store

## Status

Proposed

## Context

We have two implementations of the state store that tracks partitions and files in a Sleeper table. This takes some
effort to keep both working as the system changes, and both have problems.

The DynamoDB implementation stores partitions and files as individual items in DynamoDB tables. This means that updates
which affect many items at once require splitting into separate transactions, and we can't always apply changes as
atomically or quickly as we would like. When working with many items at once, there's also a consistency issue where as
we page through these items to load them into memory, the data may change in DynamoDB in between pages.

The S3 implementation stores one file for partitions and one for files, both in an S3 bucket. A DynamoDB table is used
to track the current revision of each file, and each change means writing a whole new file. This means that each change
takes some time to process, and if two changes happen to the same file at once, it backs out and has to retry. Under
contention, many retries may happen. It's common for updates to fail entirely due to too many retries, or to take a long
time.

## Decision

Use an [event sourced](https://martinfowler.com/eaaDev/EventSourcing.html) model to store a transaction log, as well as
snapshots of the state.

Store the transactions as items in DynamoDB. Store snapshots as S3 files.

Store each transaction with hash key of the table ID and range key of the transaction number in order. Use a conditional
check to ensure the transaction number set has not been used.

## Consequences

### Modelling state

The simplest way to do this involves holding a model in memory consisting of the whole state of a Sleeper table. This
model needs to support applying any update as an individual change in memory. We then add a way to store that model as a
snapshot of the state at a given point in time.

This means whenever a change occurs, we can apply that to the model in memory. If we store all the changes in DynamoDB
as an ordered transaction log, anywhere else that holds the model can bring itself up to date by reading just the latest
transactions from the log. With DynamoDB, consistent reads can enforce that you're really up-to-date.

There's no need to read or write the whole state at once as with the S3 state store, because the model is derived from
the transaction log. However, after some delay a separate process can write a snapshot of the whole state to S3. This
allows any process to quickly load the whole model without needing to read the whole transaction log. Only transactions
that happened after the snapshot need to be read.

### Distributed updates and ordering

A potential format for the primary key would be to take a local timestamp at the writer, and append some random data to
the end. This would provide resolution between transactions that happen at the same time, and a reader after the fact
would see a consistent view of which one happened first.

This produces a problem where if two writers' clocks are out of sync, one of them can insert a transaction into the log
in the past, according to the other writer. Ideally we would like to only ever append at the end of the log, so we know
no transaction will be inserted in between ones we have already seen.

One approach would be to allow some slack, so that every time we want to know the current state we have to start at a
previous point in the log and bring ourselves up to date. This causes problems for durability. If two updates are
mutually exclusive, one of them may insert itself before the other one and cause the original update to be lost. The
first writer may believe its update successful because there was a period of time before the second writer added a
transaction before it.

We could design the system to allow for this slack and recover from transactions being undone over a short time period.
This would be complicated to achieve, although it may allow for the highest performance.

Another approach is to give each transaction a number. When we add a transaction, we use the next number in sequence
after the current latest transaction. We use a conditional check to refuse the update if there's already a transaction
with that number. We then need to retry if we're out of date. This way each transaction can be durably stored.

This retry is comparable to an update in the S3 state store, but each change is much quicker to apply because you don't
need to store the whole state. You also don't need to reload the whole state each time, because you haven't applied your
transaction in your local copy of the model yet. You need to do the conditional check as in the S3 implementation, but
you don't need to update your local model until after the transaction is saved.

### Parallel models

So far we've assumed that we'll always the entire state of a Sleeper table at once, with one model. With a transaction
log, it can be more practical to expand on that by adding alternative models for read or update.

#### DynamoDB queries

With the DynamoDB state store, there are some advantages for queries, in that we only need to read the relevant parts
of the state. If we wish to retain this benefit, we can store the same DynamoDB items we use now.

Similarly to the S3 snapshot, we could regularly store a snapshot of the table state as items in DynamoDB tables, in
whatever format is convenient for queries.

If we want this view to be 100% up to date, we could still read the latest transactions that have happened since the
snapshot, and include that data in the result of any query.

#### Status stores for reporting

If we capture events related to jobs as transactions in the log, that would allow us to produce a separate model from
the same transactions that can show what jobs have occurred, and every detail we track about them in the state store.

This could unify any updates to jobs that we would ideally like to happen simultaneously with some change in the state
store, eg. a compaction job finishing.

#### Update models

If we ever decide it's worth avoiding holding the whole Sleeper table state in memory, we could create an alternative
model to apply a single update. Rather than hold the entire state in memory, we could load just the relevant state to
perform the conditional check. When we bring this up to date from the transaction log, this model can ignore
transactions that are not relevant to the update.

This would add complexity to the way we model the table state, so we may prefer to avoid this. It is an option we could
consider.

### Comparison to a relational database

With a relational database, large queries can be made to present a consistent view of the data. This would avoid the
consistency issue we have with DynamoDB, but would come with some costs:

- Transaction management and locking
- Server-based deployment model

With a relational database, larger transactions involve locking many records. This would pose similar problems to
DynamoDB in terms of limiting atomicity of updates, as we would be heavily incentivised to keep transactions small. We
would also need to work within the database's transactional tradeoffs, which may cause unforeseen problems.

The database would need to be deployed as a persistent instance, although we could use a managed service. This loses
some of the promise of Sleeper in terms of serverless deployment, and only running when something needs to happen.
