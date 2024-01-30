# Use PostgreSQL for the state store

## Status

Proposed

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

Store the file and partitions state in a PostgreSQL database, with a similar structure to the DynamoDB state store.

## Consequences

With a relational database, large queries can be made to present a consistent view of the data. This would avoid the
consistency issue we have with DynamoDB, but would come with some costs:

- Transaction management and locking
- Server-based deployment model

### Transaction management and locking

With a relational database, larger transactions involve locking many records. If a larger transaction takes a
significant amount of time, there is potential for waiting based on these locks. A relational database is similar to
DynamoDB in that each record needs to be updated individually. It's not clear whether this may result in slower
performance than we would like with large updates, deadlocks, or other contention issues.

We may also be affected by transaction isolation levels. PostgreSQL defaults to a read committed isolation level. This
means that during one transaction, if you make multiple queries, the database may change in between those queries, and
you may see an inconsistent state. This is similar to DynamoDB, except that PostgreSQL also supports higher levels of
transaction isolation, and larger queries across tables. With higher levels of transaction isolation that produce a
consistent view of the state, there is potential for serialization failure. For example, it may not be possible for
PostgreSQL to reconstruct a consistent view of the state at the start of the transaction if the transaction is very
large or a query is very large. In this case it's necessary to retry a transaction. See the PostgreSQL manual on
transaction isolation levels:

https://www.postgresql.org/docs/current/transaction-iso.html

### Deployment model

PostgreSQL operates as a cluster of individual server nodes. We can mitigate this by using a service with automatic
scaling.

Aurora Serverless v2 supports automatic scaling up and down between minimum and maximum limits. If you know Sleeper will
be idle for a while, we could stop the database and then only be charged for the storage. We already have a concept of
pausing Sleeper so that the periodic lambdas don't run. With Aurora Serverless this wouldn't be too much different.

This has some differences to the rest of Sleeper, which is designed to scale to zero by default. Aurora Serverless v2
does not support scaling to zero. This means there would be some persistent costs unless we explicitly pause the Sleeper
instance and stop the database entirely.
