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

The database schema may be more normalised than the DynamoDB equivalent. We can consider this during prototyping.

## Consequences

With a relational database, large queries can be made to present a consistent view of the data. This could avoid the
consistency issue we have with DynamoDB, but would come with some costs:

- Transaction management and locking
- Server-based deployment model

### Transaction management and locking

With a relational database, larger transactions involve locking many records. If a larger transaction takes a
significant amount of time, these locks may produce waiting or conflicts. A relational database is similar to DynamoDB
in that each record needs to be updated individually. It's not clear whether this may result in slower performance than
we would like, deadlocks, or other contention issues.

Since PostgreSQL supports larger queries with joins across tables, this should make it possible to produce a consistent
view of large amounts of data, in contrast to DynamoDB.

If we wanted to replicate DynamoDB's conditional updates, one way would be to make a query to check the condition, and
perform an update within the same transaction. This may result in problems with transaction isolation.

PostgreSQL defaults to a read committed isolation level. This means that during one transaction, if you make multiple
queries, the database may change in between those queries. By default, checking state before an update does not produce
a conditional update as in DynamoDB.

With higher levels of transaction isolation, you can produce the same behaviour as a conditional update in DynamoDB.
If a conflicting update occurs at the same time, this will produce a serialization failure. This would require you to
retry the update. There may be other solutions to this problem, but this may push us towards keeping transactions as
small as possible.

See the PostgreSQL manual on transaction isolation levels:

https://www.postgresql.org/docs/current/transaction-iso.html

### Deployment model

PostgreSQL operates as a cluster of individual server nodes. We can mitigate this by using a service with automatic
scaling.

Aurora Serverless v2 supports automatic scaling up and down between minimum and maximum limits. If you know Sleeper will
be idle for a while, we could stop the database and then only be charged for the storage. We already have a concept of
pausing Sleeper so that the periodic lambdas don't run. With Aurora Serverless this wouldn't be too much different. See
the AWS documentation:

https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/aurora-serverless-v2.how-it-works.html

This has some differences to the rest of Sleeper, which is designed to scale to zero by default. Aurora Serverless v2
does not support scaling to zero. This means there would be some persistent costs unless we explicitly pause the Sleeper
instance and stop the database entirely.
