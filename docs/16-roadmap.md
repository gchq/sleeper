

This section documents some of the planned functionality and improvements:

- Query optimisation:
    - Add an option for the system to automatically keep the lambdas warm by periodically
    calling them with dummy queries.
    - Optimise the start-up time of the lambdas, see https://docs.aws.amazon.com/lambda/latest/dg/snapstart.html
    and https://aws.amazon.com/blogs/compute/optimizing-aws-lambda-function-performance-for-java/.
    - Optimise the parameters used when reading and writing Parquet files. These parameters include whether
    dictionary encoding is used, the row group and page sizes, the readahead size, etc.

- Python API improvements: This is currently basic and needs further work.

- Iterators: Currently a single iterator can be provided. This should be extended so
    that a stack of iterators can be provided.

- Bulk export: Add the ability to perform a bulk export, i.e. read over all the data in
    a table, filter it and then export it to Parquet. This will not be done in a lambda.

- Metrics page: Review and extend the metrics produced.

- Purge: Add the ability to purge data from a table, i.e. delete any items matching a
    predicate.

- Create a predicate language for specifying filters on queries.

- Create a suite of automated system tests.

- Create a library of repeatable, sustained, large-scale performance tests.

- Review and extend the integrations with Athena and Trino. Review how Trino can
    be used to run SQL queries over the entire table.

- Extend the range of supported types: we should be able to support the full range
    of Arrow / Parquet types. A Sleeper schema could be specified as an Arrow schema
    with additional information about which are the row keys and sort keys.

- Service that maintains an up-to-date cache of the statestore: Various parts of the
    system need to query the state store. We could potentially reduce the cost and
    latency of these queries if we had a long-running service that maintained an
    up-to-date cache of the statestore.

- Review whether some of the performance sensitive parts of the code can be rewritten
    in Rust to take advantage of the improvements to the Parquet and Arrow Rust libraries.
