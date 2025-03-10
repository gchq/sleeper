Roadmap
=======

This section documents some of the planned functionality and improvements for Sleeper.

Here's a summary of what's being worked on actively:

- Bulk export https://github.com/gchq/sleeper/issues/3446: Add the ability to perform a bulk export, i.e. read over
    all the data in a table, filter it and then export it to Parquet. This will not be done in a lambda.

- Upgrade to AWS SDK v2. https://github.com/gchq/sleeper/issues/1389

- Java API improvements. https://github.com/gchq/sleeper/issues/4155


Here's a list of improvements that are likely to happen in the near future:

- Create a library of repeatable, sustained, large-scale performance tests. https://github.com/gchq/sleeper/issues/1391


Here's a list of other future plans and improvements:

- Support for deployment with infrastructure as code. https://github.com/gchq/sleeper/issues/3693

- Service that maintains an up-to-date cache of the state store https://github.com/gchq/sleeper/issues/4215: Various
    parts of the system need to query the state store. We could potentially reduce the cost and latency of these queries
    if we had a long-running service that maintained an up-to-date cache of the state store.

- Review whether some of the performance sensitive parts of the code can be rewritten
    in Rust to take advantage of the improvements to the Parquet and Arrow Rust libraries.

- Extend the range of supported types: we should be able to support the full range
    of Arrow / Parquet types. A Sleeper schema could be specified as an Arrow schema
    with additional information about which are the row keys and sort keys.

- Query optimisation:
    - Optimise the start-up time of the lambdas, see https://docs.aws.amazon.com/lambda/latest/dg/snapstart.html
    and https://aws.amazon.com/blogs/compute/optimizing-aws-lambda-function-performance-for-java/.
    - Optimise the parameters used when reading and writing Parquet files. These parameters include whether
    dictionary encoding is used, the row group and page sizes, the readahead size, etc.

- Python API improvements: This is currently basic and needs further work.

- Iterators: Currently a single iterator can be provided. This should be extended so
    that a stack of iterators can be provided.

- Metrics page: Review and extend the metrics produced.

- Purge: Add the ability to purge data from a table, i.e. delete any items matching a
    predicate.

- Create a predicate language for specifying filters on queries.

- Review and extend the integrations with Athena and Trino. Review how Trino can
    be used to run SQL queries over the entire table.
