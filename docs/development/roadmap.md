Roadmap
=======

This section documents some of the planned functionality and improvements for Sleeper.

Here's a summary of what's being worked on actively:

- Bulk export, issue https://github.com/gchq/sleeper/issues/3446.
- Upgrade to AWS SDK v2, issue https://github.com/gchq/sleeper/issues/1389.
- Java API improvements, issue https://github.com/gchq/sleeper/issues/4155.


Here's a list of improvements that are likely to happen in the near future:

- Create a library of repeatable, sustained, large-scale performance tests, issue https://github.com/gchq/sleeper/issues/1391.


Here's a list of other future plans and improvements:

- Support for deployment with infrastructure as code, issue https://github.com/gchq/sleeper/issues/3693.
- Service that maintains an up-to-date cache of the state store, issue https://github.com/gchq/sleeper/issues/4215.
- Review whether parts of the code can be rewritten in Rust, issue https://github.com/gchq/sleeper/issues/1388.
- Extend the range of supported types to all Arrow types, issue https://github.com/gchq/sleeper/issues/576.
- Create a predicate language for specifying filters on queries, issue https://github.com/gchq/sleeper/issues/1392.
- Review and extend the integrations with Athena and Trino, issue https://github.com/gchq/sleeper/issues/1390.

- Query optimisation.
    - Optimise the start-up time of the lambdas, see https://docs.aws.amazon.com/lambda/latest/dg/snapstart.html
    and https://aws.amazon.com/blogs/compute/optimizing-aws-lambda-function-performance-for-java/.
    - Optimise the parameters used when reading and writing Parquet files. These parameters include whether
    dictionary encoding is used, the row group and page sizes, the readahead size, etc.

- Python API improvements. This is currently basic and needs further work.
- Iterator improvements. This is likely to change based on how we use Rust.
- Metrics page. Review and extend the metrics produced.
- Purge data from a table, i.e. delete any items matching a predicate.
