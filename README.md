Sleeper
=======

## Introduction

Sleeper is a serverless, cloud-native, log-structured merge tree based, scalable key-value store. It is designed to
allow the ingest of very large volumes of data at low cost. Queries for rows where the key takes a given value takes
around 1-2 seconds, but many thousands can be run in parallel. Each individual query has a negligible cost.

Data is stored as records in tables. A table has a schema with key fields, optional sort fields, and some value fields.
Each table has a metadata store that tracks Parquet files in S3. These files contain records in sorted order, by the key
field and then by the sort field. Files exist within a partition, which each contain data for a range of keys.

Sleeper can be thought of as a cloud-native reimagining of systems such as Hbase and Accumulo. The architecture is
very different to those systems. Sleeper has no long running servers. This means that if there is no work to be done,
i.e. no data is being ingested and no background operations such as compactions are in progress, then the only cost
is the cost of the storage. There are no wasted compute cycles, i.e. it is "serverless".

The current codebase can only be deployed to AWS, but there is nothing in the design that limits it to AWS. In time
we would like to be able to deploy Sleeper to other public cloud environments such as Microsoft Azure
or to a Kubernetes cluster.

## Documentation

The [getting started guide](docs/getting-started.md) will walk through setting up and interacting with an instance
of Sleeper.

See below for further documentation:

- [Deployment guide](docs/deployment-guide.md)
- [Usage guide](docs/usage-guide.md)
- [Developer guide](docs/developer-guide.md)
- [Design](docs/design.md)
- [Design risks and mitigations](docs/design-risks-and-mitigations.md)
- [Common problems and their solutions](docs/common-problems-and-their-solutions.md)
- [Roadmap](docs/development/roadmap.md)

## License

Sleeper is licensed under the [Apache 2](http://www.apache.org/licenses/LICENSE-2.0) license.
