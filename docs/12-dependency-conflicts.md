Dependency Conflicts
====================

This is a record of conflicts that have caused problems, where two dependencies of Sleeper have a shared dependency, but
declare different versions.

This is intended to help when upgrading versions of libraries, to determine whether we're able to upgrade on both sides
of a transitive dependency conflict.

## AWS EMR

AWS EMR requires specific versions of Apache Spark and Apache Hadoop in order to run bulk import jobs on that platform.
As a result we restrict versions to match the versions used by EMR, for both of those libraries, all their dependencies,
and all libraries that have them as dependencies or share dependencies in a way which cannot be resolved.

This includes the following transitive dependencies of Hadoop:

- OkHttp
- Eclipse Jersey
- Jackson (JSON parser)

This includes the following transitive dependencies of Spark:

- Zookeeper
- Apache Avro
- Apache Arrow
- Netty
- Eclipse Jersey
- Jackson (JSON parser)
- Janino (commons-compiler)

This also includes Apache Parquet, which declares Hadoop as a dependency.

## Conflicts

Here is a list of libraries where we've had conflicts in the past between dependencies declaring them transitively. For
each library, we list the dependencies that bring them in transitively:

- Netty
  - Apache Spark
  - AWS Athena, AWS SDK v1, AWS Lambda
  - Apache Arrow

- Jetty
  - Apache Hadoop
  - WireMock
