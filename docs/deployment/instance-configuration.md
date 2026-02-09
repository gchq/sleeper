Sleeper instance configuration
==============================

Sleeper is configured with a number of properties that control its behaviour. Instance properties are set for a whole
deployed instance of Sleeper, and table properties are set for individual tables. Some instance properties are applied
during deployment by the CDK, and some are applied at runtime by the system itself. All table properties are applied at
runtime.

You can find descriptions of all properties in the system [here](../usage/property-master.md).

During automated deployment, these properties can be set to defaults. You can then edit these with the admin client.
Alternatively you can manually create configuration files to set the values you want, which you can then version, e.g.
with Git, to track and automate changes.

There are examples of configuration files for an instance with a single table [here](../../example). You can use this as
a starting point for your own configuration. As we improve support for deployment with infrastructure as code, we will
add further examples.

### Configuration folder structure

You can include the following files in your configuration:

* An `instance.properties` file - containing information about your Sleeper instance, as well as
  default values used by tables if not specified.
* A `tags.properties` file which lists the tags you want all of your Sleeper infrastructure to be tagged with.
* A `table.properties` file which contains information about a table and a link to its schema file.
* A `schema.json` file which describes the data stored in a Sleeper table.
* A `splits.txt` file which allows you to pre-split partitions in a Sleeper table.

The `.properties` files are Java properties files. The directory containing `instance.properties` will be considered
the root directory of your configuration. You can find descriptions of all properties in the
system [here](usage/property-master.md).

You can optionally create a `tags.properties` file next to your `instance.properties`, to apply tags to AWS resources
deployed by Sleeper. An example tags.properties file can be found [here](../../example/full/tags.properties).

To include a table in your instance, your `table.properties` file can be in the same folder as
your `instance.properties` file. You can add more than one by creating a `tables` directory in the same folder, with a
subfolder for each table. See [tables](../usage/tables.md) for more information on creating and working with Sleeper
tables.

Each table will also need a `schema.json` file next to the `table.properties` file.
See [create a schema](../usage/schema.md) for how to create a schema.

The `splits.txt` file defines initial split points for the table's partition tree.
See [tables](../usage/tables.md#pre-split-partitions) for how to create this.

At time of writing, the `splits.txt` file needs to be referenced from the `table.properties` file in
the `sleeper.table.splits.file` property, as an absolute path. This will change in a future version of Sleeper. See
issue https://github.com/gchq/sleeper/issues/583.

Here's a full example with two tables:

```
instance.properties
tags.properties
tables/table-1/table.properties
tables/table-1/schema.json
tables/table-1/splits.txt
tables/table-2/table.properties
tables/table-2/schema.json
tables/table-2/splits.txt
```

### Configuring an instance

The `splits.txt` file sets the starting split points for partitions when the table is created. These are the boundaries
where one partition ends and another begins. If you do not set split points, or do not create this file, the table will
be initialised with a single partition.

Note that pre-splitting a table is important for any large-scale use of Sleeper. During bulk import this will be done
automatically, based on an assumption that the data in the bulk import job is a representative sample for the table.

Also see the [tables documentation](../usage/tables.md#pre-split-partitions) for details.

Note, if you do not set the property `sleeper.retain.infra.after.destroy` to false when deploying then however you
choose to tear down Sleeper later on you will also need to destroy some further S3 buckets and DynamoDB tables manually.
This is because by default they are kept.  Similarly, by default all log groups will be retained when the Sleeper instance is
torn down. You can set `sleeper.retain.logs.after.destroy` to false as well.

### Optional Stacks

Each bit of functionality in Sleeper is deployed using a separate CDK nested stack, under one main stack.
By default all the stacks are deployed. However, if you don't need them, you can customise which stacks are deployed.

Mandatory components are the configuration bucket and data bucket, the index of Sleeper tables, the state store,
policies and roles to interact with the instance, and the `TopicStack` which creates an SNS topic used by other stacks
to report errors.

That leaves the following stacks as optional:

* `CompactionStack` - for running compactions (in practice this is essential)
* `GarbageCollectorStack` - for running garbage collection (in practice this is essential)
* `IngestStack` - for ingesting files using the "standard" ingest method
* `PartitionSplittingStack` - for splitting partitions when they get too large
* `QueryStack` - for handling queries via SQS
* `WebSocketQueryStack` - for handling queries via a web socket
* `KeepLambdaWarmStack` - for sending dummy queries to avoid waiting for lambdas to start up during queries
* `EmrServerlessBulkImportStack` - for running bulk import jobs using Spark running on EMR Serverless
* `EmrStudioStack` - to create an EMR Studio containing the EMR Serverless application
* `EmrBulkImportStack` - for running bulk import jobs using Spark running on an EMR cluster that is created on demand
* `PersistentEmrBulkImportStack` - for running bulk import jobs using Spark running on a persistent EMR cluster, i.e. one
  that is always running (and therefore always costing money). By default, this uses EMR's managed scaling to scale up
  and down on demand.
* `IngestBatcherStack` - for gathering files to be ingested or bulk imported in larger jobs
* `TableMetricsStack` - for creating CloudWatch metrics showing statistics such as the number of rows in a table over
  time
* `DashboardStack` - to create a CloudWatch dashboard showing recorded metrics
* `BulkExportStack` - to export a whole table as parquet files

The following stacks are optional and experimental:

* `AthenaStack` - for running SQL analytics over the data
* `EksBulkImportStack` - for running bulk import jobs using Spark running on EKS

By default most of the optional stacks are included but to customise it, set the `sleeper.optional.stacks` sleeper
property to a comma separated list of stack names, for example:

```properties
sleeper.optional.stacks=CompactionStack,IngestStack,QueryStack
```

Note that the system test stacks do not need to be specified. They will be included if you use the system test CDK app.
