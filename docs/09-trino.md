Sleeper Plugin for Trino
========================

### About this plugin

This plugin provides a way to use SQL to query data that is stored in Sleeper. It extends the capabilities
of [Trino](https://trino.io/), which is a fully-featured distributed SQL engine. Trino provides a JDBC client and Trino
allows queries against Sleeper to be performed just like queries against any other database. It is particularly
effective when the queries retrieve just a few rows from the underlying Sleeper tables, as the results are returned
quickly enough to support interactive work.

This plugin was originally written as an experimental investigation. It now appears to be robust code.

### Features

This plugin supports the following SQL features:

- Sleeper tables and their columns are exposed as SQL tables in Trino
- Single-value filters, such as SELECT * FROM sleepertable WHERE rowkey = 'key1'
- Multiple single-value filters, such as SELECT * FROM sleepertable WHERE rowkey IN ('key1', 'key10', 'key100')
- Range queries, such as SELECT * FROM sleepertable WHERE rowkey BETWEEN 'key1' AND 'key10'
- Pattern-based queries with a prefix, such as SELECT * FROM sleepertable WHERE rowkey LIKE 'key1%'
- Joins between tables, where the join key extracted from the first table is the same as the row key on the second
  table, such as SELECT * FROM table1 INNER JOIN table2 USING (table1.rowkey = table2.extractedkey) WHERE table2.rowkey
  = 'key1'
- Insert statements, such as INSERT INTO table1 VALUES ('Fred', 27, 'M') for small numbers of rows
- Insert statements involving many millions of rows, such as INSERT INTO table1 SELECT * FROM remotesource
- Example procedures to control Sleeper from within SQL
- Example system tables to provide up-to-date information about Sleeper, such as lists of the current partitions

### Pushdown and partitioning

Filters which appear in the SQL expression are pushed down to Sleeper, so that the minimum number of rows are retrieved
from the Sleeper data store. Unfiltered full-table-scans, where there is no rowkey filter, are not permitted in order to
protect the system and they cause an error.

All other SQL features, such as casting, arithmetic, aggregation and window functions, are supported within the Trino
execution engine and are not pushed down into the Sleeper connector.

Query scans are distributed and may be executed on multiple servers if this is required. Each Sleeper partition is
assigned to its own Trino split, and so whenever a query needs to scan more than one Sleeper partition, each split will
be executed in parallel, wherever possible, by the Trino execution engine.

This plugin supports dynamic filters, which are used to enable efficient joins between tables. The use of dynamic
filters is affected by the order in which a SQL JOIN expression is written, and this is discussed in more detail below.

Insert statements are partitioned so that the Trino partitions match Sleeper partitions. This ensures that the data for
any one Sleeper partition is written by a single Trino writer, and this means that the minimum number of files are
generated within S3. Different Sleeper partitions may be written to by different Trino writers and so writing can take
place in parallel across multiple servers.

## Deployment

This plugin has been built and tested against Trino 390. This version of Trino requires Java 17, so ensure that you have
Java 17 installed. Amazon Corretto seems to work perfectly well.

### Run Trino server

The standard Trino installation instructions are available
here: https://trino.io/docs/current/installation/deployment.html

Trino requires some configuration. The standard Trino instructions provide a great deal of detail about how to do this,
and the instructions below should be sufficient to get you started.

Scripts are available under [scripts/trino/](../scripts/cli/trino) to build the plugin and run Trino in Docker.
These use the Trino Docker image as documented here: https://trino.io/docs/current/installation/containers.html

Follow the steps below to run the Trino server.

1. In a Sleeper build/development environment, open a terminal in the [scripts/trino/](../scripts/cli/trino) directory.
2. Ensure that AWS credentials are available and valid to work with Sleeper.
3. Run `./buildMaven.sh`. This will build the plugin and copy an example Trino configuration into the current directory.
4. Edit `./etc/catalog/sleeper.properties` to set the config bucket to point to your Sleeper instance.
5. Run `./runDockerServer.sh`. This will expose Trino on port 8080, but you can edit this in the script.

This will start the Trino server with three plugins available: _Sleeper_, _TPCH_ (to provide sample data) and
_memory_ (to demonstrate joins between different data sources).

You can check the logs for the server with `docker logs trino`. If everything has run correctly, this will generate
a few pages of logs, followed by 'SERVER STARTED'. Somewhere in the final few log rows will be information about the
Sleeper plugin configuration and how many Sleeper tables it has found. This should reassure you that it has connected
to Sleeper and is ready for use.

If this fails, look in the error messages for the following:

- AmazonS3Exception: Access Denied - your AWS credentials may be invalid. Check by running _aws s3 ls_.
- AmazonS3Exception: The specified bucket does not exist - the config bucket named in etc/catalog/sleeper.properties is
  incorrect.

### Install a SQL client and connect to the Trino server

Trino uses a standard JDBC connection and there are several suitable sophisticated SQL clients available for free.
Consider installing DBeaver or Squirrel SQL. Trino also provides its own basic command-line client.

Install whichever client you prefer and set up a Trino JDBC database connection to:

```
jdbc:trino://localhost:8080
```

The default Trino user name is 'user' and the default password is blank.

Trino provides a web interface at http://localhost:8080 to monitor queries and system performance. The login credentials
for this monitoring site are the same user name and password as for the JDBC connection above.

All of the tables in the Sleeper instance should appear in a Trino schema called _sleeper.default_:

```sql
SHOW CATALOGS;

SHOW SCHEMAS FROM SLEEPER;

SHOW TABLES FROM SLEEPER.DEFAULT;
```

### Maven build

All of the Sleeper-Trino plugin code is contained in the trino subdirectory. It depends on other parts of the Sleeper
codebase and it will be built at the same time as the rest of Sleeper.

The Maven build will generate a shaded jar called `sleeper/java/trino/target/trino-XXXXXX-utility.jar`.

Note that the trino module requires Java 17 whereas other parts of Sleeper require Java 8. The Maven POM explicitly
states that the trino module is different. Maven handles this need for multiple JDKs successfully.

### Important JVM flags

There is a conflict between Apache Arrow 8.0.0 and Java 17, which causes errors such as "module java.base does not "opens java.nio" to unnamed module."
To avoid these errors, add the following flag to the JVM command line:

```
--add-opens=java.base/java.nio=ALL-UNNAMED
```

This flag has already been added to the example `etc/jvm.config` file.

## Setting up example Sleeper tables

The following examples make use of
the [standard TPCH benchmark data set](https://docs.deistercloud.com/content/Databases.30/TPCH%20Benchmark.90/Data%20generation%20tool.30.xml?embedded=true)
The three tables _customer_, _orders_ and _lineitem_ are taken directly from this data set, and conveniently Trino can
generate data which complies with the schema.

### Creating tables

There is currently no way to create tables in Sleeper using SQL, as the plugin does not support CREATE or DROP. The
tables must be created in the same way as any other Sleeper table.

Here are the relevant schemas:
```properties
sleeper.table.name=customer
sleeper.table.schema={"rowKeyFields"\:[{"name"\:"name","type"\:"StringType"}],"sortKeyFields"\:[],"valueFields"\:[{"name"\:"custkey","type"\:"LongType"},{"name"\:"address","type"\:"StringType"},{"name"\:"nationkey","type"\:"LongType"},{"name"\:"phone","type"\:"StringType"},{"name"\:"acctbal","type"\:"StringType"},{"name"\:"mktsegment","type"\:"StringType"},{"name"\:"comment","type"\:"StringType"}]}
```
```properties
sleeper.table.name=orders
sleeper.table.schema={"rowKeyFields"\:[{"name"\:"custkey","type"\:"LongType"}],"sortKeyFields"\:[],"valueFields"\:[{"name"\:"orderkey","type"\:"LongType"},{"name"\:"orderstatus","type"\:"StringType"},{"name"\:"totalprice","type"\:"StringType"},{"name"\:"orderdate","type"\:"StringType"},{"name"\:"orderpriority","type"\:"StringType"},{"name"\:"clerk","type"\:"StringType"},{"name"\:"shippriority","type"\:"IntType"},{"name"\:"comment","type"\:"StringType"}]}
```
```properties
sleeper.table.name=lineitem
sleeper.table.schema={"rowKeyFields"\:[{"name"\:"orderkey","type"\:"LongType"}],"sortKeyFields"\:[],"valueFields"\:[{"name"\:"partkey","type"\:"LongType"},{"name"\:"suppkey","type"\:"LongType"},{"name"\:"linenumber","type"\:"IntType"},{"name"\:"quantity","type"\:"IntType"},{"name"\:"extendedprice","type"\:"StringType"},{"name"\:"discount","type"\:"StringType"},{"name"\:"tax","type"\:"StringType"},{"name"\:"returnflag","type"\:"StringType"},{"name"\:"linestatus","type"\:"StringType"},{"name"\:"shipdate","type"\:"StringType"},{"name"\:"commitdate","type"\:"StringType"},{"name"\:"receiptdate","type"\:"StringType"},{"name"\:"shipinstruct","type"\:"StringType"},{"name"\:"shipmode","type"\:"StringType"},{"name"\:"comment","type"\:"StringType"}]}
```
The connector will not detect new tables as they are created and you will need to restart Trino to make them appear.

### Inserting data

The standard Trino TPCH connector provides excellent sample data of different sizes. To insert a few hundred thousand
rows, run:

```sql
INSERT INTO sleeper.default.customer SELECT * FROM tpch.sf1.customer
INSERT INTO sleeper.default.orders SELECT * FROM tpch.sf1.orders
INSERT INTO sleeper.default.lineitem SELECT * FROM tpch.sf1.lineitem
```
You may wish to look inside your Sleeper S3 buckets to confirm that the data has been uploaded correctly. Alternatively,
at this modest scale of data, it is safe to run the following:
```sql
SELECT * FROM sleeper.default.customer WHERE name LIKE 'C%' LIMIT 100
```

## Queries

### Simple queries

The columns in a Sleeper table are mapped to SQL columns and the _comment_ field indicates whether the field is a
rowkey, sortkey or value field:
```sql
SHOW COLUMNS FROM sleeper.default.customer
```

Returns:
```
name	    varchar		ROWKEY
custkey	    bigint		VALUE
address	    varchar		VALUE
nationkey   bigint		VALUE
phone	    varchar		VALUE
acctbal	    varchar		VALUE
mktsegment  varchar		VALUE
comment     varchar		VALUE
```
In this table, the rows are keyed by the _name_ field and any query that is run must include a filter on that column.
The following queries are all valid:
```sql
SELECT * FROM sleeper.default.customer WHERE name = 'Customer#000000001'
SELECT * FROM sleeper.default.customer WHERE name IN ('Customer#000000001', 'Customer#000000008')
SELECT * FROM sleeper.default.customer WHERE name BETWEEN 'Customer#000000001' AND 'Customer#000000900'
SELECT * FROM sleeper.default.customer WHERE name LIKE 'Customer#0000001%' AND mktsegment = 'MACHINERY'
SELECT mktsegment, COUNT(*) AS numcustomers FROM sleeper.default.customer WHERE name BETWEEN 'Customer#000000001' AND 'Customer#000000900' GROUP BY mktsegment
```

The following queries will generate an error:
```sql
SELECT * FROM sleeper.default.customer
SELECT * FROM sleeper.default.customer WHERE mktsegment = 'MACHINERY'
```
### Table joins

This plugin uses dynamic filters to enable efficient joins between tables.

In order to explain what dynamic filters do, and why they are essential to implementing joins between two Sleeper
tables, consider how the tables _customer_ (keyed by _name_) and _orders_ (keyed by _custkey_) would be joined in the
following query:
```sql
SELECT * FROM orders INNER JOIN customer USING (custkey) WHERE name = 'Customer#000000001'
```
Without a dynamic filter, the join will be executed as follows:

- The customer table will have the static filter (name = 'Customer#000000001') applied to it and the rows will be
  streamed to a hash join, where the hash is calculated from the custkey field.
- The orders table will be streamed in its entirety to a hash join, where the hash is calculated from the custkey field.

This query plan would be rejected as it has to fully scan the orders table, without any filter, which is prohibited in
this plugin so that enormous quantities of data are not scanned by mistake.

With a dynamic filter, the join will be executed as follows:

- The customer table will have the static filter (name = 'Customer#000000001') applied to it. The query will run until
  it has completed, at which point all of the custkey values will be collected together.
- The orders table will have the dynamic filter (custkey IN (_custkeys returned from previous stage_)) applied to it.
- The results from the previous two stages are joined together using a hash join.

This query plan is permitted because both the customer and order tables are being filtered by their rowkeys. It will
execute quickly, so long as the filters on the first table return just a few different keys to look up in the second
table and the IN clause does not grow too large.

Unfortunately the order of query execution is affected by the order that the JOIN clause is expressed in the SQL query.
The following SQL expresses exactly the same join as above, but the execution plan is different:
```sql
SELECT * FROM customer INNER JOIN orders USING (custkey) WHERE name = 'Customer#0000000001'
```
The execution plan tries to execute the query as follows:

- Scan the entire orders table with no filter.
- Apply both the static and dynamic filters to the customer table.

This query plan is rejected because no filter has been applied to the orders table.

Trino allows you to see the query plan that it will execute:
```sql
EXPLAIN SELECT * FROM orders INNER JOIN customer USING (custkey) WHERE name = 'Customer#0000000001'
```
In order to construct a query which will execute correctly:

- When tables are joined together, the last table in the SQL query is usually scanned first. In the expression `table1
  INNER JOIN table2`, table2 will be scanned first, so make sure that the rowkey for table1 is the join key extracted
  from table2.
- Look at the query plan by running `EXPLAIN SELECT...` In the query plan, look for the ScanProject and ScanFilterProject
  rows, which contain _tupledomain_ and _dynamicfilter_ fields. These tell you what static and dynamic filters are being
  applied. The table at the end of the query plan is usually the one that is scanned first.

### Table joins across different data sources

Trino allows queries to run where the data comes from different data sources. This can be very useful for activities
such as enrichment.

First of all, we put the enrichment data into memory:
```sql
CREATE TABLE memory.default.nation SELECT * FROM tpch.sf1.nation
```
The join works as usual:
```sql
SELECT * FROM memory.default.nation INNER JOIN sleeper.default.customer USING (nationkey) WHERE name = 'Customer#0000000001'
```
The enriched results are now available.

## System control features

This plugin contains a few features which allow simple control and feedback on the Sleeper system. These features do not
provide very much value at the moment, but they do demonstrate a style of system interaction which could be expanded if
desired.

### System tables

The following query will return details about all of the partitions in the customer table:
```sql
SELECT * FROM sleeper.system.partitions WHERE schemaname = 'default' AND tablename = 'customer'
```
### System procedures

The following procedure call will display a 'hello world' message in the Trino logs:
```sql
CALL sleeper.runtime.log_hello('Fred');
```
## Advanced techniques

The techniques in this section have not been tried at significant scale.

### Enrich-on-ingest

When data is stored, it is sometimes useful to store any enrichment data in the same row as the original data. This can
make the enriched data faster to retrive and filter, as the enrichment does not need to be applied when the data is
retrieved.

This can be achieved using a join:
```sql
INSERT INTO exampletable SELECT * FROM enrichment INNER JOIN sourcedata USING (key)
```
### Secondary indexing

Sleeper queries are only fast when the rows can be fitered by the row key. It is often desirable to be able to query the
data by a different column and secondary indexing is required.

This can be achieved by storing the index in a second table:
```sql
INSERT INTO basetable SELECT key, col1, col2, col3, col4, col5 FROM sourcedata
INSERT INTO indextable SELECT col1, key FROM sourcedata
```

The secondary index table can now be joined the base table as follows:
```sql
SELECT * FROM basetable INNER JOIN indextable USING (key) WHERE col1 = 'secondary_value_to_look_up'
```
This will retrieve rows which are both in the base table and have been indexed. If something goes wrong during the
insert operation, or if Sleeper has aged-off some data, it is possible to be left with base rows that have no
corresponding index row, or vice versa. The above query will ignore these rows.

## Miscellaneous

### Testing

The directory _sleeper/trino/test/java/sleeper/trino_ contains several test classes. These provide good examples of the
way in which SQL expressions can be run and tested against Trino.

The _testutils_ directory contains a standalone Trino server called _SleeperQueryRunner_. This can be run on its own
without additional configuration.

The _etc/jvm.config_ file contains the following line:
```
-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5055
```

This allows a debugger to connect to a running Trino instance on port 5055. Test classes may also be run and debugged in
the usual manner for your IDE.

### Limitation: single-valued row keys only

The current implementation only support Sleeper tables which have a row key based on a single column. This was partly
for pragmatic reasons, in order to simplify the code during initial exploratory development work, but there is a more
fundamental issue too.

Trino passes filter criteria around as a TupleDomain, essentially a Map from a column to a domain, which is a list of
values (or ranges) that the column can contain. This works well in the one-dimensional case as the permitted values can
be mapped directly to Sleeper key ranges for a query.

When the TupleDomain contains two or more columns, the domains are ANDed together. In a filter expression such as...
```sql
SELECT * FROM two_dimensions WHERE (x=1 AND y=2) OR (x=3 AND y=4)
```
...the domains will be the equivalent of...
```sql
x IN (1,3) AND y IN (2,4)
```
...which could be any of the values:
```sql
(1,2), (1,4), (2,2), (2,4)
```
This will result in Sleeper scanning more rows than is necessary. The problem becomes more severe as more and more OR
clauses are added to the original query, which result in a very large number of combinations of the two domains to scan
in Sleeper. In a large join with a multi-dimensional row key, this could become very severe.

There may be ways around this, such as features inside Trino which we have not yet explored, or we could filter on just
the first dimension (which will work quite well so long as the first dimension has high cardinality), or we could try to
merge together nearby keys to turn many short scans into fewer, longer scans, or we could implement it anyway to support
precise queries that only have one OR clause (and throw an error if there is more than one OR clause with a
multidimensional key).

### Index-based lookups

Trino supports index-based joins between tables. When an index-based join in Trino executes, a page of data from the
probe side of the join is filtered, and then any join keys that are found are passed to an index to return the rows from
the build-side table.

At first sight, this seems like a good fit for Sleeper, as the row keys in Sleeper are already large sorted index.
Experiments suggested that indexes were used wherever they were available, and this usually resulted in good query
plans.

The down-side to index-based lookups is that the lookups occur sequentially and any join with more than a handful of
lookups becomes very slow indeed. It may be possible to allow the probe-side query to run to completion, collect
together all of the join keys and look them up all at once. This would provide direct access to the join keys and may
allow us to work around the single-valued row key limitation described above, but it may not be straightforward.

Interestingly, index-based lookups are not described in the Trino documentation and they are not implemented by many
plugins.

### Statistics and ordering of joins

In a join, in the current implementation, the user has to take care to specify the order of tables correctly so that
there is always a filter applied to the Sleeper row key.

Trino allows plugins to return limited statisics about their tables. It uses these statistics to make an intelligent
choice about which order the tables should be joined in.

At the moment, the optimiser in Trino does not appear to take account of where dynamic filtering is available. All of
the joins in the Sleeper-Trino plugin rely on dynamic filters to make them efficient and so the optimiser often chooses
a bad plan.

Consequently, there is no advantage in collecting and returning accurate statistics from the plugin and so they are not
returned.

### Transactions

Database transactions would help to avoid situations such as an index table containing data, but a base table not
holding the correponding record, due to an error occurring during ingest. Trino supports transactions, but Sleeper does
not.

At the moment, during an insert operation, the Trino plugin creates a number of partition files in S3 and then updates
the Sleeper state store. The files are not available to query until the state store has been updated, and this update is
atomic when there are only a few new partition files.

If two or more tables are updated, then they will be updated separately and they could be left in an inconsistent state
if an error occurred between the two updates. This would be fixed if transactions were available in Sleeper.

Transactions could be implemented in Sleeper if the state store could support long-lasting transactions. This is a
soluble problem, but it will be an involved problem to solve reliably and it would need to be cover all of the actions
that Sleeper can do, such as compactions.

### Lambda assistance during queries

At the moment, the Sleeper-Trino plugin uses Sleeper row keys to filter the rows, and once those rows are returned, all
of the rest of the work (such as Sleeper iterators, filtering on non-row-key columns and aggregation) is handled within
Trino. Trino does scale, but increasing or decreasing a cluster does not happen instantly.

Sleeper provides the ability to use AWS lambda functions to run queries and apply Sleeper iterators. This would allow
Trino to take advantage of cloud resources to support queries, and those resources would disappear when the query is
complete.

Trino also provides the ability to push-down aggregations into the plugin. It may also be possible to pass some of the
aggregations into a Sleeper iterator, and thereby take advantage of the extra processing power that the cloud provides.

This would be experimental work to see what advantages they provide, if any.

### Rolling-summary tables

It is expensive and time-consuming to calculate summaries of large amounts of data. Rolling summaries can improve this
situation by summarising just the newest data and pre-computing those summaries so that they are available rapidly.

A Sleeper iterator could be written to incrementally summarise the data within a table, during compactions, in a rolling
manner. This _summary table_ could appear just like an ordinary table in Sleeper, and support attractive features such
as the ability to join raw data tables and summary data tables together.

This would be experimental work.

### Update and delete

Trino supports updates and deletes. Sleeper does not support them, but it could be made to support them in the following
way:

- Add a boolean column indicating whether the record has been deleted or not
- Create an iterator which returns just the most recent record for any row key, or no record if the most recent record
  has been deleted
- Updates and deletes add newer records which will eclipse the older records
- Over time, the older records will disappear through compaction iterators

The SQL commands UPDATE and DELETE could be implemented to support these operations.

### Other

- Trino supports TRUNCATE TABLE. This could be implemented by moving all the active files in a partition into an expired
  state, when they will be garbage-collected.
- The EXPLAIN ANALYSE command provides various statistics to the user, such as the number of bytes read and the time
  elapsed. These figures are useful to a user to help them to optimise the queries. The plugin does not currently make
  these values available to the Trino framework. 