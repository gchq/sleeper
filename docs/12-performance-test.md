Performance tests
=================

The following notes describe how to conduct some manual tests of performance to help understand whether changes to the
code have increased or decreased the performance. These tests are based on the system tests described in
[09-dev-guide#System-tests](09-dev-guide.md#System-tests).

Under `scripts/test` we have system tests for deploying everything, testing performance of compaction and bulk import, 
and testing functionality of partition splitting and the ingest batcher.
The performance and functional tests take control of when compactions run in order to produce more deterministic results.

## Deploy all

This will test the functionality of the system as a whole. Most of the stacks will be deployed, in addition to a 
system test stack, which will generate some random data and write it directly to Sleeper, using the 
standard ingest approach.

Run the tests:

```bash
ID=<a-unique-id-for-the-test>
VPC=<id-of-the-VPC-to-deploy-to>
SUBNETS=<ids-of-the-subnets-to-deploy-to>
./scripts/test/deployAll/buildDeployTest.sh ${ID} ${VPC} ${SUBNETS}
```

## Compaction performance

This will test the performance of standard ingest, standard compactions and compaction job creation. It will not 
perform any partition splitting. This will take around an hour. This is intended to avoid any variance that may be 
caused by the number of input files or the amount of data processed at once.

There are a variety of scenarios that can occur when compaction is performed on scheduled jobs (as in the deploy all 
system test, or normal system functioning). The compaction performance test avoids this by disabling the scheduled jobs 
and triggering those processes directly.

Run the tests:

```bash
ID=<a-unique-id-for-the-test>
VPC=<id-of-the-VPC-to-deploy-to>
SUBNETS=<ids-of-the-subnets-to-deploy-to>
./scripts/test/compactionPerformance/buildDeployTest.sh ${ID} ${VPC} ${SUBNETS}
```

Report the results:

```bash
# Ingest performance figures can be found by running the following reports
./scripts/utility/ingestTaskStatusReport.sh ${ID} standard -a
./scripts/utility/ingestJobStatusReport.sh ${ID} system-test standard -a

# Compaction performance figures can be found by running the following reports
./scripts/utility/compactionTaskStatusReport.sh ${ID} standard -a
./scripts/utility/compactionJobStatusReport.sh ${ID} system-test standard -a
```

## Partition splitting
This will test the functionality of splitting compactions. 100 records are ingested, and the partition splitting 
threshold is set to 20. The standard compaction lambda is disabled, then the partition splitting lambda is invoked 
until no more splits are required.

Run the tests:

```bash
ID=<a-unique-id-for-the-test>
VPC=<id-of-the-VPC-to-deploy-to>
SUBNETS=<ids-of-the-subnets-to-deploy-to>
./scripts/test/partitionSplitting/buildDeployTest.sh ${ID} ${VPC} ${SUBNETS}
```

Report the results:

```bash
./scripts/utility/compactionTaskStatusReport.sh ${ID} standard -a
./scripts/utility/compactionJobStatusReport.sh ${ID} system-test standard -a
```

## Bulk import performance
This will test the performance of bulk import using EMR. A bulk import job for 1 billion records will be created and 
sent to the bulk import queue 5 times. It will then wait for all EMR steps to finish, then check that there are 
5 billion records in the table.

The system test table is pre-split into 512 leaf partitions using the `scripts/test/splitpoints/512-partitions.txt` 
splitpoints file.

The standard ingest, compaction, and partition splitting stacks are not enabled, so the test finishes when all 
records are ingested through bulk import.

Run the tests
```bash
ID=<a-unique-id-for-the-test>
VPC=<id-of-the-VPC-to-deploy-to>
SUBNETS=<ids-of-the-subnets-to-deploy-to>
./scripts/test/bulkImportPerformance/buildDeployTest.sh ${ID} ${VPC} ${SUBNETS}
```

Report the results:
```bash
./scripts/utility/ingestTaskStatusReport.sh ${ID} standard -a
./scripts/utility/ingestJobStatusReport.sh ${ID} system-test standard -a
```
## Ingest batcher
This will test the functionality of the ingest batcher. As part of this test, an ingest source bucket will be created.

The ingest mode for the batcher is set to standard ingest, and the max files in a batch is set to 3. 
4 test files are sent to the batcher, which should create 2 ingest jobs and send them to the standard ingest queue. One 
with 3 files and one with the remaining 1 file. An assertion is then done to check that 2 files have been written to the 
table.

The ingest mode for the batcher is then switched to use bulk import EMR, and the max files in a batch is increased to 10.
4 test files are sent to the batcher again, which should create 1 bulk import job and send it to the bulk import EMR queue.
An assertion is then done to check that 1 file has been written to the table.

Run the tests
```bash
ID=<a-unique-id-for-the-test>
VPC=<id-of-the-VPC-to-deploy-to>
SUBNETS=<ids-of-the-subnets-to-deploy-to>
./scripts/test/ingestBatcher/buildDeployTest.sh ${ID} ${VPC} ${SUBNETS}
```

Report the results:
```bash
./scripts/utility/ingestTaskStatusReport.sh ${ID} standard -a
./scripts/utility/ingestJobStatusReport.sh ${ID} system-test standard -a
```

## Benchmarks

These figures are the average values from the system tests described above. Note that these averages are only based on
a small number of tests and are therefore not definitive results. There is also significant variability in AWS which
means that it is hard to produce reproducible figures. In future work we hope to improve the tests so that they produce
more accurate results. Nevertheless, these tests have caught several significant performance regressions that would
otherwise not have been noticed.

| Version number | Test date  | Compaction rate (records/s) | Ingest S3 write rate (records/s) |
|----------------|------------|-----------------------------|----------------------------------|
| 0.11.0         | 13/06/2022 | 366000                      | 160000                           |
| 0.12.0         | 18/10/2022 | 378000                      | 146600                           |
| 0.13.0         | 06/01/2023 | 326000                      | 144000                           |
| 0.14.0         | 20/01/2023 | 349000                      | 153000                           |
| 0.15.0         | 30/03/2023 | 336000                      | 136000                           |
| 0.16.0         | 28/04/2023 | 325000                      | 137000                           |
| 0.17.0         | 09/06/2023 | 308000                      | 163000                           |
