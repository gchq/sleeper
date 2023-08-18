Performance tests
=================

The following notes describe how to run the system tests that measure performance, to help understand whether changes
to the code have increased or decreased the performance. These tests are based on the system tests described in
[09-dev-guide#System-tests](09-dev-guide.md#System-tests).

Under `scripts/test` we have system tests for testing performance of compaction and bulk import.
The performance tests take control of when compactions run in order to produce more deterministic results.

## Compaction performance

This will test the performance of standard ingest, standard compactions and compaction job creation. It will not
perform any partition splitting. This will take around an hour. This is intended to avoid any variance that may be
caused by the number of input files or the amount of data processed at once.

There are a variety of scenarios that can occur when compaction is performed on scheduled jobs (as in the deployAll
system test, or normal system functioning), such as compactions happening halfway through ingest. The compaction
performance test avoids this by disabling the scheduled jobs and triggering those processes directly.

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

## Bulk import performance

This will test the performance of bulk import using EMR. A bulk import job for 1 billion records will be created and
sent to the bulk import queue 5 times. It will then wait for all EMR steps to finish, then check that there are
5 billion records in the table. This will take about 30 minutes.

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
| 0.18.0         | 09/08/2023 | 326000                      | 147000                           |
