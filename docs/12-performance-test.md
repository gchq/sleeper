Performance tests
=================

The following notes describe how to conduct some manual tests of performance to help understand whether changes to the
code have increased or decreased the performance. These tests are based on the system tests described in
[09-dev-guide#System-tests](09-dev-guide.md#System-tests). Note that currently the following needs to be run on
an x86 machine.

Run the tests:
```bash
ID=<a-unique-id-for-the-test>
VPC=<id-of-the-VPC-to-deploy-to>
SUBNET=<id-of-the-subnet-to-deploy-to>
 ./scripts/test/buildDeployTest.sh ${ID} ${VPC} ${SUBNET}
```

Find the ECS cluster that is running the containers that are writing data for ingest. It will have the name
`sleeper-${ID}-system-test-cluster`. There should be 11 running tasks. Wait until all those tasks have finished.
Click on one of the tasks and find the corresponding Cloudwatch log group. Use the following command in
Cloudwatch Log Insights with the above log group selected:
```
fields @message 
| filter @message like "to S3"
# If you want to see all the results, comment out the next two lines
| parse @message '* * - * at * per second' as class, level, msg, rate
| stats avg(rate)
```
This should result in a single value which summarises the performance of the containers that are ingesting data
(these are direct standard ingest tasks, i.e. they are writing data directly to Sleeper using the standard ingest
approach - they are not using the ingest queue). The table below records the performance for various versions of
Sleeper.

Now find the ECS cluster that runs compaction tasks. It will be named `sleeper-${ID}-merge-compaction-cluster`.
Click on one of the tasks and find the corresponding Cloudwatch log group. Use the following command in Cloudwatch
Log Insights with the above log group selected:
```
fields @message | filter @message like "compaction read"
# If you want to see all the results, comment out the next two lines
| parse @message '* * - * at * per second' as class, level, msg, rate
| stats avg(rate)
```
This should result a single value summarising the performance of the containers that are performing a compaction. See
the table below for the results for various versions of Sleeper.

## Benchmarks
These figures are the average values from the system tests described above. Note that these averages are only based on
a small number of tests and are therefore not definitive results. There is also significant variability in AWS which
means that it is hard to produce reproducible figures. In future work we hope to improve the tests so that they produce
more accurate results. Nevertheless, these tests have caught several significant performance regressions that would
otherwise not have been noticed.

| Version number | Test date  | Compaction rate (records/s) | Ingest S3 write rate (records/s)
|----------------|------------|-----------------------------|---------------------------------
| 0.11.0         | 13/06/2022 | 366000                      | 160000
| 0.12.0         | 18/10/2022 | 378000                      | 146600
| 0.13.0         | 06/01/2023 | 326000                      | 144000
