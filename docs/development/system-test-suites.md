# Current Slow and Expensive test suites
### This looks imbalanced but EksBulkImportST is a lot slower than the others.

These tables show which system tests run in which suite. Each suite runs in parallel to the others to speed up the time
it takes to complete the nightly system tests.

| Slow1                      | Slow2                              | Slow3                           |
|----------------------------|------------------------------------|---------------------------------|
| AutoDeleteS3ObjectsST      | CompactionOnEC2ST                  | EksAutoBulkImportST             |
| AutoStopEcsTaskST          | ECSStateStoreCommitterST           | MultipleTablesST                |
| CompactionCreationST       | ECSStateStoreCommitterThroughputST | StateStoreCommitterThroughputST |
| EmrPersistentBulkImportST  | EksFargateBulkImportST             |
| OptionalFeaturesDisabledST |
| RedeployOptionalStacksST   |

| Expensive1                        | Expensive2                 | Expensive3              |
|-----------------------------------|----------------------------|-------------------------|
| CompactionDataFusionPerformanceST | EksBulkImportPerformanceST | CompactionPerformanceST |
| CompactionVeryLargeST             | EmrBulkImportPerformanceST | ParallelCompactionsST   |
|                                   | IngestPerformanceST        |
