# Current Slow and Expensive test suites
### This looks imbalanced but EKSBulkImportST is a lot slower than others

These tables show which system tests run in which suite. Each suite runs in parallel to the others to speed up the time
it takes to complete the nightly system tests.

| Slow1                      | Slow2                              | Slow3                           |
| -------------------------- | -----------------------------------| ------------------------------- |
| AutoStopEcsTaskST          | EksBulkImportST                    | CompactionCreationST            |
| AutoDeleteS3ObjectsST      | CompactionOnEC2ST                  | MultipleTablesST                |
| RedeployOptionalStacksST   | ECSStateStoreCommitterThroughputST | StateStoreCommitterThroughputST |
| EmrPersistentBulkImportST  |
| OptionalFeaturesDisabledST |


| Expensive1                        | Expensive2                 | Expensive3            |
| --------------------------------- | -------------------------- | --------------------- |
| CompactionDataFusionPerformanceST | CompactionPerformanceST    | IngestPerformanceST   |
| CompactionVeryLargeST             | EmrBulkImportPerformanceST | ParallelCompactionsST |