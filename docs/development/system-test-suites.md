# Current Slow and Expensive test suites
### This looks imbalanced but EKSBulkImportST is a lot slower than others
| Slow1                      | Slow2                      | Slow3                           |
| -------------------------- | -------------------------- | ------------------------------- |
| AutoStopEcsTaskST          | EksBulkImportST            | CompactionCreationST            |
| AutoDeleteS3ObjectsST      | RedeployOptionalStacksST   | MultipleTablesST                |
| CompactionOnEC2ST          |                            | StateStoreCommitterThroughputST |
| EmrPersistentBulkImportST  |
| OptionalFeaturesDisabledST |


| Expensive1                        | Expensive2                 | Expensive3            |
| --------------------------------- | -------------------------- | --------------------- |
| CompactionDataFusionPerformanceST | CompactionPerformanceST    | IngestPerformanceST   |
| CompactionVeryLargeST             | EmrBulkImportPerformanceST | ParallelCompactionsST |