

- Check InMemoryFileInfoStoreTest tests everything DONEm tests
- Check DynamoDBStateStore
    - Need to change cdk code so that file-in-partition table has sort key of partition DONE
- Make sure DynamoDBStateStore tests pass DONE
- Commit DONE
- Check FileInfoStore interface DONE
- Check DynamoDBStateStoreFileInfo DONE
- Ensure DynamoDBStateStore tests cover everything DONE
    - TODO getActiveFileList DONE
- Add test of getActiveFileList to InMemoryFileInfoStore
- Add field to FileInfo saying whether number of records is exact or approximate
- Add method to atomically replace file-in-partition record with 2 child records
- Fix implementation of S3StateStore
- Comment out S3StateStore tests
- Check -pl statestore -am build
- Fix checkstyle issues
- Update compaction module
    - Check how jobs are created
    - Ensure compactions now only read data for the correct partition
- Update partition splitting module
    - Needs to deal with fact that file in partition record does not tell you how many records are in that partition
        - Only need to do this later as currently if file-in-partition record exists then it's accurate
- FileStatusCollector needs updating - see TODO and note we now don't know exactly how many records are in the table
- Update garbage collector
    - Need to identify active files in lifecyle table for which there are no partition records
    - List files in existence first then in partition
    - Set to ready for GC.
    - Then look for files that were marked as ready for GC more than N minutes ago.
- Test
- Checkstyle test

FileInfoStore - review methods - need:
    - AddFileInfos
    - GetAllFileInPartitionInfos
    - SetFileLifecycleStatusToReadyForGarbageCollection
    - GetFileLifecycleList
    - GetReadyForFCFiles
    - GetFileInPartitionInfosWithNoJobId
    - GetMapPartitionIdToFilenamesInPartition
    - AtomicallyRemoveFileInPartitionRecordsAndCreateNewActiveFile
    - AtomicallyRemoveFileInPartitionRecordsAndCreateNewActiveFiles
    - AtomicallyUpdateJobStatusOfFileInPartitionInfos
    - DeleteFileLifecycleRecord

Should StateStore own identifying files for GC?

DynamoDBStateStore:
    - Ensure there are tests for setStatusToReadyForGarbageCollection(List<FileInfo> fileInfos)
    - Ensure test for getFileInPartitionList is still valid
    - Need test for getFileLifecycleList

S3StateStore:
    - Ensure there are tests for setStatusToReadyForGarbageCollection(List<FileInfo> fileInfos)
    - Ensure test for getFileInPartitionList is still valid
    - Need test for getFileLifecycleList
    - Code duplication in getFileInPartitionList and getFileLifecycleList
