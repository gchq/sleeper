/*
 * Copyright 2022 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package sleeper.compaction.strategy.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.creation.CreateCompactionJob;
import sleeper.compaction.strategy.CompactionStrategy;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.Partition;
import sleeper.core.schema.Schema;
import sleeper.statestore.FileInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Collectors;

import static sleeper.configuration.properties.UserDefinedInstanceProperty.FILE_SYSTEM;
import static sleeper.configuration.properties.table.TableProperty.COMPACTION_FILES_BATCH_SIZE;
import static sleeper.configuration.properties.table.TableProperty.DATA_BUCKET;
import static sleeper.configuration.properties.table.TableProperty.ITERATOR_CLASS_NAME;
import static sleeper.configuration.properties.table.TableProperty.ITERATOR_CONFIG;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public abstract class AbstractCompactionStrategy implements CompactionStrategy {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractCompactionStrategy.class);

    protected Schema schema;
    protected String tableName;
    protected String tableBucket;
    protected String fs;
    protected int compactionFilesBatchSize;
    protected String iteratorClassName;
    protected String iteratorConfig;

    @Override
    public void init(InstanceProperties instanceProperties, TableProperties tableProperties) {
        schema = tableProperties.getSchema();
        tableName = tableProperties.get(TABLE_NAME);
        tableBucket = tableProperties.get(DATA_BUCKET);
        fs = instanceProperties.get(FILE_SYSTEM);
        compactionFilesBatchSize = tableProperties.getInt(COMPACTION_FILES_BATCH_SIZE);
        iteratorClassName = tableProperties.get(ITERATOR_CLASS_NAME);
        iteratorConfig = tableProperties.get(ITERATOR_CONFIG);
        LOGGER.info("Initialised AbstractCompactionStrategy with table name {}, bucket for table data {}, fs {}",
                tableName, tableBucket, fs);
    }

    abstract protected List<CompactionJob> createJobsForLeafPartition(Partition partition, List<FileInfo> fileInfos);

    protected List<CompactionJob> createJobsForNonLeafPartition(Partition partition,
                                                                List<FileInfo> fileInfos,
                                                                Map<String, Partition> partitionIdToPartition) {
        List<CompactionJob> compactionJobs = new ArrayList<>();
        List<FileInfo> filesInAscendingOrder = getFilesInAscendingOrder(partition, fileInfos);

        // Iterate through files, creating jobs for batches of maximumNumberOfFilesToCompact files
        List<FileInfo> filesForJob = new ArrayList<>();
        for (FileInfo fileInfo : filesInAscendingOrder) {
            filesForJob.add(fileInfo);
            if (filesForJob.size() >= compactionFilesBatchSize) {
                // Create job for these files
                LOGGER.info("Creating a job to compact {} files and split into 2 partitions (parent partition is {})",
                        filesForJob.size(), partition);
                List<String> childPartitions = partitionIdToPartition.get(partition.getId()).getChildPartitionIds();
                Partition leftPartition = partitionIdToPartition.get(childPartitions.get(0));
                Partition rightPartition = partitionIdToPartition.get(childPartitions.get(1));
                Object splitPoint = leftPartition.getRegion()
                        .getRange(schema.getRowKeyFieldNames().get(partition.getDimension()))
                        .getMax();
                LOGGER.info("Split point is {}", splitPoint);

                compactionJobs.add(createSplittingCompactionJob(filesForJob,
                        partition.getId(),
                        leftPartition.getId(),
                        rightPartition.getId(),
                        splitPoint,
                        partition.getDimension(),
                        tableBucket));
                filesForJob.clear();
            }
        }

        // If there are any files left (even just 1), create a job for them
        if (!filesForJob.isEmpty()) {
            // Create job for these files
            LOGGER.info("Creating a job to compact {} files in partition {}",
                    filesForJob.size(), partition);
            List<String> childPartitions = partitionIdToPartition.get(partition.getId()).getChildPartitionIds();
            Partition leftPartition = partitionIdToPartition.get(childPartitions.get(0));
            Partition rightPartition = partitionIdToPartition.get(childPartitions.get(1));
            Object splitPoint = leftPartition.getRegion()
                    .getRange(schema.getRowKeyFieldNames().get(partition.getDimension()))
                    .getMax();
            LOGGER.info("Split point is {}", splitPoint);

            compactionJobs.add(createSplittingCompactionJob(filesForJob,
                    partition.getId(),
                    leftPartition.getId(),
                    rightPartition.getId(),
                    splitPoint,
                    partition.getDimension(),
                    tableBucket));
            filesForJob.clear();
        }
        return compactionJobs;
    }

    protected List<FileInfo> getFilesInAscendingOrder(Partition partition, List<FileInfo> fileInfos) {
        // Get files in this partition
        List<FileInfo> files = fileInfos
                .stream()
                .filter(f -> f.getPartitionId().equals(partition.getId()))
                .collect(Collectors.toList());
        LOGGER.info("Creating jobs for leaf partition " + partition);
        LOGGER.info("There are " + files.size() + " files for this partition");

        // Create map of number of records in file to files, sorted by number of records in file
        SortedMap<Long, List<FileInfo>> linesToFiles = new TreeMap<>();
        for (FileInfo fileInfo : files) {
            if (!linesToFiles.containsKey(fileInfo.getNumberOfRecords())) {
                linesToFiles.put(fileInfo.getNumberOfRecords(), new ArrayList<>());
            }
            linesToFiles.get(fileInfo.getNumberOfRecords()).add(fileInfo);
        }

        // Convert to list of FileInfos in ascending order of number of lines
        List<FileInfo> fileInfosList = new ArrayList<>();
        for (Map.Entry<Long, List<FileInfo>> entry : linesToFiles.entrySet()) {
            fileInfosList.addAll(entry.getValue());
        }

        return fileInfosList;
    }

    protected CompactionJob createSplittingCompactionJob(
            List<FileInfo> files, String partition, String leftPartitionId, String rightPartitionId,
            Object splitPoint, int dimension, String s3Bucket) {

        String jobId = UUID.randomUUID().toString();
        String leftOutputFile = fs + s3Bucket + "/partition_" + leftPartitionId + "/" + jobId + ".parquet";
        String rightOutputFile = fs + s3Bucket + "/partition_" + rightPartitionId + "/" + jobId + ".parquet";
        CreateCompactionJob create = createJob().jobId(jobId).partitionId(partition).inputFiles(files)
                .splitting(split -> split.leftPartitionId(leftPartitionId).rightPartitionId(rightPartitionId)
                        .leftOutputFile(leftOutputFile).rightOutputFile(rightOutputFile)
                        .splitPoint(splitPoint).dimension(dimension)
                        .build())
                .build();
        CompactionJob compactionJob = create.buildJob();

        LOGGER.info("Created compaction job of id {} to compact and split {} files in partition {}, into partitions {} and {}, to output files {}, {}",
                jobId, files.size(), partition, leftPartitionId, rightPartitionId, leftOutputFile, rightOutputFile);

        return compactionJob;
    }

    protected CompactionJob createCompactionJob(
            List<FileInfo> files, String partition, String s3Bucket) {

        String jobId = UUID.randomUUID().toString();
        String outputFile = fs + s3Bucket + "/partition_" + partition + "/" + jobId + ".parquet";
        CreateCompactionJob create = createJob().jobId(jobId).partitionId(partition)
                .inputFiles(files).outputFilePath(outputFile).build();
        CompactionJob compactionJob = create.buildJob();

        LOGGER.info("Created compaction job of id {} to compact and split {} files in partition {} to output file {}",
                jobId, files.size(), partition, outputFile);

        return compactionJob;
    }

    private CreateCompactionJob.Builder createJob() {
        return CreateCompactionJob.builder().tableName(tableName)
                .iteratorClassName(iteratorClassName).iteratorConfig(iteratorConfig);
    }
}
