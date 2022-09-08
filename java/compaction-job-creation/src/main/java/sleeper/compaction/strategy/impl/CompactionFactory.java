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

import org.apache.commons.lang3.tuple.MutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sleeper.compaction.job.CompactionJob;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.statestore.FileInfo;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static sleeper.configuration.properties.UserDefinedInstanceProperty.FILE_SYSTEM;
import static sleeper.configuration.properties.table.TableProperty.DATA_BUCKET;
import static sleeper.configuration.properties.table.TableProperty.ITERATOR_CLASS_NAME;
import static sleeper.configuration.properties.table.TableProperty.ITERATOR_CONFIG;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public class CompactionFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(CompactionFactory.class);

    private final String tableName;
    private final String tableBucket;
    private final String fs;
    private final String iteratorClassName;
    private final String iteratorConfig;

    public CompactionFactory(InstanceProperties instanceProperties, TableProperties tableProperties) {
        tableName = tableProperties.get(TABLE_NAME);
        tableBucket = tableProperties.get(DATA_BUCKET);
        fs = instanceProperties.get(FILE_SYSTEM);
        iteratorClassName = tableProperties.get(ITERATOR_CLASS_NAME);
        iteratorConfig = tableProperties.get(ITERATOR_CONFIG);
        LOGGER.info("Initialised CompactionFactory with table name {}, bucket for table data {}, fs {}",
                tableName, tableBucket, fs);
    }

    public CompactionJob createSplittingCompactionJob(
            List<FileInfo> files, String partition,
            String leftPartitionId, String rightPartitionId,
            Object splitPoint, int dimension) {
        String jobId = UUID.randomUUID().toString();
        List<String> jobFiles = files.stream()
                .map(FileInfo::getFilename)
                .collect(Collectors.toList());
        CompactionJob compactionJob = new CompactionJob(tableName, jobId);
        compactionJob.setIsSplittingJob(true);
        compactionJob.setInputFiles(jobFiles);
        String leftOutputFile = fs + tableBucket + "/partition_" + leftPartitionId + "/" + jobId + ".parquet";
        String rightOutputFile = fs + tableBucket + "/partition_" + rightPartitionId + "/" + jobId + ".parquet";
        compactionJob.setOutputFiles(new MutablePair<>(leftOutputFile, rightOutputFile));
        compactionJob.setSplitPoint(splitPoint);
        compactionJob.setDimension(dimension);
        compactionJob.setPartitionId(partition);
        compactionJob.setChildPartitions(Arrays.asList(leftPartitionId, rightPartitionId));
        compactionJob.setIteratorClassName(iteratorClassName);
        compactionJob.setIteratorConfig(iteratorConfig);

        LOGGER.info("Created compaction job of id {} to compact and split {} files in partition {}, into partitions {} and {}, to output files {}, {}",
                jobId, files.size(), partition, leftPartitionId, rightPartitionId, leftOutputFile, rightOutputFile);

        return compactionJob;
    }

    public CompactionJob createCompactionJob(
            List<FileInfo> files, String partition) {
        for (FileInfo fileInfo : files) {
            if (!partition.equals(fileInfo.getPartitionId())) {
                throw new IllegalArgumentException("Found file with partition which is different to the provided partition (partition = "
                        + partition + ", FileInfo = " + fileInfo);
            }
        }

        String jobId = UUID.randomUUID().toString();
        String outputFile = fs + tableBucket + "/partition_" + partition + "/" + jobId + ".parquet";
        List<String> jobFiles = files.stream()
                .map(FileInfo::getFilename)
                .collect(Collectors.toList());
        CompactionJob compactionJob = new CompactionJob(tableName, jobId);
        compactionJob.setIsSplittingJob(false);
        compactionJob.setDimension(-1);
        compactionJob.setInputFiles(jobFiles);
        compactionJob.setOutputFile(outputFile);
        compactionJob.setPartitionId(partition);
        compactionJob.setIteratorClassName(iteratorClassName);
        compactionJob.setIteratorConfig(iteratorConfig);

        LOGGER.info("Created compaction job of id {} to compact and split {} files in partition {} to output file {}",
                jobId, files.size(), partition, outputFile);

        return compactionJob;
    }
}
