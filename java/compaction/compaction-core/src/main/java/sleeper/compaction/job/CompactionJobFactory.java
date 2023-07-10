/*
 * Copyright 2022-2023 Crown Copyright
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
package sleeper.compaction.job;

import org.apache.commons.lang3.tuple.MutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.statestore.FileInfo;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

import static sleeper.configuration.properties.CommonProperties.FILE_SYSTEM;
import static sleeper.configuration.properties.table.TableProperty.DATA_BUCKET;
import static sleeper.configuration.properties.table.TableProperty.ITERATOR_CLASS_NAME;
import static sleeper.configuration.properties.table.TableProperty.ITERATOR_CONFIG;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public class CompactionJobFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(CompactionJobFactory.class);

    private final String tableName;
    private final String outputFilePrefix;
    private final String iteratorClassName;
    private final String iteratorConfig;

    public CompactionJobFactory(InstanceProperties instanceProperties, TableProperties tableProperties) {
        this(withTableName(tableProperties.get(TABLE_NAME))
                .outputFilePrefix(instanceProperties.get(FILE_SYSTEM) + tableProperties.get(DATA_BUCKET))
                .iteratorClassName(tableProperties.get(ITERATOR_CLASS_NAME))
                .iteratorConfig(tableProperties.get(ITERATOR_CONFIG)));
    }

    private CompactionJobFactory(Builder builder) {
        tableName = Objects.requireNonNull(builder.tableName, "tableName must not be null");
        outputFilePrefix = Objects.requireNonNull(builder.outputFilePrefix, "outputFilePrefix must not be null");
        iteratorClassName = builder.iteratorClassName;
        iteratorConfig = builder.iteratorConfig;
        LOGGER.info("Initialised CompactionFactory with table name {}, filename prefix {}",
                this.tableName, this.outputFilePrefix);
    }

    public static Builder withTableName(String tableName) {
        return new Builder().tableName(tableName);
    }

    public CompactionJob createSplittingCompactionJob(
            List<FileInfo> files, String partition,
            String leftPartitionId, String rightPartitionId,
            Object splitPoint, int dimension) {
        String jobId = UUID.randomUUID().toString();
        List<String> jobFiles = files.stream()
                .map(FileInfo::getFilename)
                .collect(Collectors.toList());
        String leftOutputFile = outputFileForPartitionAndJob(leftPartitionId, jobId);
        String rightOutputFile = outputFileForPartitionAndJob(rightPartitionId, jobId);
        CompactionJob compactionJob = CompactionJob.builder()
                .tableName(tableName)
                .jobId(jobId)
                .isSplittingJob(true)
                .inputFiles(jobFiles)
                .outputFiles(new MutablePair<>(leftOutputFile, rightOutputFile))
                .splitPoint(splitPoint)
                .dimension(dimension)
                .partitionId(partition)
                .childPartitions(Arrays.asList(leftPartitionId, rightPartitionId))
                .iteratorClassName(iteratorClassName)
                .iteratorConfig(iteratorConfig).build();

        LOGGER.info("Created compaction job of id {} to compact and split {} files in partition {}, into partitions {} and {}, to output files {}, {}",
                jobId, files.size(), partition, leftPartitionId, rightPartitionId, leftOutputFile, rightOutputFile);

        return compactionJob;
    }

    public CompactionJob createCompactionJob(
            List<FileInfo> files, String partition) {
        CompactionJob job = createCompactionJobBuilder(files, partition).build();

        LOGGER.info("Created compaction job of id {} to compact and split {} files in partition {} to output file {}",
                job.getId(), files.size(), partition, job.getOutputFile());

        return job;
    }

    private CompactionJob.Builder createCompactionJobBuilder(List<FileInfo> files, String partition) {
        for (FileInfo fileInfo : files) {
            if (!partition.equals(fileInfo.getPartitionId())) {
                throw new IllegalArgumentException("Found file with partition which is different to the provided partition (partition = "
                        + partition + ", FileInfo = " + fileInfo);
            }
        }

        String jobId = UUID.randomUUID().toString();
        String outputFile = outputFileForPartitionAndJob(partition, jobId);
        List<String> jobFiles = files.stream()
                .map(FileInfo::getFilename)
                .collect(Collectors.toList());
        return CompactionJob.builder()
                .tableName(tableName)
                .jobId(jobId)
                .isSplittingJob(false)
                .dimension(-1)
                .inputFiles(jobFiles)
                .outputFile(outputFile)
                .partitionId(partition)
                .iteratorClassName(iteratorClassName)
                .iteratorConfig(iteratorConfig);
    }

    private String outputFileForPartitionAndJob(String partitionId, String jobId) {
        return outputFilePrefix + "/partition_" + partitionId + "/" + jobId + ".parquet";
    }

    public static final class Builder {
        private String tableName;
        private String outputFilePrefix;
        private String iteratorClassName;
        private String iteratorConfig;

        private Builder() {
        }

        public Builder tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder outputFilePrefix(String outputFilePrefix) {
            this.outputFilePrefix = outputFilePrefix;
            return this;
        }

        public Builder iteratorClassName(String iteratorClassName) {
            this.iteratorClassName = iteratorClassName;
            return this;
        }

        public Builder iteratorConfig(String iteratorConfig) {
            this.iteratorConfig = iteratorConfig;
            return this;
        }

        public CompactionJobFactory build() {
            return new CompactionJobFactory(this);
        }
    }
}
