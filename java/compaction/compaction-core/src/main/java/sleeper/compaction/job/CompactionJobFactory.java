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

import sleeper.configuration.TableUtils;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.statestore.FileInfo;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.configuration.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.configuration.properties.table.TableProperty.ITERATOR_CLASS_NAME;
import static sleeper.configuration.properties.table.TableProperty.ITERATOR_CONFIG;
import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public class CompactionJobFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(CompactionJobFactory.class);

    private final String tableId;
    private final String outputFilePrefix;
    private final String iteratorClassName;
    private final String iteratorConfig;

    public CompactionJobFactory(InstanceProperties instanceProperties, TableProperties tableProperties) {
        tableId = tableProperties.get(TABLE_ID);
        outputFilePrefix = instanceProperties.get(FILE_SYSTEM) + instanceProperties.get(DATA_BUCKET) + "/" + tableProperties.get(TABLE_NAME);
        iteratorClassName = tableProperties.get(ITERATOR_CLASS_NAME);
        iteratorConfig = tableProperties.get(ITERATOR_CONFIG);
        LOGGER.info("Initialised CompactionFactory with table {}, filename prefix {}",
                tableProperties.getId(), this.outputFilePrefix);
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
                .tableId(tableId)
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
                .tableId(tableId)
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
        return TableUtils.constructPartitionParquetFilePath(outputFilePrefix, partitionId, jobId);
    }
}
