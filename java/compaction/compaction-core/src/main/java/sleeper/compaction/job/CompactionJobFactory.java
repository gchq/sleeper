/*
 * Copyright 2022-2024 Crown Copyright
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

import sleeper.configuration.TableFilePaths;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.statestore.FileReference;

import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;

import static java.util.stream.Collectors.toList;
import static sleeper.core.properties.table.TableProperty.ITERATOR_CLASS_NAME;
import static sleeper.core.properties.table.TableProperty.ITERATOR_CONFIG;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;

public class CompactionJobFactory {

    private final String tableId;
    private final TableFilePaths outputFilePaths;
    private final String iteratorClassName;
    private final String iteratorConfig;
    private final Supplier<String> jobIdSupplier;

    public CompactionJobFactory(InstanceProperties instanceProperties, TableProperties tableProperties) {
        this(instanceProperties, tableProperties, () -> UUID.randomUUID().toString());
    }

    public CompactionJobFactory(InstanceProperties instanceProperties, TableProperties tableProperties, Supplier<String> jobIdSupplier) {
        tableId = tableProperties.get(TABLE_ID);
        outputFilePaths = TableFilePaths.buildDataFilePathPrefix(instanceProperties, tableProperties);
        iteratorClassName = tableProperties.get(ITERATOR_CLASS_NAME);
        iteratorConfig = tableProperties.get(ITERATOR_CONFIG);
        this.jobIdSupplier = jobIdSupplier;
    }

    public String getOutputFilePrefix() {
        return outputFilePaths.getFilePathPrefix();
    }

    public CompactionJob createCompactionJob(
            List<FileReference> files, String partition) {
        return createCompactionJob(jobIdSupplier.get(), files, partition);
    }

    public CompactionJob createCompactionJob(
            String jobId, List<FileReference> files, String partition) {
        for (FileReference fileReference : files) {
            if (!partition.equals(fileReference.getPartitionId())) {
                throw new IllegalArgumentException("Found file with partition which is different to the provided partition (partition = "
                        + partition + ", FileReference = " + fileReference);
            }
        }
        return createCompactionJobWithFilenames(jobId,
                files.stream().map(FileReference::getFilename).collect(toList()),
                partition);
    }

    public CompactionJob createCompactionJobWithFilenames(
            String jobId, List<String> filenames, String partitionId) {
        String outputFile = outputFilePaths.constructPartitionParquetFilePath(partitionId, jobId);
        return CompactionJob.builder()
                .tableId(tableId)
                .jobId(jobId)
                .inputFiles(filenames)
                .outputFile(outputFile)
                .partitionId(partitionId)
                .iteratorClassName(iteratorClassName)
                .iteratorConfig(iteratorConfig)
                .build();
    }
}
